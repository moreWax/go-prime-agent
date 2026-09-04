// Package goeval executes cells as Go source in one persistent Yaegi
// interpreter. Go is the tool surface: cells import the standard library and
// call bound helpers. There is no bash tool by design.
//
// Concurrency contract:
//   - The interpreter is single-threaded; the kernel serializes cells (v3
//     policy), so Run is never re-entered concurrently.
//   - `go` statements inside a cell run as REAL goroutines and outlive the
//     cell; bound helpers read the ctx slot under mu, which points at the
//     cell during Run and the kernel root between cells.
//   - sleep/host_call are context-aware: interrupting a cell cancels them.
//     A pure-compute interpreted loop cannot be interrupted mid-eval (known
//     Yaegi limitation); the mitigation is a kernel restart + restore.
package rlm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/traefik/yaegi/interp"
	"github.com/traefik/yaegi/stdlib"
)

type GoEvaluator struct {
	mu       sync.Mutex
	i        *interp.Interpreter
	imported map[string]bool // package import paths already bound
	skills   []SkillInfo     // loaded Go skills (import "rlm/<name>")

	// per-cell slots, guarded by mu
	ctx  context.Context
	out  func(string)
	host func(ctx context.Context, kind string, payload any) (json.RawMessage, error)
}

// outWriter routes interpreted print/println to the current cell's
// attributed stdout. Background goroutines printing after their cell ends
// attribute to the most recent cell (attribution caveat, documented).
type outWriter struct{ e *GoEvaluator }

func (w outWriter) Write(p []byte) (int, error) {
	w.e.mu.Lock()
	f := w.e.out
	w.e.mu.Unlock()
	if f != nil {
		f(string(p))
	}
	return len(p), nil
}

var _ io.Writer = outWriter{}

// NewGoEvaluator returns an evaluator with no skills loaded.
func NewGoEvaluator() *GoEvaluator { return newGoEvaluator("") }

// NewGoEvaluatorWithSkills loads Go skills from dir (see skill.go).
func NewGoEvaluatorWithSkills(dir string) *GoEvaluator { return newGoEvaluator(dir) }

func newGoEvaluator(skillsDir string) *GoEvaluator {
	e := &GoEvaluator{ctx: context.Background(), imported: make(map[string]bool)}
	o := interp.Options{Stdout: outWriter{e}}
	if gp, skills, err := PrepareSkillGoPath(skillsDir); err == nil && gp != "" {
		o.GoPath = gp
		e.skills = skills
	}
	e.i = interp.New(o)
	e.i.Use(stdlib.Symbols)

	// Cells import "rlm/rlm" for runtime-bound helpers (qualifier rlm).
	e.i.Use(interp.Exports{
		"rlm/rlm": map[string]reflect.Value{
			// Sleep is the interruptible sleep; time.Sleep in a cell is NOT
			// cancellable and should be avoided.
			"Sleep": reflect.ValueOf(func(ms int) error {
				ctx := e.slotCtx()
				select {
				case <-time.After(time.Duration(ms) * time.Millisecond):
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}),
			// HostCall reaches the host bridge (subagents, messaging, tools).
			// Errors panic; Yaegi converts them into cell errors.
			"HostCall": reflect.ValueOf(func(kind string, payload any) any {
				ctx, host := e.slotHost()
				v, err := host(ctx, kind, payload)
				if err != nil {
					panic(err)
				}
				return v
			}),
			// Spawn admits a subagent task; returns the child handle
			// immediately after admission (never waits for its answer).
			"Spawn": reflect.ValueOf(func(task, name string) map[string]any {
				ctx, host := e.slotHost()
				raw, err := host(ctx, "spawn_task", map[string]any{"task": task, "name": name})
				if err != nil {
					panic(err)
				}
				var ch map[string]any
				if err := json.Unmarshal(raw, &ch); err != nil {
					panic(err)
				}
				return ch
			}),
			// Send delivers an agent message (role: parent|sibling|child).
			"Send": reflect.ValueOf(func(role, name, message string) error {
				ctx, host := e.slotHost()
				_, err := host(ctx, "agent_message", map[string]any{
					"receiver_role": role, "receiver_name": name, "message": message,
				})

				return err
			}),
			// Skills lists loaded Go skills (import "rlm/<name>").
			"Skills": reflect.ValueOf(func() []string {
				e.mu.Lock()
				defer e.mu.Unlock()
				out := make([]string, 0, len(e.skills))
				for _, s := range e.skills {
					out = append(out, s.Name)
				}
				return out
			}),
			// ListAgents returns the family roster.
			"ListAgents": reflect.ValueOf(func() []map[string]any {
				ctx, host := e.slotHost()
				raw, err := host(ctx, "list_agents", nil)
				if err != nil {
					panic(err)
				}
				var out []map[string]any
				if err := json.Unmarshal(raw, &out); err != nil {
					panic(err)
				}
				return out
			}),
		},
	})
	// Pre-import the runtime helpers and every skill (Python parity: `rlm`
	// and skill modules are already in the namespace, no import needed).
	// Skills that fail to interpret are dropped, not fatal.
	if _, err := e.i.Eval(`import "rlm/rlm"`); err == nil {
		e.imported["rlm/rlm"] = true
	}
	live := e.skills[:0]
	for _, sk := range e.skills {
		if _, err := e.i.Eval("import \"rlm/" + sk.Name + "\""); err == nil {
			e.imported["rlm/"+sk.Name] = true
			live = append(live, sk)
		}
	}
	e.skills = live
	return e
}

func (e *GoEvaluator) slotCtx() context.Context {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.ctx
}

func (e *GoEvaluator) slotHost() (context.Context, func(context.Context, string, any) (json.RawMessage, error)) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.ctx, e.host
}

// Run evaluates one cell in the persistent interpreter.
func (e *GoEvaluator) Run(env Env) (res Result, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()

	e.mu.Lock()
	e.ctx = env.Ctx
	e.out = env.Stdout
	e.host = env.Host.CallHost
	e.mu.Unlock()

	// Between cells the slots fall back to the root context so background
	// goroutines survive; out/host intentionally keep the last cell's values.
	defer func() {
		e.mu.Lock()
		e.ctx = env.RootCtx
		e.mu.Unlock()
	}()

	var v reflect.Value
	for _, c := range chunk(env.Code) {
		if e.markImported(c) {
			continue // import of an already-bound package: idempotent
		}
		v, err = e.i.Eval(c)
		if err != nil {
			break
		}
	}
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return Result{}, &InterruptedError{CellID: env.CellID}
		}
		return Result{}, err
	}
	// A cancelled cell reports interrupted even if user code swallowed the
	// cancellation as a value (e.g. trailing `Sleep(...)` expression).
	if cerr := env.Ctx.Err(); cerr != nil {
		return Result{}, &InterruptedError{CellID: env.CellID}
	}
	// Register declared names in the kernel scope so list_names and snapshot
	// see Go globals. Values are markers: interpreter state is not serialized
	// yet (name-preserving revival only).
	for _, n := range declaredNames(env.Code) {
		env.Set(n, "go-global")
	}
	if v.IsValid() {
		if iv := v.Interface(); iv != nil {
			return Result{Value: fmt.Sprintf("%v", iv)}, nil
		}
	}
	return Result{}, nil
}

var importPathRe = regexp.MustCompile(`"([\w./-]+)"`)

// markImported reports whether the chunk is an import statement whose path
// is already bound in the interpreter (making re-imports idempotent — a
// persistent REPL affordance Python gets for free).
func (e *GoEvaluator) markImported(chunkSrc string) bool {
	t := strings.TrimSpace(chunkSrc)
	if !strings.HasPrefix(t, "import") {
		return false
	}
	paths := importPathRe.FindAllStringSubmatch(t, -1)
	if len(paths) == 0 {
		return false
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	all := true
	for _, m := range paths {
		if !e.imported[m[1]] {
			all = false
		}
	}
	if all {
		return true
	}
	for _, m := range paths {
		e.imported[m[1]] = true
	}
	return false
}
