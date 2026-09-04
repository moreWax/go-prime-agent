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
package goeval

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"
	"time"

	"github.com/traefik/yaegi/interp"
	"github.com/traefik/yaegi/stdlib"

	"go-prime-agent/internal/eval"
)

type Evaluator struct {
	mu sync.Mutex
	i  *interp.Interpreter

	// per-cell slots, guarded by mu
	ctx  context.Context
	out  func(string)
	host func(ctx context.Context, kind string, payload any) (json.RawMessage, error)
}

// outWriter routes interpreted print/println to the current cell's
// attributed stdout. Background goroutines printing after their cell ends
// attribute to the most recent cell (attribution caveat, documented).
type outWriter struct{ e *Evaluator }

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

func New() *Evaluator {
	e := &Evaluator{ctx: context.Background()}
	e.i = interp.New(interp.Options{Stdout: outWriter{e}})
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
		},
	})
	return e
}

func (e *Evaluator) slotCtx() context.Context {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.ctx
}

func (e *Evaluator) slotHost() (context.Context, func(context.Context, string, any) (json.RawMessage, error)) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.ctx, e.host
}

// Run evaluates one cell in the persistent interpreter.
func (e *Evaluator) Run(env eval.Env) (res eval.Result, err error) {
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
		v, err = e.i.Eval(c)
		if err != nil {
			break
		}
	}
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return eval.Result{}, &eval.InterruptedError{CellID: env.CellID}
		}
		return eval.Result{}, err
	}
	// A cancelled cell reports interrupted even if user code swallowed the
	// cancellation as a value (e.g. trailing `rlm.Sleep(...)` expression).
	if cerr := env.Ctx.Err(); cerr != nil {
		return eval.Result{}, &eval.InterruptedError{CellID: env.CellID}
	}
	// Register declared names in the kernel scope so list_names and snapshot
	// see Go globals. Values are markers: interpreter state is not serialized
	// yet (name-preserving revival only).
	for _, n := range declaredNames(env.Code) {
		env.Set(n, "go-global")
	}
	if v.IsValid() {
		if iv := v.Interface(); iv != nil {
			return eval.Result{Value: fmt.Sprintf("%v", iv)}, nil
		}
	}
	return eval.Result{}, nil
}
