// Package kernel is the Go RLM runtime: it reads NDJSON requests, runs each
// id'd request as a goroutine with its own context, and emits v3 events.
//
// Concurrency model (see ARCHITECTURE.md):
//   - reader goroutine: parses frames; interrupt + host_reply route
//     IMMEDIATELY (never queued) — this is what makes concurrent host calls
//     and mid-cell interrupts work under the otherwise-serial v3 policy.
//   - executor goroutine: drains the work queue one request at a time (v3
//     policy); swap Policy for pipelined execution in a forked host.
//   - every request gets a cancelable context; interrupt parks until its
//     target becomes active (spec: Interrupt).
//   - goroutines spawned by a cell outlive it and keep writing attributed
//     stdout events; a root context governs their lifetime.
package kernel

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"go-prime-agent/internal/eval"
	"go-prime-agent/internal/hostbridge"
	"go-prime-agent/internal/proto"
)

// Evaluator executes one cell. The kernel owns scheduling, contexts, and the
// protocol; the evaluator owns language. Two implementations ship: the op-DSL
// stub (protocol conformance tests) and the Yaegi Go evaluator.
type Evaluator interface {
	Run(eval.Env) (eval.Result, error)
}

type evalFn func(eval.Env) (eval.Result, error)

func (f evalFn) Run(env eval.Env) (eval.Result, error) { return f(env) }

type Policy int

const (
	PolicySerialV3 Policy = iota // one id'd request at a time (stock host)
	// PolicyPipelined is the v4 fork target: concurrent executes, per-id
	// ordering only. The wire format already carries ids everywhere.
)

type Config struct {
	In     io.Reader
	Out    io.Writer // private dup of original stdout
	Policy Policy
	// Eval executes cells. nil => the op-DSL stub.
	Eval Evaluator
}

type work struct {
	req proto.Request
	ctx context.Context
}

type Kernel struct {
	cfg    Config
	events *proto.Writer
	scope  *Scope
	bridge *hostbridge.Bridge

	rootCtx context.Context
	stop    context.CancelFunc

	queue chan work

	drainCh   chan struct{} // closed to begin shutdown drain
	execDone  chan struct{} // closed when the executor exits
	drainOnce sync.Once

	mu       sync.Mutex
	activeID string                         // request currently executing
	cancels  map[string]context.CancelFunc // active/queued request cancels
	parked   map[string]bool               // ids interrupted before start
	parkNext bool                          // id-less interrupt aimed at next request

	wg sync.WaitGroup // cells + background writers
}

func New(cfg Config) *Kernel {
	ctx, stop := context.WithCancel(context.Background())
	if cfg.Eval == nil {
		cfg.Eval = evalFn(eval.Run)
	}
	k := &Kernel{
		cfg:      cfg,
		events:   proto.NewWriter(cfg.Out),
		scope:    NewScope(),
		queue:    make(chan work, 1024),
		drainCh:  make(chan struct{}),
		execDone: make(chan struct{}),
		rootCtx:  ctx,
		stop:     stop,
		cancels: make(map[string]context.CancelFunc),
		parked:  make(map[string]bool),
	}
	k.bridge = hostbridge.New(k.events)
	return k
}

// Run serves until shutdown, stdin EOF, or ctx cancellation. Always emits
// the ready handshake first.
func (k *Kernel) Run(ctx context.Context) error {
	if err := k.events.Write(proto.Event{
		Event: "ready", Protocol: proto.ProtocolVersion, Runtime: runtime.Version(),
	}); err != nil {
		return err
	}

	k.wg.Add(1)
	go k.executor()

	done := make(chan struct{})
	go func() { k.readAll(); close(done) }()

	select {
	case <-ctx.Done():
	case <-done:
	}
	k.beginShutdown() // drain the queue, then stop
	k.wg.Wait()
	return nil
}

const shutdownGrace = 5 * time.Second

// beginShutdown stops intake, lets the executor finish queued requests, then
// cancels the root context. Safe to call multiple times.
func (k *Kernel) beginShutdown() {
	k.drainOnce.Do(func() {
		close(k.drainCh)
		go func() {
			select {
			case <-k.execDone:
			case <-time.After(shutdownGrace):
			}
			k.stop()
		}()
	})
}

func (k *Kernel) readAll() {
	sc := bufio.NewScanner(k.cfg.In)
	sc.Buffer(make([]byte, 1<<20), 16<<20)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var req proto.Request
		if err := json.Unmarshal(line, &req); err != nil {
			k.events.Write(proto.Event{
				Event: "error", EName: "ProtocolError", EValue: err.Error(),
				Traceback: []string{string(line)},
			}) // runtime keeps serving (spec: Requests)
			continue
		}
		k.dispatch(req)
	}
	// stdin EOF == shutdown (spec: Shutdown)
}

// dispatch runs on the reader goroutine. interrupt and host_reply NEVER go
// through the queue — they must reach a busy kernel instantly.
func (k *Kernel) dispatch(req proto.Request) {
	switch req.Type {
	case "interrupt":
		k.interrupt(req.ID)
	case "host_reply":
		var r proto.Reply
		if err := json.Unmarshal(req.Data, &r); err == nil {
			k.bridge.Resolve(req.ID, r)
		}
	case "shutdown":
		if req.ID != "" {
			k.emitDone(req.ID, "ok", nil)
		}
		k.beginShutdown()
		return
	default:
		select {
		case <-k.drainCh:
			k.events.Write(proto.Event{
				Event: "error", ID: proto.IDPtr(req.ID), EName: "ProtocolError",
				EValue: "shutting down",
			})
			k.emitDone(req.ID, "error", nil)
			return
		default:
		}
		ctx, cancel := context.WithCancel(k.rootCtx)
		k.mu.Lock()
		k.cancels[req.ID] = cancel
		k.mu.Unlock()
		select {
		case k.queue <- work{req: req, ctx: ctx}:
		case <-k.rootCtx.Done():
			cancel()
		}
	}
}

// interrupt cancels the named request, the active request (no id), or parks
// for the next one (spec: Interrupt).
func (k *Kernel) interrupt(id string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if id != "" {
		if cancel, ok := k.cancels[id]; ok {
			cancel()
		} else {
			k.parked[id] = true
		}
		return
	}
	// id-less: the running request, else park for the next one.
	if k.activeID != "" {
		if cancel, ok := k.cancels[k.activeID]; ok {
			cancel()
		}
		return
	}
	k.parkNext = true
}

func (k *Kernel) emitDone(id, status string, extra *proto.Event) {
	e := proto.Event{Event: "done", ID: proto.IDPtr(id), Status: status}
	if extra != nil {
		e.Names, e.Reason = extra.Names, extra.Reason
		e.Saved, e.Skipped, e.Pruned = extra.Saved, extra.Skipped, extra.Pruned
		e.Bytes = extra.Bytes
		e.Restored, e.Failed = extra.Restored, extra.Failed
	}
	k.events.Write(e)
}

func (k *Kernel) executor() {
	defer k.wg.Done()
	defer close(k.execDone)
	for {
		select {
		case <-k.rootCtx.Done():
			return
		case w := <-k.queue:
			k.runRequest(w)
		case <-k.drainCh:
			// Draining: finish anything already queued, then exit.
			select {
			case w := <-k.queue:
				k.runRequest(w)
			default:
				return
			}
		}
	}
}

func (k *Kernel) runRequest(w work) {
	req := w.req
	k.mu.Lock()
	k.activeID = req.ID
	// Parked interrupt? Deliver the moment the request becomes active.
	parked := k.parked[req.ID] || k.parkNext
	delete(k.parked, req.ID)
	k.parkNext = false
	cancel := k.cancels[req.ID]
	k.mu.Unlock()

	k.wg.Add(1)
	defer k.wg.Done()
	defer func() {
		k.mu.Lock()
		k.activeID = ""
		delete(k.cancels, req.ID)
		k.mu.Unlock()
		if cancel != nil {
			cancel()
		}
	}()

	if parked && cancel != nil {
		cancel()
	}

	switch req.Type {
	case "execute":
		k.runCell(w, cancel)
	case "list_names":
		names := k.scope.Names()
		sort.Strings(names)
		k.emitDone(req.ID, "ok", &proto.Event{Names: names})
	case "snapshot":
		k.snapshot(req)
	case "restore":
		k.restore(req)
	default:
		k.events.Write(proto.Event{
			Event: "error", ID: proto.IDPtr(req.ID),
			EName: "ProtocolError", EValue: fmt.Sprintf("unknown request type %q", req.Type),
		})
		k.emitDone(req.ID, "error", nil)
	}
}

func (k *Kernel) runCell(w work, cancel context.CancelFunc) {
	req := w.req
	stdout := func(text string) {
		if !strings.HasSuffix(text, "\n") {
			text += "\n"
		}
		k.events.Write(proto.Event{Event: "stdout", ID: proto.IDPtr(req.ID), Text: text})
	}
	env := eval.Env{
		Ctx:     w.ctx,
		RootCtx: k.rootCtx,
		CellID: req.ID,
		Code:   req.Code,
		Stdout: stdout,
		Host:   hostAdapter{k.bridge},
		Set:    k.scope.Set,
		Get:    k.scope.Get,
	}
	res, err := k.cfg.Eval.Run(env)
	if err != nil {
		if isInterrupt(err) {
			k.events.Write(proto.Event{
				Event: "error", ID: proto.IDPtr(req.ID),
				EName: "KeyboardInterrupt", EValue: "cell interrupted",
			})
			k.emitDone(req.ID, "error", nil)
			return
		}
		k.events.Write(proto.Event{
			Event: "error", ID: proto.IDPtr(req.ID),
			EName: "Error", EValue: err.Error(),
		})
		k.emitDone(req.ID, "error", nil)
		return
	}
	if res.Value != nil {
		k.events.Write(proto.Event{Event: "result", ID: proto.IDPtr(req.ID), Text: fmt.Sprintf("%v", res.Value)})
	}
	k.emitDone(req.ID, "ok", nil)
}

func isInterrupt(err error) bool {
	_, ok := err.(*eval.InterruptedError)
	return ok
}

// hostAdapter adapts the bridge to the evaluator's Host port. Results arrive
// as raw JSON; cells decode what they expect.
type hostAdapter struct{ b *hostbridge.Bridge }

func (h hostAdapter) Call(ctx context.Context, kind string, payload any) (any, error) {
	r, err := h.b.Call(ctx, kind, payload)
	if err != nil {
		return nil, err
	}
	if r.Status != "ok" {
		return nil, fmt.Errorf("host error: %s", r.Error)
	}
	return json.RawMessage(r.Result), nil
}

// snapshot serializes JSON-marshalable scope values under the read lock,
// writes payload + manifest atomically, and only then applies pruning
// (spec: a manifest failure means nothing is pruned). Yaegi-cell state lives
// in the interpreter, not the scope, so it is not covered yet.
func (k *Kernel) snapshot(req proto.Request) {
	maxVar := int64(256 << 10)
	if req.MaxVariableBytes != nil {
		maxVar = *req.MaxVariableBytes
	}
	maxTotal := int64(8 << 20)
	if req.MaxBytes != nil {
		maxTotal = *req.MaxBytes
	}
	prune := req.PruneOversized != nil && *req.PruneOversized

	entries := k.scope.Entries()
	names := make([]string, 0, len(entries))
	for n := range entries {
		names = append(names, n)
	}
	sort.Strings(names)

	saved := map[string]json.RawMessage{}
	var savedNames, skipped, pruned []string
	var total int64
	for _, n := range names {
		b, err := json.Marshal(entries[n])
		if err != nil {
			skipped = append(skipped, n)
			continue
		}
		if int64(len(b)) > maxVar {
			if prune {
				pruned = append(pruned, n)
			} else {
				skipped = append(skipped, n)
			}
			continue
		}
		if total+int64(len(b)) > maxTotal {
			skipped = append(skipped, n)
			continue
		}
		saved[n] = b
		savedNames = append(savedNames, n)
		total += int64(len(b))
	}

	payload, err := json.Marshal(map[string]any{"version": 1, "names": saved})
	if err == nil {
		err = atomicWrite(req.Path, payload)
	}
	if err == nil && req.ManifestPath != "" {
		m, _ := json.Marshal(map[string]any{
			"version": 1, "savedNames": savedNames, "skipped": skipped,
			"pruned": pruned, "bytes": total, "runtime": "go",
			"timestamp": time.Now().UTC().Format(time.RFC3339),
		})
		err = atomicWrite(req.ManifestPath, m)
	}
	if err != nil {
		k.emitDone(req.ID, "error", &proto.Event{Reason: err.Error()})
		return
	}
	if prune {
		for _, n := range pruned {
			k.scope.Delete(n)
		}
	}
	k.emitDone(req.ID, "ok", &proto.Event{
		Saved: savedNames, Skipped: skipped, Pruned: pruned, Bytes: total,
	})
}

func (k *Kernel) restore(req proto.Request) {
	b, err := os.ReadFile(req.Path)
	if errors.Is(err, fs.ErrNotExist) {
		k.emitDone(req.ID, "ok", &proto.Event{Restored: []string{}, Failed: []string{}, Reason: "snapshot not found"})
		return
	}
	if err != nil {
		k.emitDone(req.ID, "error", &proto.Event{Reason: err.Error()})
		return
	}
	var payload struct {
		Version int                        `json:"version"`
		Names   map[string]json.RawMessage `json:"names"`
	}
	if err := json.Unmarshal(b, &payload); err != nil {
		k.emitDone(req.ID, "error", &proto.Event{Reason: "corrupt snapshot: " + err.Error()})
		return
	}
	var restored, failed []string
	for n, raw := range payload.Names {
		var v any
		if json.Unmarshal(raw, &v) == nil {
			k.scope.Set(n, v)
			restored = append(restored, n)
		} else {
			failed = append(failed, n)
		}
	}
	sort.Strings(restored)
	sort.Strings(failed)
	k.emitDone(req.ID, "ok", &proto.Event{Restored: restored, Failed: failed})
}

func atomicWrite(path string, data []byte) error {
	if path == "" {
		return errors.New("empty path")
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp*")
	if err != nil {
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return err
	}
	return os.Rename(tmp.Name(), path)
}
