// Package kernel is the Go RLM runtime: it reads NDJSON requests, runs each
// id'd request with its own context, and emits v3 events.
//
// Concurrency model (see ARCHITECTURE.md):
//   - reader goroutine: parses frames; interrupt + host_reply route
//     IMMEDIATELY (never queued) — this is what makes concurrent host calls
//     and mid-cell interrupts work while executes stay serial (v3).
//   - executor goroutine: runs queued requests one at a time. The wire
//     format is id'd end to end, so a forked host can pipeline executes
//     without format changes.
//   - every request gets a cancelable context; interrupt parks until its
//     target becomes active (spec: Interrupt).
//   - goroutines spawned by a cell outlive it and keep writing attributed
//     stdout events; a root context governs their lifetime.
package kernel

import (
	"context"
	"io"
	"runtime"
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

type Config struct {
	In  io.Reader
	Out io.Writer // private dup of original stdout
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

	queue     chan work
	drainCh   chan struct{} // closed to begin shutdown drain
	execDone  chan struct{} // closed when the executor exits
	drainOnce sync.Once

	table requestTable // live/queued requests + parked interrupts
	wg    sync.WaitGroup
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
		table:    newRequestTable(),
	}
	k.bridge = hostbridge.New(k.events)
	return k
}

// Run serves until shutdown, stdin EOF, or ctx cancellation. Always emits
// the ready handshake first.
func (k *Kernel) Run(ctx context.Context) error {
	if err := k.events.Write(proto.Event{
		Event: proto.KindReady, Protocol: proto.ProtocolVersion, Runtime: runtime.Version(),
	}); err != nil {
		return err
	}

	k.wg.Add(1)
	go k.executor()

	readDone := make(chan struct{})
	go func() { k.readAll(); close(readDone) }()

	select {
	case <-ctx.Done():
	case <-readDone:
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
