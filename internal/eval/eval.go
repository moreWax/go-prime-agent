// Package eval abstracts the cell executor. Phase 1 ships OpDSL, a tiny
// context-aware op language used to prove the kernel's concurrency contract
// end to end. Phase 2 replaces it with a Yaegi-backed Go evaluator behind the
// same interface: cells get a persistent interpreter, and goroutines spawned
// inside a cell keep running after it finishes.
package eval

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Env is what a cell can touch. Stdout writes are attributed to the cell's
// id even from goroutines the cell spawned that outlive it.
type Env struct {
	Ctx    context.Context
	// RootCtx outlives the cell: goroutines a cell spawns keep running
	// between cells and are governed by the kernel root, not the cell.
	RootCtx context.Context
	CellID string
	Code   string
	Stdout func(text string)
	Host   interface {
		Call(ctx context.Context, kind string, payload any) (result any, err error)
	}
	Set func(k string, v any)
	Get func(k string) (any, bool)
}

// Result is the outcome of one cell. Value, when non-nil, becomes the
// protocol result event (the "trailing expression repr").
type Result struct {
	Value any
}

type InterruptedError struct{ CellID string }

func (e *InterruptedError) Error() string { return "KeyboardInterrupt" }

// Run executes one cell to completion or context cancellation.
// Op syntax: `name:arg rest...` — the leading token carries its first
// argument after a colon, remaining fields are extra arguments.
func Run(env Env) (Result, error) {
	fields := strings.Fields(strings.TrimSpace(env.Code))
	if len(fields) == 0 {
		return Result{}, nil
	}
	name, arg, _ := strings.Cut(fields[0], ":")
	rest := fields[1:]

	switch name {
	case "sleep":
		ms, _ := strconv.Atoi(arg)
		select {
		case <-time.After(time.Duration(ms) * time.Millisecond):
			return Result{}, nil
		case <-env.Ctx.Done():
			return Result{}, &InterruptedError{CellID: env.CellID}
		}
	case "hostcall":
		n, _ := strconv.Atoi(arg)
		job := "job"
		if len(rest) > 0 {
			job = rest[0]
		}
		var wg sync.WaitGroup
		results := make([]any, n)
		errCh := make(chan error, n)
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				r, err := env.Host.Call(env.Ctx, job, map[string]any{"i": i})
				if err != nil {
					errCh <- err
					return
				}
				results[i] = r
			}(i)
		}
		wg.Wait()
		close(errCh)
		if err := <-errCh; err != nil {
			return Result{}, err
		}
		ok := 0
		for _, r := range results {
			if r != nil {
				ok++
			}
		}
		return Result{Value: fmt.Sprintf("%d/%d", ok, n)}, nil
	case "bg":
		ms, _ := strconv.Atoi(arg)
		text := strings.Join(rest, " ")
		// Goroutine spawned by the cell: keeps the cell id for output
		// attribution and keeps running after the cell's done event.
		go func() {
			select {
			case <-time.After(time.Duration(ms) * time.Millisecond):
				env.Stdout(text)
			case <-env.RootCtx.Done():
			}
		}()
		return Result{Value: "spawned"}, nil
	case "set":
		env.Set(arg, strings.Join(rest, " "))
		return Result{}, nil
	case "get":
		v, ok := env.Get(arg)
		if !ok {
			return Result{}, fmt.Errorf("NameError: name %q is not defined", arg)
		}
		return Result{Value: v}, nil
	case "fail":
		return Result{}, fmt.Errorf("%s", strings.TrimSpace(arg+" "+strings.Join(rest, " ")))
	default:
		return Result{}, fmt.Errorf("SyntaxError: unknown op %q", name)
	}
}
