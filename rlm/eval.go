// Package eval defines the cell-execution contract shared by the kernel and
// every evaluator implementation.
package rlm

import (
	"context"
	"encoding/json"
)

// Host is the port a cell uses to reach the host bridge (subagents,
// messaging, future tools). Implementations return the reply's result as raw
// JSON; cells decode what they expect.
type Host interface {
	// CallHost ships one typed host_request and waits for its reply.
	CallHost(ctx context.Context, kind string, payload any) (json.RawMessage, error)
}

// Env is what one cell can touch.
type Env struct {
	// Ctx is the cell's lifecycle: cancelled on interrupt or cell end.
	Ctx context.Context
	// RootCtx outlives the cell: goroutines a cell spawns keep running
	// between cells and are governed by the kernel root, not the cell.
	RootCtx context.Context
	CellID  string
	Code    string
	// Stdout writes attributed output for this cell.
	Stdout func(text string)
	Host   Host
	// Set/Get mirror declared names into the kernel scope
	// (list_names, snapshot).
	Set func(k string, v any)
	Get func(k string) (any, bool)
}

// Result is the outcome of one cell. Value, when non-nil, becomes the
// protocol result event (the "trailing expression repr").
type Result struct {
	Value any
}

// InterruptedError reports a cell ended because its context was cancelled.
type InterruptedError struct{ CellID string }

func (e *InterruptedError) Error() string { return "KeyboardInterrupt" }
