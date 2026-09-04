// frames.go — RLM wire protocol v3 (see repl.md): NDJSON frames, requests
// on stdin, events on stdout.
package rlm

import (
	"encoding/json"
	"fmt"
	"io"
	"sync/atomic"
)

const ProtocolVersion = 3

// Event kinds (spec: Events). The v3 host validates these strictly.
const (
	KindReady       = "ready"
	KindStdout      = "stdout"
	KindStderr      = "stderr"
	KindResult      = "result"
	KindDisplay     = "display"
	KindHostRequest = "host_request"
	KindError       = "error"
	KindDone        = "done"
)

// Done statuses and error names used across the runtime.
const (
	StatusOK    = "ok"
	StatusError = "error"

	EnameProtocol  = "ProtocolError"
	EnameKeyboard  = "KeyboardInterrupt"
	EnameCellError = "Error"
)

var eventKinds = map[string]bool{
	KindReady: true, KindStdout: true, KindStderr: true, KindResult: true,
	KindDisplay: true, KindHostRequest: true, KindError: true, KindDone: true,
}

// Request is a host-to-runtime frame.
type Request struct {
	Type             string          `json:"type"`
	ID               string          `json:"id,omitempty"`
	Code             string          `json:"code,omitempty"`
	Data             json.RawMessage `json:"data,omitempty"`
	Path             string          `json:"path,omitempty"`
	ManifestPath     string          `json:"manifest_path,omitempty"`
	MaxBytes         *int64          `json:"max_bytes,omitempty"`
	MaxVariableBytes *int64          `json:"max_variable_bytes,omitempty"`
	PruneOversized   *bool           `json:"prune_oversized,omitempty"`
}

// Reply is the payload of a host_reply request:
// {"status":"ok","result":{...}} or an error envelope.
type Reply struct {
	Status string          `json:"status"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  string          `json:"error,omitempty"`
}

// Event is a runtime-to-host frame. Field set matches repl.md; optional
// fields are omitted when zero.
type Event struct {
	Event    string  `json:"event"`
	ID       *string `json:"id"` // nil => JSON null
	Protocol int     `json:"protocol,omitempty"`
	// Field name kept for v3 host compatibility; carries the Go toolchain
	// version. A v4 fork renames it to "runtime".
	Runtime   string          `json:"python,omitempty"`
	Text      string          `json:"text,omitempty"`
	Data      json.RawMessage `json:"data,omitempty"`
	EName     string          `json:"ename,omitempty"`
	EValue    string          `json:"evalue,omitempty"`
	Traceback []string        `json:"traceback,omitempty"`
	Status    string          `json:"status,omitempty"` // done only
	// done extras (spec: Events); also used by DoneEvent.
	DoneExtras
}

// DoneExtras are the optional payloads a done event may carry.
type DoneExtras struct {
	Reason   string   `json:"reason,omitempty"`
	Names    []string `json:"names,omitempty"`    // list_names
	Saved    []string `json:"saved,omitempty"`    // snapshot
	Skipped  []string `json:"skipped,omitempty"`  // snapshot
	Pruned   []string `json:"pruned,omitempty"`   // snapshot
	Bytes    int64    `json:"bytes,omitempty"`    // snapshot
	Restored []string `json:"restored,omitempty"` // restore
	Failed   []string `json:"failed,omitempty"`   // restore
}

// DoneEvent builds a done frame; extras may be nil.
func DoneEvent(id, status string, extras *DoneExtras) Event {
	e := Event{Event: KindDone, ID: IDPtr(id), Status: status}
	if extras != nil {
		e.DoneExtras = *extras
	}
	return e
}

// HostRequestEvent builds a host_request frame with the standard envelope
// {"kind": ..., "payload": ...}.
func HostRequestEvent(id, kind string, payload any) (Event, error) {
	data, err := json.Marshal(map[string]any{"kind": kind, "payload": payload})
	if err != nil {
		return Event{}, err
	}
	return Event{Event: KindHostRequest, ID: IDPtr(id), Data: data}, nil
}

func (e Event) Valid() bool { return eventKinds[e.Event] }

// IDString returns the frame id ("" when unattributed).
func (e Event) IDString() string {
	if e.ID == nil {
		return ""
	}
	return *e.ID
}

func IDPtr(s string) *string { return &s }

// Writer serializes event frames through a single writer goroutine that
// owns the output: no mutex, and frames from any goroutine never interleave
// because one goroutine writes them in channel order (spec: Channels).
// Backpressure is the channel send — a slow consumer blocks senders exactly
// like the fd would.
type Writer struct {
	ch   chan Event
	err  atomic.Pointer[error]
	done chan struct{}
}

func NewWriter(out io.Writer) *Writer {
	w := &Writer{ch: make(chan Event, 1024), done: make(chan struct{})}
	go w.run(out)
	return w
}

func (w *Writer) run(out io.Writer) {
	defer close(w.done)
	for e := range w.ch {
		b, err := json.Marshal(e)
		if err == nil {
			b = append(b, '\n')
			_, err = out.Write(b)
		}
		if err != nil {
			var pe = err
			w.err.Store(&pe)
		}
	}
}

// Write queues one frame. It reports the first prior write failure (sticky)
// or invalid-kind errors; queued frames are written by the actor.
func (w *Writer) Write(e Event) error {
	if !e.Valid() {
		return fmt.Errorf("proto: invalid event kind %q", e.Event)
	}
	if p := w.err.Load(); p != nil {
		return *p
	}
	w.ch <- e
	return nil
}

// Close stops the actor after draining queued frames. Only for orderly
// shutdown by the process owner; background senders must be quiesced first.
func (w *Writer) Close() {
	close(w.ch)
	<-w.done
}
