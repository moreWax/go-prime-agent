// Package proto implements the RLM runtime wire protocol (v3):
// newline-delimited JSON, requests on stdin, events on stdout.
// Spec: prime-agent-runtime/src/rlm/repl.md
package rlm

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"
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

func IDPtr(s string) *string { return &s }

// Writer serializes event frames: one locked write sequence per frame, so
// frames from any goroutine never interleave (spec: Channels).
type Writer struct {
	mu  sync.Mutex
	out io.Writer
}

func NewWriter(out io.Writer) *Writer { return &Writer{out: out} }

func (w *Writer) Write(e Event) error {
	if !e.Valid() {
		return fmt.Errorf("proto: invalid event kind %q", e.Event)
	}
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	_, err = w.out.Write(append(b, '\n'))
	return err
}
