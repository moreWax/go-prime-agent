// Package proto implements the RLM runtime wire protocol (v3):
// newline-delimited JSON, requests on stdin, events on stdout.
// Spec: prime-agent-runtime/src/rlm/repl.md
package proto

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"sync"
)

const ProtocolVersion = 3

// Event kinds the v3 host accepts. Unknown kinds are treated as corruption.
var EventKinds = map[string]bool{
	"ready": true, "stdout": true, "stderr": true, "result": true,
	"display": true, "host_request": true, "error": true, "done": true,
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

// Reply is the payload of a host_reply request: {"status":"ok","result":{...}}
// or an error envelope.
type Reply struct {
	Status string          `json:"status"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  string          `json:"error,omitempty"`
}

// Event is a runtime-to-host frame. Field set matches repl.md exactly;
// optional fields are omitted when zero.
type Event struct {
	Event     string          `json:"event"`
	ID        *string         `json:"id"` // nil => JSON null
	Protocol  int             `json:"protocol,omitempty"`
	// Kept the v3 field name for host compatibility; carries the Go
	// toolchain version. A v4 fork renames this to "runtime".
	Runtime   string          `json:"python,omitempty"`
	Text      string          `json:"text,omitempty"`
	Data      json.RawMessage `json:"data,omitempty"`
	EName     string          `json:"ename,omitempty"`
	EValue    string          `json:"evalue,omitempty"`
	Traceback []string        `json:"traceback,omitempty"`
	// done extras
	Status    string          `json:"status,omitempty"`
	Reason    string          `json:"reason,omitempty"`
	Names     []string        `json:"names,omitempty"`
	Saved     []string        `json:"saved,omitempty"`
	Skipped   []string        `json:"skipped,omitempty"`
	Pruned    []string        `json:"pruned,omitempty"`
	Bytes     int64           `json:"bytes,omitempty"`
	Restored  []string        `json:"restored,omitempty"`
	Failed    []string        `json:"failed,omitempty"`
}

func (e Event) Valid() bool { return EventKinds[e.Event] }

func IDPtr(s string) *string { return &s }

// Writer serializes event frames: one locked write sequence per frame, so
// frames from any goroutine never interleave (spec: Channels).
type Writer struct {
	mu  sync.Mutex
	bw  *bufio.Writer
	out io.Writer
}

func NewWriter(out io.Writer) *Writer {
	return &Writer{bw: bufio.NewWriter(out), out: out}
}

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
	if _, err := w.out.Write(append(b, '\n')); err != nil {
		return err
	}
	return w.bw.Flush()
}
