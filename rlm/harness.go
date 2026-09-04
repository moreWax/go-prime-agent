// harness.go — host bridge (harness.py counterpart): correlates concurrent
// host_request/host_reply exchanges. The pending map lives in a single
// registry goroutine fed by a command channel — no lock, and registration
// always precedes resolution for the same id by channel FIFO (a host_reply
// can only exist after its host_request frame was written, which happens
// after the register command was queued).
package rlm

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"

	rlmhost "context"
)

type bridgeCmd struct {
	op string // register | resolve | drop
	id string
	ch chan Reply
	r  Reply
}

type Bridge struct {
	w    *Writer
	cmds chan bridgeCmd
}

func NewBridge(w *Writer) *Bridge {
	b := &Bridge{w: w, cmds: make(chan bridgeCmd, 256)}
	go b.run()
	return b
}

func (b *Bridge) run() {
	pending := make(map[string]chan Reply)
	for c := range b.cmds {
		switch c.op {
		case "register":
			pending[c.id] = c.ch
		case "resolve":
			if ch, ok := pending[c.id]; ok {
				delete(pending, c.id)
				ch <- c.r // buffered(1): never blocks the registry
			}
		case "drop":
			delete(pending, c.id)
		}
	}
}

func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

// Call ships one host_request and blocks until the matching host_reply, the
// context is cancelled, or the writer fails. Any number of calls may be in
// flight — fan out from as many goroutines as you like.
func (b *Bridge) Call(ctx rlmhost.Context, kind string, payload any) (Reply, error) {
	id := newID()
	ch := make(chan Reply, 1)
	b.cmds <- bridgeCmd{op: "register", id: id, ch: ch}

	ev, err := HostRequestEvent(id, kind, payload)
	if err == nil {
		err = b.w.Write(ev)
	}
	if err != nil {
		b.cmds <- bridgeCmd{op: "drop", id: id}
		return Reply{}, err
	}

	select {
	case r := <-ch:
		return r, nil
	case <-ctx.Done():
		b.cmds <- bridgeCmd{op: "drop", id: id}
		return Reply{}, fmt.Errorf("host_request %s abandoned: %w", id, ctx.Err())
	}
}

// Resolve routes a host_reply. Unknown or abandoned ids are dropped
// (spec: Host bridge).
func (b *Bridge) Resolve(id string, r Reply) {
	b.cmds <- bridgeCmd{op: "resolve", id: id, r: r}
}

// CallHost satisfies the evaluator Host port: raw-JSON result.
func (b *Bridge) CallHost(ctx context.Context, kind string, payload any) (json.RawMessage, error) {
	r, err := b.Call(ctx, kind, payload)
	if err != nil {
		return nil, err
	}
	if r.Status != StatusOK {
		return nil, fmt.Errorf("host error: %s", r.Error)
	}
	return r.Result, nil
}
