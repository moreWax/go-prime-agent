// Package hostbridge correlates concurrent host_request/host_reply exchanges.
// Any number of calls may be in flight at once; each gets a buffered reply
// channel keyed by id. All waits are context-aware: cancelling the calling
// cell's context abandons the wait and drops the pending entry.
package hostbridge

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/moreWax/go-prime-agent/internal/proto"
)

type Bridge struct {
	w       *proto.Writer
	mu      sync.Mutex
	pending map[string]chan proto.Reply
}

func New(w *proto.Writer) *Bridge {
	return &Bridge{w: w, pending: make(map[string]chan proto.Reply)}
}

func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

// Call ships one host_request and blocks until the matching host_reply, the
// context is cancelled, or the writer fails. Safe for concurrent use: fan out
// from as many goroutines as you like.
func (b *Bridge) Call(ctx context.Context, kind string, payload any) (proto.Reply, error) {
	id := newID()
	ch := make(chan proto.Reply, 1)
	b.mu.Lock()
	b.pending[id] = ch
	b.mu.Unlock()

	data, err := json.Marshal(map[string]any{"kind": kind, "payload": payload})
	if err == nil {
		err = b.w.Write(proto.Event{Event: "host_request", ID: proto.IDPtr(id), Data: data})
	}
	if err != nil {
		b.drop(id)
		return proto.Reply{}, err
	}

	select {
	case r := <-ch:
		return r, nil
	case <-ctx.Done():
		b.drop(id)
		return proto.Reply{}, fmt.Errorf("host_request %s abandoned: %w", id, ctx.Err())
	}
}

// Resolve routes a host_reply. Unknown or abandoned ids are dropped (spec:
// Host bridge).
func (b *Bridge) Resolve(id string, r proto.Reply) {
	b.mu.Lock()
	ch, ok := b.pending[id]
	if ok {
		delete(b.pending, id)
	}
	b.mu.Unlock()
	if ok {
		ch <- r
	}
}

// CallHost satisfies eval.Host structurally: same signature, raw-JSON
// result. Defined here so the kernel can pass the bridge to cells without
// an adapter type.
func (b *Bridge) CallHost(ctx context.Context, kind string, payload any) (json.RawMessage, error) {
	r, err := b.Call(ctx, kind, payload)
	if err != nil {
		return nil, err
	}
	if r.Status != "ok" {
		return nil, fmt.Errorf("host error: %s", r.Error)
	}
	return r.Result, nil
}

func (b *Bridge) drop(id string) {
	b.mu.Lock()
	delete(b.pending, id)
	b.mu.Unlock()
}
