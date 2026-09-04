package kernel

import (
	"bufio"
	"context"
	"encoding/json"
	"sync"

	"github.com/xor/go-prime-agent/internal/proto"
)

// readAll parses request lines until EOF. Malformed lines emit a
// ProtocolError event and serving continues (spec: Requests). EOF is
// equivalent to shutdown.
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
				Event: proto.KindError, EName: proto.EnameProtocol, EValue: err.Error(),
				Traceback: []string{string(line)},
			})
			continue
		}
		k.dispatch(req)
	}
}

// dispatch runs on the reader goroutine. interrupt and host_reply NEVER go
// through the queue — they must reach a busy kernel instantly.
func (k *Kernel) dispatch(req proto.Request) {
	switch req.Type {
	case "interrupt":
		k.table.interrupt(req.ID)
	case "host_reply":
		var r proto.Reply
		if err := json.Unmarshal(req.Data, &r); err == nil {
			k.bridge.Resolve(req.ID, r)
		}
	case "shutdown":
		if req.ID != "" {
			k.done(req.ID, proto.StatusOK, nil)
		}
		k.beginShutdown()
	default:
		k.enqueue(req)
	}
}

// enqueue registers a request's cancel func and queues it. After shutdown
// begins, new requests are refused with an error done.
func (k *Kernel) enqueue(req proto.Request) {
	select {
	case <-k.drainCh:
		k.events.Write(proto.Event{
			Event: proto.KindError, ID: proto.IDPtr(req.ID),
			EName: proto.EnameProtocol, EValue: "shutting down",
		})
		k.done(req.ID, proto.StatusError, nil)
		return
	default:
	}
	ctx, cancel := context.WithCancel(k.rootCtx)
	k.table.register(req.ID, cancel)
	select {
	case k.queue <- work{req: req, ctx: ctx}:
	case <-k.rootCtx.Done():
		cancel()
	}
}

// requestTable tracks live/queued requests and parked interrupts. All
// interrupt semantics from the spec live here:
//
//	interrupt with id    -> cancel that request, or park until it starts
//	interrupt without id -> cancel the running request, or park for the next
//	parked interrupts    -> delivered the moment their target activates
type requestTable struct {
	mu       sync.Mutex
	draining bool
	activeID string
	cancels  map[string]context.CancelFunc
	parked   map[string]bool
	parkNext bool
}

func newRequestTable() requestTable {
	return requestTable{
		cancels: make(map[string]context.CancelFunc),
		parked:  make(map[string]bool),
	}
}

func (t *requestTable) register(id string, cancel context.CancelFunc) {
	t.mu.Lock()
	t.cancels[id] = cancel
	t.mu.Unlock()
}

func (t *requestTable) beginDrain() {
	t.mu.Lock()
	t.draining = true
	t.mu.Unlock()
}

func (t *requestTable) interrupt(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if id != "" {
		if cancel, ok := t.cancels[id]; ok {
			cancel()
		} else {
			t.parked[id] = true
		}
		return
	}
	// id-less: the running request, else park for the next one.
	if t.activeID != "" {
		if cancel, ok := t.cancels[t.activeID]; ok {
			cancel()
		}
		return
	}
	t.parkNext = true
}

// activate marks id as running and reports its cancel func plus whether a
// parked interrupt must fire immediately.
func (t *requestTable) activate(id string) (cancel context.CancelFunc, parked bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.activeID = id
	parked = t.parked[id] || t.parkNext
	delete(t.parked, id)
	t.parkNext = false
	return t.cancels[id], parked
}

func (t *requestTable) deactivate(id string) {
	t.mu.Lock()
	t.activeID = ""
	delete(t.cancels, id)
	t.mu.Unlock()
}

// done emits a done frame; extras may be nil.
func (k *Kernel) done(id, status string, extras *proto.DoneExtras) {
	k.events.Write(proto.DoneEvent(id, status, extras))
}
