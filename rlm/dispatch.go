package rlm

import (
	"bufio"
	"context"
	"encoding/json"
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
		var req Request
		if err := json.Unmarshal(line, &req); err != nil {
			k.events.Write(Event{
				Event: KindError, EName: EnameProtocol, EValue: err.Error(),
				Traceback: []string{string(line)},
			})
			continue
		}
		k.dispatch(req)
	}
}

// dispatch runs on the reader goroutine. interrupt and host_reply NEVER go
// through the queue — they must reach a busy kernel instantly.
func (k *Kernel) dispatch(req Request) {
	switch req.Type {
	case "interrupt":
		k.table.interrupt(req.ID)
	case "host_reply":
		var r Reply
		if err := json.Unmarshal(req.Data, &r); err == nil {
			k.bridge.Resolve(req.ID, r)
		}
	case "shutdown":
		if req.ID != "" {
			k.done(req.ID, StatusOK, nil)
		}
		k.beginShutdown()
	default:
		k.enqueue(req)
	}
}

// enqueue registers a request's cancel func and queues it. After shutdown
// begins, new requests are refused with an error done.
func (k *Kernel) enqueue(req Request) {
	select {
	case <-k.drainCh:
		k.events.Write(Event{
			Event: KindError, ID: IDPtr(req.ID),
			EName: EnameProtocol, EValue: "shutting down",
		})
		k.done(req.ID, StatusError, nil)
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

// requestTable tracks live/queued requests and parked interrupts, owned by
// a single actor goroutine — no lock. All interrupt semantics from the spec
// live here, single-threaded by construction:
//
//	interrupt with id    -> cancel that request, or park until it starts
//	interrupt without id -> cancel the running request, or park for the next
//	parked interrupts    -> delivered the moment their target activates
type tableCmd struct {
	op     string // register | activate | deactivate | interrupt
	id     string
	cancel context.CancelFunc
	reply  chan tableReply
}

type tableReply struct {
	cancel context.CancelFunc
	parked bool
}

type requestTable struct {
	cmds chan tableCmd
}

func newRequestTable() requestTable {
	t := requestTable{cmds: make(chan tableCmd, 256)}
	go t.run()
	return t
}

func (t requestTable) run() {
	var activeID string
	cancels := make(map[string]context.CancelFunc)
	parked := make(map[string]bool)
	parkNext := false
	for c := range t.cmds {
		switch c.op {
		case "register":
			cancels[c.id] = c.cancel
		case "activate":
			activeID = c.id
			p := parked[c.id] || parkNext
			delete(parked, c.id)
			parkNext = false
			c.reply <- tableReply{cancel: cancels[c.id], parked: p} // buffered(1)
		case "deactivate":
			activeID = ""
			delete(cancels, c.id)
		case "interrupt":
			if c.id != "" {
				if cancel, ok := cancels[c.id]; ok {
					cancel()
				} else {
					parked[c.id] = true
				}
			} else if activeID != "" {
				// id-less: the running request, else park for the next one.
				if cancel, ok := cancels[activeID]; ok {
					cancel()
				}
			} else {
				parkNext = true
			}
		}
	}
}

func (t requestTable) register(id string, cancel context.CancelFunc) {
	t.cmds <- tableCmd{op: "register", id: id, cancel: cancel}
}

func (t requestTable) activate(id string) (cancel context.CancelFunc, parked bool) {
	ch := make(chan tableReply, 1)
	t.cmds <- tableCmd{op: "activate", id: id, reply: ch}
	r := <-ch
	return r.cancel, r.parked
}

func (t requestTable) deactivate(id string) {
	t.cmds <- tableCmd{op: "deactivate", id: id}
}

func (t requestTable) interrupt(id string) {
	t.cmds <- tableCmd{op: "interrupt", id: id}
}

// done emits a done frame; extras may be nil.
func (k *Kernel) done(id, status string, extras *DoneExtras) {
	k.events.Write(DoneEvent(id, status, extras))
}
