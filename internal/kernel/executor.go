package kernel

import (
	"fmt"
	"sort"

	"go-prime-agent/internal/proto"
)

// executor drains the work queue one request at a time (v3 policy). When
// draining begins it finishes whatever is already queued, then exits.
func (k *Kernel) executor() {
	defer k.wg.Done()
	defer close(k.execDone)
	for {
		select {
		case <-k.rootCtx.Done():
			return
		case w := <-k.queue:
			k.runRequest(w)
		case <-k.drainCh:
			// Draining: finish anything already queued, then exit.
			select {
			case w := <-k.queue:
				k.runRequest(w)
			default:
				return
			}
		}
	}
}

// runRequest executes one queued request to its done event.
func (k *Kernel) runRequest(w work) {
	req := w.req
	cancel, parked := k.table.activate(req.ID)

	k.wg.Add(1)
	defer k.wg.Done()
	defer k.table.deactivate(req.ID)
	defer func() {
		if cancel != nil {
			cancel()
		}
	}()

	if parked && cancel != nil {
		cancel()
	}

	switch req.Type {
	case "execute":
		k.runCell(w)
	case "list_names":
		names := k.scope.Names()
		sort.Strings(names)
		k.done(req.ID, proto.StatusOK, &proto.DoneExtras{Names: names})
	case "snapshot":
		k.snapshot(req)
	case "restore":
		k.restore(req)
	default:
		k.events.Write(proto.Event{
			Event: proto.KindError, ID: proto.IDPtr(req.ID),
			EName: proto.EnameProtocol, EValue: fmt.Sprintf("unknown request type %q", req.Type),
		})
		k.done(req.ID, proto.StatusError, nil)
	}
}
