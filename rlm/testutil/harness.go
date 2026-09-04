// Package testutil provides a pipe-backed fake host shared by kernel and
// evaluator conformance tests.
package testutil

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"sync"
	"testing"
	"time"

	rlm "github.com/moreWax/go-prime-agent/rlm"
)

// Harness wires a Kernel to a fake host over in-memory pipes and collects
// every event the runtime emits. Close is registered with t.Cleanup.
type Harness struct {
	T        *testing.T
	ToKernel io.Writer
	Events   chan rlm.Event
	HostReqs chan rlm.Event // demuxed host_request frames

	cancel   context.CancelFunc
	closeEvt io.WriteCloser
	done     chan struct{}
	wg       sync.WaitGroup
}

// NewHarness starts a Kernel over in-memory pipes. cfg may be nil or mutate
// the config (e.g. inject an evaluator).
func NewHarness(t *testing.T, cfg func(*rlm.Config)) *Harness {
	t.Helper()
	kr, kw := io.Pipe()
	er, ew := io.Pipe()

	h := &Harness{
		T: t, ToKernel: kw, closeEvt: ew,
		Events:   make(chan rlm.Event, 256),
		HostReqs: make(chan rlm.Event, 64),
		done:     make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	t.Cleanup(h.Close)

	conf := rlm.Config{In: kr, Out: ew}
	if cfg != nil {
		cfg(&conf)
	}
	k := rlm.New(conf)
	h.wg.Add(1)
	go func() { defer h.wg.Done(); k.Run(ctx) }()

	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		sc := bufio.NewScanner(er)
		sc.Buffer(make([]byte, 1<<20), 16<<20)
		for sc.Scan() {
			var e rlm.Event
			if err := json.Unmarshal(sc.Bytes(), &e); err == nil {
				if e.Event == rlm.KindHostRequest {
					h.HostReqs <- e
				} else {
					h.Events <- e
				}
			}
		}
		close(h.Events)
	}()
	return h
}

// Send writes one request line to the kernel.
func (h *Harness) Send(line string) {
	if _, err := io.WriteString(h.ToKernel, line+"\n"); err != nil {
		h.T.Fatalf("send: %v", err)
	}
}

// SendReq marshals and sends a request.
func (h *Harness) SendReq(v any) {
	b, err := json.Marshal(v)
	if err != nil {
		h.T.Fatal(err)
	}
	h.Send(string(b))
}

// Await reads the next event or fails the test on timeout.
func (h *Harness) Await(timeout time.Duration) rlm.Event {
	h.T.Helper()
	select {
	case e, ok := <-h.Events:
		if !ok {
			h.T.Fatal("event stream closed")
		}
		return e
	case <-time.After(timeout):
		h.T.Fatal("timed out waiting for event")
		return rlm.Event{}
	}
}

// WantDone reads events until the done for id, returning it plus the
// intervening events.
func (h *Harness) WantDone(id string, timeout time.Duration) (rlm.Event, []rlm.Event) {
	h.T.Helper()
	var mid []rlm.Event
	deadline := time.After(timeout)
	for {
		select {
		case e := <-h.Events:
			if e.Event == rlm.KindDone && e.ID != nil && *e.ID == id {
				return e, mid
			}
			mid = append(mid, e)
		case <-deadline:
			h.T.Fatalf("timed out waiting for done %s", id)
		}
	}
}

// FindResult returns the text of the first result event, if any.
func FindResult(mid []rlm.Event) *string {
	for _, m := range mid {
		if m.Event == rlm.KindResult {
			s := m.Text
			return &s
		}
	}
	return nil
}

// HostAutoReplier replies to every host_request with fn's result until the
// harness closes. fn receives the decoded envelope's kind and payload.
func (h *Harness) HostAutoReplier(fn func(kind string, payload any) (json.RawMessage, error)) {
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		for {
			select {
			case <-h.done:
				return
			case e := <-h.HostReqs:
				if e.ID == nil {
					continue
				}
				var env struct {
					Kind    string `json:"kind"`
					Payload any    `json:"payload"`
				}
				_ = json.Unmarshal(e.Data, &env)
				reply := rlm.Reply{Status: rlm.StatusOK}
				if res, err := fn(env.Kind, env.Payload); err != nil {
					reply.Status = "error"
					reply.Error = err.Error()
				} else {
					reply.Result = res
				}
				h.SendReq(rlm.Request{Type: "host_reply", ID: *e.ID, Data: mustJSON(h.T, reply)})
			}
		}
	}()
}

// Close tears the harness down; registered with t.Cleanup.
func (h *Harness) Close() {
	close(h.done)
	h.cancel()
	h.ToKernel.(io.WriteCloser).Close()
	h.closeEvt.Close()
	h.wg.Wait()
}

func mustJSON(t *testing.T, v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
