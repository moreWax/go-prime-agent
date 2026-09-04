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

	"go-prime-agent/internal/kernel"
	"go-prime-agent/internal/proto"
)

type Harness struct {
	T         *testing.T
	ToKernel  io.Writer
	closeEvt  io.WriteCloser
	Events    chan proto.Event
	HostReqs  chan proto.Event // demuxed host_request frames
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

// NewHarness starts a Kernel over in-memory pipes. cfg may be nil or mutate
// the config (e.g. inject an evaluator).
func NewHarness(t *testing.T, cfg func(*kernel.Config)) *Harness {
	t.Helper()
	kr, kw := io.Pipe()
	er, ew := io.Pipe()

	h := &Harness{
		T: t, ToKernel: kw, closeEvt: ew,
		Events:   make(chan proto.Event, 256),
		HostReqs: make(chan proto.Event, 64),
	}
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel

	conf := kernel.Config{In: kr, Out: ew}
	if cfg != nil {
		cfg(&conf)
	}
	k := kernel.New(conf)
	h.wg.Add(1)
	go func() { defer h.wg.Done(); k.Run(ctx) }()

	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		sc := bufio.NewScanner(er)
		sc.Buffer(make([]byte, 1<<20), 16<<20)
		for sc.Scan() {
			var e proto.Event
			if err := json.Unmarshal(sc.Bytes(), &e); err == nil {
				if e.Event == "host_request" {
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

func (h *Harness) Send(line string) {
	if _, err := io.WriteString(h.ToKernel, line+"\n"); err != nil {
		h.T.Fatalf("send: %v", err)
	}
}

func (h *Harness) Await(timeout time.Duration) proto.Event {
	select {
	case e, ok := <-h.Events:
		if !ok {
			h.T.Fatal("event stream closed")
		}
		return e
	case <-time.After(timeout):
		h.T.Fatal("timed out waiting for event")
		return proto.Event{}
	}
}

// WantDone reads events until the done for id, returning it plus the
// intervening events.
func (h *Harness) WantDone(id string, timeout time.Duration) (proto.Event, []proto.Event) {
	var mid []proto.Event
	deadline := time.After(timeout)
	for {
		select {
		case e := <-h.Events:
			if e.Event == "done" && e.ID != nil && *e.ID == id {
				return e, mid
			}
			mid = append(mid, e)
		case <-deadline:
			h.T.Fatalf("timed out waiting for done %s", id)
		}
	}
}

func (h *Harness) Close() {
	h.cancel()
	h.ToKernel.(io.WriteCloser).Close()
	h.closeEvt.Close()
	h.wg.Wait()
}
