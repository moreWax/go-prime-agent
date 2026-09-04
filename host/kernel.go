// Package host is the prime-agent harness in Go, built on go-pi
// (github.com/earendil-works/go-pi): the agent loop, model registry, and
// streaming come from go-pi; this package adds the prime-specific surface —
// the Go kernel as an in-process tool, subagent machinery servicing the
// kernel's host bridge, skills, and prompt assembly.
package host

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"sync"

	"github.com/moreWax/go-prime-agent/rlm"
)

// pending is one in-flight execute. All fields are written by the event
// reader goroutine before close(done); readers synchronize on done — no lock.
type pending struct {
	id     string
	output []string
	result string
	err    *rlm.Event
	acked  chan struct{} // closed once registered with the reader
	done   chan struct{}
}

// KernelHost drives an in-process rlm.Kernel over a pipe pair — gorlm as a
// library, no subprocess. One reader goroutine owns the pending map and all
// event parsing; host_request frames are dispatched to the HostService.
type KernelHost struct {
	send    chan []byte   // request lines -> writer goroutine -> kernel stdin
	reg     chan *pending // register pending (reader goroutine owns map)
	service HostService   // services host_request frames
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// HostService services the kernel's host bridge: spawn_task, agent_message,
// list_agents (see Manager).
type HostService interface {
	HandleHostRequest(ctx context.Context, kind string, payload json.RawMessage) (json.RawMessage, error)
}

// NewKernelHost boots an in-process kernel (Go evaluator, optional skills)
// and its host-side client. Cancel stops everything.
func NewKernelHost(ctx context.Context, skillsDir string, service HostService) (*KernelHost, error) {
	ctx, cancel := context.WithCancel(ctx)
	kr, kw := io.Pipe() // host -> kernel (requests)
	er, ew := io.Pipe() // kernel -> host (events)

	var ev rlm.Evaluator
	if skillsDir != "" {
		ev = rlm.NewGoEvaluatorWithSkills(skillsDir)
	} else {
		ev = rlm.NewGoEvaluator()
	}
	k := rlm.New(rlm.Config{In: kr, Out: ew, Eval: ev})
	go func() { _ = k.Run(ctx) }()

	h := &KernelHost{
		send:    make(chan []byte, 64),
		reg:     make(chan *pending, 64),
		service: service,
		cancel:  cancel,
	}
	h.wg.Add(2)
	go h.writer(kw)
	go h.reader(er)
	return h, nil
}

// writer serializes request lines to the kernel stdin pipe.
func (h *KernelHost) writer(kw io.Writer) {
	defer h.wg.Done()
	for b := range h.send {
		if _, err := kw.Write(append(b, '\n')); err != nil {
			return
		}
	}
}

// reader owns the pending map: a line pump feeds kernel events, and this
// goroutine selects over lines AND registrations — so a registration is
// always processed before any event its request could produce.
func (h *KernelHost) reader(er io.Reader) {
	defer h.wg.Done()
	pendings := make(map[string]*pending)

	lines := make(chan []byte)
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		sc := bufio.NewScanner(er)
		sc.Buffer(make([]byte, 1<<20), 16<<20)
		for sc.Scan() {
			lines <- sc.Bytes()
		}
		close(lines)
	}()

	for {
		select {
		case p := <-h.reg:
			pendings[p.id] = p
			close(p.acked)
		case b, ok := <-lines:
			if !ok {
				for _, p := range pendings {
					close(p.done)
				}
				return
			}
			h.onEvent(b, pendings)
		}
	}
}

func (h *KernelHost) onEvent(b []byte, pendings map[string]*pending) {
	var ev rlm.Event
	if json.Unmarshal(b, &ev) != nil {
		return
	}
	switch ev.Event {
	case rlm.KindHostRequest:
		h.onHostRequest(ev)
	case rlm.KindStdout, rlm.KindStderr:
		if p := pendings[ev.IDString()]; p != nil && ev.Text != "" {
			p.output = append(p.output, ev.Text)
		}
	case rlm.KindResult:
		if p := pendings[ev.IDString()]; p != nil {
			p.result = ev.Text
		}
	case rlm.KindError:
		if p := pendings[ev.IDString()]; p != nil {
			e := ev
			p.err = &e
		}
	case rlm.KindDone:
		if p := pendings[ev.IDString()]; p != nil {
			delete(pendings, ev.IDString())
			close(p.done)
		}
	}
}

func idOf(ev rlm.Event) string { return ev.IDString() }

func (h *KernelHost) onHostRequest(ev rlm.Event) {
	if ev.ID == nil {
		return
	}
	var env struct {
		Kind    string          `json:"kind"`
		Payload json.RawMessage `json:"payload"`
	}
	_ = json.Unmarshal(ev.Data, &env)
	res, err := h.service.HandleHostRequest(context.Background(), env.Kind, env.Payload)
	reply := rlm.Reply{Status: rlm.StatusOK, Result: res}
	if err != nil {
		reply.Status = "error"
		reply.Error = err.Error()
		reply.Result = nil
	}
	b, _ := json.Marshal(reply)
	req, _ := json.Marshal(map[string]any{"type": "host_reply", "id": *ev.ID, "data": json.RawMessage(b)})
	h.send <- req
}

// CellResult is one executed cell.
type CellResult struct {
	Output []string
	Result string
	Err    *rlm.Event
}

func (c CellResult) Text() string {
	s := ""
	for _, o := range c.Output {
		s += o
	}
	if c.Result != "" {
		s += "=> " + c.Result + "\n"
	}
	if c.Err != nil {
		s += c.Err.EName + ": " + c.Err.EValue + "\n"
	}
	return s
}

// Execute runs one cell to completion; ctx cancellation interrupts it.
func (h *KernelHost) Execute(ctx context.Context, code string) (CellResult, error) {
	id := newHexID()
	p := &pending{id: id, acked: make(chan struct{}), done: make(chan struct{})}
	h.reg <- p
	<-p.acked // registered before the request can reach the kernel
	req, _ := json.Marshal(map[string]any{"type": "execute", "id": id, "code": code})
	h.send <- req

	select {
	case <-p.done:
		return CellResult{Output: p.output, Result: p.result, Err: p.err}, nil
	case <-ctx.Done():
		interrupt, _ := json.Marshal(map[string]any{"type": "interrupt", "id": id})
		h.send <- interrupt
		<-p.done // cell reports KeyboardInterrupt, then done
		return CellResult{Output: p.output, Result: p.result, Err: p.err}, nil
	}
}

// Shutdown asks the kernel to drain and stop.
func (h *KernelHost) Shutdown() {
	req, _ := json.Marshal(map[string]any{"type": "shutdown"})
	h.send <- req
	h.cancel()
}

func newHexID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
