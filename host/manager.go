package host

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/moreWax/go-prime-agent/rlm"
)

// ChildRunner executes one admitted child task. Production wiring runs a
// go-pi agent loop with its own kernel; tests inject fakes. It must return
// promptly when ctx is cancelled.
type ChildRunner interface {
	RunChild(ctx context.Context, task, name string, inbox <-chan string)
}

// Manager services the host bridge for one parent session: spawn_task,
// agent_message, list_agents (the rlm.Spawn/Send/ListAgents surface).
type Manager struct {
	self     string // this session's name
	skills   string
	runner   ChildRunner
	inbox    chan string       // messages addressed to this session
	mu       sync.Mutex        // children roster (uncontended; registry calls)
	children map[string]string // name -> rlm_child_id
}

// NewManager wires the parent-side bridge service.
func NewManager(self, skillsDir string, runner ChildRunner) *Manager {
	return &Manager{
		self:     self,
		skills:   skillsDir,
		runner:   runner,
		inbox:    make(chan string, 64),
		children: make(map[string]string),
	}
}

// Inbox is where messages addressed to this session arrive.
func (m *Manager) Inbox() <-chan string { return m.inbox }

// HandleHostRequest services one kernel host_request.
func (m *Manager) HandleHostRequest(ctx context.Context, kind string, payload json.RawMessage) (json.RawMessage, error) {
	switch kind {
	case "spawn_task":
		var p struct {
			Task string `json:"task"`
			Name string `json:"name"`
		}
		if err := json.Unmarshal(payload, &p); err != nil {
			return nil, err
		}
		return m.spawn(ctx, p.Task, p.Name)
	case "agent_message":
		var p struct {
			ReceiverRole string `json:"receiver_role"`
			ReceiverName string `json:"receiver_name"`
			Message      string `json:"message"`
		}
		if err := json.Unmarshal(payload, &p); err != nil {
			return nil, err
		}
		return m.deliver(p.ReceiverRole, p.ReceiverName, p.Message)
	case "list_agents":
		m.mu.Lock()
		out := []map[string]any{{"name": m.self, "role": "self", "id": m.self}}
		for name, id := range m.children {
			out = append(out, map[string]any{"name": name, "role": "child", "id": id})
		}
		m.mu.Unlock()
		return json.Marshal(out)
	default:
		return nil, fmt.Errorf("host kind %q not supported", kind)
	}
}

// spawn admits a child task and returns its handle immediately — never its
// answer (admission contract, same as the TS host's rlm.run).
func (m *Manager) spawn(ctx context.Context, task, name string) (json.RawMessage, error) {
	childCtx, cancel := context.WithCancel(ctx)
	inbox := make(chan string, 16)
	id := "child-" + newHexID()[:8]

	m.mu.Lock()
	m.children[name] = id
	m.mu.Unlock()

	go func() {
		defer cancel()
		m.runner.RunChild(childCtx, task, name, inbox)
		m.mu.Lock()
		delete(m.children, name)
		m.mu.Unlock()
	}()

	handle := map[string]any{
		"rlm_child_id": id,
		"name":         name,
		"model":        "inherited",
		"session_dir":  "",
	}
	return json.Marshal(handle)
}

// deliver routes an agent_message: "parent" (or empty) reaches this
// session's inbox; "child" + name reaches that child's inbox (relayed via
// the runner's inbox channel, which owns the child side).
func (m *Manager) deliver(role, name, message string) (json.RawMessage, error) {
	if role == "parent" || role == "" {
		select {
		case m.inbox <- message:
		default:
			return nil, fmt.Errorf("parent inbox full")
		}
		return json.Marshal(map[string]any{"delivered": true})
	}
	return nil, fmt.Errorf("delivery to %s %q not routed by this manager yet", role, name)
}

var _ rlm.Reply = rlm.Reply{}
