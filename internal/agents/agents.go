// Package agents is the typed subagent client riding the host bridge.
// spawn_task / agent_message are the same host_request kinds the Python
// runtime uses, so the stock Node harness services them unchanged.
//
// v3 note: replies from spawned children surface in the parent agent's LLM
// conversation as ordinary agent-message prompts, not as kernel frames. A
// forked host can add a `deliver` request kind to route child messages into
// this process; the Child handle is the natural attachment point.
package agents

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/moreWax/go-prime-agent/internal/hostbridge"
)

type Client struct {
	b *hostbridge.Bridge
}

func New(b *hostbridge.Bridge) *Client { return &Client{b: b} }

// Child is a spawned subagent handle. Field names mirror what the host
// returns for spawn_task; confirm against agent-session.js when wiring the
// real host.
type Child struct {
	ID         string `json:"rlm_child_id"`
	Name       string `json:"name"`
	SessionDir string `json:"session_dir"`
	Model      string `json:"model"`

	c *Client
}

// Spawn admits a subagent task; it returns at admission, never at completion
// (same contract as rlm() in the Python runtime). Safe for concurrent use:
// fan out from many goroutines and gather.
func (cl *Client) Spawn(ctx context.Context, task, name string) (*Child, error) {
	rep, err := cl.b.Call(ctx, "spawn_task", map[string]any{"task": task, "name": name})
	if err != nil {
		return nil, err
	}
	if rep.Status != "ok" {
		return nil, fmt.Errorf("spawn_task failed: %s", rep.Error)
	}
	var ch Child
	if err := json.Unmarshal(rep.Result, &ch); err != nil {
		return nil, fmt.Errorf("spawn_task reply: %w", err)
	}
	ch.c = cl
	return &ch, nil
}

// Send delivers a message to an agent by role+name (agent_message skill
// semantics). With no name, role "parent" reaches this agent's parent.
func (cl *Client) Send(ctx context.Context, role, name, message string) error {
	rep, err := cl.b.Call(ctx, "agent_message", map[string]any{
		"receiver_role": role, "receiver_name": name, "message": message,
	})
	if err != nil {
		return err
	}
	if rep.Status != "ok" {
		return fmt.Errorf("agent_message failed: %s", rep.Error)
	}
	return nil
}

// Send on a child is a follow-up to that child.
func (ch *Child) Send(ctx context.Context, message string) error {
	return ch.c.Send(ctx, "child", ch.Name, message)
}

type AgentInfo struct {
	Name string `json:"name"`
	Role string `json:"role"`
	ID   string `json:"id"`
}

func (cl *Client) ListAgents(ctx context.Context) ([]AgentInfo, error) {
	rep, err := cl.b.Call(ctx, "list_agents", nil)
	if err != nil {
		return nil, err
	}
	if rep.Status != "ok" {
		return nil, fmt.Errorf("list_agents failed: %s", rep.Error)
	}
	var out []AgentInfo
	if err := json.Unmarshal(rep.Result, &out); err != nil {
		return nil, err
	}
	return out, nil
}
