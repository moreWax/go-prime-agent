package rlm_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	rlm "github.com/moreWax/go-prime-agent/rlm"
)

// fakeHost reads host_request frames from the bridge's writer and resolves
// them with canned logic.
type fakeHost struct {
	t       *testing.T
	bridge  *rlm.Bridge
	handler func(kind string, payload map[string]any) (json.RawMessage, error)
}

func newFakeHost(t *testing.T, handler func(string, map[string]any) (json.RawMessage, error)) *fakeHost {
	t.Helper()
	pr, pw := io.Pipe()
	fh := &fakeHost{t: t, handler: handler}
	fh.bridge = rlm.NewBridge(rlm.NewWriter(pw))
	go fh.serve(pr)
	t.Cleanup(func() { pw.Close() })
	return fh
}

func (f *fakeHost) serve(r io.Reader) {
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		var e rlm.Event
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil || e.Event != rlm.KindHostRequest || e.ID == nil {
			continue
		}
		var env struct {
			Kind    string         `json:"kind"`
			Payload map[string]any `json:"payload"`
		}
		_ = json.Unmarshal(e.Data, &env)
		reply := rlm.Reply{Status: rlm.StatusOK}
		if res, err := f.handler(env.Kind, env.Payload); err != nil {
			reply.Status = "error"
			reply.Error = err.Error()
		} else {
			reply.Result = res
		}
		f.bridge.Resolve(*e.ID, reply)
	}
}

// Spawn fans out concurrently and gathers child handles.
func TestSpawnFanOut(t *testing.T) {
	var mu sync.Mutex
	spawned := 0
	fh := newFakeHost(t, func(kind string, payload map[string]any) (json.RawMessage, error) {
		if kind != "spawn_task" {
			t.Errorf("unexpected kind %q", kind)
		}
		mu.Lock()
		spawned++
		mu.Unlock()
		return json.Marshal(map[string]any{
			"rlm_child_id": "child-" + payload["name"].(string),
			"name":         payload["name"],
			"session_dir":  "/sessions/" + payload["name"].(string),
			"model":        "test-model",
		})
	})
	cl := rlm.NewClient(fh.bridge)

	var wg sync.WaitGroup
	children := make([]*rlm.Child, 3)
	errs := make([]error, 3)
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			children[i], errs[i] = cl.Spawn(context.Background(), "do work", "w"+string(rune('0'+i)))
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("spawn %d: %v", i, err)
		}
		if children[i].Model != "test-model" || children[i].SessionDir == "" {
			t.Fatalf("child %d not decoded: %+v", i, children[i])
		}
	}
	if children[0].ID == children[1].ID {
		t.Fatal("children share ids")
	}
}

// Send and ListAgents round-trip through the bridge.
func TestSendAndList(t *testing.T) {
	fh := newFakeHost(t, func(kind string, payload map[string]any) (json.RawMessage, error) {
		switch kind {
		case "spawn_task":
			return json.Marshal(map[string]any{"rlm_child_id": "c1", "name": payload["name"]})
		case "agent_message":
			if payload["receiver_role"] != "child" || payload["message"] == "" {
				t.Errorf("bad message payload: %v", payload)
			}
			return json.Marshal(map[string]any{"delivered": true})
		case "list_agents":
			return json.Marshal([]map[string]any{{"name": "w0", "role": "child", "id": "child-w0"}})
		}
		return nil, fmt.Errorf("unexpected kind %q", kind)
	})
	cl := rlm.NewClient(fh.bridge)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	child, err := cl.Spawn(ctx, "task", "w0")
	if err != nil {
		t.Fatal(err)
	}
	if err := child.Send(ctx, "status?"); err != nil {
		t.Fatalf("send: %v", err)
	}
	list, err := cl.ListAgents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].Name != "w0" {
		t.Fatalf("list: %+v", list)
	}
}

// A non-ok host reply surfaces as an error.
func TestSpawnError(t *testing.T) {
	fh := newFakeHost(t, func(kind string, payload map[string]any) (json.RawMessage, error) {
		return nil, fmt.Errorf("no capacity")
	})
	cl := rlm.NewClient(fh.bridge)
	if _, err := cl.Spawn(context.Background(), "task", "w"); err == nil {
		t.Fatal("expected error")
	}
}
