package host

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func skillsDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	g := filepath.Join(dir, "greet")
	os.MkdirAll(g, 0o755)
	os.WriteFile(filepath.Join(g, "skill.go"), []byte("package greet\n\nfunc Hello(n string) string { return \"hello, \" + n }\n"), 0o644)
	return dir
}

// The in-process kernel executes Go cells with skills and persistence.
func TestKernelHostGoCell(t *testing.T) {
	kh, err := NewKernelHost(context.Background(), skillsDir(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer kh.Shutdown()

	res, err := kh.Execute(context.Background(), "x := 6")
	if err != nil || res.Err != nil {
		t.Fatalf("cell 1: err=%v cellErr=%+v", err, res.Err)
	}
	res, err = kh.Execute(context.Background(), "x * 7")
	if err != nil || res.Err != nil {
		t.Fatalf("cell 2: err=%v cellErr=%+v", err, res.Err)
	}
	if res.Result != "42" {
		t.Fatalf("expected 42, got %q (out=%v)", res.Result, res.Output)
	}
	res, _ = kh.Execute(context.Background(), `greet.Hello("skills")`)
	if res.Result != "hello, skills" {
		t.Fatalf("greet: %+v", res)
	}
}

// fakeChild records admitted tasks and replies to the parent inbox.
type fakeChild struct {
	parent *Manager
}

func (f fakeChild) RunChild(ctx context.Context, task, name string, inbox <-chan string) {
	// A real child would run its own agent loop; fake replies directly via
	// the same agent_message route cells use.
	_, _ = f.parent.HandleHostRequest(ctx, "agent_message", mustJSON(map[string]any{
		"receiver_role": "parent", "receiver_name": "", "message": "child " + name + " finished: " + task,
	}))
}

// rlm.Spawn from inside a cell services through the Manager; the handle
// returns at admission and the reply lands in the parent inbox.
func TestSpawnFromCell(t *testing.T) {
	mgr := NewManager("root", "", nil)
	kh, err := NewKernelHost(context.Background(), "", mgr)
	if err != nil {
		t.Fatal(err)
	}
	defer kh.Shutdown()
	mgr.runner = fakeChild{parent: mgr}

	res, err := kh.Execute(context.Background(), `h := rlm.Spawn("do work", "w0")
h["name"]`)
	if err != nil || res.Err != nil {
		t.Fatalf("spawn cell: err=%v cellErr=%+v out=%v", err, res.Err, res.Output)
	}
	if res.Result != "w0" {
		t.Fatalf("handle name: %+v", res)
	}

	select {
	case m := <-mgr.Inbox():
		if m != "child w0 finished: do work" {
			t.Fatalf("inbox: %q", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("reply never reached the parent inbox")
	}

	// list_agents sees the roster shape (child may have finished already).
	res, _ = kh.Execute(context.Background(), "len(rlm.ListAgents()) >= 1")
	if res.Err != nil || res.Result != "true" {
		t.Fatalf("list_agents: %+v", res)
	}
}

// agent_message from a cell reaches the parent inbox.
func TestSendFromCell(t *testing.T) {
	mgr := NewManager("root", "", nil)
	kh, err := NewKernelHost(context.Background(), "", mgr)
	if err != nil {
		t.Fatal(err)
	}
	defer kh.Shutdown()

	res, err := kh.Execute(context.Background(), `rlm.Send("parent", "", "ping from cell")`)
	if err != nil || res.Err != nil {
		t.Fatalf("send cell: %+v %+v", err, res.Err)
	}
	select {
	case m := <-mgr.Inbox():
		if m != "ping from cell" {
			t.Fatalf("inbox: %q", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("message never arrived")
	}
}

func mustJSON(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}
