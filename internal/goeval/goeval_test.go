package goeval_test

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"go-prime-agent/internal/goeval"
	"go-prime-agent/internal/kernel"
	"go-prime-agent/internal/testutil"
)

func goHarness(t *testing.T) *testutil.Harness {
	h := testutil.NewHarness(t, func(c *kernel.Config) { c.Eval = goeval.New() })
	if e := h.Await(2 * time.Second); e.Event != "ready" {
		t.Fatalf("no ready: %+v", e)
	}
	return h
}

// Persistent interpreter state across cells + result events.
func TestGoPersistence(t *testing.T) {
	h := goHarness(t)
	defer h.Close()

	b, _ := json.Marshal(map[string]any{"type": "execute", "id": "a", "code": "x := 40"})
	h.Send(string(b))
	if e, _ := h.WantDone("a", 5*time.Second); e.Status != "ok" {
		t.Fatalf("a: %+v", e)
	}
	b, _ = json.Marshal(map[string]any{"type": "execute", "id": "b", "code": "x + 2"})
	h.Send(string(b))
	e, mid := h.WantDone("b", 5*time.Second)
	if e.Status != "ok" {
		t.Fatalf("b: %+v %v", e, mid)
	}
	found := ""
	for _, m := range mid {
		if m.Event == "result" {
			found = m.Text
		}
	}
	if found != "42" {
		t.Fatalf("expected 42, got %q", found)
	}
}

// Standard library is importable inside cells.
func TestGoStdlib(t *testing.T) {
	h := goHarness(t)
	defer h.Close()

	b, _ := json.Marshal(map[string]any{"type": "execute", "id": "c", "code": "import \"strings\"\nstrings.ToUpper(\"abc\")"})
	h.Send(string(b))
	e, mid := h.WantDone("c", 5*time.Second)
	if e.Status != "ok" {
		t.Fatalf("c: %+v", e)
	}
	found := ""
	for _, m := range mid {
		if m.Event == "result" {
			found = m.Text
		}
	}
	if found != "ABC" {
		t.Fatalf("expected ABC, got %q (mid %+v)", found, mid)
	}
}

// Real goroutines inside a cell fan out concurrent host_calls; replies
// correlate by id.
func TestGoConcurrentHostCalls(t *testing.T) {
	h := goHarness(t)
	defer h.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		seen := 0
		deadline := time.After(5 * time.Second)
		for seen < 3 {
			select {
			case e := <-h.HostReqs:
				if e.ID == nil {
					continue
				}
				seen++
				res, _ := json.Marshal(map[string]any{"ok": seen})
				reply, _ := json.Marshal(map[string]any{"status": "ok", "result": json.RawMessage(res)})
				h.Send(`{"type":"host_reply","id":"` + *e.ID + `","data":` + string(reply) + `}`)
			case <-deadline:
				return
			}
		}
	}()

	code := `
import "sync"
import "rlm/rlm"
var wg sync.WaitGroup
for i := 0; i < 3; i++ {
	wg.Add(1)
	go func(n int) {
		defer wg.Done()
		rlm.HostCall("job", map[string]interface{}{"i": n})
	}(i)
}
wg.Wait()
"3 calls"
`
	b, _ := json.Marshal(map[string]any{"type": "execute", "id": "fan", "code": code})
	h.Send(string(b))
	e, mid := h.WantDone("fan", 10*time.Second)
	if e.Status != "ok" {
		t.Fatalf("fan: %+v %+v", e, mid)
	}
	wg.Wait()
}

// sleep() is interruptible: interrupt => KeyboardInterrupt, kernel survives.
func TestGoInterrupt(t *testing.T) {
	h := goHarness(t)
	defer h.Close()

	b, _ := json.Marshal(map[string]any{"type": "execute", "id": "z", "code": "import \"rlm/rlm\"\nrlm.Sleep(10000)"})
	h.Send(string(b))
	time.Sleep(150 * time.Millisecond)
	h.Send(`{"type":"interrupt","id":"z"}`)

	deadline := time.After(3 * time.Second)
	gotErr, gotDone := false, false
	for !(gotErr && gotDone) {
		select {
		case e := <-h.Events:
			switch {
			case e.Event == "error" && e.EName == "KeyboardInterrupt":
				gotErr = true
			case e.Event == "done" && e.ID != nil && *e.ID == "z" && e.Status == "error":
				gotDone = true
			}
		case <-deadline:
			t.Fatalf("interrupt not delivered: err=%v done=%v", gotErr, gotDone)
		}
	}

	b2, _ := json.Marshal(map[string]any{"type": "execute", "id": "ok", "code": "1+1"})
	h.Send(string(b2))
	if e, _ := h.WantDone("ok", 5*time.Second); e.Status != "ok" {
		t.Fatalf("kernel did not keep serving: %+v", e)
	}
}

// Goroutines spawned in a cell outlive it; print stays attributed.
func TestGoBackgroundGoroutine(t *testing.T) {
	h := goHarness(t)
	defer h.Close()

	code := `
import "rlm/rlm"
go func() {
	rlm.Sleep(250)
	print("hello from the go future")
}()
"spawned"
`
	b, _ := json.Marshal(map[string]any{"type": "execute", "id": "bg", "code": code})
	h.Send(string(b))
	if e, _ := h.WantDone("bg", 5*time.Second); e.Status != "ok" {
		t.Fatalf("bg: %+v", e)
	}
	deadline := time.After(4 * time.Second)
	for {
		select {
		case e := <-h.Events:
			if e.Event == "stdout" && e.ID != nil && *e.ID == "bg" &&
				strings.Contains(e.Text, "hello from the go future") {
				return
			}
		case <-deadline:
			t.Fatal("background goroutine output never arrived")
		}
	}
}
