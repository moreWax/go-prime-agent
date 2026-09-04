package goeval_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/xor/go-prime-agent/internal/goeval"
	"github.com/xor/go-prime-agent/internal/kernel"
	"github.com/xor/go-prime-agent/internal/proto"
	"github.com/xor/go-prime-agent/internal/testutil"
)

func goHarness(t *testing.T) *testutil.Harness {
	t.Helper()
	h := testutil.NewHarness(t, func(c *kernel.Config) { c.Eval = goeval.New() })
	if e := h.Await(2 * time.Second); e.Event != proto.KindReady {
		t.Fatalf("no ready: %+v", e)
	}
	return h
}

func exec(t *testing.T, h *testutil.Harness, id, code string) (proto.Event, []proto.Event) {
	t.Helper()
	h.SendReq(map[string]any{"type": "execute", "id": id, "code": code})
	return h.WantDone(id, 8*time.Second)
}

// Persistent interpreter state across cells + result events.
func TestGoPersistence(t *testing.T) {
	h := goHarness(t)
	if e, _ := exec(t, h, "a", "x := 40"); e.Status != proto.StatusOK {
		t.Fatalf("a: %+v", e)
	}
	e, mid := exec(t, h, "b", "x + 2")
	if e.Status != proto.StatusOK {
		t.Fatalf("b: %+v", e)
	}
	if r := testutil.FindResult(mid); r == nil || *r != "42" {
		t.Fatalf("expected 42, got %v", r)
	}
}

// Standard library is importable inside cells.
func TestGoStdlib(t *testing.T) {
	h := goHarness(t)
	e, mid := exec(t, h, "c", `import "strings"
strings.ToUpper("abc")`)
	if e.Status != proto.StatusOK {
		t.Fatalf("c: %+v", e)
	}
	if r := testutil.FindResult(mid); r == nil || *r != "ABC" {
		t.Fatalf("expected ABC, got %v", r)
	}
}

// Real goroutines inside a cell fan out concurrent host_calls; replies
// correlate by id.
func TestGoConcurrentHostCalls(t *testing.T) {
	h := goHarness(t)
	h.HostAutoReplier(func(kind string, payload any) (json.RawMessage, error) {
		return json.Marshal(map[string]any{"echo": payload})
	})

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
	e, mid := exec(t, h, "fan", code)
	if e.Status != proto.StatusOK {
		t.Fatalf("fan: %+v %+v", e, mid)
	}
}

// sleep() is interruptible: interrupt => KeyboardInterrupt, kernel survives.
func TestGoInterrupt(t *testing.T) {
	h := goHarness(t)
	h.SendReq(map[string]any{"type": "execute", "id": "z", "code": `import "rlm/rlm"
rlm.Sleep(10000)`})
	time.Sleep(150 * time.Millisecond)
	h.Send(`{"type":"interrupt","id":"z"}`)

	deadline := time.After(3 * time.Second)
	gotErr, gotDone := false, false
	for !(gotErr && gotDone) {
		select {
		case e := <-h.Events:
			switch {
			case e.Event == proto.KindError && e.EName == proto.EnameKeyboard:
				gotErr = true
			case e.Event == proto.KindDone && e.ID != nil && *e.ID == "z" && e.Status == proto.StatusError:
				gotDone = true
			}
		case <-deadline:
			t.Fatalf("interrupt not delivered: err=%v done=%v", gotErr, gotDone)
		}
	}

	if e, _ := exec(t, h, "ok", "1+1"); e.Status != proto.StatusOK {
		t.Fatalf("kernel did not keep serving: %+v", e)
	}
}

// Goroutines spawned in a cell outlive it; print stays attributed.
func TestGoBackgroundGoroutine(t *testing.T) {
	h := goHarness(t)
	code := `
import "rlm/rlm"
go func() {
	rlm.Sleep(250)
	print("hello from the go future")
}()
"spawned"
`
	if e, _ := exec(t, h, "bg", code); e.Status != proto.StatusOK {
		t.Fatalf("bg: %+v", e)
	}
	deadline := time.After(4 * time.Second)
	for {
		select {
		case e := <-h.Events:
			if e.Event == proto.KindStdout && e.ID != nil && *e.ID == "bg" &&
				strings.Contains(e.Text, "hello from the go future") {
				return
			}
		case <-deadline:
			t.Fatal("background goroutine output never arrived")
		}
	}
}
