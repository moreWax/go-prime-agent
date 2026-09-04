package kernel_test

import (
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/moreWax/go-prime-agent/internal/eval"
	"github.com/moreWax/go-prime-agent/internal/kernel"
	"github.com/moreWax/go-prime-agent/internal/proto"
	"github.com/moreWax/go-prime-agent/internal/testutil"
)

func ready(t *testing.T, h *testutil.Harness) {
	t.Helper()
	if e := h.Await(2 * time.Second); e.Event != proto.KindReady || e.Protocol != 3 {
		t.Fatalf("expected ready protocol 3, got %+v", e)
	}
}

// v3 happy path: handshake, persistent scope, result events.
func TestReadyHandshakeAndExecute(t *testing.T) {
	h := testutil.NewHarness(t, nil)
	ready(t, h)

	h.SendReq(map[string]any{"type": "execute", "id": "c1", "code": "set:answer 42"})
	if e, _ := h.WantDone("c1", 2*time.Second); e.Status != proto.StatusOK {
		t.Fatalf("c1 done status = %s", e.Status)
	}
	h.SendReq(map[string]any{"type": "execute", "id": "c2", "code": "get:answer"})
	e, mid := h.WantDone("c2", 2*time.Second)
	if e.Status != proto.StatusOK {
		t.Fatalf("c2 done status = %s", e.Status)
	}
	if r := testutil.FindResult(mid); r == nil || *r != "42" {
		t.Fatalf("expected result 42, got %v", r)
	}
}

// One cell fans out 3 concurrent host_requests; replies arrive out of order
// and must correlate by id.
func TestConcurrentHostRequests(t *testing.T) {
	h := testutil.NewHarness(t, nil)
	ready(t, h)

	var delay time.Duration
	h.HostAutoReplier(func(kind string, payload any) (json.RawMessage, error) {
		delay += 50 * time.Millisecond
		time.Sleep(delay)
		return json.Marshal(payload)
	})

	h.SendReq(map[string]any{"type": "execute", "id": "fan", "code": "hostcall:3 job"})
	e, mid := h.WantDone("fan", 4*time.Second)
	if e.Status != proto.StatusOK {
		t.Fatalf("fan done status = %s, events %+v", e.Status, mid)
	}
	if r := testutil.FindResult(mid); r == nil || *r != "3/3" {
		t.Fatalf("expected result 3/3, got %v", r)
	}
}

// Interrupt cancels the running cell; kernel keeps serving; parked
// interrupts deliver at activation.
func TestInterruptCancelsRunningCell(t *testing.T) {
	h := testutil.NewHarness(t, nil)
	ready(t, h)

	h.SendReq(map[string]any{"type": "execute", "id": "slow", "code": "sleep:10000"})
	time.Sleep(100 * time.Millisecond)
	h.Send(`{"type":"interrupt","id":"slow"}`)

	deadline := time.After(2 * time.Second)
	gotErr, gotDone := false, false
	for !(gotErr && gotDone) {
		select {
		case e := <-h.Events:
			switch {
			case e.Event == proto.KindError && e.EName == proto.EnameKeyboard:
				gotErr = true
			case e.Event == proto.KindDone && e.ID != nil && *e.ID == "slow" && e.Status == proto.StatusError:
				gotDone = true
			}
		case <-deadline:
			t.Fatalf("interrupt not delivered: err=%v done=%v", gotErr, gotDone)
		}
	}

	h.SendReq(map[string]any{"type": "execute", "id": "after", "code": "sleep:10"})
	if e, _ := h.WantDone("after", 2*time.Second); e.Status != proto.StatusOK {
		t.Fatalf("kernel did not keep serving: %+v", e)
	}

	h.Send(`{"type":"interrupt","id":"parked"}`)
	h.SendReq(map[string]any{"type": "execute", "id": "parked", "code": "sleep:10000"})
	if e, _ := h.WantDone("parked", 2*time.Second); e.Status != proto.StatusError {
		t.Fatalf("parked interrupt not delivered on activation: %+v", e)
	}
}

// A goroutine spawned by a cell outlives the cell; stdout stays attributed.
func TestBackgroundGoroutineOutlivesCell(t *testing.T) {
	h := testutil.NewHarness(t, nil)
	ready(t, h)

	h.SendReq(map[string]any{"type": "execute", "id": "bgcell", "code": "bg:250 hello from the future"})
	if e, _ := h.WantDone("bgcell", 2*time.Second); e.Status != proto.StatusOK {
		t.Fatalf("bgcell done status = %s", e.Status)
	}
	deadline := time.After(3 * time.Second)
	for {
		select {
		case e := <-h.Events:
			if e.Event == proto.KindStdout && e.ID != nil && *e.ID == "bgcell" &&
				strings.Contains(e.Text, "hello from the future") {
				return
			}
		case <-deadline:
			t.Fatal("background goroutine output never arrived")
		}
	}
}

// Snapshot serializes scope values with manifest; restore revives them;
// missing files report ok with a reason.
func TestSnapshotRestore(t *testing.T) {
	h := testutil.NewHarness(t, nil)
	ready(t, h)

	h.SendReq(map[string]any{"type": "execute", "id": "s1", "code": "set:x 1"})
	h.WantDone("s1", 2*time.Second)
	h.SendReq(map[string]any{"type": "execute", "id": "s2", "code": "set:y str"})
	h.WantDone("s2", 2*time.Second)

	dir := t.TempDir()
	h.SendReq(map[string]any{"type": "snapshot", "id": "snap", "path": dir + "/payload.json", "manifest_path": dir + "/manifest.json"})
	e, _ := h.WantDone("snap", 2*time.Second)
	if e.Status != proto.StatusOK || !contains(e.Saved, "x") || !contains(e.Saved, "y") || e.Bytes <= 0 {
		t.Fatalf("snapshot done: %+v", e)
	}

	h.SendReq(map[string]any{"type": "restore", "id": "rest", "path": dir + "/payload.json"})
	if e, _ := h.WantDone("rest", 2*time.Second); e.Status != proto.StatusOK || !contains(e.Restored, "x") || !contains(e.Restored, "y") {
		t.Fatalf("restore done: %+v", e)
	}

	h.SendReq(map[string]any{"type": "restore", "id": "miss", "path": dir + "/nope.json"})
	if e, _ := h.WantDone("miss", 2*time.Second); e.Status != proto.StatusOK || e.Reason != "snapshot not found" {
		t.Fatalf("missing snapshot done: %+v", e)
	}
}

// The Python-bootstrap ack path provisions the kernel without evaluating
// the bootstrap; normal cells still evaluate.
func TestAckPythonBootstrap(t *testing.T) {
	ev := &countingEvaluator{}
	h := testutil.NewHarness(t, func(c *kernel.Config) {
		c.Eval = ev
		c.AckPythonBootstrap = true
	})
	ready(t, h)

	boot := "import asyncio\nimport os as _prime_agent_os\n_prime_agent_os.environ[\"NO_COLOR\"] = \"1\""
	h.SendReq(map[string]any{"type": "execute", "id": "boot", "code": boot})
	if e, _ := h.WantDone("boot", 2*time.Second); e.Status != proto.StatusOK {
		t.Fatalf("bootstrap ack: %+v", e)
	}
	if n := ev.calls(); n != 0 {
		t.Fatalf("evaluator ran %d times for acked bootstrap", n)
	}

	h.SendReq(map[string]any{"type": "execute", "id": "c", "code": "set:k v"})
	if e, _ := h.WantDone("c", 2*time.Second); e.Status != proto.StatusOK {
		t.Fatalf("post-bootstrap cell: %+v", e)
	}
	if n := ev.calls(); n != 1 {
		t.Fatalf("expected 1 evaluation after bootstrap, got %d", n)
	}
}

// countingEvaluator delegates to the op DSL and counts calls.
type countingEvaluator struct{ n atomic.Int32 }

func (c *countingEvaluator) Run(env eval.Env) (eval.Result, error) {
	c.n.Add(1)
	return eval.Run(env)
}

func (c *countingEvaluator) calls() int { return int(c.n.Load()) }

func contains(xs []string, v string) bool {
	for _, x := range xs {
		if x == v {
			return true
		}
	}
	return false
}
