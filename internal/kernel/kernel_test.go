package kernel_test

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"go-prime-agent/internal/proto"
	"go-prime-agent/internal/testutil"
)

func ready(t *testing.T, h *testutil.Harness) {
	t.Helper()
	if e := h.Await(2 * time.Second); e.Event != "ready" || e.Protocol != 3 {
		t.Fatalf("expected ready protocol 3, got %+v", e)
	}
}

// v3 happy path: handshake, persistent scope, result events.
func TestReadyHandshakeAndExecute(t *testing.T) {
	h := testutil.NewHarness(t, nil)
	defer h.Close()
	ready(t, h)

	h.Send(`{"type":"execute","id":"c1","code":"set:answer 42"}`)
	if e, _ := h.WantDone("c1", 2*time.Second); e.Status != "ok" {
		t.Fatalf("c1 done status = %s", e.Status)
	}
	h.Send(`{"type":"execute","id":"c2","code":"get:answer"}`)
	e, mid := h.WantDone("c2", 2*time.Second)
	if e.Status != "ok" {
		t.Fatalf("c2 done status = %s", e.Status)
	}
	if result := findResult(mid); result == nil || *result != "42" {
		t.Fatalf("expected result 42, got %v", result)
	}
}

// One cell fans out 3 concurrent host_requests; replies arrive out of order
// and must correlate by id.
func TestConcurrentHostRequests(t *testing.T) {
	h := testutil.NewHarness(t, nil)
	defer h.Close()
	ready(t, h)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		var ids []string
		var payloads []map[string]any
		deadline := time.After(3 * time.Second)
		for len(ids) < 3 {
			select {
			case e := <-h.HostReqs:
				if e.ID == nil {
					continue
				}
				var d map[string]any
				json.Unmarshal(e.Data, &d)
				ids = append(ids, *e.ID)
				payloads = append(payloads, d)
			case <-deadline:
				return
			}
		}
		for i := len(ids) - 1; i >= 0; i-- { // reply slowest-first
			time.Sleep(50 * time.Millisecond)
			res, _ := json.Marshal(payloads[i]["payload"])
			reply, _ := json.Marshal(map[string]any{"status": "ok", "result": json.RawMessage(res)})
			h.Send(`{"type":"host_reply","id":"` + ids[i] + `","data":` + string(reply) + `}`)
		}
	}()

	h.Send(`{"type":"execute","id":"fan","code":"hostcall:3 job"}`)
	e, mid := h.WantDone("fan", 4*time.Second)
	if e.Status != "ok" {
		t.Fatalf("fan done status = %s, events %+v", e.Status, mid)
	}
	if result := findResult(mid); result == nil || *result != "3/3" {
		t.Fatalf("expected result 3/3, got %v", result)
	}
	wg.Wait()
}

// Interrupt cancels the running cell; kernel keeps serving; parked
// interrupts deliver at activation.
func TestInterruptCancelsRunningCell(t *testing.T) {
	h := testutil.NewHarness(t, nil)
	defer h.Close()
	ready(t, h)

	h.Send(`{"type":"execute","id":"slow","code":"sleep:10000"}`)
	time.Sleep(100 * time.Millisecond)
	h.Send(`{"type":"interrupt","id":"slow"}`)

	deadline := time.After(2 * time.Second)
	gotErr, gotDone := false, false
	for !(gotErr && gotDone) {
		select {
		case e := <-h.Events:
			switch {
			case e.Event == "error" && e.EName == "KeyboardInterrupt":
				gotErr = true
			case e.Event == "done" && e.ID != nil && *e.ID == "slow" && e.Status == "error":
				gotDone = true
			}
		case <-deadline:
			t.Fatalf("interrupt not delivered: err=%v done=%v", gotErr, gotDone)
		}
	}

	h.Send(`{"type":"execute","id":"after","code":"sleep:10"}`)
	if e, _ := h.WantDone("after", 2*time.Second); e.Status != "ok" {
		t.Fatalf("kernel did not keep serving: %+v", e)
	}

	h.Send(`{"type":"interrupt","id":"parked"}`)
	h.Send(`{"type":"execute","id":"parked","code":"sleep:10000"}`)
	if e, _ := h.WantDone("parked", 2*time.Second); e.Status != "error" {
		t.Fatalf("parked interrupt not delivered on activation: %+v", e)
	}
}

// A goroutine spawned by a cell outlives the cell; stdout stays attributed.
func TestBackgroundGoroutineOutlivesCell(t *testing.T) {
	h := testutil.NewHarness(t, nil)
	defer h.Close()
	ready(t, h)

	h.Send(`{"type":"execute","id":"bgcell","code":"bg:250 hello from the future"}`)
	if e, _ := h.WantDone("bgcell", 2*time.Second); e.Status != "ok" {
		t.Fatalf("bgcell done status = %s", e.Status)
	}
	deadline := time.After(3 * time.Second)
	for {
		select {
		case e := <-h.Events:
			if e.Event == "stdout" && e.ID != nil && *e.ID == "bgcell" &&
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
	defer h.Close()
	ready(t, h)

	h.Send(`{"type":"execute","id":"s1","code":"set:x 1"}`)
	h.WantDone("s1", 2*time.Second)
	h.Send(`{"type":"execute","id":"s2","code":"set:y str"}`)
	h.WantDone("s2", 2*time.Second)

	dir := t.TempDir()
	h.Send(`{"type":"snapshot","id":"snap","path":"` + dir + `/payload.json","manifest_path":"` + dir + `/manifest.json"}`)
	e, _ := h.WantDone("snap", 2*time.Second)
	if e.Status != "ok" {
		t.Fatalf("snapshot done: %+v", e)
	}
	if !contains(e.Saved, "x") || !contains(e.Saved, "y") || e.Bytes <= 0 {
		t.Fatalf("snapshot extras: %+v", e)
	}

	h.Send(`{"type":"restore","id":"rest","path":"` + dir + `/payload.json"}`)
	e, _ = h.WantDone("rest", 2*time.Second)
	if e.Status != "ok" || !contains(e.Restored, "x") || !contains(e.Restored, "y") {
		t.Fatalf("restore done: %+v", e)
	}

	h.Send(`{"type":"restore","id":"miss","path":"` + dir + `/nope.json"}`)
	e, _ = h.WantDone("miss", 2*time.Second)
	if e.Status != "ok" || e.Reason != "snapshot not found" {
		t.Fatalf("missing snapshot done: %+v", e)
	}
}

func findResult(mid []proto.Event) *string {
	for _, m := range mid {
		if m.Event == "result" {
			s := m.Text
			return &s
		}
	}
	return nil
}

func contains(xs []string, v string) bool {
	for _, x := range xs {
		if x == v {
			return true
		}
	}
	return false
}
