# go-prime-agent — agent instructions

This repo is a Go reimplementation of the prime-agent RLM kernel. The
ipython tool here executes **Go, not Python**.

## Cell language

- Cells are Go source, evaluated in a persistent Yaegi interpreter with the
  standard library importable. State persists across cells (`x := 6` then
  `x * 7` works).
- Bound helpers come from `import "rlm/rlm"`:
  - `rlm.Sleep(ms)` — interruptible sleep (prefer over `time.Sleep`)
  - `rlm.HostCall(kind, payload)` — host bridge (subagents, messaging)
- `print` writes attributed stdout.
- Goroutines spawned in a cell are real goroutines and outlive the cell.

## Working rules

- Run `go vet ./...` and `go test ./... -race -count=1` after changes.
- Always wrap test runs in `timeout` (e.g. `timeout 200 go test ...`).
- Keep the protocol contract in `internal/proto` exactly v3-shaped.
- Architecture and design decisions live in ARCHITECTURE.md — read it
  before changing concurrency or lifecycle behavior.
