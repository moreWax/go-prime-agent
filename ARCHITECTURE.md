# go-prime-agent — concurrency-first RLM runtime

A Go reimplementation of the prime-agent RLM kernel (`python -m rlm.repl`),
speaking wire protocol v3 (NDJSON on stdio) so the stock Node harness can
spawn it unchanged. MIT-licensed upstream; see repl.md in the installed
package for the protocol contract.

## Concurrency ladder

Every layer is goroutine + `context.Context` end to end. Nothing waits on a
mutex it could wait on a channel for.

- **L0 wire.** v3 keeps `execute` requests serial (host policy), but
  `interrupt` and `host_reply` frames route on the reader goroutine the moment
  they arrive — never through the request queue. All frames are id'd with
  per-id `done`, so a forked host can switch to pipelined concurrent executes
  without a format change (`PolicyPipelined`).
- **L1 cells.** Each request gets its own goroutine-scoped cancelable context.
  Interrupt = context cancel. Interrupts that arrive before their target
  starts are parked and delivered at activation (spec: Interrupt).
- **L2 state.** The scope is an RWMutex-guarded namespace. Snapshots (phase 2)
  take the read lock and serialize off the critical path. Concurrent cells
  (v4) mutate under the write lock.
- **L3 host bridge.** `hostbridge.Bridge` supports any number of concurrent
  in-flight `host_request`s; replies correlate by id through buffered
  channels; abandoned calls are dropped when the calling cell is cancelled.
- **L4 subagents.** `spawn_task` / `agent_message` ride the same bridge.
  Child handles will expose reply channels; gathering N children is a select,
  not event-loop bookkeeping.
- **L5 subprocesses.** The bash tool wraps per-call process groups; cancel
  kills the group synchronously. Heavy or CPU-parallel work belongs in child
  processes — the interpreter Eval lock is the only serializer in the process.

## What replaces each Python affordance

| Python runtime | Go runtime |
|---|---|
| persistent `__main__` namespace | scope registry (RWMutex) + persistent interpreter state |
| top-level `await` | goroutines/channels — no event loop to suspend |
| asyncio tasks outliving cells | goroutines outliving cells, root-context governed |
| SIGINT → KeyboardInterrupt | context cancel → cooperative checks |
| dill snapshots | JSON/gob value snapshots (data only; goroutines/functions excluded) |
| `sys.stdout` interception | attributed writer port passed to cells |
| pre-imported Python skills | Go CLI skills (static binaries) + markdown skills |

## Design decision: no bash tool

The agent thinks and executes in Go. Cells are Go source run in a persistent
Yaegi interpreter with the standard library importable. There is no shell
string tool; when a real subprocess is genuinely needed, cells call a typed
Go API with process-group cancellation (planned `rlm.Run(...)`), never bash.

Cell language specifics:
- Cells are chunked (declarations vs statement runs) to reproduce yaegi REPL
  semantics; globals persist across chunks and cells.
- `import "rlm/rlm"` binds runtime helpers: `rlm.Sleep` (interruptible),
  `rlm.HostCall` (host bridge). `print` writes attributed stdout.
- Goroutines spawned in a cell are real goroutines and outlive the cell.
- Interrupt cancels the cell context; `rlm.Sleep` and host calls abort. A
  pure-compute interpreted loop cannot be interrupted mid-eval (yaegi
  limitation) — mitigation: kernel restart + restore.
- Declared names mirror into the kernel scope (list_names, snapshot); values
  are markers until interpreter-state serialization exists.
- Shutdown drains queued requests, then exits; background goroutines die with
  the process (same as the Python runtime).

## Status

- [x] v3 frames, locked event writer, ready handshake
- [x] Reader/dispatcher with immediate interrupt + host_reply routing
- [x] Per-request contexts, parked interrupts, serial v3 executor
- [x] Concurrent host bridge with id correlation
- [x] Kernel test suite over pipes (handshake, fan-out, interrupt, background)
- [x] Snapshot/restore (JSON values, atomic writes, manifest, prune rules)
- [x] Graceful shutdown drain
- [x] Yaegi Go evaluator: persistence, stdlib, goroutine fan-out via host
      bridge, interruptible sleep, background goroutines, name mirroring
- [x] `cmd/gorlm` binary: fd redirection pumps, `-eval go|ops`
- [x] Typed subagent client (`internal/agents`: Spawn/Send/ListAgents)
- [ ] MCP client; subagent replies as channels (needs host fork / v4 `deliver`)
- [ ] Interpreter-state snapshot (currently name markers only)
- [x] Stock-host integration verified: `PRIME_AGENT_KERNEL_PYTHON` points at
      a shim that execs gorlm (host `-c` probes pass), `GORLM_ACK_PYTHON_BOOTSTRAP=1`
      acks the Python bootstrap cell, and a real glm-5.2 round-trip through the
      unpatched harness executed Go cells with persistent state.
- [ ] Fork the host: Go bootstrap cell, Go-oriented system prompt, gorlm as
      the default kernel (drop shim + ack)
