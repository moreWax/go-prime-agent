# go-prime-agent

A Go-native RLM kernel for [Prime Agent](https://github.com/PrimeIntellect-ai/prime-agent):
the agent's execution engine speaks Go instead of Python. Cells are Go source
running in a persistent [Yaegi](https://github.com/traefik/yaegi) interpreter
with the standard library importable, real goroutines, and context-aware
interrupts — a drop-in replacement for `python -m rlm.repl` on the RLM
protocol v3 wire.

## Why

Prime Agent's harness is language-agnostic on the wire (newline-delimited
JSON over stdio, per-request ids, per-request `done`), but its runtime is
Python. This project proves the other half: one static Go binary that
implements the same contract with Go-native concurrency at every layer —
interrupts are context cancellations, host calls are correlated channels,
cells spawn goroutines that outlive them. No bash tool by design: Go is the
tool surface.

## Install

Requires Go 1.24+.

```sh
make build          # -> bin/gorlm (static binary)
make test           # vet + full suite with -race
```

## Use with Prime Agent

### Extension (recommended)

The repo ships a Prime Agent extension that registers a `go` tool owning a
`gorlm` process and blocks the Python `ipython` tool:

```sh
cd /path/to/go-prime-agent
prime-agent          # extension auto-loads from .prime/agent/extensions/
```

The kernel binary resolves from `$GORLM_BIN` or `bin/gorlm` in the repo.
Inside cells, `import "rlm/rlm"` binds runtime helpers: `rlm.Sleep(ms)`
(interruptible) and `rlm.HostCall(kind, payload)` (host bridge).

### Kernel swap (needs the upstream env contract)

With `PRIME_AGENT_KERNEL_CMD` / `PRIME_AGENT_KERNEL_BOOTSTRAP` merged
upstream, the stock `ipython` tool can drive gorlm directly:

```sh
PRIME_AGENT_KERNEL_CMD='["/path/to/bin/gorlm"]' \
PRIME_AGENT_KERNEL_BOOTSTRAP=none prime-agent
```

## Go harness (headless)

`goprime` is the non-UI Prime Agent host in Go, built on
[go-pi](https://github.com/earendil-works/go-pi). It runs gorlm in-process
(no subprocess), owns the model loop, exposes the `go` tool, loads Go
skills, and services the subagent/message host bridge.

```sh
make build
PI_CODING_AGENT_DIR=~/.prime/agent bin/goprime \
  -p "Compute F(15) using code execution" \
  --provider glm --model glm-5.2 --cwd "$PWD"
```

The TUI stays in the TypeScript fork for now. The headless host covers the
non-UI execution path: provider registry (go-pi), agent loop (go-pi),
Prime prompt/skills (this repo), persistent Go kernel (this repo),
subagent admission/message bridge, and context cancellation.

## Cell language

- Go source; state persists across cells (`x := 40` then `x + 2` → `42`)
- imports work (`import "strings"` / grouped), declarations and statement
  runs are chunked automatically (REPL semantics)
- `go func() { ... }()` runs real goroutines that outlive the cell; `print`
  output stays attributed to the spawning cell
- `rlm.Sleep` is the interruptible sleep (prefer over `time.Sleep`)
- `rlm.HostCall(kind, payload)` reaches the host bridge; the extension
  currently replies with errors for unbridged kinds

See [ARCHITECTURE.md](ARCHITECTURE.md) for the concurrency model, the
protocol implementation map, and known limitations (interpreter-state
snapshots, mid-eval interrupts).

## Development

```sh
make fmt vet test build
```

CI runs the same on every push. The protocol contract lives in
`internal/proto`; the kernel splits into lifecycle / dispatch / executor /
cell / snapshot files; evaluators plug in behind `kernel.Evaluator`
(op-DSL stub for conformance tests, Yaegi for real cells).

## Status

v0.0.1 — kernel + extension verified end-to-end against the stock harness
(glm-5.2 round-trips, persistent state, ipython interception). See
ARCHITECTURE.md's checklist for the roadmap (MCP client, subagent bridge
ergonomics, interpreter-state snapshots, protocol v4).

## License

MIT
