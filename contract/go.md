# Go cells in a persistent kernel

Execute Go source in a persistent interpreter kernel (Yaegi). State persists across calls: variables, functions, and types declared in one cell are visible in the next. The trailing expression of a cell is returned as its result. Imports work per cell (`import "strings"`, grouped forms too).

Go is the orchestration language: use Go for loops, conditionals, parsing, state, and concurrency. There is no bash tool and no `await` — concurrency is native.

Concurrency is the heart of the environment:

- Spawn background work with `go func() { ... }()` — goroutines are real and outlive the cell that started them; `print` output stays attributed to the spawning cell.
- Coordinate with channels, `sync.WaitGroup`, and `context.Context` — the idiomatic tools, not an event loop.
- Fan out independent work (HTTP calls, host requests, subprocess-style tasks) as goroutines and gather with a WaitGroup or a result channel; never poll sequentially.
- `import "rlm/rlm"` binds runtime helpers: `rlm.Sleep(ms)` is the interruptible sleep (prefer it over `time.Sleep` so interrupts cancel promptly) and `rlm.HostCall(kind, payload)` reaches the host bridge for subagents, messaging, and future tools.
- Interrupting a cell cancels its context; helpers return promptly. A pure-compute interpreted loop cannot be interrupted mid-eval — keep cells responsive by awaiting nothing and blocking on nothing long.

Practical rules:

- Probe unfamiliar state with a small cell before writing a large one; iterate one step at a time.
- Do not assume the kernel is the native runtime of the external thing being investigated. A repository, package, service, dataset, or API may have its own environment and normal interface — evaluate external systems through their own interface (`go run`, `go test` in the project's module), then use cells to coordinate the process and analyze what comes back.
- Compaction preserves declared names, not live values; keep large data on disk and reload it when needed.

Subagents: `rlm.HostCall("spawn_task", map[string]any{"task": "...", "name": "..."})` admits a child and returns its handle immediately after admission — it never waits for the child's answer. Fan out children concurrently and gather replies as they arrive.
