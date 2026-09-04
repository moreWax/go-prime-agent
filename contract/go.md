# Go cells in a persistent kernel

Execute Go source in a persistent interpreter kernel (Yaegi). State persists across calls: variables, functions, and types declared in one cell are visible in the next. The trailing expression of a cell is returned as its result. Imports work per cell (`import "strings"`, grouped forms too).

Go is the orchestration language: use Go for loops, conditionals, parsing, state, and concurrency. There is no bash tool and no `await` — concurrency is native.

Concurrency is the heart of the environment, with one precise rule:

- Goroutine bodies may contain only native calls (`rlm.*`, `print`) and captured values. That pattern is race-safe, mirrors Python's await points, and is how you fan out: `go func(n int) { rlm.HostCall("job", n) }(i)` for concurrent host requests, `go func() { rlm.Sleep(ms); print("done") }()` for background timers. Gather with `sync.WaitGroup` in the serial cell body.
- Goroutines outlive the cell that started them; `print` output stays attributed to the spawning cell.
- Interpreted computation — arithmetic, channel operations, calls to interpreted functions — runs in the serial cell body only, never concurrently inside goroutine bodies (the interpreter is single-threaded by design, like Python's event loop).
- CPU-parallel work belongs in subprocesses, fanned out natively.
- `import "rlm/rlm"` binds runtime helpers (pre-imported): `rlm.Sleep(ms)` interruptible sleep, `rlm.HostCall(kind, payload)` host bridge, `rlm.Spawn`, `rlm.Send`, `rlm.ListAgents`, `rlm.Skills`.
- Interrupting a cell cancels its context; native helpers return promptly. A pure-compute interpreted loop cannot be interrupted mid-eval — keep cells responsive by blocking on nothing long.

Skills: capability packages loaded into the kernel as Go source — `rlm.Skills()` lists them, and each is pre-imported (call directly, e.g. `edit.Run(path, old, new)`; an explicit `import "rlm/edit"` also works). Each skill's SKILL.md documents its API; read it with `os.ReadFile` from the skills directory.

Practical rules:

- Probe unfamiliar state with a small cell before writing a large one; iterate one step at a time.
- Do not assume the kernel is the native runtime of the external thing being investigated. A repository, package, service, dataset, or API may have its own environment and normal interface — evaluate external systems through their own interface (`go run`, `go test` in the project's module), then use cells to coordinate the process and analyze what comes back.
- Compaction preserves declared names, not live values; keep large data on disk and reload it when needed.

Subagents and harness methods are native Go calls on the `rlm` package: `rlm.Spawn(task, name)` admits a child and returns its handle map immediately after admission — it never waits for the child's answer; `rlm.Send(role, name, message)` delivers an agent message; `rlm.ListAgents()` returns the family roster; `rlm.HostCall(kind, payload)` reaches any other host capability. Fan out children concurrently and gather replies as they arrive.

If a cell fails with a syntax or name error, you are almost certainly writing the wrong language — cells are Go, not Python. Rewrite the cell in Go and continue.
