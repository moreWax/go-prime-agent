---
name: greet
description: Smoke-test Go skill — hello world through the rlm/<skill> import path.
---

# greet

Pre-imported: `greet.Hello(name)` returns a greeting string (an explicit
`import "rlm/greet"` also works).

Concurrency rule for skills and cells: goroutine bodies may contain only
native calls (rlm.*, print) and captured values — that pattern is
race-safe and mirrors Python's await points. Interpreted computation
(compute, channel ops, calls to interpreted functions) stays in the serial
cell body; CPU-parallel work belongs in subprocesses.
