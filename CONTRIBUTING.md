# Contributing

- Keep the wire contract in `internal/proto` exactly v3-shaped; protocol
  changes belong behind a version handshake.
- Concurrency rules live in ARCHITECTURE.md — read it before touching
  lifecycle or interrupt behavior.
- Every change ships with tests under `-race`; wrap long test runs in
  `timeout` so a hang fails loudly instead of stalling CI.
- `make fmt vet test` must pass before you commit.
