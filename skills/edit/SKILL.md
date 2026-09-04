---
name: edit
description: Targeted exact-string file edits with per-path serialization — the Go edit skill.
---

# edit

`import "rlm/edit"` then:

```go
n, err := edit.Run(path, oldStr, newStr)
```

Run replaces the first exact occurrence of oldStr with newStr in path,
writing atomically (temp file + rename). Edits to the same path are
serialized behind a per-path lock, so concurrent cells cannot interleave
read-modify-write cycles on one file. oldStr must appear exactly once.
