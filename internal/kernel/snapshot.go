package kernel

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/moreWax/go-prime-agent/internal/proto"
)

// Default snapshot limits (spec defaults mirrored from the Python runtime).
const (
	defaultMaxVariableBytes = 256 << 10
	defaultMaxTotalBytes    = 8 << 20
)

// snapshotLimits bound a snapshot.
type snapshotLimits struct {
	maxVar   int64
	maxTotal int64
	prune    bool
}

// snapshotPlan is the pure result of planning a snapshot over scope entries.
type snapshotPlan struct {
	saved      map[string]json.RawMessage
	savedNames []string
	skipped    []string
	pruned     []string
	bytes      int64
}

// planSnapshot selects JSON-marshalable entries within the limits. Names are
// processed in sorted order so plans are deterministic.
func planSnapshot(entries map[string]any, lim snapshotLimits) snapshotPlan {
	names := make([]string, 0, len(entries))
	for n := range entries {
		names = append(names, n)
	}
	sort.Strings(names)

	p := snapshotPlan{saved: make(map[string]json.RawMessage)}
	for _, n := range names {
		b, err := json.Marshal(entries[n])
		if err != nil {
			p.skipped = append(p.skipped, n)
			continue
		}
		if int64(len(b)) > lim.maxVar {
			if lim.prune {
				p.pruned = append(p.pruned, n)
			} else {
				p.skipped = append(p.skipped, n)
			}
			continue
		}
		if p.bytes+int64(len(b)) > lim.maxTotal {
			p.skipped = append(p.skipped, n)
			continue
		}
		p.saved[n] = b
		p.savedNames = append(p.savedNames, n)
		p.bytes += int64(len(b))
	}
	return p
}

// snapshot serializes JSON-marshalable scope values, writes payload +
// manifest atomically, and only then applies pruning (spec: a manifest
// failure means nothing is pruned). Interpreter state (Yaegi globals) is
// mirrored as name markers and is not value-covered yet.
func (k *Kernel) snapshot(req proto.Request) {
	lim := snapshotLimits{
		maxVar:   defaultMaxVariableBytes,
		maxTotal: defaultMaxTotalBytes,
	}
	if req.MaxVariableBytes != nil {
		lim.maxVar = *req.MaxVariableBytes
	}
	if req.MaxBytes != nil {
		lim.maxTotal = *req.MaxBytes
	}
	lim.prune = req.PruneOversized != nil && *req.PruneOversized

	plan := planSnapshot(k.scope.Entries(), lim)

	payload, err := json.Marshal(map[string]any{"version": 1, "names": plan.saved})
	if err == nil {
		err = atomicWrite(req.Path, payload)
	}
	if err == nil && req.ManifestPath != "" {
		m, _ := json.Marshal(map[string]any{
			"version": 1, "savedNames": plan.savedNames, "skipped": plan.skipped,
			"pruned": plan.pruned, "bytes": plan.bytes, "runtime": "go",
			"timestamp": time.Now().UTC().Format(time.RFC3339),
		})
		err = atomicWrite(req.ManifestPath, m)
	}
	if err != nil {
		k.done(req.ID, proto.StatusError, &proto.DoneExtras{Reason: err.Error()})
		return
	}
	if lim.prune {
		for _, n := range plan.pruned {
			k.scope.Delete(n)
		}
	}
	k.done(req.ID, proto.StatusOK, &proto.DoneExtras{
		Saved: plan.savedNames, Skipped: plan.skipped, Pruned: plan.pruned, Bytes: plan.bytes,
	})
}

// restore revives snapshot entries into the scope. A missing file is ok with
// reason "snapshot not found"; a corrupt file fails with a reason (spec:
// Snapshot / restore).
func (k *Kernel) restore(req proto.Request) {
	b, err := os.ReadFile(req.Path)
	if errors.Is(err, fs.ErrNotExist) {
		k.done(req.ID, proto.StatusOK, &proto.DoneExtras{
			Restored: []string{}, Failed: []string{}, Reason: "snapshot not found",
		})
		return
	}
	if err != nil {
		k.done(req.ID, proto.StatusError, &proto.DoneExtras{Reason: err.Error()})
		return
	}
	var payload struct {
		Version int                        `json:"version"`
		Names   map[string]json.RawMessage `json:"names"`
	}
	if err := json.Unmarshal(b, &payload); err != nil {
		k.done(req.ID, proto.StatusError, &proto.DoneExtras{Reason: "corrupt snapshot: " + err.Error()})
		return
	}
	var restored, failed []string
	for n, raw := range payload.Names {
		var v any
		if json.Unmarshal(raw, &v) == nil {
			k.scope.Set(n, v)
			restored = append(restored, n)
		} else {
			failed = append(failed, n)
		}
	}
	sort.Strings(restored)
	sort.Strings(failed)
	k.done(req.ID, proto.StatusOK, &proto.DoneExtras{Restored: restored, Failed: failed})
}

// atomicWrite writes data to path via a temp file in the same directory and
// an os.Rename, so readers never observe a partial file.
func atomicWrite(path string, data []byte) error {
	if path == "" {
		return errors.New("empty path")
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp*")
	if err != nil {
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return err
	}
	return os.Rename(tmp.Name(), path)
}
