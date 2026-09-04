// Package edit performs targeted exact-string file edits, serialized per
// path so concurrent cells cannot interleave mutations on one file.
package edit

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var (
	mu    sync.Mutex
	locks = make(map[string]*sync.Mutex)
)

func lockFor(path string) *sync.Mutex {
	mu.Lock()
	defer mu.Unlock()
	l, ok := locks[path]
	if !ok {
		l = &sync.Mutex{}
		locks[path] = l
	}
	return l
}

// Run replaces the single exact occurrence of oldStr with newStr in path.
// The write is atomic (temp file + rename). Returns the number of
// replacements (0 or 1).
func Run(path, oldStr, newStr string) (int, error) {
	if oldStr == "" {
		return 0, fmt.Errorf("edit: empty old string")
	}
	l := lockFor(path)
	l.Lock()
	defer l.Unlock()

	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	s := string(data)
	count := strings.Count(s, oldStr)
	if count == 0 {
		return 0, fmt.Errorf("edit: old string not found in %s", path)
	}
	if count > 1 {
		return 0, fmt.Errorf("edit: old string appears %d times in %s; make it unique", count, path)
	}
	s = strings.Replace(s, oldStr, newStr, 1)
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".edit*")
	if err != nil {
		return 0, err
	}
	if _, err := tmp.WriteString(s); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return 0, err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return 0, err
	}
	return 1, os.Rename(tmp.Name(), path)
}
