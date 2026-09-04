// skill.go — Go skills: source packages loaded into the interpreter at
// startup, mirroring the Python runtime's pre-imported skill modules.
//
// Layout: <skillsDir>/<name>/SKILL.md (model-facing docs) and
// <skillsDir>/<name>/skill.go declaring `package <name>`. The loader copies
// each skill.go into a private GoPath as src/rlm/<name>/<name>.go, so cells
// import them with real package semantics: `import "rlm/edit"` then
// `edit.Run(...)`. Skill packages are interpreted lazily on first import.
package rlm

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// SkillInfo describes one loaded skill.
type SkillInfo struct {
	Name string // import qualifier: rlm/<Name>
	Path string // original skill directory
}

// PrepareSkillGoPath scans dir for skill.go files and materializes a private
// GoPath the interpreter can import from. Missing dir yields an empty path
// and no error (skills are optional).
func PrepareSkillGoPath(dir string) (gopath string, skills []SkillInfo, err error) {
	if dir == "" {
		return "", nil, nil
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil, nil
		}
		return "", nil, err
	}
	tmp, err := os.MkdirTemp("", "gorlm-skills-")
	if err != nil {
		return "", nil, err
	}
	src := filepath.Join(tmp, "src", "rlm")
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		origin := filepath.Join(dir, name, "skill.go")
		data, err := os.ReadFile(origin)
		if err != nil {
			continue // not a Go skill (markdown-only skills are fine)
		}
		if !strings.Contains(string(data), "package "+name) {
			continue // package name must match the directory
		}
		pkgDir := filepath.Join(src, name)
		if err := os.MkdirAll(pkgDir, 0o755); err != nil {
			return "", nil, err
		}
		if err := os.WriteFile(filepath.Join(pkgDir, name+".go"), data, 0o644); err != nil {
			return "", nil, err
		}
		skills = append(skills, SkillInfo{Name: name, Path: filepath.Join(dir, name)})
	}
	return tmp, skills, nil
}

// skillImports renders the import prelude for loaded skills (used by tests
// and diagnostics).
func skillImports(skills []SkillInfo) []string {
	var out []string
	for _, s := range skills {
		out = append(out, fmt.Sprintf("rlm/%s", s.Name))
	}
	return out
}

var _ io.Reader = (io.Reader)(nil)
