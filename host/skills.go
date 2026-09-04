package host

import (
	"os"
	"path/filepath"
	"strings"
)

// SkillInfo is one discoverable skill.
type SkillInfo struct {
	Name        string
	Description string
	Dir         string
}

// ScanSkills lists skills under dir by SKILL.md frontmatter (name,
// description). Go skills additionally carry skill.go and load into the
// kernel via NewGoEvaluatorWithSkills.
func ScanSkills(dir string) ([]SkillInfo, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var out []SkillInfo
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, e.Name(), "SKILL.md"))
		if err != nil {
			continue
		}
		name, desc := parseFrontmatter(string(data))
		if name == "" {
			name = e.Name()
		}
		out = append(out, SkillInfo{Name: name, Description: desc, Dir: filepath.Join(dir, e.Name())})
	}
	return out, nil
}

// parseFrontmatter extracts name and description from a leading `---` block.
func parseFrontmatter(md string) (name, description string) {
	if !strings.HasPrefix(md, "---") {
		return "", ""
	}
	end := strings.Index(md[3:], "---")
	if end < 0 {
		return "", ""
	}
	block := md[3 : end+3]
	for _, line := range strings.Split(block, "\n") {
		k, v, ok := strings.Cut(strings.TrimSpace(line), ":")
		if !ok {
			continue
		}
		switch strings.TrimSpace(k) {
		case "name":
			name = strings.TrimSpace(v)
		case "description":
			description = strings.TrimSpace(v)
		}
	}
	return name, description
}
