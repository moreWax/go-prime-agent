package goeval

import (
	"regexp"
	"strings"
)

// chunk splits cell source into eval units reproducing yaegi REPL semantics:
// each top-level declaration (import/var/type/const/func, brace-balanced) is
// its own chunk; runs of statements share one. Globals persist across
// chunks and cells; the last chunk's expression value becomes the result.
func chunk(src string) []string {
	lines := strings.Split(src, "\n")
	var chunks []string
	var cur []string
	depth := 0
	inDecl := false
	flush := func() {
		if len(cur) > 0 {
			if t := strings.TrimSpace(strings.Join(cur, "\n")); t != "" {
				chunks = append(chunks, strings.Join(cur, "\n"))
			}
			cur = nil
		}
	}
	for _, l := range lines {
		t := strings.TrimSpace(l)
		if t == "" && len(cur) == 0 {
			continue
		}
		isDecl := depth == 0 && !inDecl && (strings.HasPrefix(t, "var ") || strings.HasPrefix(t, "type ") ||
			strings.HasPrefix(t, "const ") || strings.HasPrefix(t, "func ") ||
			strings.HasPrefix(t, "import "))
		if isDecl {
			flush()
			inDecl = true
		}
		cur = append(cur, l)
		depth += strings.Count(l, "{") - strings.Count(l, "}")
		if inDecl && depth <= 0 {
			flush()
			inDecl = false
		}
	}
	flush()
	return chunks
}

var (
	declNameRe   = regexp.MustCompile(`^(?:var|type|const|func)\s+(\w+)`)
	assignNameRe = regexp.MustCompile(`^((?:\w+\s*,\s*)*\w+)\s*:?=`)
	blockNameRe  = regexp.MustCompile(`^\s*(\w+)\s+\w`)
)

// declaredNames extracts top-level names a cell declares or assigns. Used to
// mirror interpreter globals into the kernel scope (list_names, snapshot).
func declaredNames(src string) []string {
	var out []string
	for _, c := range chunk(src) {
		lines := strings.Split(c, "\n")
		first := strings.TrimSpace(lines[0])
		if m := declNameRe.FindStringSubmatch(first); m != nil {
			if !strings.HasPrefix(m[1], "_") {
				out = append(out, m[1])
			}
			continue
		}
		if strings.HasSuffix(first, "(") && strings.HasPrefix(first, "var") {
			for _, l := range lines[1:] { // grouped var block
				if m := blockNameRe.FindStringSubmatch(l); m != nil && !strings.HasPrefix(m[1], "_") {
					out = append(out, m[1])
				}
			}
			continue
		}
		if m := assignNameRe.FindStringSubmatch(first); m != nil {
			for _, n := range strings.Split(m[1], ",") {
				n = strings.TrimSpace(n)
				if n != "" && n != "_" && !strings.HasPrefix(n, "_") {
					out = append(out, n)
				}
			}
		}
	}
	return out
}
