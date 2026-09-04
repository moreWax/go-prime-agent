package host

import (
	"os"
	"strings"
)

// PromptOptions assembles the prime-flavored system prompt for the Go
// harness (port of core/prompts/rlm.ts + system-prompt.ts, Go-kernel mode).
type PromptOptions struct {
	Cwd          string
	ContractPath string // contract/go.md; required for the cell language
	Skills       []SkillInfo
	Depth        int
	ParentAgent  string
}

// BuildSystemPrompt renders the prompt.
func BuildSystemPrompt(o PromptOptions) string {
	var b strings.Builder
	b.WriteString("You are a general purpose agent that uses code to solve tasks.\n")
	b.WriteString("You solve tasks by breaking down problems into sub-tasks, writing and executing code, observing results, and iterating one step at a time.\n")
	b.WriteString("When you are done, stop calling tools and state your final answer.\n\n")
	b.WriteString("Working directory: " + o.Cwd + "\n")
	if o.Depth > 0 {
		b.WriteString("You are a child agent spawned by " + o.ParentAgent + ". Task prompts are labeled `[task from parent]`.\n")
	}
	if o.Depth > 0 {
		b.WriteString("When a task calls for an answer, reply explicitly from a cell with rlm.Send(\"parent\", \"\", \"...\"). Not every task needs a reply.\n")
	}

	// The cell contract (language, concurrency rules, helpers).
	if o.ContractPath != "" {
		if data, err := os.ReadFile(o.ContractPath); err == nil {
			b.WriteString("\n" + strings.TrimSpace(string(data)) + "\n")
		}
	}

	// Skills.
	if len(o.Skills) > 0 {
		b.WriteString("\nSkills loaded in the kernel as Go packages (pre-imported; also importable as rlm/<name>):\n")
		for _, s := range o.Skills {
			line := "  - " + s.Name
			if s.Description != "" {
				line += ": " + s.Description
			}
			b.WriteString(line + "\n")
		}
		b.WriteString("Each skill's SKILL.md documents its API; read it from its directory with os.ReadFile. rlm.Skills() lists them at runtime.\n")
	}

	b.WriteString("\nCurrent working directory: " + o.Cwd + "\n")
	return b.String()
}
