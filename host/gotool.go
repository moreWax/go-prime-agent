package host

import (
	"context"
	"fmt"

	"github.com/earendil-works/go-pi/packages/agent"
	"github.com/earendil-works/go-pi/packages/ai"
)

// NewGoTool exposes the in-process Go kernel as the agent's `go` tool.
// Cells are Go source in a persistent interpreter (see contract/go.md).
func NewGoTool(kh *KernelHost) agent.AgentTool {
	return agent.AgentTool{
		Tool: ai.Tool{
			Name:        "go",
			Description: "Execute Go source in a persistent interpreter kernel. State persists across calls: variables, functions, types. The trailing expression value is returned. This is the code-execution and orchestration tool.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"code": map[string]any{
						"type":        "string",
						"description": "Go source. Imports work per cell and are idempotent; the rlm package is pre-imported (rlm.Sleep, rlm.HostCall, rlm.Spawn, rlm.Send, rlm.ListAgents, rlm.Skills).",
					},
				},
				"required": []string{"code"},
			},
		},
		Execute: func(ctx context.Context, toolCallID string, args map[string]any, onUpdate agent.AgentToolUpdateCallback) (agent.AgentToolResult, error) {
			code, _ := args["code"].(string)
			if code == "" {
				return agent.AgentToolResult{}, fmt.Errorf("missing required argument: code")
			}
			res, err := kh.Execute(ctx, code)
			if err != nil {
				return agent.AgentToolResult{}, err
			}
			text := res.Text()
			return agent.AgentToolResult{
				Content: []ai.ContentBlock{ai.TextContent{Text: text}},
				Details: map[string]any{"hasResult": res.Result != "", "errored": res.Err != nil},
			}, nil
		},
	}
}
