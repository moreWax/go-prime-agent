// goprime is the Go prime-agent harness: go-pi agent loop + go-prime-agent
// kernel and subagent machinery. Print mode only tonight; UI modes later.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/earendil-works/go-pi/packages/agent"
	"github.com/earendil-works/go-pi/packages/ai"
	"github.com/earendil-works/go-pi/packages/coding-agent"

	"github.com/moreWax/go-prime-agent/host"
)

func main() {
	var (
		prompt     = flag.String("p", "", "print mode: send one prompt, output the result, exit")
		providerId = flag.String("provider", "", "provider id from models.json")
		modelId    = flag.String("model", "", "model id from models.json")
		cwd        = flag.String("cwd", "", "working directory (default: current)")
		skillsDir  = flag.String("skills", "", "skills directory (Go skills load into the kernel)")
		apiKey     = flag.String("api-key", "", "explicit API key (overrides auth resolution)")
		contract   = flag.String("contract", defaultContractPath(), "cell-contract file")
	)
	flag.Parse()
	if *prompt == "" {
		fmt.Fprintln(os.Stderr, "goprime: -p <prompt> required")
		os.Exit(2)
	}
	if *cwd == "" {
		*cwd, _ = os.Getwd()
	}
	if *skillsDir == "" {
		*skillsDir = filepath.Join(repoRoot(*cwd), "skills")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	config := codingagent.LoadModelsConfig(codingagent.GetModelsPath())
	if err := config.Error(); err != "" {
		fmt.Fprintln(os.Stderr, "goprime: models.json:", err)
		os.Exit(1)
	}
	registry, err := codingagent.BuildRegistry(config, codingagent.NewAuthStorage(codingagent.GetAuthPath()))
	if err != nil {
		fmt.Fprintln(os.Stderr, "goprime:", err)
		os.Exit(1)
	}
	model := registry.GetModel(*providerId, *modelId)
	if model == nil {
		fmt.Fprintf(os.Stderr, "goprime: model %s/%s not found\n", *providerId, *modelId)
		os.Exit(1)
	}

	skills, _ := host.ScanSkills(*skillsDir)
	sysPrompt := host.BuildSystemPrompt(host.PromptOptions{
		Cwd: *cwd, ContractPath: *contract, Skills: skills,
	})

	// Parent session: kernel + manager + go tool.
	var mgr *host.Manager
	kh, err := host.NewKernelHost(ctx, *skillsDir, serviceFunc(func(c context.Context, k string, p json.RawMessage) (json.RawMessage, error) {
		return mgr.HandleHostRequest(c, k, p)
	}))
	if err != nil {
		fmt.Fprintln(os.Stderr, "goprime:", err)
		os.Exit(1)
	}
	mgr = host.NewManager("root", *skillsDir, childRunner(ctx, registry, *model, *contract, *skillsDir))

	streamFn := func(ctx context.Context, m ai.Model, conv ai.Context, opts agent.StreamOptions) *ai.AssistantStream {
		return registry.StreamSimple(ctx, m, conv, ai.SimpleStreamOptions{
			StreamOptions: ai.StreamOptions{APIKey: *apiKey},
		}, nil)
	}

	messages := []agent.AgentMessage{ai.UserMessage{Content: ai.UserContent{Text: *prompt}, Timestamp: time.Now().UnixMilli()}}
	for round := 0; round < 4; round++ {
		agentCtx := &agent.AgentContext{
			SystemPrompt: sysPrompt,
			Messages:     messages,
			Tools:        []agent.AgentTool{host.NewGoTool(kh)},
		}
		cfg := agent.AgentLoopConfig{
			Model:        *model,
			ConvertToLlm: func(ms []agent.AgentMessage) []ai.Message { return ms },
		}
		loop := agent.Loop(ctx, messages, agentCtx, cfg, streamFn)
		var lastText string
		var lastMsg ai.AssistantMessage
		for ev := range loop.Chan() {
			if e, ok := ev.(agent.MessageEndEvent); ok {
				if am, ok := e.Message.(ai.AssistantMessage); ok {
					lastMsg = am
					lastText = assistantText(am)
				}
			}
		}
		if lastText == "" {
			fmt.Fprintf(os.Stderr, "goprime: empty assistant turn: stop=%s err=%q provider=%s model=%s usage=%+v\n",
				lastMsg.StopReason, lastMsg.ErrorMessage, lastMsg.Provider, lastMsg.Model, lastMsg.Usage)
		}
		// Drain child replies; none -> done.
		var inbox []string
		for {
			select {
			case m := <-mgr.Inbox():
				inbox = append(inbox, m)
			default:
				goto drained
			}
		}
	drained:
		if len(inbox) == 0 {
			fmt.Println(lastText)
			kh.Shutdown()
			return
		}
		for _, m := range inbox {
			messages = append(messages, ai.UserMessage{
				Content:   ai.UserContent{Text: "message from child: " + m},
				Timestamp: time.Now().UnixMilli(),
			})
		}
	}
	fmt.Fprintln(os.Stderr, "goprime: inbox rounds exhausted")
	kh.Shutdown()
}

// childRunner runs admitted child tasks as full agent sessions with their
// own kernel; replies flow via the parent manager's inbox (agent_message).
func childRunner(ctx context.Context, registry *ai.Models, model ai.Model, contract, skillsDir string) host.ChildRunner {
	return childRun{ctx: ctx, registry: registry, model: model, contract: contract, skillsDir: skillsDir}
}

type childRun struct {
	ctx       context.Context
	registry  *ai.Models
	model     ai.Model
	contract  string
	skillsDir string
}

func (c childRun) RunChild(ctx context.Context, task, name string, inbox <-chan string) {
	skills, _ := host.ScanSkills(c.skillsDir)
	sysPrompt := host.BuildSystemPrompt(host.PromptOptions{
		Cwd: ".", ContractPath: c.contract, Skills: skills, Depth: 1, ParentAgent: "root",
	})
	// Child kernel whose bridge messages route to the PARENT inbox by
	// default role "parent" — v1 simplification: child manager relays.
	var cmgr *host.Manager
	kh, err := host.NewKernelHost(ctx, c.skillsDir, serviceFunc(func(c2 context.Context, k string, p json.RawMessage) (json.RawMessage, error) {
		return cmgr.HandleHostRequest(c2, k, p)
	}))
	if err != nil {
		return
	}
	cmgr = host.NewManager(name, c.skillsDir, childRunner(c.ctx, c.registry, c.model, c.contract, c.skillsDir))

	streamFn := func(ctx context.Context, m ai.Model, conv ai.Context, opts agent.StreamOptions) *ai.AssistantStream {
		return c.registry.StreamSimple(ctx, m, conv, ai.SimpleStreamOptions{}, nil)
	}
	messages := []agent.AgentMessage{ai.UserMessage{
		Content:   ai.UserContent{Text: "[task from parent] " + task},
		Timestamp: time.Now().UnixMilli(),
	}}
	agentCtx := &agent.AgentContext{SystemPrompt: sysPrompt, Messages: messages, Tools: []agent.AgentTool{host.NewGoTool(kh)}}
	cfg := agent.AgentLoopConfig{Model: c.model, ConvertToLlm: func(ms []agent.AgentMessage) []ai.Message { return ms }}
	loop := agent.Loop(ctx, messages, agentCtx, cfg, streamFn)
	for range loop.Chan() {
	}
	kh.Shutdown()
}

type serviceFunc func(context.Context, string, json.RawMessage) (json.RawMessage, error)

func (f serviceFunc) HandleHostRequest(ctx context.Context, kind string, payload json.RawMessage) (json.RawMessage, error) {
	return f(ctx, kind, payload)
}

func assistantText(m ai.AssistantMessage) string {
	s := ""
	for _, b := range m.Content {
		if t, ok := b.(ai.TextContent); ok {
			s += t.Text
		}
	}
	return s
}

func repoRoot(cwd string) string {
	// default contract/skills live next to the module when running from it
	if _, err := os.Stat(filepath.Join(cwd, "contract", "go.md")); err == nil {
		return cwd
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, "go-prime-agent")
}

func defaultContractPath() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, "go-prime-agent", "contract", "go.md")
}
