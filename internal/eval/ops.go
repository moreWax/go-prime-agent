package eval

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Run is the op-DSL stub evaluator used by protocol conformance tests. It is
// a tiny context-aware language: `name:arg rest...`.
//
//	ops: sleep:<ms> | hostcall:<n> <job> | bg:<ms> <text...> |
//	      set:<k> <v...> | get:<k> | fail:<msg...>
func Run(env Env) (Result, error) {
	name, arg, rest := parseOp(env.Code)
	switch name {
	case "":
		return Result{}, nil
	case "sleep":
		ms, _ := strconv.Atoi(arg)
		select {
		case <-time.After(time.Duration(ms) * time.Millisecond):
			return Result{}, nil
		case <-env.Ctx.Done():
			return Result{}, &InterruptedError{CellID: env.CellID}
		}
	case "hostcall":
		return hostcallOp(env, arg, rest)
	case "bg":
		ms, _ := strconv.Atoi(arg)
		text := strings.Join(rest, " ")
		// Goroutine spawned by the cell: keeps the cell id for output
		// attribution and keeps running after the cell's done event.
		go func() {
			select {
			case <-time.After(time.Duration(ms) * time.Millisecond):
				env.Stdout(text)
			case <-env.RootCtx.Done():
			}
		}()
		return Result{Value: "spawned"}, nil
	case "set":
		env.Set(arg, strings.Join(rest, " "))
		return Result{}, nil
	case "get":
		v, ok := env.Get(arg)
		if !ok {
			return Result{}, fmt.Errorf("NameError: name %q is not defined", arg)
		}
		return Result{Value: v}, nil
	case "fail":
		return Result{}, fmt.Errorf("%s", strings.TrimSpace(arg+" "+strings.Join(rest, " ")))
	default:
		return Result{}, fmt.Errorf("SyntaxError: unknown op %q", name)
	}
}

// parseOp splits `name:arg rest...`; name and arg may be empty.
func parseOp(code string) (name, arg string, rest []string) {
	fields := strings.Fields(strings.TrimSpace(code))
	if len(fields) == 0 {
		return "", "", nil
	}
	name, arg, _ = strings.Cut(fields[0], ":")
	return name, arg, fields[1:]
}

// hostcallOp fans out n concurrent host calls and reports how many replied.
func hostcallOp(env Env, arg string, rest []string) (Result, error) {
	n, _ := strconv.Atoi(arg)
	job := "job"
	if len(rest) > 0 {
		job = rest[0]
	}
	var wg sync.WaitGroup
	ok := make([]bool, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if res, err := env.Host.CallHost(env.Ctx, job, map[string]any{"i": i}); err == nil && len(res) > 0 {
				ok[i] = true
			}
		}(i)
	}
	wg.Wait()
	good := 0
	for _, b := range ok {
		if b {
			good++
		}
	}
	return Result{Value: fmt.Sprintf("%d/%d", good, n)}, nil
}
