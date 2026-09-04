package rlm

import (
	"errors"
	"fmt"
	"strings"
)

// pythonBootstrapMarks identify the stock host's Python bootstrap cell by
// construction (RLM_BOOTSTRAP_HEADER_CODE in prime-agent's ipython tool).
var pythonBootstrapMarks = [2]string{
	"import asyncio",
	`_prime_agent_os.environ[`,
}

// looksLikePythonBootstrap reports whether code is the stock host's Python
// bootstrap cell.
func looksLikePythonBootstrap(code string) bool {
	trimmed := strings.TrimSpace(code)
	for _, m := range pythonBootstrapMarks {
		if !strings.Contains(trimmed, m) {
			return false
		}
	}
	return strings.HasPrefix(trimmed, pythonBootstrapMarks[0])
}

// runCell evaluates one execute request and maps the outcome to protocol
// events: result on success-with-value, KeyboardInterrupt on cancellation,
// Error otherwise, then exactly one done (spec: Events).
func (k *Kernel) runCell(w work) {
	req := w.req
	// Fork-integration affordance (GORLM_ACK_PYTHON_BOOTSTRAP): the stock
	// host bootstraps kernels with a Python cell (rlm, bash, skills). A Go
	// kernel cannot run it; ack it so the host sees a provisioned namespace.
	// The fork's host sends a Go bootstrap instead and this becomes dead.
	if k.cfg.AckPythonBootstrap && looksLikePythonBootstrap(req.Code) {
		k.attributedStdout(req.ID)("go kernel: python bootstrap acked (not evaluated)")
		k.done(req.ID, StatusOK, nil)
		return
	}
	env := Env{
		Ctx:     w.ctx,
		RootCtx: k.rootCtx,
		CellID:  req.ID,
		Code:    req.Code,
		Stdout:  k.attributedStdout(req.ID),
		Host:    k.bridge, // *Bridge satisfies Host
		Set:     k.scope.Set,
		Get:     k.scope.Get,
	}

	res, err := k.cfg.Eval.Run(env)
	switch {
	case err == nil:
		if res.Value != nil {
			k.events.Write(Event{
				Event: KindResult, ID: IDPtr(req.ID),
				Text: fmt.Sprintf("%v", res.Value),
			})
		}
		k.done(req.ID, StatusOK, nil)
	case isInterrupt(err):
		k.events.Write(Event{
			Event: KindError, ID: IDPtr(req.ID),
			EName: EnameKeyboard, EValue: "cell interrupted",
		})
		k.done(req.ID, StatusError, nil)
	default:
		k.events.Write(Event{
			Event: KindError, ID: IDPtr(req.ID),
			EName: EnameCellError, EValue: err.Error(),
		})
		k.done(req.ID, StatusError, nil)
	}
}

// attributedStdout returns a writer that tags output with the cell's id,
// appending a newline when missing.
func (k *Kernel) attributedStdout(id string) func(string) {
	return func(text string) {
		if !strings.HasSuffix(text, "\n") {
			text += "\n"
		}
		k.events.Write(Event{Event: KindStdout, ID: IDPtr(id), Text: text})
	}
}

func isInterrupt(err error) bool {
	var ie *InterruptedError
	return errors.As(err, &ie)
}
