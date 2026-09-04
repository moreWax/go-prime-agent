package kernel

import (
	"errors"
	"fmt"
	"strings"

	"go-prime-agent/internal/eval"
	"go-prime-agent/internal/proto"
)

// runCell evaluates one execute request and maps the outcome to protocol
// events: result on success-with-value, KeyboardInterrupt on cancellation,
// Error otherwise, then exactly one done (spec: Events).
func (k *Kernel) runCell(w work) {
	req := w.req
	env := eval.Env{
		Ctx:     w.ctx,
		RootCtx: k.rootCtx,
		CellID:  req.ID,
		Code:    req.Code,
		Stdout:  k.attributedStdout(req.ID),
		Host:    k.bridge, // *hostbridge.Bridge satisfies eval.Host
		Set:     k.scope.Set,
		Get:     k.scope.Get,
	}

	res, err := k.cfg.Eval.Run(env)
	switch {
	case err == nil:
		if res.Value != nil {
			k.events.Write(proto.Event{
				Event: proto.KindResult, ID: proto.IDPtr(req.ID),
				Text: fmt.Sprintf("%v", res.Value),
			})
		}
		k.done(req.ID, proto.StatusOK, nil)
	case isInterrupt(err):
		k.events.Write(proto.Event{
			Event: proto.KindError, ID: proto.IDPtr(req.ID),
			EName: proto.EnameKeyboard, EValue: "cell interrupted",
		})
		k.done(req.ID, proto.StatusError, nil)
	default:
		k.events.Write(proto.Event{
			Event: proto.KindError, ID: proto.IDPtr(req.ID),
			EName: proto.EnameCellError, EValue: err.Error(),
		})
		k.done(req.ID, proto.StatusError, nil)
	}
}

// attributedStdout returns a writer that tags output with the cell's id,
// appending a newline when missing.
func (k *Kernel) attributedStdout(id string) func(string) {
	return func(text string) {
		if !strings.HasSuffix(text, "\n") {
			text += "\n"
		}
		k.events.Write(proto.Event{Event: proto.KindStdout, ID: proto.IDPtr(id), Text: text})
	}
}

func isInterrupt(err error) bool {
	var ie *eval.InterruptedError
	return errors.As(err, &ie)
}
