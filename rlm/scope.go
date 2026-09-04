package rlm

// Scope is the persistent user namespace shared by all cells, owned by a
// single actor goroutine — no lock. Get/Set ordering follows channel FIFO,
// which the serial executor already relies on.
type scopeCmd struct {
	op    string // set | get | delete | names | entries
	k     string
	v     any
	reply chan scopeReply
}

type scopeReply struct {
	v       any
	ok      bool
	names   []string
	entries map[string]any
}

type Scope struct {
	cmds chan scopeCmd
}

func NewScope() *Scope {
	s := &Scope{cmds: make(chan scopeCmd, 256)}
	go s.run()
	return s
}

func (s *Scope) run() {
	names := make(map[string]any)
	for c := range s.cmds {
		switch c.op {
		case "set":
			names[c.k] = c.v
		case "get":
			v, ok := names[c.k]
			c.reply <- scopeReply{v: v, ok: ok}
		case "delete":
			delete(names, c.k)
		case "names":
			out := make([]string, 0, len(names))
			for k := range names {
				out = append(out, k)
			}
			c.reply <- scopeReply{names: out}
		case "entries":
			out := make(map[string]any, len(names))
			for k, v := range names {
				out[k] = v
			}
			c.reply <- scopeReply{entries: out}
		}
	}
}

func (s *Scope) Set(k string, v any) { s.cmds <- scopeCmd{op: "set", k: k, v: v} }

func (s *Scope) Get(k string) (any, bool) {
	ch := make(chan scopeReply, 1)
	s.cmds <- scopeCmd{op: "get", k: k, reply: ch}
	r := <-ch
	return r.v, r.ok
}

func (s *Scope) Delete(k string) { s.cmds <- scopeCmd{op: "delete", k: k} }

// Names returns unsorted names; callers sort for deterministic output.
func (s *Scope) Names() []string {
	ch := make(chan scopeReply, 1)
	s.cmds <- scopeCmd{op: "names", reply: ch}
	return (<-ch).names
}

// Entries copies the namespace (snapshot input).
func (s *Scope) Entries() map[string]any {
	ch := make(chan scopeReply, 1)
	s.cmds <- scopeCmd{op: "entries", reply: ch}
	return (<-ch).entries
}
