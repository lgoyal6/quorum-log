package sim

import (
	"fmt"

	"quorumlog/internal/check"
	"quorumlog/internal/engine"
	"quorumlog/internal/sm"
)

// ClientConfig shapes a simulated client's workload.
type ClientConfig struct {
	NumOps    int
	Keys      []string
	GetFrac   float64 // fraction of ops that are linearizable reads
	DelFrac   float64 // fraction of ops that are deletes
	OpTimeout int64   // steps before a pending op is retried (same ReqID)
}

// Client is a simulated client. It issues one operation at a time, retries
// on timeout or leader rejection with the same request ID (relying on
// state-machine dedup for exactly-once application of writes), and records
// every operation in the cluster's history recorder.
type Client struct {
	c    *Cluster
	idx  int    // porcupine client id
	name string // sm.Op ClientID
	cfg  ClientConfig

	opsDone   int
	reqID     uint64
	gen       int // increments per logical op; guards stale callbacks
	pendingOp *pendingOp
	target    uint64
}

type pendingOp struct {
	recID    int
	op       sm.Op // for writes
	isRead   bool
	readKey  string
	inflight bool
	deadline int64
}

// AddClient registers a workload client on the cluster.
func (c *Cluster) AddClient(cfg ClientConfig) *Client {
	if cfg.OpTimeout == 0 {
		cfg.OpTimeout = defaultOpTimeout
	}
	if len(cfg.Keys) == 0 {
		cfg.Keys = []string{"k0", "k1", "k2", "k3"}
	}
	cl := &Client{
		c:    c,
		idx:  len(c.clients),
		name: fmt.Sprintf("c%d", len(c.clients)),
		cfg:  cfg,
	}
	c.clients = append(c.clients, cl)
	return cl
}

// Done reports whether the client has completed all of its operations.
func (cl *Client) Done() bool { return cl.pendingOp == nil && cl.opsDone >= cl.cfg.NumOps }

// DoneOps returns the number of completed operations.
func (cl *Client) DoneOps() int { return cl.opsDone }

func (cl *Client) step() {
	if cl.Done() {
		return
	}
	if cl.pendingOp == nil {
		cl.startNextOp()
	}
	p := cl.pendingOp
	if p == nil {
		return
	}
	if p.inflight {
		if cl.c.now < p.deadline {
			return
		}
		// Timed out: the target may be alive but partitioned or leaderless;
		// force a new target for the retry.
		cl.target = 0
		p.inflight = false
	}
	cl.attempt()
}

func (cl *Client) startNextOp() {
	c := cl.c
	key := cl.cfg.Keys[c.rng.Intn(len(cl.cfg.Keys))]
	r := c.rng.Float64()
	p := &pendingOp{}
	switch {
	case r < cl.cfg.GetFrac:
		p.isRead = true
		p.readKey = key
		p.recID = c.Recorder.Invoke(cl.idx, check.KVInput{Op: "get", Key: key})
	case r < cl.cfg.GetFrac+cl.cfg.DelFrac:
		cl.reqID++
		p.op = sm.Op{Type: sm.OpDelete, Key: key, ClientID: cl.name, ReqID: cl.reqID}
		p.recID = c.Recorder.Invoke(cl.idx, check.KVInput{Op: sm.OpDelete, Key: key})
	default:
		cl.reqID++
		val := fmt.Sprintf("%s.%d;", cl.name, cl.reqID)
		typ := sm.OpPut
		if c.rng.Float64() < 0.5 {
			typ = sm.OpAppend
		}
		p.op = sm.Op{Type: typ, Key: key, Val: val, ClientID: cl.name, ReqID: cl.reqID}
		p.recID = c.Recorder.Invoke(cl.idx, check.KVInput{Op: typ, Key: key, Val: val})
	}
	cl.gen++
	cl.pendingOp = p
}

// attempt (re)submits the current op to some node.
func (cl *Client) attempt() {
	c := cl.c
	p := cl.pendingOp
	// Choose a target: keep the current one if alive, otherwise pick random.
	tn := c.nodes[cl.target]
	if cl.target == 0 || tn == nil || !tn.Alive || tn.Eng.Removed() {
		cl.target = c.pickAlive()
	}
	if cl.target == 0 {
		return // nobody alive; try next step
	}
	eng := c.nodes[cl.target].Eng
	gen := cl.gen

	if p.isRead {
		target := cl.target
		eng.ReadIndex(func(err error) {
			if cl.gen != gen || cl.pendingOp == nil || err != nil {
				return
			}
			n := c.nodes[target]
			if n == nil || !n.Alive || n.Eng == nil {
				return
			}
			v, ok := n.Eng.SM().Get(p.readKey)
			c.Recorder.Return(p.recID, check.KVOutput{Val: v, Found: ok})
			cl.pendingOp = nil
			cl.opsDone++
		})
		p.inflight = true
		p.deadline = c.now + cl.cfg.OpTimeout
		return
	}

	err := eng.Propose(p.op, func(res sm.Result, err error) {
		if cl.gen != gen || cl.pendingOp == nil {
			return
		}
		if err != nil {
			return // retry via timeout
		}
		c.Recorder.Return(p.recID, check.KVOutput{Val: res.Val, Found: res.OK})
		cl.pendingOp = nil
		cl.opsDone++
	})
	if err != nil {
		if nl, ok := err.(engine.ErrNotLeader); ok && nl.Lead != 0 {
			cl.target = nl.Lead
		} else {
			cl.target = 0 // re-pick next attempt
		}
		// Not inflight; retry next step.
		p.inflight = false
		return
	}
	p.inflight = true
	p.deadline = c.now + cl.cfg.OpTimeout
}

// PutSync synchronously applies a put through the cluster, driving steps.
// Used by scenario setup code (not part of the recorded client history).
func (c *Cluster) PutSync(clientID string, reqID uint64, key, val string, maxSteps int) error {
	return c.writeSync(sm.Op{Type: sm.OpPut, Key: key, Val: val, ClientID: clientID, ReqID: reqID}, maxSteps)
}

// AppendSync synchronously applies an append through the cluster.
func (c *Cluster) AppendSync(clientID string, reqID uint64, key, val string, maxSteps int) error {
	return c.writeSync(sm.Op{Type: sm.OpAppend, Key: key, Val: val, ClientID: clientID, ReqID: reqID}, maxSteps)
}

func (c *Cluster) writeSync(op sm.Op, maxSteps int) error {
	done := false
	inflight := false
	var deadline int64
	for i := 0; i < maxSteps; i++ {
		if done {
			return nil
		}
		if !inflight || c.now >= deadline {
			if lead := c.Leader(); lead != 0 {
				err := c.nodes[lead].Eng.Propose(op, func(sm.Result, error) { done = true })
				if err == nil {
					inflight = true
					deadline = c.now + defaultOpTimeout
				}
			}
		}
		c.Step()
		if c.err != nil {
			return c.err
		}
	}
	if done {
		return nil
	}
	return fmt.Errorf("op %s/%d not applied after %d steps", op.ClientID, op.ReqID, maxSteps)
}

// ReadSync performs a linearizable read via node id, driving steps until it
// completes.
func (c *Cluster) ReadSync(id uint64, key string, maxSteps int) (string, bool, error) {
	var val string
	var found, done bool
	inflight := false
	var deadline int64
	for i := 0; i < maxSteps; i++ {
		if done {
			return val, found, nil
		}
		n := c.nodes[id]
		if n != nil && n.Alive && (!inflight || c.now >= deadline) {
			n.Eng.ReadIndex(func(err error) {
				if err != nil || done {
					return
				}
				nn := c.nodes[id]
				if nn == nil || !nn.Alive {
					return
				}
				val, found = nn.Eng.SM().Get(key)
				done = true
			})
			inflight = true
			deadline = c.now + defaultOpTimeout
		}
		c.Step()
		if c.err != nil {
			return "", false, c.err
		}
	}
	if done {
		return val, found, nil
	}
	return "", false, fmt.Errorf("read of %q via node %d timed out", key, id)
}
