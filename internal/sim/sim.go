// Package sim is a deterministic single-goroutine network and process
// simulator for the quorum-log cluster. Given the same seed, a run is fully
// reproducible: message drops, delays, duplications, reordering, partitions,
// crashes, and elections all derive from one seeded PRNG advanced by a
// single-threaded step loop.
//
// Determinism notes:
//   - Nodes are always iterated in sorted ID order; queued messages are
//     delivered in (deliverAt, enqueueSeq) order.
//   - etcd/raft's internal randomized election timeout is neutralized by an
//     enormous ElectionTick; elections are instead triggered explicitly by
//     the simulator's seeded supervisor via RawNode.Campaign. Production
//     nodes (cmd/quorumlogd) use normal raft election timeouts.
package sim

import (
	"fmt"
	"hash/fnv"
	"math/rand"
	"path/filepath"
	"sort"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"quorumlog/internal/check"
	"quorumlog/internal/engine"
)

const (
	// simElectionTick is large enough that raft's internal randomized
	// election timer can never fire within any simulated run.
	simElectionTick  = 1 << 20
	simHeartbeatTick = 5
	electionPatience = 25 // steps without a quorum-connected leader before the supervisor campaigns
	defaultOpTimeout = 60 // client retry timeout in steps
)

// NetConfig controls the simulated network fault model.
type NetConfig struct {
	DropProb float64 // probability an enqueued message is dropped
	DupProb  float64 // probability a message is delivered twice
	MinDelay int64   // minimum delivery delay in steps (>= 1)
	MaxDelay int64   // maximum delivery delay in steps
}

// DefaultNet is a mildly asynchronous network: no drops, small random delays
// (which already cause reordering).
var DefaultNet = NetConfig{DropProb: 0, DupProb: 0, MinDelay: 1, MaxDelay: 3}

// Config configures a simulated cluster.
type Config struct {
	Seed              int64
	NumNodes          int
	Net               NetConfig
	SnapshotThreshold uint64
	SnapshotTrailing  uint64
	Dir               string // root directory for node storage
}

type event struct {
	at   int64
	seq  uint64
	from uint64
	msg  raftpb.Message
}

// Node is one simulated process.
type Node struct {
	ID    uint64
	Dir   string
	Eng   *engine.Engine
	Alive bool
}

// Cluster is the simulated cluster. All methods must be called from a single
// goroutine.
type Cluster struct {
	cfg   Config
	rng   *rand.Rand
	now   int64
	seq   uint64
	inc   uint64 // incarnation counter for restarts
	nodes map[uint64]*Node
	ids   []uint64
	queue []event
	group map[uint64]int // partition group; nodes in different groups cannot exchange messages

	Recorder *check.Recorder
	clients  []*Client

	noLeaderSince int64
	traceHash     uint64 // running digest of delivered messages, for determinism tests
	Delivered     uint64
	Dropped       uint64
	err           error
}

// NewCluster creates and boots a cluster of cfg.NumNodes voters (IDs 1..N).
func NewCluster(cfg Config) (*Cluster, error) {
	if cfg.Net.MinDelay < 1 {
		cfg.Net.MinDelay = 1
	}
	if cfg.Net.MaxDelay < cfg.Net.MinDelay {
		cfg.Net.MaxDelay = cfg.Net.MinDelay
	}
	if cfg.SnapshotThreshold == 0 {
		cfg.SnapshotThreshold = 100
	}
	if cfg.SnapshotTrailing == 0 {
		cfg.SnapshotTrailing = 8
	}
	c := &Cluster{
		cfg:       cfg,
		rng:       rand.New(rand.NewSource(cfg.Seed)),
		nodes:     map[uint64]*Node{},
		group:     map[uint64]int{},
		traceHash: fnv.New64a().Sum64(),
	}
	c.Recorder = check.NewRecorder(func() int64 { return c.now })
	var peers []raft.Peer
	for i := 1; i <= cfg.NumNodes; i++ {
		peers = append(peers, raft.Peer{ID: uint64(i)})
	}
	for i := 1; i <= cfg.NumNodes; i++ {
		if err := c.startNode(uint64(i), peers); err != nil {
			return nil, err
		}
	}
	return c, nil
}

func (c *Cluster) startNode(id uint64, bootstrap []raft.Peer) error {
	c.inc++
	dir := filepath.Join(c.cfg.Dir, fmt.Sprintf("n%d", id))
	eng, err := engine.New(engine.Config{
		ID:                id,
		Dir:               dir,
		ElectionTick:      simElectionTick,
		HeartbeatTick:     simHeartbeatTick,
		SnapshotThreshold: c.cfg.SnapshotThreshold,
		SnapshotTrailing:  c.cfg.SnapshotTrailing,
		BootstrapPeers:    bootstrap,
		Logger:            quietLogger{},
		Incarnation:       c.inc,
	})
	if err != nil {
		return err
	}
	if _, ok := c.nodes[id]; !ok {
		c.ids = append(c.ids, id)
		sort.Slice(c.ids, func(i, j int) bool { return c.ids[i] < c.ids[j] })
	}
	c.nodes[id] = &Node{ID: id, Dir: dir, Eng: eng, Alive: true}
	return nil
}

// AddNodeProcess starts a brand-new joining process (no bootstrap); the
// caller must separately propose the membership change via ProposeAddNode.
func (c *Cluster) AddNodeProcess(id uint64) error { return c.startNode(id, nil) }

// Crash stops a node abruptly. Its storage directory survives.
func (c *Cluster) Crash(id uint64) {
	n := c.nodes[id]
	n.Alive = false
	n.Eng.Close()
	n.Eng = nil
}

// Restart brings a crashed node back from its persisted state.
func (c *Cluster) Restart(id uint64) error {
	n := c.nodes[id]
	if n.Alive {
		return fmt.Errorf("node %d is alive", id)
	}
	return c.startNode(id, nil)
}

// Partition splits the cluster into groups; nodes in different groups cannot
// exchange messages. Nodes not listed default to group 0.
func (c *Cluster) Partition(groups ...[]uint64) {
	c.group = map[uint64]int{}
	for gi, g := range groups {
		for _, id := range g {
			c.group[id] = gi
		}
	}
}

// Heal removes all partitions.
func (c *Cluster) Heal() { c.group = map[uint64]int{} }

func (c *Cluster) connected(a, b uint64) bool { return c.group[a] == c.group[b] }

// Node returns the simulated node by ID.
func (c *Cluster) Node(id uint64) *Node { return c.nodes[id] }

// Now returns the logical time (step count).
func (c *Cluster) Now() int64 { return c.now }

// Err returns the first internal error (storage/apply failures).
func (c *Cluster) Err() error { return c.err }

// TraceHash digests every delivered message; two runs with the same seed
// must produce identical hashes.
func (c *Cluster) TraceHash() uint64 { return c.traceHash }

// Leader returns the ID of a quorum-connected leader, or 0.
func (c *Cluster) Leader() uint64 {
	for _, id := range c.ids {
		n := c.nodes[id]
		if n.Alive && !n.Eng.Removed() && n.Eng.IsLeader() && c.reachesQuorum(id) {
			return id
		}
	}
	return 0
}

// reachesQuorum reports whether node id can currently reach a quorum of its
// voter set (counting itself if it is a voter).
func (c *Cluster) reachesQuorum(id uint64) bool {
	n := c.nodes[id]
	if n == nil || !n.Alive || n.Eng == nil {
		return false
	}
	voters := n.Eng.Voters()
	if len(voters) == 0 {
		return false
	}
	reach := 0
	for _, v := range voters {
		vn := c.nodes[v]
		if vn == nil || !vn.Alive {
			continue
		}
		if v == id || c.connected(id, v) {
			reach++
		}
	}
	return reach > len(voters)/2
}

// Step advances the simulation by one logical time unit.
func (c *Cluster) Step() {
	c.now++
	// 1. Tick raft clocks.
	for _, id := range c.ids {
		n := c.nodes[id]
		if n.Alive {
			n.Eng.Tick()
		}
	}
	// 2. Election supervision (seeded, deterministic).
	c.superviseElection()
	// 3. Clients act.
	for _, cl := range c.clients {
		cl.step()
	}
	// 4. Deliver due messages.
	c.deliverDue()
	// 5. Drain raft Ready work and enqueue outbound messages.
	c.drainReady()
}

// Steps advances the simulation n steps.
func (c *Cluster) Steps(n int) {
	for i := 0; i < n; i++ {
		c.Step()
	}
}

func (c *Cluster) superviseElection() {
	if c.Leader() != 0 {
		c.noLeaderSince = c.now
		return
	}
	if c.now-c.noLeaderSince < electionPatience {
		return
	}
	var eligible []uint64
	for _, id := range c.ids {
		n := c.nodes[id]
		if n.Alive && !n.Eng.Removed() && c.reachesQuorum(id) {
			eligible = append(eligible, id)
		}
	}
	if len(eligible) == 0 {
		c.noLeaderSince = c.now // keep waiting
		return
	}
	cand := eligible[c.rng.Intn(len(eligible))]
	_ = c.nodes[cand].Eng.Campaign()
	c.noLeaderSince = c.now // cooldown before next attempt
}

func (c *Cluster) drainReady() {
	for {
		progress := false
		for _, id := range c.ids {
			n := c.nodes[id]
			if !n.Alive || !n.Eng.HasReady() {
				continue
			}
			msgs, err := n.Eng.HandleReady()
			if err != nil && c.err == nil {
				c.err = fmt.Errorf("node %d: %w", id, err)
			}
			progress = true
			c.enqueue(id, msgs)
		}
		if !progress {
			return
		}
	}
}

func (c *Cluster) enqueue(from uint64, msgs []raftpb.Message) {
	for _, m := range msgs {
		isSnap := m.Type == raftpb.MsgSnap
		if c.rng.Float64() < c.cfg.Net.DropProb {
			c.Dropped++
			if isSnap {
				c.reportSnap(from, m.To, false)
			}
			continue
		}
		copies := 1
		if c.rng.Float64() < c.cfg.Net.DupProb {
			copies = 2
		}
		for i := 0; i < copies; i++ {
			delay := c.cfg.Net.MinDelay
			if c.cfg.Net.MaxDelay > c.cfg.Net.MinDelay {
				delay += c.rng.Int63n(c.cfg.Net.MaxDelay - c.cfg.Net.MinDelay + 1)
			}
			c.seq++
			c.queue = append(c.queue, event{at: c.now + delay, seq: c.seq, from: from, msg: m})
		}
	}
}

func (c *Cluster) deliverDue() {
	var due, future []event
	for _, ev := range c.queue {
		if ev.at <= c.now {
			due = append(due, ev)
		} else {
			future = append(future, ev)
		}
	}
	c.queue = future
	sort.Slice(due, func(i, j int) bool {
		if due[i].at != due[j].at {
			return due[i].at < due[j].at
		}
		return due[i].seq < due[j].seq
	})
	for _, ev := range due {
		c.deliver(ev)
	}
}

func (c *Cluster) deliver(ev event) {
	to := c.nodes[ev.msg.To]
	isSnap := ev.msg.Type == raftpb.MsgSnap
	// Partitioned or dead targets do not receive.
	if to == nil || !to.Alive || !c.connected(ev.from, ev.msg.To) {
		c.Dropped++
		if isSnap {
			c.reportSnap(ev.from, ev.msg.To, false)
		}
		return
	}
	c.Delivered++
	h := fnv.New64a()
	fmt.Fprintf(h, "%x|%d|%d|%d|%d|%d|%d", c.traceHash, c.now, ev.from, ev.msg.To, ev.msg.Type, ev.msg.Index, ev.msg.Term)
	c.traceHash = h.Sum64()
	_ = to.Eng.Step(ev.msg)
	if isSnap {
		c.reportSnap(ev.from, ev.msg.To, true)
	}
}

func (c *Cluster) reportSnap(from, to uint64, ok bool) {
	sender := c.nodes[from]
	if sender != nil && sender.Alive && sender.Eng != nil {
		sender.Eng.ReportSnapshot(to, ok)
	}
}

// pickAlive returns a seeded-random alive, non-removed node ID.
func (c *Cluster) pickAlive() uint64 {
	var alive []uint64
	for _, id := range c.ids {
		n := c.nodes[id]
		if n.Alive && !n.Eng.Removed() {
			alive = append(alive, id)
		}
	}
	if len(alive) == 0 {
		return 0
	}
	return alive[c.rng.Intn(len(alive))]
}

// AliveIDs returns alive, non-removed node IDs in sorted order.
func (c *Cluster) AliveIDs() []uint64 {
	var out []uint64
	for _, id := range c.ids {
		n := c.nodes[id]
		if n.Alive && !n.Eng.Removed() {
			out = append(out, id)
		}
	}
	return out
}

// Converged reports whether all alive non-removed nodes have identical
// applied indexes and state hashes.
func (c *Cluster) Converged() bool {
	ids := c.AliveIDs()
	if len(ids) == 0 {
		return false
	}
	first := c.nodes[ids[0]]
	for _, id := range ids[1:] {
		n := c.nodes[id]
		if n.Eng.AppliedIndex() != first.Eng.AppliedIndex() || n.Eng.SM().Hash() != first.Eng.SM().Hash() {
			return false
		}
	}
	return true
}

// WaitConverged runs steps until convergence (bounded by maxSteps).
func (c *Cluster) WaitConverged(maxSteps int) error {
	for i := 0; i < maxSteps; i++ {
		c.Step()
		if c.err != nil {
			return c.err
		}
		if c.Leader() != 0 && c.Converged() {
			return nil
		}
	}
	return fmt.Errorf("no convergence after %d steps (leader=%d)", maxSteps, c.Leader())
}

// ProposeConfChangeSync proposes a membership change and steps until applied.
func (c *Cluster) ProposeConfChangeSync(cc raftpb.ConfChange, maxSteps int) error {
	done := false
	inflight := false
	var deadline int64
	for i := 0; i < maxSteps; i++ {
		if done {
			return nil
		}
		if !inflight || c.now >= deadline {
			if lead := c.Leader(); lead != 0 {
				err := c.nodes[lead].Eng.ProposeConfChange(cc, func(error) { done = true })
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
	return fmt.Errorf("conf change %d not applied after %d steps", cc.ID, maxSteps)
}

// quietLogger silences etcd/raft's internal logging in tests.
type quietLogger struct{}

func (quietLogger) Debug(v ...interface{})                   {}
func (quietLogger) Debugf(format string, v ...interface{})   {}
func (quietLogger) Error(v ...interface{})                   {}
func (quietLogger) Errorf(format string, v ...interface{})   {}
func (quietLogger) Info(v ...interface{})                    {}
func (quietLogger) Infof(format string, v ...interface{})    {}
func (quietLogger) Warning(v ...interface{})                 {}
func (quietLogger) Warningf(format string, v ...interface{}) {}
func (quietLogger) Fatal(v ...interface{})                   { panic(fmt.Sprint(v...)) }
func (quietLogger) Fatalf(format string, v ...interface{})   { panic(fmt.Sprintf(format, v...)) }
func (quietLogger) Panic(v ...interface{})                   { panic(fmt.Sprint(v...)) }
func (quietLogger) Panicf(format string, v ...interface{})   { panic(fmt.Sprintf(format, v...)) }
