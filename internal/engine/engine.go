// Package engine drives a single raft node: it owns the etcd/raft RawNode,
// the persistent storage, and the state machine, and turns raft Ready
// batches into persisted state, applied commands, and outbound messages.
//
// The engine is deliberately passive and single-threaded: it has no
// goroutines, timers, or channels. Callers (the deterministic simulator in
// tests, or the server runtime in production) decide when to tick, when to
// step incoming messages, and when to process Ready batches. That is what
// makes the simulator fully deterministic.
package engine

import (
	"errors"
	"fmt"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"quorumlog/internal/sm"
	"quorumlog/internal/storage"
)

// ErrNotLeader is returned for proposals on a non-leader node. Lead carries
// the last known leader ID (0 if unknown) so clients can retry there.
type ErrNotLeader struct{ Lead uint64 }

func (e ErrNotLeader) Error() string {
	return fmt.Sprintf("not leader (known leader: %d)", e.Lead)
}

// ErrStopped is reported to waiters that can never complete (node removed).
var ErrStopped = errors.New("engine stopped")

// Config configures an engine.
type Config struct {
	ID            uint64
	Dir           string // storage directory
	ElectionTick  int
	HeartbeatTick int
	// SnapshotThreshold: take a snapshot after this many applied entries
	// since the last snapshot.
	SnapshotThreshold uint64
	// SnapshotTrailing: log entries retained behind a snapshot.
	SnapshotTrailing uint64
	// BootstrapPeers, when non-nil and storage is empty, bootstraps a new
	// cluster with these voters. Leave nil for nodes joining via conf change
	// and for restarts.
	BootstrapPeers []raft.Peer
	Logger         raft.Logger
	// OnConfChange, if set, is invoked after a config change is applied.
	OnConfChange func(cc raftpb.ConfChange)
	// Incarnation distinguishes restarts of the same node so that stale
	// in-flight ReadIndex contexts from a previous incarnation can never be
	// confused with new ones.
	Incarnation uint64
}

// Engine is not goroutine-safe. Serialize all calls.
type Engine struct {
	id  uint64
	rn  *raft.RawNode
	st  *storage.DiskStorage
	sm  *sm.SM
	cfg Config

	lead         uint64
	appliedIndex uint64
	snapIndex    uint64
	confState    raftpb.ConfState
	removed      bool

	propWaiters map[string]func(sm.Result, error)
	ccWaiters   map[uint64]func(error)

	readSeq     uint64
	readWaiters map[string]func(error) // rctx -> callback (fires when safe to read locally)
	pendingRead []raft.ReadState       // readstates waiting for apply to catch up
}

// New opens storage in cfg.Dir and starts (or restarts) the raft node.
func New(cfg Config) (*Engine, error) {
	st, hasState, err := storage.Open(cfg.Dir)
	if err != nil {
		return nil, err
	}
	e := &Engine{
		id:          cfg.ID,
		st:          st,
		sm:          sm.New(),
		cfg:         cfg,
		propWaiters: map[string]func(sm.Result, error){},
		ccWaiters:   map[uint64]func(error){},
		readWaiters: map[string]func(error){},
	}
	// Restore the state machine from the latest snapshot, if any.
	snap, err := st.Snapshot()
	if err != nil {
		return nil, err
	}
	if !raft.IsEmptySnap(snap) {
		if err := e.sm.Restore(snap.Data); err != nil {
			return nil, fmt.Errorf("engine: restore snapshot: %w", err)
		}
		e.appliedIndex = snap.Metadata.Index
		e.snapIndex = snap.Metadata.Index
		e.confState = snap.Metadata.ConfState
	}
	rcfg := &raft.Config{
		ID:              cfg.ID,
		ElectionTick:    cfg.ElectionTick,
		HeartbeatTick:   cfg.HeartbeatTick,
		Storage:         st,
		Applied:         e.appliedIndex,
		MaxSizePerMsg:   1 << 20,
		MaxInflightMsgs: 256,
		PreVote:         true,
		Logger:          cfg.Logger,
	}
	rn, err := raft.NewRawNode(rcfg)
	if err != nil {
		return nil, err
	}
	e.rn = rn
	if !hasState && len(cfg.BootstrapPeers) > 0 {
		if err := rn.Bootstrap(cfg.BootstrapPeers); err != nil {
			return nil, err
		}
	}
	return e, nil
}

// Close releases storage resources. It does not fail outstanding waiters.
func (e *Engine) Close() error { return e.st.Close() }

// ID returns the node ID.
func (e *Engine) ID() uint64 { return e.id }

// SM exposes the local state machine for reads.
func (e *Engine) SM() *sm.SM { return e.sm }

// Storage exposes the disk storage (for metrics like snapshot size).
func (e *Engine) Storage() *storage.DiskStorage { return e.st }

// Lead returns the last observed leader ID (0 if unknown).
func (e *Engine) Lead() uint64 { return e.lead }

// IsLeader reports whether this node currently believes it is leader.
func (e *Engine) IsLeader() bool {
	return e.rn.BasicStatus().RaftState == raft.StateLeader
}

// AppliedIndex returns the highest applied log index.
func (e *Engine) AppliedIndex() uint64 { return e.appliedIndex }

// SnapIndex returns the index of the latest snapshot.
func (e *Engine) SnapIndex() uint64 { return e.snapIndex }

// Removed reports whether this node has been removed from the cluster.
func (e *Engine) Removed() bool { return e.removed }

// Voters returns the current voter set from the applied configuration.
func (e *Engine) Voters() []uint64 {
	st := e.rn.Status()
	return st.Config.Voters[0].Slice()
}

// Tick advances the raft logical clock by one tick.
func (e *Engine) Tick() { e.rn.Tick() }

// Campaign asks this node to start an election.
func (e *Engine) Campaign() error { return e.rn.Campaign() }

// Step feeds an incoming raft message from a peer.
func (e *Engine) Step(m raftpb.Message) error { return e.rn.Step(m) }

// ReportSnapshot tells raft whether a sent snapshot reached the follower.
func (e *Engine) ReportSnapshot(to uint64, ok bool) {
	status := raft.SnapshotFinish
	if !ok {
		status = raft.SnapshotFailure
	}
	e.rn.ReportSnapshot(to, status)
}

func propKey(clientID string, reqID uint64) string {
	return fmt.Sprintf("%s/%d", clientID, reqID)
}

// Propose submits op through raft. cb fires when the op is applied on this
// node (with the state-machine result) or when it is known to have failed.
// A proposal can also be silently dropped by raft (e.g. leadership change);
// callers must retry with the same ClientID/ReqID after a timeout, which the
// state machine deduplicates. Non-leaders reject with ErrNotLeader.
func (e *Engine) Propose(op sm.Op, cb func(sm.Result, error)) error {
	if !e.IsLeader() {
		return ErrNotLeader{Lead: e.lead}
	}
	if cb != nil {
		e.propWaiters[propKey(op.ClientID, op.ReqID)] = cb
	}
	if err := e.rn.Propose(op.Encode()); err != nil {
		delete(e.propWaiters, propKey(op.ClientID, op.ReqID))
		return err
	}
	return nil
}

// CancelProposal drops the waiter for a proposal (client gave up).
func (e *Engine) CancelProposal(clientID string, reqID uint64) {
	delete(e.propWaiters, propKey(clientID, reqID))
}

// ProposeConfChange submits a membership change. cb fires once the change is
// applied on this node. cc.ID must be unique per in-flight change.
func (e *Engine) ProposeConfChange(cc raftpb.ConfChange, cb func(error)) error {
	if !e.IsLeader() {
		return ErrNotLeader{Lead: e.lead}
	}
	if cb != nil {
		e.ccWaiters[cc.ID] = cb
	}
	if err := e.rn.ProposeConfChange(cc); err != nil {
		delete(e.ccWaiters, cc.ID)
		return err
	}
	return nil
}

// ReadIndex starts a linearizable read barrier. cb fires with nil once it is
// safe to serve the read from the local state machine (the node has applied
// at least up to the quorum-confirmed read index). Like proposals, a read
// barrier can be dropped (no leader); callers retry on timeout.
func (e *Engine) ReadIndex(cb func(error)) {
	e.readSeq++
	rctx := fmt.Sprintf("%d/%d/%d", e.id, e.cfg.Incarnation, e.readSeq)
	e.readWaiters[rctx] = cb
	e.rn.ReadIndex([]byte(rctx))
}

// HasReady reports whether there is work for HandleReady.
func (e *Engine) HasReady() bool { return e.rn.HasReady() }

// HandleReady processes one Ready batch: persist snapshot/entries/hard state,
// apply committed entries, resolve waiters, maybe trigger a snapshot, and
// return the outbound messages for the transport to deliver. The returned
// messages must be handed to the network only after this call returns, which
// guarantees the persist-before-send ordering raft requires.
func (e *Engine) HandleReady() ([]raftpb.Message, error) {
	if !e.rn.HasReady() {
		return nil, nil
	}
	rd := e.rn.Ready()

	// 1. Persist an incoming snapshot (install), then entries and hard state.
	if !raft.IsEmptySnap(rd.Snapshot) {
		if err := e.st.SaveSnapshot(rd.Snapshot); err != nil {
			return nil, err
		}
		if err := e.sm.Restore(rd.Snapshot.Data); err != nil {
			return nil, err
		}
		e.appliedIndex = rd.Snapshot.Metadata.Index
		e.snapIndex = rd.Snapshot.Metadata.Index
		e.confState = rd.Snapshot.Metadata.ConfState
	}
	if len(rd.Entries) > 0 {
		if err := e.st.Append(rd.Entries); err != nil {
			return nil, err
		}
	}
	if !raft.IsEmptyHardState(rd.HardState) {
		if err := e.st.SetHardState(rd.HardState); err != nil {
			return nil, err
		}
	}

	// 2. Track soft state (leader hint).
	if rd.SoftState != nil {
		e.lead = rd.SoftState.Lead
	}

	// 3. Collect read states; they resolve once applied catches up.
	if len(rd.ReadStates) > 0 {
		e.pendingRead = append(e.pendingRead, rd.ReadStates...)
	}

	// 4. Apply committed entries.
	for i := range rd.CommittedEntries {
		if err := e.applyEntry(rd.CommittedEntries[i]); err != nil {
			return nil, err
		}
	}

	msgs := rd.Messages
	e.rn.Advance(rd)

	// 5. Snapshot if the log has grown enough.
	if err := e.maybeSnapshot(); err != nil {
		return nil, err
	}

	// 6. Resolve read barriers whose index has been applied.
	e.flushReads()

	return msgs, nil
}

func (e *Engine) applyEntry(entry raftpb.Entry) error {
	switch entry.Type {
	case raftpb.EntryNormal:
		if len(entry.Data) == 0 {
			// Leader no-op entry at term start.
			e.appliedIndex = entry.Index
			return nil
		}
		op, err := sm.DecodeOp(entry.Data)
		if err != nil {
			return fmt.Errorf("engine: decode entry %d: %w", entry.Index, err)
		}
		res := e.sm.Apply(op)
		e.appliedIndex = entry.Index
		key := propKey(op.ClientID, op.ReqID)
		if cb, ok := e.propWaiters[key]; ok {
			delete(e.propWaiters, key)
			cb(res, nil)
		}
	case raftpb.EntryConfChange:
		var cc raftpb.ConfChange
		if err := cc.Unmarshal(entry.Data); err != nil {
			return err
		}
		e.confState = *e.rn.ApplyConfChange(cc)
		e.appliedIndex = entry.Index
		if cc.Type == raftpb.ConfChangeRemoveNode && cc.NodeID == e.id {
			e.removed = true
		}
		if cb, ok := e.ccWaiters[cc.ID]; ok {
			delete(e.ccWaiters, cc.ID)
			cb(nil)
		}
		if e.cfg.OnConfChange != nil {
			e.cfg.OnConfChange(cc)
		}
	default:
		e.appliedIndex = entry.Index
	}
	return nil
}

func (e *Engine) maybeSnapshot() error {
	if e.cfg.SnapshotThreshold == 0 || e.appliedIndex-e.snapIndex < e.cfg.SnapshotThreshold {
		return nil
	}
	data := e.sm.Snapshot()
	if _, err := e.st.CreateSnapshot(e.appliedIndex, &e.confState, data, e.cfg.SnapshotTrailing); err != nil {
		if err == raft.ErrSnapOutOfDate {
			return nil
		}
		return err
	}
	e.snapIndex = e.appliedIndex
	return nil
}

func (e *Engine) flushReads() {
	if len(e.pendingRead) == 0 {
		return
	}
	var keep []raft.ReadState
	for _, rs := range e.pendingRead {
		if e.appliedIndex >= rs.Index {
			rctx := string(rs.RequestCtx)
			if cb, ok := e.readWaiters[rctx]; ok {
				delete(e.readWaiters, rctx)
				cb(nil)
			}
		} else {
			keep = append(keep, rs)
		}
	}
	e.pendingRead = keep
}
