package multihost

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// Scenarios.
const (
	// ScenarioFull is the gate scenario: kill the leader, isolate a
	// follower, isolate the leader from the majority, restore, restart all.
	ScenarioFull = "full"
	// ScenarioLeaderIsolation is the shortened scenario used by the negative
	// control: isolate the leader from the majority, restore, restart all.
	ScenarioLeaderIsolation = "leader-isolation"
)

// FaultEvent is one injected fault or observed transition, with the exact
// time it happened on the driver's clock.
type FaultEvent struct {
	Name       string   `json:"name"`
	Detail     string   `json:"detail"`
	AtOp       int      `json:"at_op"`
	AtNanos    int64    `json:"at_nanos"`
	WallClock  string   `json:"wall_clock"`
	DurationMs float64  `json:"duration_ms,omitempty"`
	NodeID     uint64   `json:"node_id,omitempty"`
	Peers      []uint64 `json:"peers,omitempty"`
}

// OutageMeasurement is a write outage as the client observed it.
type OutageMeasurement struct {
	Event              string  `json:"event"`
	EventAtNanos       int64   `json:"event_at_nanos"`
	LastAckBeforeNanos int64   `json:"last_acknowledged_write_before_nanos"`
	FirstAckAfterNanos int64   `json:"first_acknowledged_write_after_nanos"`
	OutageMs           float64 `json:"outage_ms"`
	Method             string  `json:"method"`
	Complete           bool    `json:"complete"`
}

// ProgressMeasurement records client progress during a fault window.
type ProgressMeasurement struct {
	WindowStartNanos   int64   `json:"window_start_nanos"`
	WindowEndNanos     int64   `json:"window_end_nanos"`
	WindowMs           float64 `json:"window_ms"`
	AcknowledgedWrites int     `json:"acknowledged_writes"`
	AcknowledgedReads  int     `json:"acknowledged_reads"`
	WritesPerSecond    float64 `json:"writes_per_second"`
	MadeProgress       bool    `json:"made_progress"`
	Method             string  `json:"method"`
}

// DurationMeasurement is a single measured duration.
type DurationMeasurement struct {
	Ms     float64 `json:"ms"`
	Method string  `json:"method"`
}

// Measurements collects every number the gate checks.
type Measurements struct {
	LeaderKillWriteOutage      *OutageMeasurement   `json:"leader_kill_write_outage,omitempty"`
	LeaderKillElection         *DurationMeasurement `json:"leader_kill_election,omitempty"`
	FollowerIsolationProgress  *ProgressMeasurement `json:"follower_isolation_progress,omitempty"`
	LeaderIsolationWriteOutage *OutageMeasurement   `json:"leader_isolation_write_outage,omitempty"`
	LeaderIsolationElection    *DurationMeasurement `json:"leader_isolation_election,omitempty"`
	RestartReadyRecovery       *DurationMeasurement `json:"restart_ready_recovery,omitempty"`
	RestartFirstReadRecovery   *DurationMeasurement `json:"restart_first_read_recovery,omitempty"`
}

// FaultReport is written as multihost-faults.json (or the dry-run
// equivalent).
type FaultReport struct {
	Mode            string       `json:"mode"`
	Boundary        string       `json:"boundary"`
	GeneratedAt     string       `json:"generated_at"`
	Scenario        string       `json:"scenario"`
	Executor        string       `json:"executor"`
	Events          []FaultEvent `json:"events"`
	Measurements    Measurements `json:"measurements"`
	AckedWrites     int          `json:"acknowledged_writes"`
	AckedWriteLoss  []Mismatch   `json:"acknowledged_write_loss"`
	VerifiedKeys    int          `json:"verified_keys"`
	RestartVerified bool         `json:"restart_verified"`
	Notes           []string     `json:"notes"`
}

// FaultRunner injects the scenario's faults while traffic runs, and records
// exactly when each one happened.
type FaultRunner struct {
	Inv      *Inventory
	Dep      *Deployment
	Ex       Executor
	Rec      *Recorder
	Work     *Workload
	Scenario string

	events []FaultEvent
	notes  []string

	killAtNanos           int64
	isolateFollowerNanos  int64
	clearFollowerNanos    int64
	isolateLeaderNanos    int64
	restoreNanos          int64
	restartIssuedNanos    int64
	restartReadyNanos     int64
	killElectionMs        float64
	isolationElectionMs   float64
	restartVerified       bool
	restartVerifiedKeys   int
	restartMismatches     []Mismatch
	isolatedFollowerID    uint64
	isolatedLeaderID      uint64
	killedLeaderID        uint64
	newLeaderAfterKill    uint64
	newLeaderAfterIsolate uint64
}

// Note records a judgment call or an observation for the artifacts.
func (f *FaultRunner) Note(format string, args ...interface{}) {
	f.notes = append(f.notes, fmt.Sprintf(format, args...))
}

func (f *FaultRunner) event(name, detail string, nodeID uint64, peers []uint64, dur time.Duration) int64 {
	now := time.Now()
	at := f.Rec.Since(now)
	ev := FaultEvent{
		Name:      name,
		Detail:    detail,
		AtOp:      f.Work.Completed(),
		AtNanos:   at,
		WallClock: now.UTC().Format(time.RFC3339Nano),
		NodeID:    nodeID,
		Peers:     peers,
	}
	if dur > 0 {
		ev.DurationMs = float64(dur.Microseconds()) / 1000
	}
	f.events = append(f.events, ev)
	return at
}

// Events returns the recorded fault timeline.
func (f *FaultRunner) Events() []FaultEvent { return f.events }

// Notes returns the recorded notes.
func (f *FaultRunner) Notes() []string { return f.notes }

// waitOp blocks until the workload has completed at least n operations.
func (f *FaultRunner) waitOp(ctx context.Context, n int) error {
	for {
		if f.Work.Completed() >= n {
			return nil
		}
		if f.Work.Finished() {
			return fmt.Errorf("traffic stopped after %d operations before reaching operation %d, so this fault could not be injected", f.Work.Completed(), n)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// isolate blackholes raft traffic between h and the given peers, in both
// directions, through h's loopback chaos endpoint. The request runs on the
// host itself (over ssh in a multi-host run), because the endpoint is not
// reachable from anywhere else.
func (f *FaultRunner) isolate(ctx context.Context, h Host, peers []uint64) error {
	ids := make([]string, 0, len(peers))
	for _, p := range peers {
		ids = append(ids, fmt.Sprint(p))
	}
	body := fmt.Sprintf(`{"peers":[%s],"drop_inbound":true,"drop_outbound":true}`, strings.Join(ids, ","))
	cmd := fmt.Sprintf("curl -sS -f -X POST %s/chaos/isolate -d %s", h.ChaosURL(), ShellQuote(body))
	_, err := f.Ex.Run(ctx, h, cmd)
	return err
}

// clearIsolation removes every drop rule on h.
func (f *FaultRunner) clearIsolation(ctx context.Context, h Host) error {
	cmd := fmt.Sprintf("curl -sS -f -X DELETE %s/chaos/isolate", h.ChaosURL())
	_, err := f.Ex.Run(ctx, h, cmd)
	return err
}

// chaosState reads the current drop sets on h, for the artifacts.
func (f *FaultRunner) chaosState(ctx context.Context, h Host) string {
	out, err := f.Ex.Run(ctx, h, fmt.Sprintf("curl -sS -f %s/chaos", h.ChaosURL()))
	if err != nil {
		return "unavailable: " + err.Error()
	}
	return strings.TrimSpace(out)
}

// waitAllAgreeOnLeader waits until every host reports the same leader, which
// is how the driver knows a partition has really healed.
func (f *FaultRunner) waitAllAgreeOnLeader(ctx context.Context, timeout time.Duration) (uint64, time.Duration, error) {
	start := time.Now()
	deadline := start.Add(timeout)
	for {
		leaders := map[uint64]int{}
		ok := true
		for _, h := range f.Inv.Hosts {
			st, err := FetchStatus(ctx, httpStatusClient(), h)
			if err != nil || st.Leader == 0 {
				ok = false
				break
			}
			leaders[st.Leader]++
		}
		if ok && len(leaders) == 1 {
			for id := range leaders {
				return id, time.Since(start), nil
			}
		}
		if time.Now().After(deadline) {
			return 0, time.Since(start), fmt.Errorf("hosts did not agree on one leader within %s", timeout)
		}
		select {
		case <-ctx.Done():
			return 0, time.Since(start), ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// Run injects the scenario's faults. It always closes done, so the traffic
// workers can finish even when a fault step fails.
func (f *FaultRunner) Run(ctx context.Context, done chan struct{}) error {
	defer close(done)
	switch f.Scenario {
	case ScenarioLeaderIsolation:
		return f.runLeaderIsolationOnly(ctx)
	default:
		return f.runFull(ctx)
	}
}

func (f *FaultRunner) runFull(ctx context.Context) error {
	plan := f.Inv.Faults

	// 1. Kill the leader with SIGKILL and restart it from disk afterwards.
	if plan.KillLeaderAtOp > 0 {
		if err := f.waitOp(ctx, plan.KillLeaderAtOp); err != nil {
			return err
		}
		leader, _, err := FindLeader(ctx, f.Inv.Hosts)
		if err != nil {
			return fmt.Errorf("kill leader: %w", err)
		}
		f.killedLeaderID = leader.ID
		if err := f.Dep.Kill(ctx, leader); err != nil {
			return fmt.Errorf("kill leader %d: %w", leader.ID, err)
		}
		f.killAtNanos = f.event("kill_leader", fmt.Sprintf("SIGKILL to node %d on %s", leader.ID, leader.Target()), leader.ID, nil, 0)

		survivors := OtherHosts(f.Inv.Hosts, leader.ID)
		newLeader, elapsed, err := WaitLeader(ctx, survivors, leader.ID, 30*time.Second)
		if err != nil {
			return fmt.Errorf("no new leader after killing node %d: %w", leader.ID, err)
		}
		f.killElectionMs = float64(elapsed.Microseconds()) / 1000
		f.newLeaderAfterKill = newLeader.ID
		f.event("new_leader_elected", fmt.Sprintf("node %d took over from killed node %d", newLeader.ID, leader.ID), newLeader.ID, nil, elapsed)

		// Restart the killed node from its own disk.
		if _, err := f.Dep.Start(ctx, leader); err != nil {
			return fmt.Errorf("restart killed node %d: %w", leader.ID, err)
		}
		if err := f.Dep.WaitReady(ctx, []Host{leader}, 30*time.Second); err != nil {
			return fmt.Errorf("killed node %d did not come back: %w", leader.ID, err)
		}
		f.event("restart_killed_node", fmt.Sprintf("node %d restarted from its persisted state", leader.ID), leader.ID, nil, 0)
		_, settled, err := f.waitAllAgreeOnLeader(ctx, 30*time.Second)
		if err != nil {
			return fmt.Errorf("cluster did not settle after restarting node %d: %w", leader.ID, err)
		}
		f.event("cluster_settled", "all three nodes report the same leader again", 0, nil, settled)
	}

	// 2. Isolate one follower; the majority must keep making progress.
	if plan.IsolateFollowerAtOp > 0 {
		if err := f.waitOp(ctx, plan.IsolateFollowerAtOp); err != nil {
			return err
		}
		leader, _, err := FindLeader(ctx, f.Inv.Hosts)
		if err != nil {
			return fmt.Errorf("isolate follower: %w", err)
		}
		followers := OtherHosts(f.Inv.Hosts, leader.ID)
		follower := followers[0]
		peers := HostIDs(OtherHosts(f.Inv.Hosts, follower.ID))
		if err := f.isolate(ctx, follower, peers); err != nil {
			return fmt.Errorf("isolate follower %d: %w", follower.ID, err)
		}
		f.isolatedFollowerID = follower.ID
		f.isolateFollowerNanos = f.event("isolate_follower",
			fmt.Sprintf("node %d blackholed from nodes %v in both directions; chaos state: %s", follower.ID, peers, f.chaosState(ctx, follower)),
			follower.ID, peers, 0)
	}

	// 3. Clear the follower isolation, then isolate the leader from the
	//    majority. The order matters: with a follower still cut off, no
	//    quorum could survive isolating the leader as well, so the two
	//    faults are deliberately sequential rather than overlapping.
	if plan.IsolateLeaderAtOp > 0 {
		if err := f.waitOp(ctx, plan.IsolateLeaderAtOp); err != nil {
			return err
		}
		if f.isolatedFollowerID != 0 {
			follower, _ := f.Inv.HostByID(f.isolatedFollowerID)
			if err := f.clearIsolation(ctx, follower); err != nil {
				return fmt.Errorf("clear follower isolation: %w", err)
			}
			f.clearFollowerNanos = f.event("clear_follower_isolation",
				fmt.Sprintf("node %d reconnected before the leader is isolated, so a quorum can still exist", follower.ID),
				follower.ID, nil, 0)
			_, settled, err := f.waitAllAgreeOnLeader(ctx, 30*time.Second)
			if err != nil {
				return fmt.Errorf("cluster did not settle after reconnecting node %d: %w", follower.ID, err)
			}
			f.event("cluster_settled", "all three nodes report the same leader again", 0, nil, settled)
		}
		if err := f.isolateLeaderFromMajority(ctx); err != nil {
			return err
		}
	}

	// 4. Restore the network.
	if plan.RestoreAtOp > 0 {
		if err := f.waitOp(ctx, plan.RestoreAtOp); err != nil {
			return err
		}
		if err := f.restoreAll(ctx); err != nil {
			return err
		}
	}
	return nil
}

// runLeaderIsolationOnly is the negative-control scenario: it isolates the
// leader from the majority and restores, with thresholds derived from the
// operation target so a shortened run still exercises the fault.
func (f *FaultRunner) runLeaderIsolationOnly(ctx context.Context) error {
	ops := f.Inv.Client.Ops
	isolateAt := ops * 3 / 10
	if isolateAt < 1 {
		isolateAt = 1
	}
	restoreAt := ops * 3 / 4
	if restoreAt <= isolateAt {
		restoreAt = isolateAt + 1
	}
	f.Note("scenario %s: isolate the leader at op %d and restore at op %d, derived from the %d-operation target", ScenarioLeaderIsolation, isolateAt, restoreAt, ops)
	if err := f.waitOp(ctx, isolateAt); err != nil {
		return err
	}
	if err := f.isolateLeaderFromMajority(ctx); err != nil {
		return err
	}
	if err := f.waitOp(ctx, restoreAt); err != nil {
		return err
	}
	return f.restoreAll(ctx)
}

// isolateLeaderFromMajority cuts the leader off from the other two nodes and
// waits for a new leader among them.
func (f *FaultRunner) isolateLeaderFromMajority(ctx context.Context) error {
	leader, _, err := FindLeader(ctx, f.Inv.Hosts)
	if err != nil {
		return fmt.Errorf("isolate leader: %w", err)
	}
	majority := OtherHosts(f.Inv.Hosts, leader.ID)
	peers := HostIDs(majority)
	if err := f.isolate(ctx, leader, peers); err != nil {
		return fmt.Errorf("isolate leader %d: %w", leader.ID, err)
	}
	f.isolatedLeaderID = leader.ID
	f.isolateLeaderNanos = f.event("isolate_leader",
		fmt.Sprintf("leader node %d blackholed from nodes %v in both directions; chaos state: %s", leader.ID, peers, f.chaosState(ctx, leader)),
		leader.ID, peers, 0)

	newLeader, elapsed, err := WaitLeader(ctx, majority, leader.ID, 30*time.Second)
	if err != nil {
		return fmt.Errorf("no new leader after isolating node %d: %w", leader.ID, err)
	}
	f.isolationElectionMs = float64(elapsed.Microseconds()) / 1000
	f.newLeaderAfterIsolate = newLeader.ID
	f.event("new_leader_elected", fmt.Sprintf("node %d elected while node %d was isolated", newLeader.ID, leader.ID), newLeader.ID, nil, elapsed)
	return nil
}

// restoreAll clears the chaos rules on every host and waits for the cluster
// to agree on one leader again.
func (f *FaultRunner) restoreAll(ctx context.Context) error {
	for _, h := range f.Inv.Hosts {
		if err := f.clearIsolation(ctx, h); err != nil {
			return fmt.Errorf("restore network on node %d: %w", h.ID, err)
		}
	}
	f.restoreNanos = f.event("restore_network", "every chaos isolation cleared on all three nodes", 0, nil, 0)
	leader, elapsed, err := f.waitAllAgreeOnLeader(ctx, 60*time.Second)
	if err != nil {
		return fmt.Errorf("cluster did not heal after restore: %w", err)
	}
	f.event("cluster_healed", fmt.Sprintf("all three nodes agree on leader node %d", leader), leader, nil, elapsed)
	return nil
}

// RestartAllAndVerify stops every node, starts it again from its persisted
// state, and reads back every acknowledged write with a linearizable read.
// It runs after the traffic phase.
func (f *FaultRunner) RestartAllAndVerify(ctx context.Context) error {
	start := time.Now()
	f.restartIssuedNanos = f.event("restart_all_stop", "SIGTERM to all three nodes", 0, nil, 0)
	if errs := f.Dep.StopAll(ctx); len(errs) > 0 {
		return fmt.Errorf("stop all: %v", errs)
	}
	if err := f.Dep.StartAll(ctx); err != nil {
		return fmt.Errorf("restart all: %w", err)
	}
	leader, _, err := f.waitAllAgreeOnLeader(ctx, 60*time.Second)
	if err != nil {
		return fmt.Errorf("cluster did not elect a leader after the full restart: %w", err)
	}
	ready := time.Since(start)
	f.restartReadyNanos = f.event("restart_all_ready",
		fmt.Sprintf("all three nodes restarted from disk and agree on leader node %d", leader), leader, nil, ready)

	f.Rec.SetPhase(PhaseVerify)
	mismatches, verified, err := f.Work.VerifyAcked(ctx, PhaseVerify)
	if err != nil {
		return fmt.Errorf("verify acknowledged writes: %w", err)
	}
	f.restartMismatches = mismatches
	f.restartVerifiedKeys = verified
	f.restartVerified = len(mismatches) == 0
	f.event("verify_acknowledged_writes",
		fmt.Sprintf("%d acknowledged writes re-read with linearizable reads; %d missing or changed", verified, len(mismatches)),
		0, nil, 0)
	return nil
}

// Report assembles the fault artifact, including every measurement derived
// from the recorded history.
func (f *FaultRunner) Report(mode, boundary string, hist *History) *FaultReport {
	m := Measurements{}
	ops := hist.Ops
	if f.killAtNanos > 0 {
		out := measureOutage(ops, "kill_leader", f.killAtNanos)
		m.LeaderKillWriteOutage = &out
		m.LeaderKillElection = &DurationMeasurement{
			Ms:     f.killElectionMs,
			Method: fmt.Sprintf("driver polled /status until a node other than the killed node %d reported itself leader", f.killedLeaderID),
		}
	}
	if f.isolateFollowerNanos > 0 {
		end := f.clearFollowerNanos
		if end == 0 {
			end = hist.lastReturnNanos()
		}
		p := measureProgress(ops, f.isolateFollowerNanos, end)
		m.FollowerIsolationProgress = &p
	}
	if f.isolateLeaderNanos > 0 {
		out := measureOutage(ops, "isolate_leader", f.isolateLeaderNanos)
		m.LeaderIsolationWriteOutage = &out
		m.LeaderIsolationElection = &DurationMeasurement{
			Ms:     f.isolationElectionMs,
			Method: fmt.Sprintf("driver polled /status on the majority until one of them reported itself leader while node %d was isolated", f.isolatedLeaderID),
		}
	}
	if f.restartReadyNanos > 0 {
		m.RestartReadyRecovery = &DurationMeasurement{
			Ms:     float64(f.restartReadyNanos-f.restartIssuedNanos) / 1e6,
			Method: "from the stop signal to all three nodes ready and agreeing on one leader",
		}
		if first := firstVerifyReadNanos(ops); first > 0 {
			m.RestartFirstReadRecovery = &DurationMeasurement{
				Ms:     float64(first-f.restartIssuedNanos) / 1e6,
				Method: "from the stop signal to the first successful linearizable read after the restart",
			}
		}
	}
	return &FaultReport{
		Mode:            mode,
		Boundary:        boundary,
		GeneratedAt:     time.Now().UTC().Format(time.RFC3339),
		Scenario:        f.Scenario,
		Executor:        f.Ex.Kind(),
		Events:          f.events,
		Measurements:    m,
		AckedWrites:     f.Work.Acked.Len(),
		AckedWriteLoss:  f.restartMismatches,
		VerifiedKeys:    f.restartVerifiedKeys,
		RestartVerified: f.restartVerified,
		Notes:           f.notes,
	}
}

// AckedWriteLoss reports the acknowledged writes that were missing after the
// full restart.
func (f *FaultRunner) AckedWriteLoss() []Mismatch { return f.restartMismatches }

// measureOutage is the externally observed write outage around an event: the
// gap between the last acknowledged write before it and the first
// acknowledged write after it.
func measureOutage(ops []OpRecord, event string, eventNanos int64) OutageMeasurement {
	out := OutageMeasurement{
		Event:        event,
		EventAtNanos: eventNanos,
		Method:       "client-observed: last acknowledged write returning before the event to the first acknowledged write returning after it",
	}
	var before, after int64
	after = -1
	for _, op := range ops {
		if op.Op != "put" || op.Status != StatusOK {
			continue
		}
		if op.ReturnNanos <= eventNanos {
			if op.ReturnNanos > before {
				before = op.ReturnNanos
			}
			continue
		}
		if after < 0 || op.ReturnNanos < after {
			after = op.ReturnNanos
		}
	}
	out.LastAckBeforeNanos = before
	if after < 0 {
		return out
	}
	out.FirstAckAfterNanos = after
	out.OutageMs = float64(after-before) / 1e6
	out.Complete = before > 0
	return out
}

// measureProgress counts acknowledged operations inside a fault window.
func measureProgress(ops []OpRecord, startNanos, endNanos int64) ProgressMeasurement {
	p := ProgressMeasurement{
		WindowStartNanos: startNanos,
		WindowEndNanos:   endNanos,
		WindowMs:         float64(endNanos-startNanos) / 1e6,
		Method:           "acknowledged client operations returning inside the fault window, measured by the driver",
	}
	for _, op := range ops {
		if op.Status != StatusOK || op.ReturnNanos < startNanos || op.ReturnNanos > endNanos {
			continue
		}
		if op.Op == "put" {
			p.AcknowledgedWrites++
		} else {
			p.AcknowledgedReads++
		}
	}
	if p.WindowMs > 0 {
		p.WritesPerSecond = float64(p.AcknowledgedWrites) / (p.WindowMs / 1000)
	}
	p.MadeProgress = p.AcknowledgedWrites > 0
	return p
}

func firstVerifyReadNanos(ops []OpRecord) int64 {
	var first int64 = -1
	for _, op := range ops {
		if op.Phase != PhaseVerify || op.Status != StatusOK {
			continue
		}
		if first < 0 || op.ReturnNanos < first {
			first = op.ReturnNanos
		}
	}
	return first
}

func (h *History) lastReturnNanos() int64 {
	var last int64
	for _, op := range h.Ops {
		if op.ReturnNanos > last {
			last = op.ReturnNanos
		}
	}
	return last
}
