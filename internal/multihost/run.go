package multihost

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Options configures one gate run.
type Options struct {
	InventoryPath string
	OutDir        string        // where the artifacts of this run belong
	ScratchDir    string        // local working directory (never committed)
	RepoRoot      string        // repository root, for go build
	BuildTags     string        // empty for the shipped build
	Scenario      string        // ScenarioFull or ScenarioLeaderIsolation
	OpsOverride   int           // 0 keeps the inventory's operation count
	ExpectReject  bool          // negative control: the history must be rejected
	Timeout       time.Duration // overall deadline for the run
	Logf          func(format string, args ...interface{})
}

// Criterion is one gate requirement and whether this run met it.
type Criterion struct {
	Name   string `json:"name"`
	Met    bool   `json:"met"`
	Detail string `json:"detail"`
}

// NegativeControlReport is written as negative-control.json.
type NegativeControlReport struct {
	Mode            string     `json:"mode"`
	Boundary        string     `json:"boundary"`
	GeneratedAt     string     `json:"generated_at"`
	BuildTags       string     `json:"build_tags"`
	Scenario        string     `json:"scenario"`
	PlantedFault    string     `json:"planted_fault"`
	Expectation     string     `json:"expectation"`
	Rejected        bool       `json:"rejected"`
	RejectionsFound []string   `json:"rejections_found"`
	PorcupineResult string     `json:"porcupine_result"`
	Ops             int        `json:"ops"`
	AckedWrites     int        `json:"acknowledged_writes"`
	LostWrites      int        `json:"lost_acknowledged_writes"`
	LostSample      []Mismatch `json:"lost_acknowledged_write_sample,omitempty"`
	FailingLine     string     `json:"failing_line"`
	Restoration     string     `json:"restoration"`
}

// Result is the outcome of one run.
type Result struct {
	Mode             string                 `json:"mode"`
	DistinctHosts    bool                   `json:"distinct_hosts"`
	Passed           bool                   `json:"passed"`
	BlockedReason    string                 `json:"blocked_reason,omitempty"`
	Criteria         []Criterion            `json:"criteria"`
	Artifacts        []string               `json:"artifacts"`
	Rejected         bool                   `json:"rejected,omitempty"`
	RejectionsFound  []string               `json:"rejections_found,omitempty"`
	Inventory        *InventoryReport       `json:"-"`
	History          *History               `json:"-"`
	Faults           *FaultReport           `json:"-"`
	Check            *CheckReport           `json:"-"`
	NegativeControl  *NegativeControlReport `json:"-"`
	TrafficOps       int                    `json:"traffic_ops"`
	VerificationOps  int                    `json:"verification_ops"`
	NodeProcessPids  map[uint64]int         `json:"node_process_pids"`
	PlantedBuildTags string                 `json:"planted_build_tags,omitempty"`
}

func (o *Options) logf(format string, args ...interface{}) {
	if o.Logf != nil {
		o.Logf(format, args...)
	}
}

// PreflightOnly collects host facts and writes the inventory artifact
// without touching the cluster. The gate script calls this first to decide
// whether a run can produce multi-host evidence at all.
func PreflightOnly(ctx context.Context, opts Options, outPath string) (*InventoryReport, error) {
	inv, err := LoadInventory(opts.InventoryPath)
	if err != nil {
		return nil, err
	}
	ex, err := NewExecutor(inv)
	if err != nil {
		return nil, err
	}
	repoRoot := opts.RepoRoot
	if repoRoot == "" {
		repoRoot = "."
	}
	rep, err := Preflight(ctx, inv, ex, opts.InventoryPath, repoRoot)
	if err != nil {
		return nil, err
	}
	if outPath != "" {
		if err := writeJSONFile(outPath, rep); err != nil {
			return nil, err
		}
	}
	return rep, nil
}

// Run performs the whole flow: preflight, deploy, traffic with faults,
// restart verification, linearizability check, and artifacts.
func Run(ctx context.Context, opts Options) (*Result, error) {
	if opts.Timeout == 0 {
		opts.Timeout = 15 * time.Minute
	}
	if opts.Scenario == "" {
		opts.Scenario = ScenarioFull
	}
	ctx, cancel := context.WithTimeout(ctx, opts.Timeout)
	defer cancel()

	inv, err := LoadInventory(opts.InventoryPath)
	if err != nil {
		return nil, err
	}
	var scaleNote string
	if opts.OpsOverride > 0 && opts.OpsOverride != inv.Client.Ops {
		scaleNote = scaleFaultPlan(inv, opts.OpsOverride)
		inv.Client.Ops = opts.OpsOverride
	}
	ex, err := NewExecutor(inv)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(opts.OutDir, 0o755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(opts.ScratchDir, 0o755); err != nil {
		return nil, err
	}

	opts.logf("preflight: collecting host identities with the %s executor", ex.Kind())
	invReport, err := Preflight(ctx, inv, ex, opts.InventoryPath, opts.RepoRoot)
	if err != nil {
		return nil, err
	}
	mode := invReport.Mode
	boundary := invReport.Boundary
	opts.logf("preflight: mode=%s distinct_hosts=%v", mode, invReport.DistinctHosts)
	for _, reason := range invReport.DistinctnessReasons {
		opts.logf("preflight: %s", reason)
	}

	if err := requireCurl(ctx, inv, ex); err != nil {
		return nil, err
	}

	dep, err := NewDeployment(inv, ex, opts.ScratchDir, opts.RepoRoot, opts.BuildTags)
	if err != nil {
		return nil, err
	}
	tags := opts.BuildTags
	if tags == "" {
		tags = "(none: the shipped build)"
	}
	opts.logf("deploy: building quorumlogd with tags %s", tags)
	if err := dep.Build(ctx); err != nil {
		return nil, err
	}
	for _, h := range inv.Hosts {
		if err := cleanNodeState(ctx, ex, h); err != nil {
			return nil, err
		}
		if err := dep.Deploy(ctx, h); err != nil {
			return nil, err
		}
		opts.logf("deploy: node %d on %s (binary and start script only)", h.ID, h.Target())
	}

	defer func() {
		collectNodeLogs(context.Background(), dep, inv, opts)
		if errs := dep.StopAll(context.Background()); len(errs) > 0 {
			for _, e := range errs {
				opts.logf("teardown: %v", e)
			}
		}
		opts.logf("teardown: stopped the node processes this run started: %v", dep.Pids())
	}()

	if err := dep.StartAll(ctx); err != nil {
		return nil, fmt.Errorf("start cluster: %w", err)
	}
	opts.logf("cluster: three node processes started, pids %v", dep.Pids())
	leaderHost, _, err := WaitLeader(ctx, inv.Hosts, 0, 60*time.Second)
	if err != nil {
		return nil, fmt.Errorf("no initial leader: %w", err)
	}
	opts.logf("cluster: initial leader is node %d", leaderHost.ID)

	rec := NewRecorder(time.Now())
	acked := NewAckedWrites()
	work := NewWorkload(inv, rec, acked)
	faults := &FaultRunner{Inv: inv, Dep: dep, Ex: ex, Rec: rec, Work: work, Scenario: opts.Scenario}
	if opts.BuildTags != "" {
		faults.Note("node binary built with build tags %q; this is a planted-fault build, not the shipped code", opts.BuildTags)
	}
	faults.Note("client traffic and fault injection both ran from this driver; every timestamp is on the driver's clock")
	if scaleNote != "" {
		faults.Note("%s", scaleNote)
	}

	faultsDone := make(chan struct{})
	faultErr := make(chan error, 1)
	go func() {
		faultErr <- faults.Run(ctx, faultsDone)
	}()

	opts.logf("traffic: %d operations, %.0f%% reads, concurrency %d", inv.Client.Ops, inv.Client.ReadFraction*100, inv.Client.Concurrency)
	if err := work.Run(ctx, faultsDone); err != nil {
		return nil, err
	}
	trafficOps := work.Completed()
	opts.logf("traffic: %d operations completed, %d acknowledged writes", trafficOps, acked.Len())
	if err := <-faultErr; err != nil {
		return nil, fmt.Errorf("fault injection: %w", err)
	}

	if inv.Faults.RestartAllAfter {
		opts.logf("restart: stopping and restarting all three node processes from disk")
		if err := faults.RestartAllAndVerify(ctx); err != nil {
			return nil, err
		}
		loss := faults.AckedWriteLoss()
		opts.logf("restart: re-read %d acknowledged writes, %d missing or changed", acked.Len(), len(loss))
	}

	hist := rec.History(mode, boundary, work.Endpoints())
	faultReport := faults.Report(mode, boundary, hist)
	checkReport := RunCheck(hist, mode, boundary, faults.AckedWriteLoss(), faultReport.VerifiedKeys)
	opts.logf("check: porcupine result %s over %d operations in %.0f ms", checkReport.Result, checkReport.Ops, checkReport.CheckDurationMs)

	res := &Result{
		Mode:             mode,
		DistinctHosts:    invReport.DistinctHosts,
		Inventory:        invReport,
		History:          hist,
		Faults:           faultReport,
		Check:            checkReport,
		TrafficOps:       trafficOps,
		VerificationOps:  faultReport.VerifiedKeys,
		NodeProcessPids:  dep.Pids(),
		PlantedBuildTags: opts.BuildTags,
	}

	if opts.ExpectReject {
		res.NegativeControl = buildNegativeControl(opts, mode, boundary, hist, checkReport, faults)
		res.Rejected = res.NegativeControl.Rejected
		res.RejectionsFound = res.NegativeControl.RejectionsFound
	}
	res.Criteria = evaluateCriteria(inv, res, faultReport, checkReport)
	res.Passed = allMet(res.Criteria) && invReport.DistinctHosts
	if !invReport.DistinctHosts {
		res.BlockedReason = BlockedReasonDryRun
	}

	reportMD := RenderReport(inv, res)
	artifacts, err := writeArtifacts(opts, mode, res, reportMD)
	if err != nil {
		return nil, err
	}
	res.Artifacts = artifacts
	return res, nil
}

// scaleFaultPlan keeps the fault schedule inside a shortened run: the
// thresholds are scaled by the same ratio as the operation count, so a
// smaller run still injects every fault at the same point in the workload.
func scaleFaultPlan(inv *Inventory, newOps int) string {
	oldOps := inv.Client.Ops
	if oldOps <= 0 {
		return ""
	}
	scale := func(v int) int {
		if v <= 0 {
			return v
		}
		scaled := v * newOps / oldOps
		if scaled < 1 {
			scaled = 1
		}
		return scaled
	}
	before := inv.Faults
	inv.Faults.KillLeaderAtOp = scale(inv.Faults.KillLeaderAtOp)
	inv.Faults.IsolateFollowerAtOp = scale(inv.Faults.IsolateFollowerAtOp)
	inv.Faults.IsolateLeaderAtOp = scale(inv.Faults.IsolateLeaderAtOp)
	inv.Faults.RestoreAtOp = scale(inv.Faults.RestoreAtOp)
	return fmt.Sprintf("operation count overridden from %d to %d, so the fault schedule was scaled from %d/%d/%d/%d to %d/%d/%d/%d (kill leader, isolate follower, isolate leader, restore)",
		oldOps, newOps,
		before.KillLeaderAtOp, before.IsolateFollowerAtOp, before.IsolateLeaderAtOp, before.RestoreAtOp,
		inv.Faults.KillLeaderAtOp, inv.Faults.IsolateFollowerAtOp, inv.Faults.IsolateLeaderAtOp, inv.Faults.RestoreAtOp)
}

// requireCurl fails early when a host cannot reach its own chaos endpoint.
func requireCurl(ctx context.Context, inv *Inventory, ex Executor) error {
	for _, h := range inv.Hosts {
		if _, err := ex.Run(ctx, h, "command -v curl >/dev/null 2>&1"); err != nil {
			return fmt.Errorf("host %s has no curl; the driver drives the loopback chaos endpoint with curl on the host itself", h.Target())
		}
	}
	return nil
}

// cleanNodeState removes only the files a previous run of this harness
// created, so a run always starts from an empty log without ever deleting a
// directory wholesale.
func cleanNodeState(ctx context.Context, ex Executor, h Host) error {
	script := fmt.Sprintf("mkdir -p %s && rm -rf %s/raft %s/peers.json %s/peers.json.tmp %s/node.log %s/node.pid",
		h.DataDir, h.DataDir, h.DataDir, h.DataDir, h.DataDir, h.DataDir)
	_, err := ex.Run(ctx, h, script)
	return err
}

// collectNodeLogs copies the tail of every node log into the scratch
// directory so a failure can be diagnosed after teardown.
func collectNodeLogs(ctx context.Context, dep *Deployment, inv *Inventory, opts Options) {
	for _, h := range inv.Hosts {
		out := dep.TailLog(ctx, h, 200)
		if out == "" {
			continue
		}
		path := filepath.Join(opts.ScratchDir, fmt.Sprintf("node-%d.log", h.ID))
		os.WriteFile(path, []byte(out), 0o644)
	}
}

func buildNegativeControl(opts Options, mode, boundary string, hist *History, chk *CheckReport, faults *FaultRunner) *NegativeControlReport {
	loss := faults.AckedWriteLoss()
	var reasons []string
	if chk.Result == "Illegal" {
		reasons = append(reasons, "porcupine rejected the externally observed history (result Illegal)")
	}
	if len(loss) > 0 {
		reasons = append(reasons, fmt.Sprintf("%d acknowledged writes were missing or changed when re-read after the restart", len(loss)))
	}
	sample := loss
	if len(sample) > 10 {
		sample = sample[:10]
	}
	rep := &NegativeControlReport{
		Mode:            mode,
		Boundary:        boundary,
		GeneratedAt:     time.Now().UTC().Format(time.RFC3339),
		BuildTags:       opts.BuildTags,
		Scenario:        opts.Scenario,
		PlantedFault:    "engine applies a proposed entry on the leader before quorum commit, so the client is acknowledged before the entry is committed",
		Expectation:     "the harness must reject this history: either porcupine reports Illegal or an acknowledged write is missing afterwards",
		Rejected:        len(reasons) > 0,
		RejectionsFound: reasons,
		PorcupineResult: chk.Result,
		Ops:             chk.Ops,
		AckedWrites:     faults.Work.Acked.Len(),
		LostWrites:      len(loss),
		LostSample:      sample,
		Restoration:     "the planted fault exists only behind the build tag; the shipped build excludes it and is rebuilt and rerun immediately after this control",
	}
	if rep.Rejected {
		rep.FailingLine = fmt.Sprintf("NEGATIVE CONTROL REJECTED THE PLANTED FAULT: %s", strings.Join(reasons, "; "))
	} else {
		rep.FailingLine = "NEGATIVE CONTROL FAILED TO FAIL: the planted apply-before-quorum build produced a history the harness accepted"
	}
	return rep
}

func evaluateCriteria(inv *Inventory, res *Result, faults *FaultReport, chk *CheckReport) []Criterion {
	var out []Criterion
	add := func(name string, met bool, format string, args ...interface{}) {
		out = append(out, Criterion{Name: name, Met: met, Detail: fmt.Sprintf(format, args...)})
	}

	add("three_distinct_hosts", res.DistinctHosts,
		"machine identity, hostname, and boot id distinct across all three hosts: %v", res.DistinctHosts)
	add("at_least_1000_mixed_operations", res.TrafficOps >= 1000,
		"%d client operations completed during the traffic phase (%d writes, %d reads recorded in the history)",
		res.TrafficOps, res.History.Writes, res.History.Reads)

	if res.Faults.Scenario == ScenarioFull {
		p := faults.Measurements.FollowerIsolationProgress
		met := p != nil && p.MadeProgress
		detail := "follower isolation was not part of this scenario"
		if p != nil {
			detail = fmt.Sprintf("%d writes and %d reads acknowledged during the %.0f ms the follower was isolated (%.1f writes/s)",
				p.AcknowledgedWrites, p.AcknowledgedReads, p.WindowMs, p.WritesPerSecond)
		}
		add("majority_progress_while_follower_isolated", met, "%s", detail)

		k := faults.Measurements.LeaderKillWriteOutage
		metKill := k != nil && k.Complete
		detailKill := "the leader was not killed in this scenario"
		if k != nil {
			detailKill = fmt.Sprintf("write outage after SIGKILL of the leader: %.0f ms", k.OutageMs)
		}
		add("leader_kill_outage_recorded", metKill, "%s", detailKill)
	}

	i := faults.Measurements.LeaderIsolationWriteOutage
	metIso := i != nil && i.Complete
	detailIso := "the leader was not isolated in this scenario"
	if i != nil {
		detailIso = fmt.Sprintf("write outage while the leader was isolated from the majority: %.0f ms", i.OutageMs)
	}
	add("leader_isolation_outage_recorded", metIso, "%s", detailIso)

	e := faults.Measurements.LeaderIsolationElection
	add("new_leader_elected_after_isolation", e != nil && e.Ms > 0,
		"new leader observed %.0f ms after the isolation", durOrZero(e))

	if inv.Faults.RestartAllAfter {
		add("no_acknowledged_write_lost", faults.RestartVerified && len(faults.AckedWriteLoss) == 0,
			"%d acknowledged writes re-read with linearizable reads after restarting every node from disk; %d missing or changed",
			faults.VerifiedKeys, len(faults.AckedWriteLoss))
		add("restart_recovery_recorded", faults.Measurements.RestartReadyRecovery != nil,
			"recovery to a ready cluster: %.0f ms; to the first successful linearizable read: %.0f ms",
			durOrZero(faults.Measurements.RestartReadyRecovery), durOrZero(faults.Measurements.RestartFirstReadRecovery))
	}

	add("history_linearizable", chk.Linearizable,
		"porcupine result %s over %d operations in %.0f ms", chk.Result, chk.Ops, chk.CheckDurationMs)
	return out
}

func durOrZero(d *DurationMeasurement) float64 {
	if d == nil {
		return 0
	}
	return d.Ms
}

func allMet(cs []Criterion) bool {
	for _, c := range cs {
		if !c.Met {
			return false
		}
	}
	return true
}

// artifactNames maps the artifact kinds to file names for a mode. A dry run
// never writes the multihost-* names, so a reader cannot mistake harness
// validation for multi-host evidence.
func artifactNames(mode string) map[string]string {
	if mode == ModeLabelMultiHost {
		return map[string]string{
			"inventory": "multihost-inventory.json",
			"history":   "multihost-history.json",
			"faults":    "multihost-faults.json",
			"porcupine": "multihost-porcupine.json",
			"report":    "multihost-report.md",
		}
	}
	return map[string]string{
		"inventory": "inventory.json",
		"history":   "history.json",
		"faults":    "faults.json",
		"porcupine": "porcupine.json",
		"report":    "report.md",
	}
}

func writeArtifacts(opts Options, mode string, res *Result, reportMD string) ([]string, error) {
	names := artifactNames(mode)
	// The negative control is evidence, not a result: its bulky artifacts go
	// to the scratch directory and only its verdict lands in the out
	// directory.
	dir := opts.OutDir
	if opts.ExpectReject {
		dir = opts.ScratchDir
	}
	var written []string
	write := func(path string, v interface{}) error {
		if err := writeJSONFile(path, v); err != nil {
			return err
		}
		written = append(written, path)
		return nil
	}
	if err := write(filepath.Join(dir, names["inventory"]), res.Inventory); err != nil {
		return nil, err
	}
	if err := write(filepath.Join(dir, names["history"]), res.History); err != nil {
		return nil, err
	}
	if err := write(filepath.Join(dir, names["faults"]), res.Faults); err != nil {
		return nil, err
	}
	if err := write(filepath.Join(dir, names["porcupine"]), res.Check); err != nil {
		return nil, err
	}
	reportPath := filepath.Join(dir, names["report"])
	if err := os.WriteFile(reportPath, []byte(reportMD), 0o644); err != nil {
		return nil, err
	}
	written = append(written, reportPath)
	if res.NegativeControl != nil {
		path := filepath.Join(opts.OutDir, "negative-control.json")
		if err := write(path, res.NegativeControl); err != nil {
			return nil, err
		}
	}
	return written, nil
}

func writeJSONFile(path string, v interface{}) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return os.WriteFile(path, b, 0o644)
}
