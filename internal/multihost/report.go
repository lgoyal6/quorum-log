package multihost

import (
	"fmt"
	"strings"
)

// RenderReport writes the human-readable run report. Every claim in it says
// which kind of run produced it and how each number was obtained.
func RenderReport(inv *Inventory, res *Result) string {
	var b strings.Builder
	title := "Multi-host gate run"
	if res.Mode != ModeLabelMultiHost {
		title = "Multi-host gate harness: single-host dry run"
	}
	fmt.Fprintf(&b, "# %s\n\n", title)
	fmt.Fprintf(&b, "- Mode: `%s`\n", res.Mode)
	fmt.Fprintf(&b, "- Executor: `%s` (%s)\n", res.Inventory.Executor, executorNote(res.Inventory.Executor))
	fmt.Fprintf(&b, "- Inventory: `%s` (declared mode `%s`)\n", res.Inventory.InventoryPath, res.Inventory.InventoryMode)
	fmt.Fprintf(&b, "- Scenario: `%s`\n", res.Faults.Scenario)
	fmt.Fprintf(&b, "- Generated: %s\n", res.Inventory.GeneratedAt)
	fmt.Fprintf(&b, "- Distinct hosts: **%v**\n", res.DistinctHosts)
	fmt.Fprintf(&b, "- Gate result: **%s**\n", gateWord(res))
	if res.BlockedReason != "" {
		fmt.Fprintf(&b, "- Blocked reason: %s\n", res.BlockedReason)
	}
	if res.PlantedBuildTags != "" {
		fmt.Fprintf(&b, "- Node build tags: `%s` (planted-fault build, not the shipped code)\n", res.PlantedBuildTags)
	}
	fmt.Fprintf(&b, "\n## Boundary\n\n%s\n", res.Inventory.Boundary)

	b.WriteString("\n## Host inventory\n\n")
	b.WriteString("| node | target | hostname | machine identity | boot id | kernel | data device | build target |\n")
	b.WriteString("|---|---|---|---|---|---|---|---|\n")
	for _, h := range res.Inventory.Hosts {
		fmt.Fprintf(&b, "| %d | %s | %s | %s | %s | %s | %s | %s |\n",
			h.ID, code(h.Target), code(h.Hostname), code(shorten(h.MachineID, 40)), code(shorten(h.BootID, 40)),
			shorten(h.Kernel, 60), code(h.DataDevice), code(h.BuildTarget))
	}
	b.WriteString("\nDistinctness findings:\n\n")
	for _, r := range res.Inventory.DistinctnessReasons {
		fmt.Fprintf(&b, "- %s\n", r)
	}

	b.WriteString("\n## Operations\n\n")
	fmt.Fprintf(&b, "- Traffic phase: %d operations completed (target %d, concurrency %d, read fraction %.2f, seed %d)\n",
		res.TrafficOps, inv.Client.Ops, inv.Client.Concurrency, inv.Client.ReadFraction, inv.Client.Seed)
	fmt.Fprintf(&b, "- Recorded history: %d operations (%d writes, %d reads)\n", len(res.History.Ops), res.History.Writes, res.History.Reads)
	fmt.Fprintf(&b, "- Indeterminate: %d writes and %d reads never learned their outcome and are checked as indeterminate\n",
		res.History.IndeterminateWrites, res.History.IndeterminateReads)
	fmt.Fprintf(&b, "- Acknowledged writes tracked: %d\n", res.Faults.AckedWrites)
	fmt.Fprintf(&b, "- Acknowledged writes re-read after the full restart: %d (%d missing or changed)\n",
		res.Faults.VerifiedKeys, len(res.Faults.AckedWriteLoss))

	b.WriteString("\n## Fault timeline\n\n")
	b.WriteString("| at op | t+ms | event | node | detail | waited ms |\n")
	b.WriteString("|---|---|---|---|---|---|\n")
	for _, e := range res.Faults.Events {
		node := ""
		if e.NodeID != 0 {
			node = fmt.Sprint(e.NodeID)
		}
		waited := ""
		if e.DurationMs > 0 {
			waited = fmt.Sprintf("%.0f", e.DurationMs)
		}
		fmt.Fprintf(&b, "| %d | %.0f | `%s` | %s | %s | %s |\n",
			e.AtOp, float64(e.AtNanos)/1e6, e.Name, node, sanitizeCell(e.Detail), waited)
	}

	b.WriteString("\n## Measurements\n\n")
	m := res.Faults.Measurements
	if m.LeaderKillWriteOutage != nil {
		writeOutage(&b, "Write outage after SIGKILL of the leader", m.LeaderKillWriteOutage)
	}
	if m.LeaderKillElection != nil {
		fmt.Fprintf(&b, "- New leader after the kill: **%.0f ms** (%s)\n", m.LeaderKillElection.Ms, m.LeaderKillElection.Method)
	}
	if m.FollowerIsolationProgress != nil {
		p := m.FollowerIsolationProgress
		fmt.Fprintf(&b, "- Majority progress while one follower was isolated: **%d writes and %d reads acknowledged** over %.0f ms (%.1f writes/s); progress: **%v** (%s)\n",
			p.AcknowledgedWrites, p.AcknowledgedReads, p.WindowMs, p.WritesPerSecond, p.MadeProgress, p.Method)
	}
	if m.LeaderIsolationWriteOutage != nil {
		writeOutage(&b, "Write outage while the leader was isolated from the majority", m.LeaderIsolationWriteOutage)
	}
	if m.LeaderIsolationElection != nil {
		fmt.Fprintf(&b, "- New leader after isolating the leader: **%.0f ms** (%s)\n", m.LeaderIsolationElection.Ms, m.LeaderIsolationElection.Method)
	}
	if m.RestartReadyRecovery != nil {
		fmt.Fprintf(&b, "- Restart recovery to a ready cluster: **%.0f ms** (%s)\n", m.RestartReadyRecovery.Ms, m.RestartReadyRecovery.Method)
	}
	if m.RestartFirstReadRecovery != nil {
		fmt.Fprintf(&b, "- Restart recovery to the first successful linearizable read: **%.0f ms** (%s)\n",
			m.RestartFirstReadRecovery.Ms, m.RestartFirstReadRecovery.Method)
	}

	b.WriteString("\n## Linearizability\n\n")
	fmt.Fprintf(&b, "- Checker: %s\n", res.Check.Checker)
	fmt.Fprintf(&b, "- Model: %s\n", res.Check.Model)
	fmt.Fprintf(&b, "- Result: **%s** over %d operations in %.0f ms\n", res.Check.Result, res.Check.Ops, res.Check.CheckDurationMs)
	fmt.Fprintf(&b, "- %s\n", res.Check.Note)
	if len(res.Faults.AckedWriteLoss) > 0 {
		fmt.Fprintf(&b, "- Acknowledged writes missing after the restart: %d (a hard failure)\n", len(res.Faults.AckedWriteLoss))
		for i, mm := range res.Faults.AckedWriteLoss {
			if i >= 10 {
				fmt.Fprintf(&b, "  - ... and %d more\n", len(res.Faults.AckedWriteLoss)-10)
				break
			}
			fmt.Fprintf(&b, "  - `%s`: expected `%s`, observed `%s` (found=%v, status=%s)\n",
				mm.Key, mm.Expected, mm.ObservedVal, mm.ObservedFound, mm.Status)
		}
	}

	if res.NegativeControl != nil {
		b.WriteString("\n## Negative control\n\n")
		nc := res.NegativeControl
		fmt.Fprintf(&b, "- Planted fault: %s\n", nc.PlantedFault)
		fmt.Fprintf(&b, "- Build tags: `%s`\n", nc.BuildTags)
		fmt.Fprintf(&b, "- Expectation: %s\n", nc.Expectation)
		fmt.Fprintf(&b, "- Rejected: **%v**\n", nc.Rejected)
		for _, r := range nc.RejectionsFound {
			fmt.Fprintf(&b, "  - %s\n", r)
		}
		fmt.Fprintf(&b, "- %s\n", nc.FailingLine)
		fmt.Fprintf(&b, "- Restoration: %s\n", nc.Restoration)
	}

	b.WriteString("\n## Gate criteria\n\n")
	b.WriteString("| criterion | met | detail |\n|---|---|---|\n")
	for _, c := range res.Criteria {
		fmt.Fprintf(&b, "| `%s` | %s | %s |\n", c.Name, yesNo(c.Met), sanitizeCell(c.Detail))
	}

	if len(res.Faults.Notes) > 0 {
		b.WriteString("\n## Notes and judgment calls\n\n")
		for _, n := range res.Faults.Notes {
			fmt.Fprintf(&b, "- %s\n", n)
		}
	}

	b.WriteString("\n## What this run is and is not\n\n")
	if res.Mode == ModeLabelMultiHost {
		b.WriteString("Implemented and measured: every number above was measured by the driver against three distinct hosts over a real network.\n")
	} else {
		b.WriteString("Implemented, and measured only as a single-host dry run: the harness ran end to end, so the code paths for deployment, fault injection, history recording, and the linearizability check are exercised and their numbers are real measurements of this machine. They are not multi-host evidence. The gate stays blocked until three distinct authorized hosts are available, and no remote host was contacted by this run.\n")
	}
	return b.String()
}

func writeOutage(b *strings.Builder, label string, o *OutageMeasurement) {
	if !o.Complete {
		fmt.Fprintf(b, "- %s: not measurable (no acknowledged write on one side of the event)\n", label)
		return
	}
	fmt.Fprintf(b, "- %s: **%.0f ms** (last acknowledged write at t+%.0f ms, first acknowledged write after at t+%.0f ms; %s)\n",
		label, o.OutageMs, float64(o.LastAckBeforeNanos)/1e6, float64(o.FirstAckAfterNanos)/1e6, o.Method)
}

func executorNote(kind string) string {
	if kind == "ssh" {
		return "each host command ran on a separate machine over ssh"
	}
	return "every command ran locally on this one machine"
}

func gateWord(res *Result) string {
	switch {
	case res.NegativeControl != nil && res.NegativeControl.Rejected:
		return "negative control rejected the planted fault, as required"
	case res.NegativeControl != nil:
		return "negative control failed to fail"
	case res.Passed:
		return "passed"
	case res.BlockedReason != "":
		return "blocked"
	default:
		return "failed"
	}
}

func yesNo(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

func code(s string) string {
	if s == "" {
		return ""
	}
	return "`" + s + "`"
}

func shorten(s string, n int) string {
	s = strings.TrimSpace(strings.ReplaceAll(s, "\n", " "))
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

func sanitizeCell(s string) string {
	s = strings.ReplaceAll(s, "|", "/")
	return strings.ReplaceAll(s, "\n", " ")
}
