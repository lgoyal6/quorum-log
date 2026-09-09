// Command qlmultihost is the driver for the multi-host gate.
//
// It reads an inventory of three hosts, collects their identities, deploys
// and starts one quorum-log node per host, drives client traffic while
// injecting faults, records an externally observed history, checks that
// history for linearizability, and writes the artifacts.
//
// Modes:
//
//	qlmultihost preflight -inventory inventories/example-three-hosts.json
//	qlmultihost run -inventory inventories/local-dry-run.json -out results/dry-run
//
// Exit codes:
//
//	0   the run completed and met every gate criterion (or, with
//	    -expect-reject, the planted fault was rejected as required)
//	1   the flow failed, or a criterion was not met
//	2   usage error
//	3   the flow completed but cannot produce multi-host evidence: the three
//	    hosts were not distinct, so this was a single-host dry run
//	10  (preflight only) the three hosts are not distinct
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"quorumlog/internal/multihost"
)

const (
	exitOK       = 0
	exitFailure  = 1
	exitUsage    = 2
	exitBlocked  = 3
	exitNotThree = 10
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: qlmultihost <preflight|run> [flags]")
		os.Exit(exitUsage)
	}
	switch os.Args[1] {
	case "preflight":
		os.Exit(runPreflight(os.Args[2:]))
	case "run":
		os.Exit(runGate(os.Args[2:]))
	default:
		fmt.Fprintf(os.Stderr, "unknown mode %q; want preflight or run\n", os.Args[1])
		os.Exit(exitUsage)
	}
}

func logf(format string, args ...interface{}) {
	fmt.Printf("qlmultihost: "+format+"\n", args...)
}

func runPreflight(args []string) int {
	fs := flag.NewFlagSet("preflight", flag.ExitOnError)
	inventory := fs.String("inventory", "", "inventory JSON file (required)")
	out := fs.String("out", "", "optional path for the inventory artifact")
	fs.Parse(args)
	if *inventory == "" {
		fmt.Fprintln(os.Stderr, "preflight: -inventory is required")
		return exitUsage
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	rep, err := multihost.PreflightOnly(ctx, multihost.Options{InventoryPath: *inventory}, *out)
	if err != nil {
		fmt.Fprintf(os.Stderr, "preflight: %v\n", err)
		return exitFailure
	}
	logf("mode=%s executor=%s distinct_hosts=%v", rep.Mode, rep.Executor, rep.DistinctHosts)
	for _, h := range rep.Hosts {
		logf("host %d target=%s hostname=%s machine_id=%s boot_id=%s device=%s",
			h.ID, h.Target, h.Hostname, h.MachineID, h.BootID, h.DataDevice)
		for _, p := range h.Problems {
			logf("host %d problem: %s", h.ID, p)
		}
	}
	for _, r := range rep.DistinctnessReasons {
		logf("distinctness: %s", r)
	}
	if *out != "" {
		logf("wrote %s", *out)
	}
	if !rep.DistinctHosts {
		return exitNotThree
	}
	return exitOK
}

func runGate(args []string) int {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	inventory := fs.String("inventory", "", "inventory JSON file (required)")
	out := fs.String("out", "", "directory for this run's artifacts (required)")
	scratch := fs.String("scratch", ".agent-work/multihost-gate", "local working directory for binaries, scripts, and node logs")
	repoRoot := fs.String("repo-root", ".", "repository root used to build quorumlogd")
	tags := fs.String("node-build-tags", "", "build tags for the node binary; set only for the planted-fault negative control")
	scenario := fs.String("scenario", multihost.ScenarioFull, "full or leader-isolation")
	ops := fs.Int("ops", 0, "override the inventory operation count (0 keeps it)")
	expectReject := fs.Bool("expect-reject", false, "negative control: succeed only when the harness rejects the history")
	timeout := fs.Duration("timeout", 15*time.Minute, "overall deadline for the run")
	fs.Parse(args)
	if *inventory == "" || *out == "" {
		fmt.Fprintln(os.Stderr, "run: -inventory and -out are required")
		return exitUsage
	}

	opts := multihost.Options{
		InventoryPath: *inventory,
		OutDir:        *out,
		ScratchDir:    *scratch,
		RepoRoot:      *repoRoot,
		BuildTags:     *tags,
		Scenario:      *scenario,
		OpsOverride:   *ops,
		ExpectReject:  *expectReject,
		Timeout:       *timeout,
		Logf:          logf,
	}
	res, err := multihost.Run(context.Background(), opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "qlmultihost: run failed: %v\n", err)
		return exitFailure
	}
	for _, path := range res.Artifacts {
		logf("wrote %s", path)
	}
	for _, c := range res.Criteria {
		status := "not met"
		if c.Met {
			status = "met"
		}
		logf("criterion %s: %s (%s)", c.Name, status, c.Detail)
	}

	if opts.ExpectReject {
		nc := res.NegativeControl
		fmt.Println(nc.FailingLine)
		if !nc.Rejected {
			return exitFailure
		}
		return exitOK
	}

	if !res.DistinctHosts {
		logf("mode %s: %s", res.Mode, res.BlockedReason)
		return exitBlocked
	}
	if !res.Passed {
		logf("gate failed: not every criterion was met")
		return exitFailure
	}
	logf("gate passed: three distinct hosts, every criterion met")
	return exitOK
}
