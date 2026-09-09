package multihost

import (
	"context"
	"strings"
	"testing"
)

func fakeFacts(machineIDs, hostnames, bootIDs [3]string) []HostFacts {
	facts := make([]HostFacts, 3)
	for i := range facts {
		facts[i] = HostFacts{
			ID:        uint64(i + 1),
			MachineID: machineIDs[i],
			Hostname:  hostnames[i],
			BootID:    bootIDs[i],
		}
	}
	return facts
}

func TestEvaluateDistinctAcceptsThreeMachines(t *testing.T) {
	facts := fakeFacts(
		[3]string{"m-aaa", "m-bbb", "m-ccc"},
		[3]string{"host-a", "host-b", "host-c"},
		[3]string{"boot-1", "boot-2", "boot-3"},
	)
	distinct, reasons := EvaluateDistinct(facts)
	if !distinct {
		t.Fatalf("three distinct machines rejected: %v", reasons)
	}
}

func TestEvaluateDistinctRejectsOneMachine(t *testing.T) {
	// What a single-host dry run looks like: one machine, one boot, one name.
	facts := fakeFacts(
		[3]string{"m-aaa", "m-aaa", "m-aaa"},
		[3]string{"host-a", "host-a", "host-a"},
		[3]string{"boot-1", "boot-1", "boot-1"},
	)
	distinct, reasons := EvaluateDistinct(facts)
	if distinct {
		t.Fatal("one machine described three times must not count as three hosts")
	}
	joined := strings.Join(reasons, "; ")
	for _, want := range []string{"machine identity", "hostname", "boot id"} {
		if !strings.Contains(joined, want) {
			t.Errorf("reasons %q do not mention %s", joined, want)
		}
	}
}

func TestEvaluateDistinctRejectsPartialOverlap(t *testing.T) {
	facts := fakeFacts(
		[3]string{"m-aaa", "m-bbb", "m-bbb"},
		[3]string{"host-a", "host-b", "host-c"},
		[3]string{"boot-1", "boot-2", "boot-3"},
	)
	if distinct, _ := EvaluateDistinct(facts); distinct {
		t.Fatal("two hosts sharing a machine identity must fail distinctness")
	}
}

func TestEvaluateDistinctRejectsMissingIdentity(t *testing.T) {
	facts := fakeFacts(
		[3]string{"m-aaa", "", "m-ccc"},
		[3]string{"host-a", "host-b", "host-c"},
		[3]string{"boot-1", "boot-2", "boot-3"},
	)
	distinct, reasons := EvaluateDistinct(facts)
	if distinct {
		t.Fatal("an unknown machine identity must fail distinctness rather than pass by default")
	}
	if !strings.Contains(strings.Join(reasons, "; "), "is empty") {
		t.Fatalf("reasons %v do not explain the missing identity", reasons)
	}
}

func TestEvaluateDistinctRejectsTooFewHosts(t *testing.T) {
	facts := fakeFacts(
		[3]string{"m-aaa", "m-bbb", "m-ccc"},
		[3]string{"host-a", "host-b", "host-c"},
		[3]string{"boot-1", "boot-2", "boot-3"},
	)
	if distinct, _ := EvaluateDistinct(facts[:2]); distinct {
		t.Fatal("two hosts must not satisfy a three-host gate")
	}
}

func TestParseDF(t *testing.T) {
	device, mount := parseDF("/dev/disk3s1s1  976490576 21230624 106061704    17%    /")
	if device != "/dev/disk3s1s1" || mount != "/" {
		t.Fatalf("parseDF = (%q, %q)", device, mount)
	}
	if d, m := parseDF("garbage"); d != "" || m != "" {
		t.Fatalf("parseDF of a short line = (%q, %q), want empty", d, m)
	}
}

// A dry run on this machine must report the dry-run mode, distinct_hosts
// false, and the dry-run boundary, no matter what the inventory says.
func TestPreflightDryRunIsNeverDistinct(t *testing.T) {
	dir := t.TempDir()
	inv := &Inventory{
		Mode: ModeLocalDryRun,
		Hosts: []Host{
			{ID: 1, Name: "local-1", Advertise: "127.0.0.1", RaftPort: 19101, ChaosPort: 19301, DataDir: dir + "/n1"},
			{ID: 2, Name: "local-2", Advertise: "127.0.0.1", RaftPort: 19102, ChaosPort: 19302, DataDir: dir + "/n2"},
			{ID: 3, Name: "local-3", Advertise: "127.0.0.1", RaftPort: 19103, ChaosPort: 19303, DataDir: dir + "/n3"},
		},
		Client: ClientPlan{Ops: 10, Concurrency: 1},
	}
	if err := inv.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	rep, err := Preflight(context.Background(), inv, &LocalExec{}, "test-inventory.json")
	if err != nil {
		t.Fatalf("Preflight: %v", err)
	}
	if rep.DistinctHosts {
		t.Fatal("a local dry run must never report distinct hosts")
	}
	if rep.Mode != ModeLabelDryRun {
		t.Fatalf("mode = %q, want %q", rep.Mode, ModeLabelDryRun)
	}
	if rep.Boundary != BoundaryDryRun {
		t.Fatalf("boundary = %q, want the dry-run boundary", rep.Boundary)
	}
	if len(rep.Hosts) != 3 {
		t.Fatalf("collected %d host facts, want 3", len(rep.Hosts))
	}
	for _, f := range rep.Hosts {
		if f.Hostname == "" || f.Kernel == "" {
			t.Fatalf("host %d facts incomplete: %+v", f.ID, f)
		}
		if f.DataDevice == "" {
			t.Fatalf("host %d: data device not identified: %+v", f.ID, f)
		}
	}
}
