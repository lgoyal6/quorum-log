package main

import (
	"os/exec"
	"path/filepath"
	"testing"
)

// Exercise the actual executable/API path. All processes and listeners belong
// to this test, and all persisted data is under its disposable directory.
func TestOwnedHTTPClusterFaults(t *testing.T) {
	if testing.Short() {
		t.Skip("real local child-process integration")
	}
	binary := filepath.Join(t.TempDir(), "quorumlogd")
	build := exec.Command("go", "build", "-race", "-o", binary, "../quorumlogd")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("node build: %v: %s", err, out)
	}
	c := newCluster(3, binary, t.TempDir())
	defer c.close()
	for seed := 0; seed < 6; seed++ {
		row := runSchedule(c, seed)
		if row.Verdict == "VIOLATION" {
			t.Fatalf("%s: illegal history: %+v", row.Fault, row)
		}
		if !row.FinalReadAcknowledged {
			t.Fatalf("%s: no definitive recovery read", row.Fault)
		}
		if seed == 2 && row.PartitionedMessages == 0 {
			t.Fatal("partition did not intercept messages")
		}
		if seed == 3 && row.DelayedMessages == 0 {
			t.Fatal("delay did not intercept messages")
		}
		if seed == 4 && row.DroppedCommittedResponses != 1 {
			t.Fatal("committed response was not dropped")
		}
		if (seed == 1 || seed == 4 || seed == 5) && row.KilledProcesses != 1 {
			t.Fatal("expected process kill did not fire")
		}
		if seed == 5 && row.SnapshotsAdvanced == 0 {
			t.Fatal("no snapshot advanced during follower restart workload")
		}
		t.Logf("fault=%s verdict=%s ack=%d unknown=%d snapshots_advanced=%d", row.Fault, row.Verdict, row.Acknowledged, row.Unknown, row.SnapshotsAdvanced)
	}
}
