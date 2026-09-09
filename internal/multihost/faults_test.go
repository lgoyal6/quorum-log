package multihost

import "testing"

func TestMeasureOutageSpansTheGapAroundTheEvent(t *testing.T) {
	ops := []OpRecord{
		{Op: "put", Status: StatusOK, CallNanos: 0, ReturnNanos: 1_000_000},     // 1 ms
		{Op: "put", Status: StatusOK, CallNanos: 1, ReturnNanos: 5_000_000},     // 5 ms
		{Op: "put", Status: StatusIndeterminate, ReturnNanos: 7_000_000},        // ignored
		{Op: "get", Status: StatusOK, ReturnNanos: 8_000_000},                   // reads do not count
		{Op: "put", Status: StatusOK, CallNanos: 2, ReturnNanos: 1_205_000_000}, // 1205 ms
	}
	out := measureOutage(ops, "isolate_leader", 6_000_000)
	if out.LastAckBeforeNanos != 5_000_000 {
		t.Fatalf("last ack before = %d, want 5000000", out.LastAckBeforeNanos)
	}
	if out.FirstAckAfterNanos != 1_205_000_000 {
		t.Fatalf("first ack after = %d, want 1205000000", out.FirstAckAfterNanos)
	}
	if out.OutageMs != 1200 {
		t.Fatalf("outage = %v ms, want 1200", out.OutageMs)
	}
	if !out.Complete {
		t.Fatal("outage should be marked complete when both ends exist")
	}
}

func TestMeasureOutageIncompleteWhenNoWriteEverSucceeded(t *testing.T) {
	ops := []OpRecord{{Op: "put", Status: StatusIndeterminate, ReturnNanos: 10}}
	out := measureOutage(ops, "isolate_leader", 5)
	if out.Complete {
		t.Fatal("an outage with no acknowledged write on either side must not be reported as complete")
	}
	if out.OutageMs != 0 {
		t.Fatalf("outage = %v, want 0", out.OutageMs)
	}
}

func TestMeasureProgressCountsOnlyTheWindow(t *testing.T) {
	ops := []OpRecord{
		{Op: "put", Status: StatusOK, ReturnNanos: 500},
		{Op: "put", Status: StatusOK, ReturnNanos: 1_500_000_000},
		{Op: "get", Status: StatusOK, ReturnNanos: 1_600_000_000},
		{Op: "put", Status: StatusIndeterminate, ReturnNanos: 1_700_000_000},
		{Op: "put", Status: StatusOK, ReturnNanos: 9_000_000_000},
	}
	p := measureProgress(ops, 1_000_000_000, 2_000_000_000)
	if p.AcknowledgedWrites != 1 || p.AcknowledgedReads != 1 {
		t.Fatalf("progress = %d writes / %d reads, want 1/1", p.AcknowledgedWrites, p.AcknowledgedReads)
	}
	if p.WindowMs != 1000 {
		t.Fatalf("window = %v ms, want 1000", p.WindowMs)
	}
	if p.WritesPerSecond != 1 {
		t.Fatalf("writes per second = %v, want 1", p.WritesPerSecond)
	}
	if !p.MadeProgress {
		t.Fatal("one acknowledged write in the window is progress")
	}
}

func TestMeasureProgressReportsNoProgress(t *testing.T) {
	ops := []OpRecord{{Op: "put", Status: StatusIndeterminate, ReturnNanos: 1_500_000_000}}
	p := measureProgress(ops, 1_000_000_000, 2_000_000_000)
	if p.MadeProgress {
		t.Fatal("an indeterminate write is not progress")
	}
}

func TestFirstVerifyReadNanos(t *testing.T) {
	ops := []OpRecord{
		{Phase: PhaseTraffic, Status: StatusOK, ReturnNanos: 10},
		{Phase: PhaseVerify, Status: StatusIndeterminate, ReturnNanos: 20},
		{Phase: PhaseVerify, Status: StatusOK, ReturnNanos: 40},
		{Phase: PhaseVerify, Status: StatusOK, ReturnNanos: 30},
	}
	if got := firstVerifyReadNanos(ops); got != 30 {
		t.Fatalf("firstVerifyReadNanos = %d, want 30", got)
	}
	if got := firstVerifyReadNanos(ops[:1]); got != -1 {
		t.Fatalf("firstVerifyReadNanos with no verify reads = %d, want -1", got)
	}
}
