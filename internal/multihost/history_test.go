package multihost

import (
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"quorumlog/internal/check"
)

func TestPorcupineOpsAcceptsSaneHistory(t *testing.T) {
	h := &History{Ops: []OpRecord{
		{ClientID: 0, Op: "put", Key: "a", Value: "1", CallNanos: 10, ReturnNanos: 20, Status: StatusOK},
		{ClientID: 1, Op: "get", Key: "a", CallNanos: 30, ReturnNanos: 40, Status: StatusOK, ObservedVal: "1", ObservedFound: true},
	}}
	if res := check.Check(h.PorcupineOps()); res != porcupine.Ok {
		t.Fatalf("check = %v, want Ok", res)
	}
}

func TestPorcupineOpsRejectsLostAcknowledgedWrite(t *testing.T) {
	h := &History{Ops: []OpRecord{
		{ClientID: 0, Op: "put", Key: "a", Value: "1", CallNanos: 10, ReturnNanos: 20, Status: StatusOK},
		{ClientID: 1, Op: "get", Key: "a", CallNanos: 30, ReturnNanos: 40, Status: StatusOK, ObservedFound: false},
	}}
	if res := check.Check(h.PorcupineOps()); res != porcupine.Illegal {
		t.Fatalf("check = %v, want Illegal: an acknowledged write that vanished is not linearizable", res)
	}
}

// A write whose outcome the client never learned may or may not have
// committed, so a later read that does not see it is still linearizable.
func TestPorcupineOpsToleratesIndeterminateWrite(t *testing.T) {
	h := &History{Ops: []OpRecord{
		{ClientID: 0, Op: "put", Key: "a", Value: "1", CallNanos: 10, ReturnNanos: 20, Status: StatusIndeterminate},
		{ClientID: 1, Op: "get", Key: "a", CallNanos: 30, ReturnNanos: 40, Status: StatusOK, ObservedFound: false},
	}}
	ops := h.PorcupineOps()
	var indeterminate porcupine.Operation
	for _, op := range ops {
		if op.Output.(check.KVOutput).Unknown {
			indeterminate = op
		}
	}
	if indeterminate.Return <= 40 {
		t.Fatalf("indeterminate op return = %d, want it moved to the end of the history", indeterminate.Return)
	}
	if res := check.Check(ops); res != porcupine.Ok {
		t.Fatalf("check = %v, want Ok", res)
	}
}

func TestPorcupineOpsToleratesIndeterminateRead(t *testing.T) {
	h := &History{Ops: []OpRecord{
		{ClientID: 0, Op: "put", Key: "a", Value: "1", CallNanos: 10, ReturnNanos: 20, Status: StatusOK},
		{ClientID: 1, Op: "get", Key: "a", CallNanos: 30, ReturnNanos: 40, Status: StatusIndeterminate},
	}}
	if res := check.Check(h.PorcupineOps()); res != porcupine.Ok {
		t.Fatalf("check = %v, want Ok: a read that never returned constrains nothing", res)
	}
}

func TestRecorderOrdersAndCounts(t *testing.T) {
	base := time.Now()
	r := NewRecorder(base)
	r.Add(OpRecord{Op: "put", Key: "b", CallNanos: 50, ReturnNanos: 60, Status: StatusOK})
	r.Add(OpRecord{Op: "get", Key: "b", CallNanos: 10, ReturnNanos: 20, Status: StatusIndeterminate})
	r.SetPhase(PhaseVerify)
	r.Add(OpRecord{Op: "get", Key: "b", CallNanos: 70, ReturnNanos: 80, Status: StatusOK})

	hist := r.History(ModeLabelDryRun, BoundaryDryRun, []string{"http://127.0.0.1:9401"})
	if len(hist.Ops) != 3 {
		t.Fatalf("history has %d ops, want 3", len(hist.Ops))
	}
	if hist.Ops[0].CallNanos != 10 {
		t.Fatalf("history is not ordered by invocation: %+v", hist.Ops)
	}
	if hist.Writes != 1 || hist.Reads != 2 {
		t.Fatalf("counts = %d writes / %d reads, want 1/2", hist.Writes, hist.Reads)
	}
	if hist.IndeterminateReads != 1 || hist.IndeterminateWrites != 0 {
		t.Fatalf("indeterminate counts = %d reads / %d writes, want 1/0", hist.IndeterminateReads, hist.IndeterminateWrites)
	}
	if hist.Ops[2].Phase != PhaseVerify {
		t.Fatalf("phase = %q, want %q", hist.Ops[2].Phase, PhaseVerify)
	}
	if hist.Mode != ModeLabelDryRun || hist.Boundary != BoundaryDryRun {
		t.Fatal("history must carry its mode and boundary statement")
	}
}

func TestAckedWrites(t *testing.T) {
	a := NewAckedWrites()
	if a.Sample(0) != "" {
		t.Fatal("an empty set must sample nothing")
	}
	a.Record("k1", "v1")
	a.Record("k2", "v2")
	a.Record("k1", "v1")
	if a.Len() != 2 {
		t.Fatalf("Len = %d, want 2", a.Len())
	}
	all := a.All()
	if all[0].Key != "k1" || all[1].Key != "k2" {
		t.Fatalf("All = %+v, want write order", all)
	}
	if s := a.Sample(3); s != "k2" {
		t.Fatalf("Sample(3) = %q, want k2", s)
	}
}

func TestLeaderFromLocation(t *testing.T) {
	if got := leaderFromLocation("http://10.0.0.12:9101/kv/hello"); got != "http://10.0.0.12:9101" {
		t.Fatalf("leaderFromLocation = %q", got)
	}
	if got := leaderFromLocation("garbage"); got != "" {
		t.Fatalf("leaderFromLocation(garbage) = %q, want empty", got)
	}
}
