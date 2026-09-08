package check

import (
	"testing"

	"github.com/anishathalye/porcupine"

	"quorumlog/internal/sm"
)

func TestAcceptsSequentialHistory(t *testing.T) {
	var now int64
	r := NewRecorder(func() int64 { now++; return now })
	id := r.Invoke(0, KVInput{Op: sm.OpPut, Key: "a", Val: "1"})
	r.Return(id, KVOutput{Val: "1"})
	id = r.Invoke(0, KVInput{Op: "get", Key: "a"})
	r.Return(id, KVOutput{Val: "1", Found: true})
	id = r.Invoke(1, KVInput{Op: sm.OpAppend, Key: "a", Val: "2"})
	r.Return(id, KVOutput{Val: "12"})
	id = r.Invoke(1, KVInput{Op: "get", Key: "a"})
	r.Return(id, KVOutput{Val: "12", Found: true})
	if res := Check(r.History()); res != porcupine.Ok {
		t.Fatalf("expected Ok, got %v", res)
	}
}

func TestRejectsLostAcknowledgedWrite(t *testing.T) {
	// put(a,1) acked, then a later non-overlapping read sees nothing: the
	// acknowledged write was lost, which is not linearizable.
	hist := []porcupine.Operation{
		{ClientId: 0, Input: KVInput{Op: sm.OpPut, Key: "a", Val: "1"}, Call: 1, Output: KVOutput{Val: "1"}, Return: 2},
		{ClientId: 1, Input: KVInput{Op: "get", Key: "a"}, Call: 3, Output: KVOutput{Found: false}, Return: 4},
	}
	if res := Check(hist); res != porcupine.Illegal {
		t.Fatalf("expected Illegal, got %v", res)
	}
}

func TestRejectsDoubleApply(t *testing.T) {
	// One acked append of "x" but a read observes "xx": duplicate application.
	hist := []porcupine.Operation{
		{ClientId: 0, Input: KVInput{Op: sm.OpAppend, Key: "a", Val: "x"}, Call: 1, Output: KVOutput{Val: "x"}, Return: 2},
		{ClientId: 1, Input: KVInput{Op: "get", Key: "a"}, Call: 3, Output: KVOutput{Val: "xx", Found: true}, Return: 4},
	}
	if res := Check(hist); res != porcupine.Illegal {
		t.Fatalf("expected Illegal, got %v", res)
	}
}

func TestUnackedWriteMayOrMayNotApply(t *testing.T) {
	var now int64
	r := NewRecorder(func() int64 { now++; return now })
	pending := r.Invoke(0, KVInput{Op: sm.OpPut, Key: "a", Val: "1"})
	_ = pending // never returns
	id := r.Invoke(1, KVInput{Op: "get", Key: "a"})
	r.Return(id, KVOutput{Found: false}) // did not apply: fine
	if res := Check(r.History()); res != porcupine.Ok {
		t.Fatalf("expected Ok, got %v", res)
	}
}
