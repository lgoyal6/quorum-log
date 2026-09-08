package sm

import "testing"

func TestApplyAndGet(t *testing.T) {
	s := New()
	s.Apply(Op{Type: OpPut, Key: "a", Val: "1", ClientID: "c1", ReqID: 1})
	if v, ok := s.Get("a"); !ok || v != "1" {
		t.Fatalf("got %q %v, want 1 true", v, ok)
	}
	s.Apply(Op{Type: OpAppend, Key: "a", Val: "2", ClientID: "c1", ReqID: 2})
	if v, _ := s.Get("a"); v != "12" {
		t.Fatalf("append: got %q, want 12", v)
	}
	s.Apply(Op{Type: OpDelete, Key: "a", ClientID: "c1", ReqID: 3})
	if _, ok := s.Get("a"); ok {
		t.Fatal("delete: key still present")
	}
}

func TestDuplicateAppliesOnce(t *testing.T) {
	s := New()
	op := Op{Type: OpAppend, Key: "k", Val: "x", ClientID: "c1", ReqID: 1}
	r1 := s.Apply(op)
	r2 := s.Apply(op) // retry of the same request
	if v, _ := s.Get("k"); v != "x" {
		t.Fatalf("duplicate applied twice: got %q, want x", v)
	}
	if r1 != r2 {
		t.Fatalf("cached result mismatch: %v vs %v", r1, r2)
	}
}

func TestSnapshotRestoreAndHash(t *testing.T) {
	s := New()
	s.Apply(Op{Type: OpPut, Key: "a", Val: "1", ClientID: "c1", ReqID: 1})
	s.Apply(Op{Type: OpAppend, Key: "b", Val: "zz", ClientID: "c2", ReqID: 5})
	snap := s.Snapshot()

	s2 := New()
	if err := s2.Restore(snap); err != nil {
		t.Fatal(err)
	}
	if s.Hash() != s2.Hash() {
		t.Fatalf("hash mismatch after restore: %x vs %x", s.Hash(), s2.Hash())
	}
	// Dedup state must survive the snapshot.
	s2.Apply(Op{Type: OpAppend, Key: "b", Val: "zz", ClientID: "c2", ReqID: 5})
	if v, _ := s2.Get("b"); v != "zz" {
		t.Fatalf("dedup lost across snapshot: got %q, want zz", v)
	}
}

func TestEncodeDecode(t *testing.T) {
	op := Op{Type: OpPut, Key: "k", Val: "v", ClientID: "c", ReqID: 9}
	got, err := DecodeOp(op.Encode())
	if err != nil || got != op {
		t.Fatalf("roundtrip: %v %v", got, err)
	}
}
