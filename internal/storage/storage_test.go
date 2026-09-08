package storage

import (
	"testing"

	"go.etcd.io/raft/v3/raftpb"
)

func ent(i, t uint64, data string) raftpb.Entry {
	return raftpb.Entry{Index: i, Term: t, Type: raftpb.EntryNormal, Data: []byte(data)}
}

func TestPersistAndReload(t *testing.T) {
	dir := t.TempDir()
	ds, hasState, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if hasState {
		t.Fatal("fresh dir reported state")
	}
	if err := ds.Append([]raftpb.Entry{ent(1, 1, "a"), ent(2, 1, "b"), ent(3, 1, "c")}); err != nil {
		t.Fatal(err)
	}
	if err := ds.SetHardState(raftpb.HardState{Term: 1, Vote: 1, Commit: 3}); err != nil {
		t.Fatal(err)
	}
	ds.Close()

	ds2, hasState, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !hasState {
		t.Fatal("reload reported no state")
	}
	hs, _, err := ds2.InitialState()
	if err != nil || hs.Commit != 3 || hs.Term != 1 {
		t.Fatalf("hardstate: %+v %v", hs, err)
	}
	last, _ := ds2.LastIndex()
	if last != 3 {
		t.Fatalf("last index %d, want 3", last)
	}
	es, err := ds2.Entries(1, 4, 1<<20)
	if err != nil || len(es) != 3 || string(es[2].Data) != "c" {
		t.Fatalf("entries: %v %v", es, err)
	}
	ds2.Close()
}

func TestTruncationReplay(t *testing.T) {
	dir := t.TempDir()
	ds, _, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	ds.Append([]raftpb.Entry{ent(1, 1, "a"), ent(2, 1, "b"), ent(3, 1, "c")})
	// Raft overwrites a conflicting suffix from index 2 at a higher term.
	ds.Append([]raftpb.Entry{ent(2, 2, "B"), ent(3, 2, "C"), ent(4, 2, "D")})
	ds.Close()

	ds2, _, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	es, err := ds2.Entries(1, 5, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(es) != 4 || es[1].Term != 2 || string(es[1].Data) != "B" {
		t.Fatalf("truncation not honored: %+v", es)
	}
	ds2.Close()
}

func TestSnapshotAndCompaction(t *testing.T) {
	dir := t.TempDir()
	ds, _, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	var es []raftpb.Entry
	for i := uint64(1); i <= 10; i++ {
		es = append(es, ent(i, 1, "x"))
	}
	ds.Append(es)
	ds.SetHardState(raftpb.HardState{Term: 1, Commit: 10})
	cs := &raftpb.ConfState{Voters: []uint64{1, 2, 3}}
	if _, err := ds.CreateSnapshot(8, cs, []byte("snapdata"), 2); err != nil {
		t.Fatal(err)
	}
	if sz, err := ds.SnapshotFileSize(); err != nil || sz == 0 {
		t.Fatalf("snapshot file: %d %v", sz, err)
	}
	ds.Close()

	ds2, _, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	snap, err := ds2.Snapshot()
	if err != nil || snap.Metadata.Index != 8 || string(snap.Data) != "snapdata" {
		t.Fatalf("snapshot: %+v %v", snap.Metadata, err)
	}
	// In-session the log kept trailing entries 7..10; after reload, entries
	// covered by the snapshot (<= 8) are dropped and the log resumes at 9.
	first, _ := ds2.FirstIndex()
	last, _ := ds2.LastIndex()
	if first != 9 || last != 10 {
		t.Fatalf("first=%d last=%d, want 9 and 10", first, last)
	}
	ds2.Close()
}

func TestInstalledSnapshotResetsLog(t *testing.T) {
	dir := t.TempDir()
	ds, _, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	ds.Append([]raftpb.Entry{ent(1, 1, "a"), ent(2, 1, "b")})
	snap := raftpb.Snapshot{
		Data: []byte("installed"),
		Metadata: raftpb.SnapshotMetadata{
			Index: 50, Term: 3,
			ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}},
		},
	}
	if err := ds.SaveSnapshot(snap); err != nil {
		t.Fatal(err)
	}
	ds.SetHardState(raftpb.HardState{Term: 3, Commit: 50})
	ds.Close()

	ds2, _, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ds2.Snapshot()
	if err != nil || got.Metadata.Index != 50 || string(got.Data) != "installed" {
		t.Fatalf("reloaded snapshot: %+v %v", got.Metadata, err)
	}
	first, _ := ds2.FirstIndex()
	last, _ := ds2.LastIndex()
	if first != 51 || last != 50 {
		t.Fatalf("log not reset: first=%d last=%d", first, last)
	}
	ds2.Close()
}
