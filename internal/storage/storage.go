// Package storage persists raft state to disk: an append-only entry log
// (wal.log), the raft hard state (hardstate.json), and the latest snapshot
// (snapshot.json). In memory it is backed by raft.MemoryStorage, which the
// raft state machine reads; the disk files exist so a node can rebuild that
// in-memory state after a restart.
package storage

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

const (
	walName       = "wal.log"
	hardStateName = "hardstate.json"
	snapshotName  = "snapshot.json"
)

// walRecord is one persisted log entry (JSON line in wal.log). Appending a
// record whose index is <= a previously written index means raft truncated
// the log; replay honors the later record and drops the conflicting suffix.
type walRecord struct {
	Index uint64 `json:"i"`
	Term  uint64 `json:"t"`
	Type  int32  `json:"y"`
	Data  string `json:"d,omitempty"`
}

type hardStateFile struct {
	Term   uint64 `json:"term"`
	Vote   uint64 `json:"vote"`
	Commit uint64 `json:"commit"`
}

type snapshotFile struct {
	Index    uint64   `json:"index"`
	Term     uint64   `json:"term"`
	Voters   []uint64 `json:"voters"`
	Learners []uint64 `json:"learners,omitempty"`
	Data     string   `json:"data"`
}

// DiskStorage implements raft.Storage (via the embedded MemoryStorage) and
// persists every mutation to dir. Not goroutine-safe beyond what
// MemoryStorage provides; the engine serializes writes.
type DiskStorage struct {
	*raft.MemoryStorage
	dir string
	wal *os.File
}

// Open loads (or initializes) storage in dir. hasState reports whether any
// persisted raft state was found, which callers use to decide whether to
// bootstrap a fresh node.
func Open(dir string) (ds *DiskStorage, hasState bool, err error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, false, err
	}
	ms := raft.NewMemoryStorage()
	ds = &DiskStorage{MemoryStorage: ms, dir: dir}

	var snapIndex uint64
	snap, ok, err := ds.loadSnapshotFile()
	if err != nil {
		return nil, false, fmt.Errorf("storage: load snapshot: %w", err)
	}
	if ok {
		hasState = true
		snapIndex = snap.Metadata.Index
		if err := ms.ApplySnapshot(snap); err != nil {
			return nil, false, err
		}
	}

	ents, found, err := replayWAL(filepath.Join(dir, walName), snapIndex)
	if err != nil {
		return nil, false, err
	}
	if found {
		hasState = true
	}
	if len(ents) > 0 {
		if ents[0].Index > snapIndex+1 {
			return nil, false, fmt.Errorf("storage: wal starts at %d but snapshot covers only up to %d: missing snapshot data", ents[0].Index, snapIndex)
		}
		if err := ms.Append(ents); err != nil {
			return nil, false, err
		}
	}

	hs, ok, err := ds.loadHardStateFile()
	if err != nil {
		return nil, false, fmt.Errorf("storage: load hardstate: %w", err)
	}
	if ok {
		hasState = true
		if err := ms.SetHardState(hs); err != nil {
			return nil, false, err
		}
	}

	f, err := os.OpenFile(filepath.Join(dir, walName), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, false, err
	}
	ds.wal = f
	return ds, hasState, nil
}

// Close closes the WAL file handle.
func (ds *DiskStorage) Close() error {
	if ds.wal != nil {
		return ds.wal.Close()
	}
	return nil
}

// Append persists entries to the WAL and the in-memory storage.
func (ds *DiskStorage) Append(entries []raftpb.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	w := bufio.NewWriter(ds.wal)
	enc := json.NewEncoder(w)
	for i := range entries {
		rec := walRecord{
			Index: entries[i].Index,
			Term:  entries[i].Term,
			Type:  int32(entries[i].Type),
			Data:  base64.StdEncoding.EncodeToString(entries[i].Data),
		}
		if err := enc.Encode(&rec); err != nil {
			return err
		}
	}
	if err := w.Flush(); err != nil {
		return err
	}
	if err := ds.wal.Sync(); err != nil {
		return err
	}
	return ds.MemoryStorage.Append(entries)
}

// SetHardState persists the hard state and updates in-memory storage.
func (ds *DiskStorage) SetHardState(hs raftpb.HardState) error {
	b, err := json.Marshal(hardStateFile{Term: hs.Term, Vote: hs.Vote, Commit: hs.Commit})
	if err != nil {
		return err
	}
	if err := atomicWrite(filepath.Join(ds.dir, hardStateName), b); err != nil {
		return err
	}
	return ds.MemoryStorage.SetHardState(hs)
}

// SaveSnapshot persists an incoming (installed) snapshot, resets the WAL, and
// replaces in-memory storage contents with the snapshot.
func (ds *DiskStorage) SaveSnapshot(snap raftpb.Snapshot) error {
	if err := ds.writeSnapshotFile(snap); err != nil {
		return err
	}
	if err := ds.resetWAL(nil); err != nil {
		return err
	}
	return ds.MemoryStorage.ApplySnapshot(snap)
}

// CreateSnapshot makes a snapshot at appliedIndex from local state, persists
// it, and compacts the log, retaining `trailing` entries behind the snapshot
// so slightly-lagging followers can still be caught up by log replication.
func (ds *DiskStorage) CreateSnapshot(appliedIndex uint64, cs *raftpb.ConfState, data []byte, trailing uint64) (raftpb.Snapshot, error) {
	snap, err := ds.MemoryStorage.CreateSnapshot(appliedIndex, cs, data)
	if err != nil {
		return snap, err
	}
	if err := ds.writeSnapshotFile(snap); err != nil {
		return snap, err
	}
	compactTo := uint64(0)
	if appliedIndex > trailing {
		compactTo = appliedIndex - trailing
	}
	first, _ := ds.MemoryStorage.FirstIndex()
	if compactTo >= first {
		if err := ds.MemoryStorage.Compact(compactTo); err != nil && err != raft.ErrCompacted {
			return snap, err
		}
	}
	// Rewrite the WAL keeping only entries still present in memory.
	firstKept, _ := ds.MemoryStorage.FirstIndex()
	last, _ := ds.MemoryStorage.LastIndex()
	var keep []raftpb.Entry
	if last >= firstKept {
		keep, err = ds.MemoryStorage.Entries(firstKept, last+1, 1<<30)
		if err != nil {
			return snap, err
		}
	}
	if err := ds.resetWAL(keep); err != nil {
		return snap, err
	}
	return snap, nil
}

// SnapshotFileSize returns the size in bytes of the persisted snapshot file.
func (ds *DiskStorage) SnapshotFileSize() (int64, error) {
	fi, err := os.Stat(filepath.Join(ds.dir, snapshotName))
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

func (ds *DiskStorage) writeSnapshotFile(snap raftpb.Snapshot) error {
	sf := snapshotFile{
		Index:    snap.Metadata.Index,
		Term:     snap.Metadata.Term,
		Voters:   snap.Metadata.ConfState.Voters,
		Learners: snap.Metadata.ConfState.Learners,
		Data:     base64.StdEncoding.EncodeToString(snap.Data),
	}
	b, err := json.Marshal(&sf)
	if err != nil {
		return err
	}
	return atomicWrite(filepath.Join(ds.dir, snapshotName), b)
}

func (ds *DiskStorage) loadSnapshotFile() (raftpb.Snapshot, bool, error) {
	b, err := os.ReadFile(filepath.Join(ds.dir, snapshotName))
	if os.IsNotExist(err) {
		return raftpb.Snapshot{}, false, nil
	}
	if err != nil {
		return raftpb.Snapshot{}, false, err
	}
	var sf snapshotFile
	if err := json.Unmarshal(b, &sf); err != nil {
		return raftpb.Snapshot{}, false, err
	}
	data, err := base64.StdEncoding.DecodeString(sf.Data)
	if err != nil {
		return raftpb.Snapshot{}, false, err
	}
	return raftpb.Snapshot{
		Data: data,
		Metadata: raftpb.SnapshotMetadata{
			Index:     sf.Index,
			Term:      sf.Term,
			ConfState: raftpb.ConfState{Voters: sf.Voters, Learners: sf.Learners},
		},
	}, true, nil
}

func (ds *DiskStorage) loadHardStateFile() (raftpb.HardState, bool, error) {
	b, err := os.ReadFile(filepath.Join(ds.dir, hardStateName))
	if os.IsNotExist(err) {
		return raftpb.HardState{}, false, nil
	}
	if err != nil {
		return raftpb.HardState{}, false, err
	}
	var hf hardStateFile
	if err := json.Unmarshal(b, &hf); err != nil {
		return raftpb.HardState{}, false, err
	}
	return raftpb.HardState{Term: hf.Term, Vote: hf.Vote, Commit: hf.Commit}, true, nil
}

// resetWAL atomically replaces wal.log with the given entries.
func (ds *DiskStorage) resetWAL(entries []raftpb.Entry) error {
	tmp := filepath.Join(ds.dir, walName+".tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	enc := json.NewEncoder(w)
	for i := range entries {
		rec := walRecord{
			Index: entries[i].Index,
			Term:  entries[i].Term,
			Type:  int32(entries[i].Type),
			Data:  base64.StdEncoding.EncodeToString(entries[i].Data),
		}
		if err := enc.Encode(&rec); err != nil {
			f.Close()
			return err
		}
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if ds.wal != nil {
		ds.wal.Close()
	}
	if err := os.Rename(tmp, filepath.Join(ds.dir, walName)); err != nil {
		return err
	}
	nf, err := os.OpenFile(filepath.Join(ds.dir, walName), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	ds.wal = nf
	return nil
}

// replayWAL reads wal.log, applying truncation semantics, and returns the
// surviving entries with index > snapIndex.
func replayWAL(path string, snapIndex uint64) (ents []raftpb.Entry, found bool, err error) {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	defer f.Close()
	found = true
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<26)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec walRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			// A torn final line (crash mid-append) is tolerated; anything
			// else is corruption.
			if !sc.Scan() {
				break
			}
			return nil, true, fmt.Errorf("storage: corrupt wal record: %w", err)
		}
		data, err := base64.StdEncoding.DecodeString(rec.Data)
		if err != nil {
			return nil, true, err
		}
		e := raftpb.Entry{Index: rec.Index, Term: rec.Term, Type: raftpb.EntryType(rec.Type), Data: data}
		// Truncation: drop any previously read entries at >= this index.
		for len(ents) > 0 && ents[len(ents)-1].Index >= e.Index {
			ents = ents[:len(ents)-1]
		}
		ents = append(ents, e)
	}
	if err := sc.Err(); err != nil {
		return nil, true, err
	}
	// Drop entries covered by the snapshot.
	for len(ents) > 0 && ents[0].Index <= snapIndex {
		ents = ents[1:]
	}
	return ents, found, nil
}

func atomicWrite(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
