// Package sm implements the deterministic replicated state machine: a small
// metadata/lease key-value store with client sessions for exactly-once
// application of retried requests.
package sm

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sort"
)

// Op types.
const (
	OpPut    = "put"
	OpAppend = "append"
	OpDelete = "delete"
)

// Op is a single client command. ClientID plus ReqID identify a request;
// a client must issue ReqIDs in increasing order and may retry a request
// with the same ReqID any number of times. The state machine applies each
// (ClientID, ReqID) pair at most once.
type Op struct {
	Type     string `json:"type"`
	Key      string `json:"key"`
	Val      string `json:"val,omitempty"`
	ClientID string `json:"client_id,omitempty"`
	ReqID    uint64 `json:"req_id,omitempty"`
}

// Result is the outcome of applying an Op.
type Result struct {
	Val string `json:"val"`
	OK  bool   `json:"ok"`
}

// Encode serializes an Op for a raft proposal.
func (o Op) Encode() []byte {
	b, err := json.Marshal(o)
	if err != nil {
		panic(fmt.Sprintf("sm: encode op: %v", err))
	}
	return b
}

// DecodeOp deserializes an Op from a raft entry.
func DecodeOp(data []byte) (Op, error) {
	var o Op
	err := json.Unmarshal(data, &o)
	return o, err
}

type session struct {
	LastReq uint64 `json:"last_req"`
	LastRes Result `json:"last_res"`
}

// SM is the state machine. It is not goroutine-safe; callers serialize access.
type SM struct {
	kv       map[string]string
	sessions map[string]session
}

// New returns an empty state machine.
func New() *SM {
	return &SM{kv: map[string]string{}, sessions: map[string]session{}}
}

// Apply applies op deterministically. A duplicate request (ReqID <= the
// client's last applied ReqID) is not re-applied; the cached result of the
// most recent request is returned instead.
func (s *SM) Apply(op Op) Result {
	if op.ClientID != "" {
		if sess, ok := s.sessions[op.ClientID]; ok && op.ReqID <= sess.LastReq {
			return sess.LastRes
		}
	}
	var res Result
	switch op.Type {
	case OpPut:
		s.kv[op.Key] = op.Val
		res = Result{Val: op.Val, OK: true}
	case OpAppend:
		s.kv[op.Key] += op.Val
		res = Result{Val: s.kv[op.Key], OK: true}
	case OpDelete:
		delete(s.kv, op.Key)
		res = Result{OK: true}
	default:
		res = Result{OK: false}
	}
	if op.ClientID != "" {
		s.sessions[op.ClientID] = session{LastReq: op.ReqID, LastRes: res}
	}
	return res
}

// Get reads a key from local state (no consensus involved).
func (s *SM) Get(key string) (string, bool) {
	v, ok := s.kv[key]
	return v, ok
}

// Len returns the number of keys.
func (s *SM) Len() int { return len(s.kv) }

type snapshotData struct {
	KV       map[string]string  `json:"kv"`
	Sessions map[string]session `json:"sessions"`
}

// Snapshot serializes the full state machine.
func (s *SM) Snapshot() []byte {
	b, err := json.Marshal(snapshotData{KV: s.kv, Sessions: s.sessions})
	if err != nil {
		panic(fmt.Sprintf("sm: snapshot: %v", err))
	}
	return b
}

// Restore replaces the state machine contents from a snapshot.
func (s *SM) Restore(data []byte) error {
	var sd snapshotData
	if err := json.Unmarshal(data, &sd); err != nil {
		return err
	}
	if sd.KV == nil {
		sd.KV = map[string]string{}
	}
	if sd.Sessions == nil {
		sd.Sessions = map[string]session{}
	}
	s.kv = sd.KV
	s.sessions = sd.Sessions
	return nil
}

// Hash returns a deterministic digest of the state (kv and sessions), used to
// detect replica divergence in tests.
func (s *SM) Hash() uint64 {
	h := fnv.New64a()
	keys := make([]string, 0, len(s.kv))
	for k := range s.kv {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(h, "k|%s|%s\n", k, s.kv[k])
	}
	cids := make([]string, 0, len(s.sessions))
	for c := range s.sessions {
		cids = append(cids, c)
	}
	sort.Strings(cids)
	for _, c := range cids {
		sess := s.sessions[c]
		fmt.Fprintf(h, "s|%s|%d|%s|%v\n", c, sess.LastReq, sess.LastRes.Val, sess.LastRes.OK)
	}
	return h.Sum64()
}
