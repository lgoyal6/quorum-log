// Package server is the production runtime around the engine: a mutex-
// serialized engine driven by a wall-clock ticker, an HTTP transport for
// raft messages between peers, and an HTTP client API (KV operations,
// linearizable and local reads, membership changes, status).
package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"quorumlog/internal/engine"
	"quorumlog/internal/sm"
)

// Config configures a server node.
type Config struct {
	ID                uint64
	DataDir           string
	ListenAddr        string            // host:port for both client API and raft transport
	Peers             map[uint64]string // initial peer URLs (id -> http://host:port), including self
	Join              bool              // join an existing cluster (no bootstrap)
	SnapshotThreshold uint64
	SnapshotTrailing  uint64
	TickInterval      time.Duration
}

// Server drives one node.
type Server struct {
	cfg Config

	mu    sync.Mutex // guards eng and peers
	eng   *engine.Engine
	peers map[uint64]string

	httpc *http.Client
	http  *http.Server
	stop  chan struct{}
	done  chan struct{}
}

type propResult struct {
	res sm.Result
	err error
}

// New creates the server and its engine (recovering persisted state).
func New(cfg Config) (*Server, error) {
	if cfg.TickInterval == 0 {
		cfg.TickInterval = 100 * time.Millisecond
	}
	if cfg.SnapshotThreshold == 0 {
		cfg.SnapshotThreshold = 10000
	}
	if cfg.SnapshotTrailing == 0 {
		cfg.SnapshotTrailing = 64
	}
	s := &Server{
		cfg:   cfg,
		peers: map[uint64]string{},
		httpc: &http.Client{Timeout: 5 * time.Second},
		stop:  make(chan struct{}),
		done:  make(chan struct{}),
	}
	for id, u := range cfg.Peers {
		s.peers[id] = u
	}
	s.loadPeersFile() // persisted membership overrides/extends flags

	var bootstrap []raft.Peer
	if !cfg.Join {
		for id, u := range cfg.Peers {
			bootstrap = append(bootstrap, raft.Peer{ID: id, Context: []byte(u)})
		}
	}
	eng, err := engine.New(engine.Config{
		ID:                cfg.ID,
		Dir:               filepath.Join(cfg.DataDir, "raft"),
		ElectionTick:      10,
		HeartbeatTick:     1,
		SnapshotThreshold: cfg.SnapshotThreshold,
		SnapshotTrailing:  cfg.SnapshotTrailing,
		BootstrapPeers:    bootstrap,
		Incarnation:       uint64(time.Now().UnixNano()),
		OnConfChange:      s.onConfChange, // called with s.mu held (inside engine calls)
	})
	if err != nil {
		return nil, err
	}
	s.eng = eng
	return s, nil
}

// Start runs the HTTP listener and the tick loop until Stop is called.
func (s *Server) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/raft", s.handleRaft)
	mux.HandleFunc("/kv/", s.handleKV)
	mux.HandleFunc("/members", s.handleMembers)
	mux.HandleFunc("/members/", s.handleMembers)
	mux.HandleFunc("/status", s.handleStatus)
	s.http = &http.Server{Addr: s.cfg.ListenAddr, Handler: mux}

	go s.tickLoop()
	log.Printf("quorumlogd: node %d listening on %s", s.cfg.ID, s.cfg.ListenAddr)
	err := s.http.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Stop shuts the server down gracefully (flushing raft state is implicit:
// everything is persisted before acknowledgement).
func (s *Server) Stop() {
	close(s.stop)
	<-s.done
	if s.http != nil {
		s.http.Close()
	}
	s.mu.Lock()
	s.eng.Close()
	s.mu.Unlock()
}

func (s *Server) tickLoop() {
	t := time.NewTicker(s.cfg.TickInterval)
	defer t.Stop()
	defer close(s.done)
	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
			s.mu.Lock()
			s.eng.Tick()
			s.processReadyLocked()
			s.mu.Unlock()
		}
	}
}

// processReadyLocked drains raft Ready work; caller holds s.mu.
func (s *Server) processReadyLocked() {
	for s.eng.HasReady() {
		msgs, err := s.eng.HandleReady()
		if err != nil {
			log.Fatalf("quorumlogd: ready processing failed: %v", err)
		}
		if len(msgs) > 0 {
			batches := map[uint64][]raftpb.Message{}
			for _, m := range msgs {
				batches[m.To] = append(batches[m.To], m)
			}
			for to, batch := range batches {
				url := s.peers[to]
				go s.sendBatch(to, url, batch)
			}
		}
	}
}

func (s *Server) sendBatch(to uint64, url string, msgs []raftpb.Message) {
	for _, m := range msgs {
		ok := s.sendMsg(url, m)
		if m.Type == raftpb.MsgSnap {
			s.mu.Lock()
			s.eng.ReportSnapshot(to, ok)
			s.processReadyLocked()
			s.mu.Unlock()
		}
	}
}

func (s *Server) sendMsg(url string, m raftpb.Message) bool {
	if url == "" {
		return false
	}
	data, err := m.Marshal()
	if err != nil {
		return false
	}
	resp, err := s.httpc.Post(url+"/raft", "application/x-protobuf", bytes.NewReader(data))
	if err != nil {
		return false
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode == http.StatusNoContent
}

func (s *Server) handleRaft(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var m raftpb.Message
	if err := m.Unmarshal(body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	err = s.eng.Step(m)
	s.processReadyLocked()
	s.mu.Unlock()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// onConfChange runs inside engine.HandleReady (s.mu held): update the peer
// map and persist it so membership survives restarts and log compaction.
func (s *Server) onConfChange(cc raftpb.ConfChange) {
	switch cc.Type {
	case raftpb.ConfChangeAddNode:
		if len(cc.Context) > 0 {
			s.peers[cc.NodeID] = string(cc.Context)
		}
	case raftpb.ConfChangeRemoveNode:
		delete(s.peers, cc.NodeID)
	}
	s.savePeersFileLocked()
}

type peersFile struct {
	Peers map[string]string `json:"peers"`
}

func (s *Server) peersPath() string { return filepath.Join(s.cfg.DataDir, "peers.json") }

func (s *Server) loadPeersFile() {
	b, err := os.ReadFile(s.peersPath())
	if err != nil {
		return
	}
	var pf peersFile
	if json.Unmarshal(b, &pf) != nil {
		return
	}
	for idStr, u := range pf.Peers {
		if id, err := strconv.ParseUint(idStr, 10, 64); err == nil {
			s.peers[id] = u
		}
	}
}

func (s *Server) savePeersFileLocked() {
	pf := peersFile{Peers: map[string]string{}}
	for id, u := range s.peers {
		pf.Peers[strconv.FormatUint(id, 10)] = u
	}
	b, _ := json.MarshalIndent(pf, "", "  ")
	tmp := s.peersPath() + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err == nil {
		os.Rename(tmp, s.peersPath())
	}
}

// --- client API --------------------------------------------------------------

type kvWriteReq struct {
	Op       string `json:"op"` // put (default), append, delete
	Value    string `json:"value"`
	ClientID string `json:"client_id"`
	ReqID    uint64 `json:"req_id"`
}

type kvResp struct {
	Value string `json:"value"`
	Found bool   `json:"found"`
}

type errResp struct {
	Error     string `json:"error"`
	LeaderID  uint64 `json:"leader_id,omitempty"`
	LeaderURL string `json:"leader_url,omitempty"`
}

func (s *Server) handleKV(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}
	switch r.Method {
	case http.MethodGet:
		s.handleRead(w, r, key)
	case http.MethodPut, http.MethodPost, http.MethodDelete:
		s.handleWrite(w, r, key)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleWrite(w http.ResponseWriter, r *http.Request, key string) {
	var req kvWriteReq
	if r.Method == http.MethodDelete {
		req.Op = sm.OpDelete
		req.ClientID = r.URL.Query().Get("client_id")
		if v := r.URL.Query().Get("req_id"); v != "" {
			req.ReqID, _ = strconv.ParseUint(v, 10, 64)
		}
	} else if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.Op == "" {
		req.Op = sm.OpPut
	}
	op := sm.Op{Type: req.Op, Key: key, Val: req.Value, ClientID: req.ClientID, ReqID: req.ReqID}

	ch := make(chan propResult, 1)
	s.mu.Lock()
	err := s.eng.Propose(op, func(res sm.Result, err error) {
		select {
		case ch <- propResult{res, err}:
		default:
		}
	})
	if err == nil {
		s.processReadyLocked()
	}
	s.mu.Unlock()
	if err != nil {
		s.writeProposeErr(w, r, err)
		return
	}
	select {
	case pr := <-ch:
		if pr.err != nil {
			writeJSON(w, http.StatusInternalServerError, errResp{Error: pr.err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, kvResp{Value: pr.res.Val, Found: pr.res.OK})
	case <-time.After(5 * time.Second):
		s.mu.Lock()
		s.eng.CancelProposal(op.ClientID, op.ReqID)
		s.mu.Unlock()
		// The proposal may still commit later; the client must retry with the
		// same client_id/req_id, which the state machine deduplicates.
		writeJSON(w, http.StatusServiceUnavailable, errResp{Error: "proposal timed out; retry with same client_id/req_id"})
	}
}

func (s *Server) handleRead(w http.ResponseWriter, r *http.Request, key string) {
	consistency := r.URL.Query().Get("consistency")
	if consistency == "" {
		consistency = "linearizable"
	}
	switch consistency {
	case "local":
		s.mu.Lock()
		v, ok := s.eng.SM().Get(key)
		s.mu.Unlock()
		writeJSON(w, http.StatusOK, kvResp{Value: v, Found: ok})
	case "linearizable":
		ch := make(chan error, 1)
		s.mu.Lock()
		s.eng.ReadIndex(func(err error) {
			select {
			case ch <- err:
			default:
			}
		})
		s.processReadyLocked()
		s.mu.Unlock()
		select {
		case err := <-ch:
			if err != nil {
				writeJSON(w, http.StatusInternalServerError, errResp{Error: err.Error()})
				return
			}
			s.mu.Lock()
			v, ok := s.eng.SM().Get(key)
			s.mu.Unlock()
			writeJSON(w, http.StatusOK, kvResp{Value: v, Found: ok})
		case <-time.After(5 * time.Second):
			writeJSON(w, http.StatusServiceUnavailable, errResp{Error: "read index timed out (no leader?); retry"})
		}
	default:
		http.Error(w, "consistency must be linearizable or local", http.StatusBadRequest)
	}
}

// writeProposeErr answers a proposal rejected because this node is not the
// leader: 307 redirect when the leader is known, 503 otherwise.
func (s *Server) writeProposeErr(w http.ResponseWriter, r *http.Request, err error) {
	var nl engine.ErrNotLeader
	if errors.As(err, &nl) {
		s.mu.Lock()
		url := s.peers[nl.Lead]
		s.mu.Unlock()
		if nl.Lead != 0 && url != "" {
			w.Header().Set("Location", url+r.URL.RequestURI())
			writeJSON(w, http.StatusTemporaryRedirect, errResp{Error: err.Error(), LeaderID: nl.Lead, LeaderURL: url})
			return
		}
		writeJSON(w, http.StatusServiceUnavailable, errResp{Error: "no leader known; retry"})
		return
	}
	writeJSON(w, http.StatusInternalServerError, errResp{Error: err.Error()})
}

type memberReq struct {
	ID  uint64 `json:"id"`
	URL string `json:"url"`
}

func (s *Server) handleMembers(w http.ResponseWriter, r *http.Request) {
	var cc raftpb.ConfChange
	switch r.Method {
	case http.MethodPost:
		var req memberReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ID == 0 || req.URL == "" {
			http.Error(w, "body must be {\"id\":N,\"url\":\"http://...\"}", http.StatusBadRequest)
			return
		}
		cc = raftpb.ConfChange{Type: raftpb.ConfChangeAddNode, NodeID: req.ID, Context: []byte(req.URL)}
	case http.MethodDelete:
		idStr := strings.TrimPrefix(r.URL.Path, "/members/")
		id, err := strconv.ParseUint(idStr, 10, 64)
		if err != nil || id == 0 {
			http.Error(w, "DELETE /members/{id}", http.StatusBadRequest)
			return
		}
		cc = raftpb.ConfChange{Type: raftpb.ConfChangeRemoveNode, NodeID: id}
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	cc.ID = uint64(time.Now().UnixNano())

	ch := make(chan error, 1)
	s.mu.Lock()
	err := s.eng.ProposeConfChange(cc, func(err error) {
		select {
		case ch <- err:
		default:
		}
	})
	if err == nil {
		s.processReadyLocked()
	}
	s.mu.Unlock()
	if err != nil {
		s.writeProposeErr(w, r, err)
		return
	}
	select {
	case err := <-ch:
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, errResp{Error: err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]bool{"applied": true})
	case <-time.After(5 * time.Second):
		writeJSON(w, http.StatusServiceUnavailable, errResp{Error: "conf change timed out; retry"})
	}
}

type statusResp struct {
	ID        uint64            `json:"id"`
	Leader    uint64            `json:"leader"`
	IsLeader  bool              `json:"is_leader"`
	Applied   uint64            `json:"applied"`
	SnapIndex uint64            `json:"snap_index"`
	Voters    []uint64          `json:"voters"`
	Peers     map[uint64]string `json:"peers"`
	Keys      int               `json:"keys"`
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	st := statusResp{
		ID:        s.cfg.ID,
		Leader:    s.eng.Lead(),
		IsLeader:  s.eng.IsLeader(),
		Applied:   s.eng.AppliedIndex(),
		SnapIndex: s.eng.SnapIndex(),
		Voters:    s.eng.Voters(),
		Peers:     map[uint64]string{},
	}
	for id, u := range s.peers {
		st.Peers[id] = u
	}
	st.Keys = s.eng.SM().Len()
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, st)
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

// SnapshotFileSize is exposed for measurements.
func (s *Server) SnapshotFileSize() (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.eng.Storage().SnapshotFileSize()
}
