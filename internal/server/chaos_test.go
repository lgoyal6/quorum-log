package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"go.etcd.io/raft/v3/raftpb"
)

func TestChaosStateDirections(t *testing.T) {
	c := newChaosState()
	if !c.allowInbound(2) || !c.allowOutbound(2) {
		t.Fatal("fresh chaos state must allow everything")
	}
	c.isolate([]uint64{2}, true, false)
	if c.allowInbound(2) {
		t.Fatal("inbound from 2 must be dropped")
	}
	if !c.allowOutbound(2) {
		t.Fatal("outbound to 2 must still be allowed")
	}
	c.isolate([]uint64{2, 3}, false, true)
	if c.allowOutbound(2) || c.allowOutbound(3) {
		t.Fatal("outbound to 2 and 3 must be dropped")
	}
	if !c.allowInbound(3) {
		t.Fatal("inbound from 3 must still be allowed")
	}
	in, out := c.current()
	if len(in) != 1 || in[0] != 2 {
		t.Fatalf("inbound drop set = %v, want [2]", in)
	}
	if len(out) != 2 || out[0] != 2 || out[1] != 3 {
		t.Fatalf("outbound drop set = %v, want [2 3]", out)
	}
	c.clear()
	if !c.allowInbound(2) || !c.allowOutbound(3) {
		t.Fatal("clear must remove every rule")
	}
}

func TestCheckLoopbackAddr(t *testing.T) {
	ok := []string{"127.0.0.1:9301", "localhost:9301", "[::1]:9301", "127.0.0.2:1"}
	for _, addr := range ok {
		if err := checkLoopbackAddr(addr); err != nil {
			t.Errorf("checkLoopbackAddr(%q) = %v, want nil", addr, err)
		}
	}
	bad := []string{"0.0.0.0:9301", "10.0.0.11:9301", ":9301", "9301", "example.com:9301", "127.0.0.1"}
	for _, addr := range bad {
		if err := checkLoopbackAddr(addr); err == nil {
			t.Errorf("checkLoopbackAddr(%q) = nil, want an error", addr)
		}
	}
}

// peerCounter is a stand-in for peer 2's /raft endpoint.
type peerCounter struct {
	hits atomic.Int64
	srv  *httptest.Server
}

func newPeerCounter(t *testing.T) *peerCounter {
	t.Helper()
	pc := &peerCounter{}
	pc.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/raft" {
			pc.hits.Add(1)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(pc.srv.Close)
	return pc
}

// newChaosTestNode builds node 1 of a two-voter configuration whose peer 2
// points at pc. It never starts the client listener; the tests drive the
// transport directly.
func newChaosTestNode(t *testing.T, pc *peerCounter) *Server {
	t.Helper()
	s, err := New(Config{
		ID:           1,
		DataDir:      t.TempDir(),
		ListenAddr:   "127.0.0.1:0",
		Peers:        map[uint64]string{1: "http://127.0.0.1:1", 2: pc.srv.URL},
		TickInterval: time.Hour, // the tests never want a spontaneous tick
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		close(s.stop)
		s.mu.Lock()
		s.eng.Close()
		s.mu.Unlock()
	})
	// Flush the bootstrap Ready batch so later counts are attributable.
	s.mu.Lock()
	s.processReadyLocked()
	s.mu.Unlock()
	pc.hits.Store(0)
	return s
}

func waitForHits(t *testing.T, pc *peerCounter, want int64) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if pc.hits.Load() >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("peer saw %d messages, want at least %d", pc.hits.Load(), want)
}

func TestTransportDropsOutboundToIsolatedPeer(t *testing.T) {
	pc := newPeerCounter(t)
	s := newChaosTestNode(t, pc)

	send := func() {
		s.mu.Lock()
		s.enqueueLocked(raftpb.Message{Type: raftpb.MsgHeartbeat, From: 1, To: 2, Term: 1})
		s.mu.Unlock()
	}

	send()
	waitForHits(t, pc, 1)

	s.chaos.isolate([]uint64{2}, false, true)
	send()
	time.Sleep(300 * time.Millisecond)
	if got := pc.hits.Load(); got != 1 {
		t.Fatalf("peer saw %d messages while isolated outbound, want 1", got)
	}

	s.chaos.clear()
	send()
	waitForHits(t, pc, 2)
}

func TestTransportDropsInboundFromIsolatedPeer(t *testing.T) {
	pc := newPeerCounter(t)
	s := newChaosTestNode(t, pc)

	// A heartbeat from peer 2 at a higher term makes node 1 follow 2 and
	// answer with a heartbeat response, which the counter observes.
	post := func(term uint64) *httptest.ResponseRecorder {
		m := raftpb.Message{Type: raftpb.MsgHeartbeat, From: 2, To: 1, Term: term}
		body, err := m.Marshal()
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		rec := httptest.NewRecorder()
		s.handleRaft(rec, httptest.NewRequest(http.MethodPost, "/raft", bytes.NewReader(body)))
		return rec
	}

	if rec := post(5); rec.Code != http.StatusNoContent {
		t.Fatalf("POST /raft = %d, want 204", rec.Code)
	}
	waitForHits(t, pc, 1)

	// Drop inbound only, so a dropped message cannot be confused with a
	// response that was itself dropped on the way out.
	s.chaos.isolate([]uint64{2}, true, false)
	before := pc.hits.Load()
	if rec := post(6); rec.Code != http.StatusNoContent {
		t.Fatalf("POST /raft while isolated = %d, want 204 (a blackhole looks delivered)", rec.Code)
	}
	time.Sleep(300 * time.Millisecond)
	if got := pc.hits.Load(); got != before {
		t.Fatalf("node answered %d messages after an inbound-dropped message, want %d", got, before)
	}

	s.chaos.clear()
	if rec := post(7); rec.Code != http.StatusNoContent {
		t.Fatalf("POST /raft after clear = %d, want 204", rec.Code)
	}
	waitForHits(t, pc, before+1)
}

func TestChaosHTTPAPI(t *testing.T) {
	pc := newPeerCounter(t)
	s := newChaosTestNode(t, pc)
	api := httptest.NewServer(s.chaosMux())
	defer api.Close()

	get := func() chaosStateResp {
		t.Helper()
		resp, err := http.Get(api.URL + "/chaos")
		if err != nil {
			t.Fatalf("GET /chaos: %v", err)
		}
		defer resp.Body.Close()
		var st chaosStateResp
		if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
			t.Fatalf("decode: %v", err)
		}
		return st
	}

	if st := get(); len(st.DropInbound) != 0 || len(st.DropOutbound) != 0 || st.NodeID != 1 {
		t.Fatalf("initial state = %+v, want node 1 with empty drop sets", st)
	}

	// Omitting both directions isolates the peer in both directions.
	resp, err := http.Post(api.URL+"/chaos/isolate", "application/json", bytes.NewReader([]byte(`{"peers":[2,3]}`)))
	if err != nil {
		t.Fatalf("POST /chaos/isolate: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /chaos/isolate = %d, want 200", resp.StatusCode)
	}
	st := get()
	if len(st.DropInbound) != 2 || len(st.DropOutbound) != 2 {
		t.Fatalf("state after isolate = %+v, want both directions for 2 and 3", st)
	}

	req, _ := http.NewRequest(http.MethodDelete, api.URL+"/chaos/isolate", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE /chaos/isolate: %v", err)
	}
	resp.Body.Close()
	if st := get(); len(st.DropInbound) != 0 || len(st.DropOutbound) != 0 {
		t.Fatalf("state after delete = %+v, want empty drop sets", st)
	}

	// A one-directional request must not set the other direction.
	resp, err = http.Post(api.URL+"/chaos/isolate", "application/json", bytes.NewReader([]byte(`{"peers":[2],"drop_outbound":true}`)))
	if err != nil {
		t.Fatalf("POST /chaos/isolate outbound: %v", err)
	}
	resp.Body.Close()
	st = get()
	if len(st.DropInbound) != 0 || len(st.DropOutbound) != 1 || st.DropOutbound[0] != 2 {
		t.Fatalf("state = %+v, want outbound-only drop of peer 2", st)
	}

	// An empty peer list is a client error.
	resp, err = http.Post(api.URL+"/chaos/isolate", "application/json", bytes.NewReader([]byte(`{"peers":[]}`)))
	if err != nil {
		t.Fatalf("POST /chaos/isolate empty: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("POST with no peers = %d, want 400", resp.StatusCode)
	}
}
