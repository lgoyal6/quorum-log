// Chaos is a test and fault-injection hook, not a production feature. When
// the node is started with -chaos-listen, it serves a tiny control API on a
// loopback address only, which lets a test driver blackhole raft messages to
// and from selected peers at the transport layer. Both the sending and the
// receiving side drop, so a partition can be expressed from either end.
//
// The endpoint is unauthenticated by construction: it binds a loopback
// address, so it is reachable only from the machine running the node (a
// driver reaches it over ssh). Without -chaos-listen nothing is served and
// the drop checks are no-ops.
package server

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sort"
	"sync"
)

// chaosState holds the current transport-level drop sets.
type chaosState struct {
	mu      sync.Mutex
	dropIn  map[uint64]bool // peer id -> drop messages received from it
	dropOut map[uint64]bool // peer id -> drop messages sent to it
}

func newChaosState() *chaosState {
	return &chaosState{dropIn: map[uint64]bool{}, dropOut: map[uint64]bool{}}
}

// allowInbound reports whether a raft message from peer "from" may be stepped.
func (c *chaosState) allowInbound(from uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.dropIn[from]
}

// allowOutbound reports whether a raft message to peer "to" may be sent.
func (c *chaosState) allowOutbound(to uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.dropOut[to]
}

// isolate adds peers to the requested drop directions. Directions are
// additive: isolating the same peer twice in different directions keeps both.
func (c *chaosState) isolate(peers []uint64, dropInbound, dropOutbound bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, p := range peers {
		if p == 0 {
			continue
		}
		if dropInbound {
			c.dropIn[p] = true
		}
		if dropOutbound {
			c.dropOut[p] = true
		}
	}
}

// clear removes every drop rule.
func (c *chaosState) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dropIn = map[uint64]bool{}
	c.dropOut = map[uint64]bool{}
}

// current returns the sorted drop sets.
func (c *chaosState) current() (inbound, outbound []uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	inbound = sortedIDs(c.dropIn)
	outbound = sortedIDs(c.dropOut)
	return inbound, outbound
}

func sortedIDs(m map[uint64]bool) []uint64 {
	ids := make([]uint64, 0, len(m))
	for id, drop := range m {
		if drop {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

type chaosIsolateReq struct {
	Peers []uint64 `json:"peers"`
	// Directions; when both are omitted the peer is isolated in both
	// directions, which is the common case for expressing a partition.
	DropInbound  *bool `json:"drop_inbound,omitempty"`
	DropOutbound *bool `json:"drop_outbound,omitempty"`
}

type chaosStateResp struct {
	NodeID       uint64   `json:"node_id"`
	DropInbound  []uint64 `json:"drop_inbound"`
	DropOutbound []uint64 `json:"drop_outbound"`
}

func (s *Server) chaosMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/chaos", s.handleChaosState)
	mux.HandleFunc("/chaos/isolate", s.handleChaosIsolate)
	return mux
}

func (s *Server) handleChaosState(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	in, out := s.chaos.current()
	writeJSON(w, http.StatusOK, chaosStateResp{NodeID: s.cfg.ID, DropInbound: in, DropOutbound: out})
}

func (s *Server) handleChaosIsolate(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var req chaosIsolateReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if len(req.Peers) == 0 {
			http.Error(w, "body must be {\"peers\":[id,...],\"drop_inbound\":true,\"drop_outbound\":true}", http.StatusBadRequest)
			return
		}
		dropIn, dropOut := true, true
		if req.DropInbound != nil || req.DropOutbound != nil {
			dropIn = req.DropInbound != nil && *req.DropInbound
			dropOut = req.DropOutbound != nil && *req.DropOutbound
		}
		s.chaos.isolate(req.Peers, dropIn, dropOut)
	case http.MethodDelete:
		s.chaos.clear()
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	in, out := s.chaos.current()
	writeJSON(w, http.StatusOK, chaosStateResp{NodeID: s.cfg.ID, DropInbound: in, DropOutbound: out})
}

// checkLoopbackAddr refuses any chaos listen address that is not loopback.
// The endpoint has no authentication, so binding it anywhere reachable from
// another machine is a configuration error, not a supported mode.
func checkLoopbackAddr(addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("chaos listen address %q must be host:port: %w", addr, err)
	}
	if port == "" {
		return fmt.Errorf("chaos listen address %q must include a port", addr)
	}
	if host == "localhost" {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return fmt.Errorf("chaos listen host %q must be a loopback IP address or localhost", host)
	}
	if !ip.IsLoopback() {
		return fmt.Errorf("chaos listen host %q is not loopback; the chaos endpoint is unauthenticated and must bind loopback only", host)
	}
	return nil
}
