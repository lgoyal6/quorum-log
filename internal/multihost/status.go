package multihost

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// NodeStatus is the subset of GET /status the driver uses.
type NodeStatus struct {
	ID       uint64 `json:"id"`
	Leader   uint64 `json:"leader"`
	IsLeader bool   `json:"is_leader"`
	Applied  uint64 `json:"applied"`
	Keys     int    `json:"keys"`
}

// FetchStatus reads one node's status.
func FetchStatus(ctx context.Context, client *http.Client, h Host) (NodeStatus, error) {
	var st NodeStatus
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, h.RaftURL()+"/status", nil)
	if err != nil {
		return st, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return st, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return st, fmt.Errorf("status %s: http %d", h.Target(), resp.StatusCode)
	}
	err = json.NewDecoder(resp.Body).Decode(&st)
	return st, err
}

// FindLeader asks the given hosts who the leader is and returns the host
// that reports itself leader.
func FindLeader(ctx context.Context, hosts []Host) (Host, NodeStatus, error) {
	client := &http.Client{Timeout: 2 * time.Second}
	for _, h := range hosts {
		st, err := FetchStatus(ctx, client, h)
		if err != nil {
			continue
		}
		if st.IsLeader {
			return h, st, nil
		}
	}
	return Host{}, NodeStatus{}, fmt.Errorf("no host reports itself leader")
}

// WaitLeader waits until one of the hosts reports itself leader, optionally
// excluding a node id (used to require a new leader after a fault). It
// returns the winning host and how long the wait took.
func WaitLeader(ctx context.Context, hosts []Host, exclude uint64, timeout time.Duration) (Host, time.Duration, error) {
	start := time.Now()
	deadline := start.Add(timeout)
	for {
		h, st, err := FindLeader(ctx, hosts)
		if err == nil && st.ID != exclude {
			return h, time.Since(start), nil
		}
		if time.Now().After(deadline) {
			return Host{}, time.Since(start), fmt.Errorf("no leader other than node %d within %s", exclude, timeout)
		}
		select {
		case <-ctx.Done():
			return Host{}, time.Since(start), ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// OtherHosts returns every host except the one with the given id.
func OtherHosts(hosts []Host, id uint64) []Host {
	out := make([]Host, 0, len(hosts))
	for _, h := range hosts {
		if h.ID != id {
			out = append(out, h)
		}
	}
	return out
}

// HostIDs lists the node ids of the given hosts.
func HostIDs(hosts []Host) []uint64 {
	out := make([]uint64, 0, len(hosts))
	for _, h := range hosts {
		out = append(out, h.ID)
	}
	return out
}
