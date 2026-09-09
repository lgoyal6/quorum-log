// Package multihost implements the driver for the multi-host gate: it reads
// an inventory of hosts, collects host identities, deploys and starts one
// quorum-log node per host, drives client traffic while injecting faults,
// records an externally observed history, and checks that history for
// linearizability.
//
// The same code path runs in two modes. In "multi-host" mode every command
// runs on a separate machine over ssh. In "local-dry-run" mode every command
// runs locally, which exercises the whole harness on one machine but proves
// nothing about a distributed deployment; artifacts from that mode are
// labeled single-host dry run and the gate always reports blocked.
package multihost

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
)

// Inventory modes.
const (
	// ModeMultiHost runs every host command over ssh on a separate machine.
	ModeMultiHost = "multi-host"
	// ModeLocalDryRun runs every host command locally. It is a single-host
	// dry run of the harness, never evidence of multi-host behavior.
	ModeLocalDryRun = "local-dry-run"
)

// Host describes one machine that runs one quorum-log node.
type Host struct {
	ID        uint64 `json:"id"`
	Name      string `json:"name"`
	SSH       string `json:"ssh"`       // ssh target, e.g. user@host-a (multi-host mode only)
	Advertise string `json:"advertise"` // address peers and the driver connect to
	RaftPort  int    `json:"raft_port"`
	ChaosPort int    `json:"chaos_port"` // loopback-only fault-injection port on that host
	DataDir   string `json:"data_dir"`
	GOOS      string `json:"goos"`   // empty means this machine's GOOS
	GOARCH    string `json:"goarch"` // empty means this machine's GOARCH
}

// ClientPlan describes the driver-side workload.
type ClientPlan struct {
	Ops          int     `json:"ops"`
	ReadFraction float64 `json:"read_fraction"`
	Concurrency  int     `json:"concurrency"`
	Seed         int64   `json:"seed"`
}

// FaultPlan schedules faults by completed operation count.
type FaultPlan struct {
	KillLeaderAtOp      int  `json:"kill_leader_at_op"`
	IsolateFollowerAtOp int  `json:"isolate_follower_at_op"`
	IsolateLeaderAtOp   int  `json:"isolate_leader_at_op"`
	RestoreAtOp         int  `json:"restore_at_op"`
	RestartAllAfter     bool `json:"restart_all_after"`
}

// Inventory is the driver's input file.
type Inventory struct {
	Mode   string     `json:"mode"`
	Hosts  []Host     `json:"hosts"`
	Client ClientPlan `json:"client"`
	Faults FaultPlan  `json:"faults"`
}

// LoadInventory reads and validates an inventory file.
func LoadInventory(path string) (*Inventory, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var inv Inventory
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&inv); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if err := inv.Validate(); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	// A dry-run inventory may use a repo-relative data directory so the file
	// itself carries no machine-specific path; resolve it now so every later
	// step works with absolute paths.
	if inv.Mode == ModeLocalDryRun {
		for i := range inv.Hosts {
			abs, err := filepath.Abs(inv.Hosts[i].DataDir)
			if err != nil {
				return nil, fmt.Errorf("%s: hosts[%d]: %w", path, i, err)
			}
			inv.Hosts[i].DataDir = abs
		}
	}
	return &inv, nil
}

// Validate checks the inventory for the invariants the gate depends on.
func (inv *Inventory) Validate() error {
	switch inv.Mode {
	case ModeMultiHost, ModeLocalDryRun:
	default:
		return fmt.Errorf("mode must be %q or %q, got %q", ModeMultiHost, ModeLocalDryRun, inv.Mode)
	}
	if len(inv.Hosts) != 3 {
		return fmt.Errorf("the gate requires exactly 3 hosts, got %d", len(inv.Hosts))
	}
	seenID := map[uint64]bool{}
	seenAddr := map[string]bool{}
	for i, h := range inv.Hosts {
		if h.ID == 0 {
			return fmt.Errorf("hosts[%d]: id must be >= 1", i)
		}
		if seenID[h.ID] {
			return fmt.Errorf("hosts[%d]: duplicate id %d", i, h.ID)
		}
		seenID[h.ID] = true
		if h.Name == "" {
			return fmt.Errorf("hosts[%d]: name is required", i)
		}
		if h.Advertise == "" {
			return fmt.Errorf("hosts[%d]: advertise is required", i)
		}
		if h.RaftPort <= 0 || h.RaftPort > 65535 {
			return fmt.Errorf("hosts[%d]: raft_port %d out of range", i, h.RaftPort)
		}
		if h.ChaosPort <= 0 || h.ChaosPort > 65535 {
			return fmt.Errorf("hosts[%d]: chaos_port %d out of range", i, h.ChaosPort)
		}
		if h.RaftPort == h.ChaosPort {
			return fmt.Errorf("hosts[%d]: raft_port and chaos_port must differ", i)
		}
		if h.DataDir == "" {
			return fmt.Errorf("hosts[%d]: data_dir is required", i)
		}
		if strings.ContainsAny(h.DataDir, " \t'\"") {
			return fmt.Errorf("hosts[%d]: data_dir %q must not contain whitespace or quotes; it is interpolated into host shell commands", i, h.DataDir)
		}
		addr := net.JoinHostPort(h.Advertise, fmt.Sprint(h.RaftPort))
		if seenAddr[addr] {
			return fmt.Errorf("hosts[%d]: duplicate advertise address %s", i, addr)
		}
		seenAddr[addr] = true

		ip := net.ParseIP(h.Advertise)
		switch inv.Mode {
		case ModeMultiHost:
			if h.SSH == "" {
				return fmt.Errorf("hosts[%d]: ssh target is required in %s mode", i, ModeMultiHost)
			}
			if ip != nil && ip.IsLoopback() {
				return fmt.Errorf("hosts[%d]: advertise %s is loopback, which cannot be a separate host", i, h.Advertise)
			}
			if !path.IsAbs(h.DataDir) {
				return fmt.Errorf("hosts[%d]: data_dir must be an absolute path on the host, got %q", i, h.DataDir)
			}
		case ModeLocalDryRun:
			if ip == nil || !ip.IsLoopback() {
				return fmt.Errorf("hosts[%d]: advertise %q must be a loopback IP in %s mode", i, h.Advertise, ModeLocalDryRun)
			}
		}
	}
	if inv.Client.Ops <= 0 {
		return fmt.Errorf("client.ops must be >= 1")
	}
	if inv.Client.Concurrency <= 0 {
		return fmt.Errorf("client.concurrency must be >= 1")
	}
	if inv.Client.ReadFraction < 0 || inv.Client.ReadFraction > 1 {
		return fmt.Errorf("client.read_fraction must be in [0,1]")
	}
	return nil
}

// HostByID returns the host with the given node id.
func (inv *Inventory) HostByID(id uint64) (Host, bool) {
	for _, h := range inv.Hosts {
		if h.ID == id {
			return h, true
		}
	}
	return Host{}, false
}

// PeersFlag builds the -peers value for quorumlogd.
func (inv *Inventory) PeersFlag() string {
	parts := make([]string, 0, len(inv.Hosts))
	for _, h := range inv.Hosts {
		parts = append(parts, fmt.Sprintf("%d=%s", h.ID, h.RaftURL()))
	}
	return strings.Join(parts, ",")
}

// RaftURL is the client and peer URL for this host's node.
func (h Host) RaftURL() string {
	return "http://" + net.JoinHostPort(h.Advertise, fmt.Sprint(h.RaftPort))
}

// ListenAddr is the address the node binds for its client API and transport.
func (h Host) ListenAddr() string {
	return net.JoinHostPort(h.Advertise, fmt.Sprint(h.RaftPort))
}

// ChaosListenAddr is the loopback address the node binds for fault
// injection. It is never reachable from another machine; the driver reaches
// it by running curl on the host itself.
func (h Host) ChaosListenAddr() string {
	return net.JoinHostPort("127.0.0.1", fmt.Sprint(h.ChaosPort))
}

// ChaosURL is the fault-injection URL as seen from the host itself.
func (h Host) ChaosURL() string { return "http://" + h.ChaosListenAddr() }

// Target names the host in log lines.
func (h Host) Target() string {
	if h.SSH != "" {
		return h.SSH
	}
	return h.Name
}

// Paths on the host. Only the node binary and the start script are
// transferred; everything else is created on the host.
func (h Host) BinaryPath() string      { return h.DataDir + "/quorumlogd" }
func (h Host) StartScriptPath() string { return h.DataDir + "/start.sh" }
func (h Host) PidPath() string         { return h.DataDir + "/node.pid" }
func (h Host) LogPath() string         { return h.DataDir + "/node.log" }

// BuildOS and BuildArch resolve the cross-compilation target, defaulting to
// this machine so a dry run needs no platform fields.
func (h Host) BuildOS() string {
	if h.GOOS != "" {
		return h.GOOS
	}
	return runtime.GOOS
}

func (h Host) BuildArch() string {
	if h.GOARCH != "" {
		return h.GOARCH
	}
	return runtime.GOARCH
}
