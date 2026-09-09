package multihost

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func threeHostInventory() *Inventory {
	return &Inventory{
		Mode: ModeMultiHost,
		Hosts: []Host{
			{ID: 1, Name: "host-a", SSH: "user@host-a", Advertise: "10.0.0.11", RaftPort: 9101, ChaosPort: 9301, DataDir: "/var/tmp/quorum-log/n1", GOOS: "linux", GOARCH: "amd64"},
			{ID: 2, Name: "host-b", SSH: "user@host-b", Advertise: "10.0.0.12", RaftPort: 9101, ChaosPort: 9301, DataDir: "/var/tmp/quorum-log/n2", GOOS: "linux", GOARCH: "amd64"},
			{ID: 3, Name: "host-c", SSH: "user@host-c", Advertise: "10.0.0.13", RaftPort: 9101, ChaosPort: 9301, DataDir: "/var/tmp/quorum-log/n3", GOOS: "linux", GOARCH: "amd64"},
		},
		Client: ClientPlan{Ops: 1000, ReadFraction: 0.5, Concurrency: 8, Seed: 20260909},
		Faults: FaultPlan{KillLeaderAtOp: 300, IsolateFollowerAtOp: 500, IsolateLeaderAtOp: 700, RestoreAtOp: 850, RestartAllAfter: true},
	}
}

func TestValidateAcceptsThreeHosts(t *testing.T) {
	if err := threeHostInventory().Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestValidateRejections(t *testing.T) {
	cases := map[string]func(inv *Inventory){
		"bad mode":              func(inv *Inventory) { inv.Mode = "cluster" },
		"two hosts":             func(inv *Inventory) { inv.Hosts = inv.Hosts[:2] },
		"duplicate id":          func(inv *Inventory) { inv.Hosts[2].ID = 1 },
		"duplicate address":     func(inv *Inventory) { inv.Hosts[2].Advertise = inv.Hosts[1].Advertise },
		"missing ssh target":    func(inv *Inventory) { inv.Hosts[1].SSH = "" },
		"loopback in multihost": func(inv *Inventory) { inv.Hosts[1].Advertise = "127.0.0.1" },
		"relative data dir":     func(inv *Inventory) { inv.Hosts[0].DataDir = "data/n1" },
		"raft port zero":        func(inv *Inventory) { inv.Hosts[0].RaftPort = 0 },
		"same ports":            func(inv *Inventory) { inv.Hosts[0].ChaosPort = inv.Hosts[0].RaftPort },
		"no ops":                func(inv *Inventory) { inv.Client.Ops = 0 },
		"no concurrency":        func(inv *Inventory) { inv.Client.Concurrency = 0 },
		"read fraction > 1":     func(inv *Inventory) { inv.Client.ReadFraction = 1.5 },
	}
	for name, mutate := range cases {
		inv := threeHostInventory()
		mutate(inv)
		if err := inv.Validate(); err == nil {
			t.Errorf("%s: Validate = nil, want an error", name)
		}
	}
}

func TestValidateDryRunRequiresLoopback(t *testing.T) {
	inv := threeHostInventory()
	inv.Mode = ModeLocalDryRun
	if err := inv.Validate(); err == nil {
		t.Fatal("a local dry run must not point at non-loopback addresses")
	}
	for i := range inv.Hosts {
		inv.Hosts[i].Advertise = "127.0.0.1"
		inv.Hosts[i].RaftPort = 9101 + i
		inv.Hosts[i].ChaosPort = 9301 + i
		inv.Hosts[i].SSH = ""
	}
	if err := inv.Validate(); err != nil {
		t.Fatalf("loopback dry run rejected: %v", err)
	}
}

func TestPeersFlagAndURLs(t *testing.T) {
	inv := threeHostInventory()
	want := "1=http://10.0.0.11:9101,2=http://10.0.0.12:9101,3=http://10.0.0.13:9101"
	if got := inv.PeersFlag(); got != want {
		t.Fatalf("PeersFlag = %q, want %q", got, want)
	}
	h, ok := inv.HostByID(2)
	if !ok {
		t.Fatal("HostByID(2) not found")
	}
	if h.ChaosURL() != "http://127.0.0.1:9301" {
		t.Fatalf("ChaosURL = %q, want the loopback chaos port", h.ChaosURL())
	}
	if h.ListenAddr() != "10.0.0.12:9101" {
		t.Fatalf("ListenAddr = %q", h.ListenAddr())
	}
	if h.BinaryPath() != "/var/tmp/quorum-log/n2/quorumlogd" {
		t.Fatalf("BinaryPath = %q", h.BinaryPath())
	}
}

func TestBuildTargetDefaultsToThisMachine(t *testing.T) {
	h := Host{}
	if h.BuildOS() != runtime.GOOS || h.BuildArch() != runtime.GOARCH {
		t.Fatalf("empty goos/goarch = %s/%s, want this machine's %s/%s", h.BuildOS(), h.BuildArch(), runtime.GOOS, runtime.GOARCH)
	}
	h = Host{GOOS: "linux", GOARCH: "arm64"}
	if h.BuildOS() != "linux" || h.BuildArch() != "arm64" {
		t.Fatalf("explicit goos/goarch not honored: %s/%s", h.BuildOS(), h.BuildArch())
	}
}

func TestLoadInventoryRejectsUnknownFields(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "inv.json")
	body := `{"mode":"local-dry-run","hosts":[],"client":{"ops":1},"nonsense":true}`
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadInventory(path); err == nil {
		t.Fatal("an inventory with an unknown field must be rejected, not silently ignored")
	}
}

func TestLoadInventoryResolvesDryRunDataDir(t *testing.T) {
	dir := t.TempDir()
	invPath := filepath.Join(dir, "dry.json")
	body := `{
      "mode": "local-dry-run",
      "hosts": [
        {"id": 1, "name": "local-1", "advertise": "127.0.0.1", "raft_port": 9101, "chaos_port": 9301, "data_dir": ".agent-work/dry-run/n1"},
        {"id": 2, "name": "local-2", "advertise": "127.0.0.1", "raft_port": 9102, "chaos_port": 9302, "data_dir": ".agent-work/dry-run/n2"},
        {"id": 3, "name": "local-3", "advertise": "127.0.0.1", "raft_port": 9103, "chaos_port": 9303, "data_dir": ".agent-work/dry-run/n3"}
      ],
      "client": {"ops": 10, "read_fraction": 0.5, "concurrency": 2, "seed": 1},
      "faults": {"kill_leader_at_op": 3, "isolate_follower_at_op": 5, "isolate_leader_at_op": 7, "restore_at_op": 9, "restart_all_after": true}
    }`
	if err := os.WriteFile(invPath, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	inv, err := LoadInventory(invPath)
	if err != nil {
		t.Fatalf("LoadInventory: %v", err)
	}
	for _, h := range inv.Hosts {
		if !filepath.IsAbs(h.DataDir) {
			t.Fatalf("host %d data_dir %q was not resolved to an absolute path", h.ID, h.DataDir)
		}
	}
}

func TestValidateRejectsDataDirWithSpaces(t *testing.T) {
	inv := threeHostInventory()
	inv.Hosts[0].DataDir = "/var/tmp/quorum log/n1"
	if err := inv.Validate(); err == nil {
		t.Fatal("a data_dir with a space must be rejected; it is interpolated into shell commands")
	}
}
