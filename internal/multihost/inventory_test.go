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
