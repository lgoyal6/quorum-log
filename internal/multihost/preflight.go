package multihost

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// Artifact mode labels. Every artifact carries one of these, and the dry-run
// label is used whenever the hosts are not proven distinct, whatever the
// inventory claims.
const (
	ModeLabelMultiHost = "multi-host"
	ModeLabelDryRun    = "single-host-dry-run"
)

// BlockedReasonDryRun is the exact reason the gate reports when it did not
// see three distinct hosts.
const BlockedReasonDryRun = "three distinct authorized hosts required; this was a single-host dry run"

// Boundary statements. They are copied verbatim into every artifact so a
// reader cannot mistake one kind of run for the other.
const (
	BoundaryMultiHost = "Multi-host run: each quorum-log node ran on a separate machine with its own kernel, network identity, and disk; the driver and client ran on this machine. Numbers are measured, not simulated."
	BoundaryDryRun    = "Single-host dry run: all three quorum-log node processes, the fault injection, and the client ran on one machine over loopback. This validates the harness only. It is not multi-host evidence, no remote host was contacted, and nothing here measures a network between machines."
)

// HostFacts is what the driver observed about one host.
type HostFacts struct {
	ID              uint64   `json:"id"`
	ConfiguredName  string   `json:"configured_name"`
	Target          string   `json:"target"`
	Executor        string   `json:"executor"`
	Hostname        string   `json:"hostname"`
	OSKind          string   `json:"os_kind"`
	Kernel          string   `json:"kernel"`
	MachineID       string   `json:"machine_id"`
	MachineIDSource string   `json:"machine_id_source"`
	BootID          string   `json:"boot_id"`
	BootIDSource    string   `json:"boot_id_source"`
	PrimaryAddress  string   `json:"primary_address"`
	Advertise       string   `json:"advertise"`
	RaftPort        int      `json:"raft_port"`
	ChaosPort       int      `json:"chaos_port"`
	DataDir         string   `json:"data_dir"`
	DataDevice      string   `json:"data_device"`
	DataMountPoint  string   `json:"data_mount_point"`
	BuildTarget     string   `json:"build_target"`
	Problems        []string `json:"problems,omitempty"`
}

// InventoryReport is written as multihost-inventory.json (or, for a dry run,
// results/dry-run/inventory.json).
type InventoryReport struct {
	Mode                string      `json:"mode"`
	Executor            string      `json:"executor"`
	InventoryMode       string      `json:"inventory_mode"`
	InventoryPath       string      `json:"inventory_path"`
	GeneratedAt         string      `json:"generated_at"`
	DistinctHosts       bool        `json:"distinct_hosts"`
	DistinctnessReasons []string    `json:"distinctness_reasons"`
	Hosts               []HostFacts `json:"hosts"`
	Boundary            string      `json:"boundary"`
}

// Preflight collects host facts and decides whether the three hosts are
// really three distinct machines.
func Preflight(ctx context.Context, inv *Inventory, ex Executor, inventoryPath string) (*InventoryReport, error) {
	facts := make([]HostFacts, 0, len(inv.Hosts))
	for _, h := range inv.Hosts {
		f, err := collectHostFacts(ctx, h, ex)
		if err != nil {
			return nil, err
		}
		facts = append(facts, f)
	}
	distinct, reasons := EvaluateDistinct(facts)
	mode := ModeLabelDryRun
	boundary := BoundaryDryRun
	if distinct && inv.Mode == ModeMultiHost {
		mode = ModeLabelMultiHost
		boundary = BoundaryMultiHost
	}
	return &InventoryReport{
		Mode:                mode,
		Executor:            ex.Kind(),
		InventoryMode:       inv.Mode,
		InventoryPath:       inventoryPath,
		GeneratedAt:         time.Now().UTC().Format(time.RFC3339),
		DistinctHosts:       distinct && inv.Mode == ModeMultiHost,
		DistinctnessReasons: reasons,
		Hosts:               facts,
		Boundary:            boundary,
	}, nil
}

// collectHostFacts runs one simple command per fact so a single unsupported
// command degrades that one field instead of the whole preflight.
func collectHostFacts(ctx context.Context, h Host, ex Executor) (HostFacts, error) {
	f := HostFacts{
		ID:             h.ID,
		ConfiguredName: h.Name,
		Target:         h.Target(),
		Executor:       ex.Kind(),
		Advertise:      h.Advertise,
		RaftPort:       h.RaftPort,
		ChaosPort:      h.ChaosPort,
		DataDir:        h.DataDir,
		BuildTarget:    h.BuildOS() + "/" + h.BuildArch(),
	}
	ask := func(script string) string {
		out, err := ex.Run(ctx, h, script)
		if err != nil {
			f.Problems = append(f.Problems, fmt.Sprintf("%s: %v", script, err))
			return ""
		}
		return strings.TrimSpace(out)
	}

	f.OSKind = ask("uname -s")
	f.Hostname = ask("uname -n")
	f.Kernel = ask("uname -a")

	switch f.OSKind {
	case "Linux":
		f.MachineIDSource = "/etc/machine-id or /var/lib/dbus/machine-id"
		f.MachineID = ask("cat /etc/machine-id 2>/dev/null || cat /var/lib/dbus/machine-id 2>/dev/null || true")
		f.BootIDSource = "/proc/sys/kernel/random/boot_id"
		f.BootID = ask("cat /proc/sys/kernel/random/boot_id 2>/dev/null || true")
		f.PrimaryAddress = ask("hostname -I 2>/dev/null | awk '{print $1}' || true")
	case "Darwin":
		f.MachineIDSource = "ioreg IOPlatformUUID"
		f.MachineID = ask("ioreg -rd1 -c IOPlatformExpertDevice | awk -F'\"' '/IOPlatformUUID/{print $4}'")
		// macOS has no boot_id; the kernel boot time is the closest
		// per-boot identity available.
		f.BootIDSource = "sysctl kern.boottime (macOS has no boot_id)"
		f.BootID = ask("sysctl -n kern.boottime 2>/dev/null || true")
		f.PrimaryAddress = ask("ipconfig getifaddr en0 2>/dev/null || ipconfig getifaddr en1 2>/dev/null || true")
	default:
		f.Problems = append(f.Problems, fmt.Sprintf("unsupported os kind %q: machine and boot identity not collected", f.OSKind))
	}

	device, mount := parseDF(ask(fmt.Sprintf("mkdir -p %s && df -P %s | tail -1", h.DataDir, h.DataDir)))
	f.DataDevice = device
	f.DataMountPoint = mount
	return f, nil
}

// parseDF extracts the device and mount point from a df -P data line.
func parseDF(line string) (device, mount string) {
	fields := strings.Fields(line)
	if len(fields) < 6 {
		return "", ""
	}
	return fields[0], fields[len(fields)-1]
}

// EvaluateDistinct reports whether the facts describe three distinct
// machines. Every machine identity, hostname, and boot id must be present
// and different; anything less is one machine wearing several names.
func EvaluateDistinct(facts []HostFacts) (bool, []string) {
	var reasons []string
	if len(facts) < 3 {
		reasons = append(reasons, fmt.Sprintf("only %d hosts described; the gate requires 3", len(facts)))
	}
	check := func(field string, value func(HostFacts) string) {
		seen := map[string][]uint64{}
		for _, f := range facts {
			v := value(f)
			if v == "" {
				reasons = append(reasons, fmt.Sprintf("host %d: %s is empty, so identity cannot be established", f.ID, field))
				continue
			}
			seen[v] = append(seen[v], f.ID)
		}
		for v, ids := range seen {
			if len(ids) > 1 {
				reasons = append(reasons, fmt.Sprintf("hosts %s share the same %s (%s), so they are the same machine", joinIDs(ids), field, v))
			}
		}
	}
	check("machine identity", func(f HostFacts) string { return f.MachineID })
	check("hostname", func(f HostFacts) string { return f.Hostname })
	check("boot id", func(f HostFacts) string { return f.BootID })
	if len(reasons) == 0 {
		return true, []string{"machine identity, hostname, and boot id all differ across the three hosts"}
	}
	return false, reasons
}

func joinIDs(ids []uint64) string {
	parts := make([]string, 0, len(ids))
	for _, id := range ids {
		parts = append(parts, fmt.Sprint(id))
	}
	return strings.Join(parts, " and ")
}
