package multihost

import (
	"time"

	"github.com/anishathalye/porcupine"

	"quorumlog/internal/check"
)

// CheckReport is written as multihost-porcupine.json (or the dry-run
// equivalent).
type CheckReport struct {
	Mode                string     `json:"mode"`
	Boundary            string     `json:"boundary"`
	GeneratedAt         string     `json:"generated_at"`
	Checker             string     `json:"checker"`
	Model               string     `json:"model"`
	Linearizable        bool       `json:"linearizable"`
	Result              string     `json:"result"`
	Ops                 int        `json:"ops"`
	Writes              int        `json:"writes"`
	Reads               int        `json:"reads"`
	IndeterminateOps    int        `json:"indeterminate_ops"`
	CheckDurationMs     float64    `json:"check_duration_ms"`
	AckedWritesVerified int        `json:"acknowledged_writes_verified"`
	AckedWriteLoss      []Mismatch `json:"acknowledged_write_loss"`
	Note                string     `json:"note"`
}

// ResultString names a porcupine result.
func ResultString(res porcupine.CheckResult) string {
	switch res {
	case porcupine.Ok:
		return "Ok"
	case porcupine.Illegal:
		return "Illegal"
	default:
		return "Unknown"
	}
}

// RunCheck checks the externally observed history against the sequential KV
// model in internal/check (porcupine), and reports it with the
// acknowledged-write verification result.
func RunCheck(hist *History, mode, boundary string, loss []Mismatch, verified int) *CheckReport {
	ops := hist.PorcupineOps()
	start := time.Now()
	res := check.Check(ops)
	elapsed := time.Since(start)
	return &CheckReport{
		Mode:                mode,
		Boundary:            boundary,
		GeneratedAt:         time.Now().UTC().Format(time.RFC3339),
		Checker:             "github.com/anishathalye/porcupine via internal/check",
		Model:               "internal/check.KVModel (sequential key-value store, partitioned by key)",
		Linearizable:        res == porcupine.Ok,
		Result:              ResultString(res),
		Ops:                 len(ops),
		Writes:              hist.Writes,
		Reads:               hist.Reads,
		IndeterminateOps:    hist.IndeterminateWrites + hist.IndeterminateReads,
		CheckDurationMs:     float64(elapsed.Microseconds()) / 1000,
		AckedWritesVerified: verified,
		AckedWriteLoss:      loss,
		Note:                "history recorded by the driver (the client), not by any node; operations whose outcome the client never learned are checked as indeterminate",
	}
}
