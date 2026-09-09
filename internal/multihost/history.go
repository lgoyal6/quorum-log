package multihost

import (
	"sort"
	"sync"
	"time"

	"github.com/anishathalye/porcupine"

	"quorumlog/internal/check"
	"quorumlog/internal/sm"
)

// Operation statuses. A write is "ok" only when the node answered 200; an
// "indeterminate" write may or may not have committed (timeout, no leader,
// connection error), which is exactly what the checker must be told.
const (
	StatusOK            = "ok"
	StatusIndeterminate = "indeterminate"
)

// Phases of the run, recorded per operation.
const (
	PhaseTraffic = "traffic"
	PhaseVerify  = "post-restart-verify"
)

// OpRecord is one externally observed client operation: what the driver
// asked for, when it asked, when it heard back, and what it heard.
type OpRecord struct {
	Index         int    `json:"index"`
	ClientID      int    `json:"client_id"`      // porcupine client (one per worker)
	ClientSession string `json:"client_session"` // quorum-log client_id used for dedup
	ReqID         uint64 `json:"req_id,omitempty"`
	Op            string `json:"op"` // put or get
	Key           string `json:"key"`
	Value         string `json:"value,omitempty"`
	CallNanos     int64  `json:"call_nanos"`   // since run start
	ReturnNanos   int64  `json:"return_nanos"` // since run start
	Status        string `json:"status"`
	HTTPCode      int    `json:"http_code,omitempty"`
	Attempts      int    `json:"attempts"`
	Redirects     int    `json:"redirects"`
	Endpoint      string `json:"endpoint,omitempty"`
	ObservedVal   string `json:"observed_val,omitempty"`
	ObservedFound bool   `json:"observed_found"`
	Error         string `json:"error,omitempty"`
	Phase         string `json:"phase"`
}

// History is the recorded client history, written as JSON.
type History struct {
	Mode                string     `json:"mode"`
	Boundary            string     `json:"boundary"`
	GeneratedAt         string     `json:"generated_at"`
	ClockBaseUnixNanos  int64      `json:"clock_base_unix_nanos"`
	ClockNote           string     `json:"clock_note"`
	Endpoints           []string   `json:"endpoints"`
	Ops                 []OpRecord `json:"ops"`
	Writes              int        `json:"writes"`
	Reads               int        `json:"reads"`
	IndeterminateWrites int        `json:"indeterminate_writes"`
	IndeterminateReads  int        `json:"indeterminate_reads"`
}

// Recorder collects operations from the traffic workers.
type Recorder struct {
	mu    sync.Mutex
	base  time.Time
	ops   []OpRecord
	next  int
	phase string
}

// NewRecorder starts a recorder whose clock is relative to base.
func NewRecorder(base time.Time) *Recorder {
	return &Recorder{base: base, phase: PhaseTraffic}
}

// Base returns the run's clock origin.
func (r *Recorder) Base() time.Time { return r.base }

// Since converts a wall-clock instant into the history's relative clock.
func (r *Recorder) Since(t time.Time) int64 { return t.Sub(r.base).Nanoseconds() }

// SetPhase labels the operations recorded from now on.
func (r *Recorder) SetPhase(phase string) {
	r.mu.Lock()
	r.phase = phase
	r.mu.Unlock()
}

// Add records one completed operation.
func (r *Recorder) Add(op OpRecord) {
	r.mu.Lock()
	defer r.mu.Unlock()
	op.Index = r.next
	r.next++
	if op.Phase == "" {
		op.Phase = r.phase
	}
	r.ops = append(r.ops, op)
}

// Ops returns the recorded operations ordered by invocation time.
func (r *Recorder) Ops() []OpRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := append([]OpRecord(nil), r.ops...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].CallNanos != out[j].CallNanos {
			return out[i].CallNanos < out[j].CallNanos
		}
		return out[i].Index < out[j].Index
	})
	return out
}

// History finalizes the recorded operations into the artifact structure.
func (r *Recorder) History(mode, boundary string, endpoints []string) *History {
	ops := r.Ops()
	h := &History{
		Mode:               mode,
		Boundary:           boundary,
		GeneratedAt:        time.Now().UTC().Format(time.RFC3339),
		ClockBaseUnixNanos: r.base.UnixNano(),
		ClockNote:          "call_nanos and return_nanos are nanoseconds since clock_base_unix_nanos, measured by the driver (the client), not by any node",
		Endpoints:          endpoints,
		Ops:                ops,
	}
	for _, op := range ops {
		switch op.Op {
		case "get":
			h.Reads++
			if op.Status != StatusOK {
				h.IndeterminateReads++
			}
		default:
			h.Writes++
			if op.Status != StatusOK {
				h.IndeterminateWrites++
			}
		}
	}
	return h
}

// PorcupineOps converts the recorded history into porcupine operations for
// the model in internal/check.
//
// An operation whose outcome the client never learned is given an Unknown
// output and an end-of-history return time. That is the standard treatment
// for an indeterminate operation: it may be linearized anywhere after its
// invocation, including after every other operation, so it can also have
// effectively never happened.
func (h *History) PorcupineOps() []porcupine.Operation {
	var maxReturn int64
	for _, op := range h.Ops {
		if op.ReturnNanos > maxReturn {
			maxReturn = op.ReturnNanos
		}
	}
	end := maxReturn + 1
	out := make([]porcupine.Operation, 0, len(h.Ops))
	for _, op := range h.Ops {
		in := check.KVInput{Op: op.Op, Key: op.Key, Val: op.Value}
		if op.Op == "get" {
			in.Op = "get"
		} else {
			in.Op = sm.OpPut
		}
		pop := porcupine.Operation{
			ClientId: op.ClientID,
			Input:    in,
			Call:     op.CallNanos,
			Return:   op.ReturnNanos,
		}
		if op.Status == StatusOK {
			pop.Output = check.KVOutput{Val: op.ObservedVal, Found: op.ObservedFound}
			if op.Op != "get" {
				pop.Output = check.KVOutput{Val: op.Value, Found: true}
			}
		} else {
			pop.Output = check.KVOutput{Unknown: true}
			pop.Return = end
		}
		if pop.Return <= pop.Call {
			pop.Return = pop.Call + 1
		}
		out = append(out, pop)
	}
	return out
}

// AckedWrites tracks every write the cluster acknowledged, so the driver can
// prove afterwards that none of them was lost. Each key is written at most
// once by construction, so the acknowledged value for a key is unambiguous.
type AckedWrites struct {
	mu     sync.Mutex
	values map[string]string
	order  []string
}

func NewAckedWrites() *AckedWrites {
	return &AckedWrites{values: map[string]string{}}
}

// Record notes an acknowledged write.
func (a *AckedWrites) Record(key, value string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, seen := a.values[key]; !seen {
		a.order = append(a.order, key)
	}
	a.values[key] = value
}

// Len reports how many keys have an acknowledged value.
func (a *AckedWrites) Len() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.order)
}

// All returns the acknowledged key/value pairs in write order.
func (a *AckedWrites) All() []KeyValue {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]KeyValue, 0, len(a.order))
	for _, k := range a.order {
		out = append(out, KeyValue{Key: k, Value: a.values[k]})
	}
	return out
}

// Sample returns one acknowledged key chosen by n modulo the set size, or
// "" when nothing has been acknowledged yet.
func (a *AckedWrites) Sample(n int) string {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.order) == 0 {
		return ""
	}
	if n < 0 {
		n = -n
	}
	return a.order[n%len(a.order)]
}

// KeyValue is one acknowledged write.
type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}
