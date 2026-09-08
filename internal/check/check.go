// Package check provides a history recorder and a linearizability checker
// for the quorum-log KV operations. The checker uses porcupine
// (github.com/anishathalye/porcupine), a well-known linearizability checker;
// this package only supplies the sequential KV model.
package check

import (
	"fmt"
	"sync"
	"time"

	"github.com/anishathalye/porcupine"

	"quorumlog/internal/sm"
)

// KVInput describes an invoked operation.
type KVInput struct {
	Op  string // sm.OpPut, sm.OpAppend, sm.OpDelete, or "get"
	Key string
	Val string
}

// KVOutput describes an observed response. Unknown marks operations that
// never received a response (client gave up / run ended); the checker allows
// them to take effect at any point after invocation, or effectively never
// (they can always be linearized at the end of the history).
type KVOutput struct {
	Val     string
	Found   bool
	Unknown bool
}

// Recorder collects a concurrent history of operations for the checker.
type Recorder struct {
	mu   sync.Mutex
	next int
	ops  map[int]*porcupine.Operation
	done []porcupine.Operation
	now  func() int64
}

// NewRecorder creates a recorder. now supplies timestamps; the simulator
// passes its logical clock, the live server passes wall time.
func NewRecorder(now func() int64) *Recorder {
	return &Recorder{ops: map[int]*porcupine.Operation{}, now: now}
}

// Invoke records the start of an operation and returns its handle.
func (r *Recorder) Invoke(clientID int, in KVInput) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	id := r.next
	r.next++
	r.ops[id] = &porcupine.Operation{ClientId: clientID, Input: in, Call: r.now()}
	return id
}

// Return records the completion of an operation.
func (r *Recorder) Return(id int, out KVOutput) {
	r.mu.Lock()
	defer r.mu.Unlock()
	op, ok := r.ops[id]
	if !ok {
		return
	}
	delete(r.ops, id)
	op.Output = out
	op.Return = r.now()
	r.done = append(r.done, *op)
}

// History finalizes and returns all operations. Operations that never
// returned are closed with an Unknown output and an end-of-history return
// time, the standard treatment for indeterminate operations.
func (r *Recorder) History() []porcupine.Operation {
	r.mu.Lock()
	defer r.mu.Unlock()
	hist := append([]porcupine.Operation(nil), r.done...)
	end := r.now() + 1
	for _, op := range r.ops {
		o := *op
		o.Output = KVOutput{Unknown: true}
		o.Return = end
		hist = append(hist, o)
	}
	return hist
}

type kvState struct {
	val   string
	found bool
}

// KVModel is the sequential specification of the store, partitioned by key.
var KVModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		m := map[string][]porcupine.Operation{}
		for _, op := range history {
			key := op.Input.(KVInput).Key
			m[key] = append(m[key], op)
		}
		parts := make([][]porcupine.Operation, 0, len(m))
		for _, ops := range m {
			parts = append(parts, ops)
		}
		return parts
	},
	Init: func() interface{} { return kvState{} },
	Step: func(state, input, output interface{}) (bool, interface{}) {
		st := state.(kvState)
		in := input.(KVInput)
		out := output.(KVOutput)
		switch in.Op {
		case sm.OpPut:
			return true, kvState{val: in.Val, found: true}
		case sm.OpAppend:
			return true, kvState{val: st.val + in.Val, found: true}
		case sm.OpDelete:
			return true, kvState{}
		case "get":
			if out.Unknown {
				// Read never returned: imposes no constraint.
				return true, st
			}
			return out.Found == st.found && out.Val == st.val, st
		default:
			return false, st
		}
	},
	DescribeOperation: func(input, output interface{}) string {
		in := input.(KVInput)
		out := output.(KVOutput)
		if in.Op == "get" {
			return fmt.Sprintf("get(%q) -> (%q,%v,unk=%v)", in.Key, out.Val, out.Found, out.Unknown)
		}
		return fmt.Sprintf("%s(%q,%q)", in.Op, in.Key, in.Val)
	},
}

// Check runs porcupine over the history and reports whether it is
// linearizable with respect to the KV model.
func Check(history []porcupine.Operation) porcupine.CheckResult {
	res, _ := porcupine.CheckOperationsVerbose(KVModel, history, 30*time.Second)
	return res
}
