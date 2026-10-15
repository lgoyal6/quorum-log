package check

import (
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/anishathalye/porcupine"
	"quorumlog/internal/sm"
)

// HTTPEvent is one client-side observation around an actual HTTP operation.
// Timestamps are monotonic nanoseconds from one process clock. A missing return
// is retained as an indeterminate operation, never reclassified as a failure.
type HTTPEvent struct {
	ID       string    `json:"id"`
	ClientID int       `json:"client_id"`
	At       int64     `json:"at"`
	Input    *KVInput  `json:"input,omitempty"`
	Output   *KVOutput `json:"output,omitempty"`
}

// HTTPHistory converts invocation/response observations to the existing
// Porcupine history format. Unknown outputs remain unknown; malformed or
// ambiguous event identities fail closed.
func HTTPHistory(events []HTTPEvent) ([]porcupine.Operation, error) {
	type pending struct {
		client int
		input  KVInput
		call   int64
	}
	open := map[string]pending{}
	seen := map[string]bool{}
	var operations []porcupine.Operation
	ordered := append([]HTTPEvent(nil), events...)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].At < ordered[j].At })
	var end int64
	for _, event := range ordered {
		if event.At < 0 || event.At == math.MaxInt64 {
			return nil, fmt.Errorf("event %q has invalid timestamp", event.ID)
		}
		if event.ID == "" {
			return nil, fmt.Errorf("event has empty operation id")
		}
		if event.At > end {
			end = event.At
		}
		if event.Input != nil {
			if event.Output != nil {
				return nil, fmt.Errorf("event %q has both invocation and response", event.ID)
			}
			switch event.Input.Op {
			case "get", sm.OpPut, sm.OpAppend, sm.OpDelete:
			default:
				return nil, fmt.Errorf("event %q has unsupported operation", event.ID)
			}
			if event.Input.Key == "" || event.Input.Op == "" {
				return nil, fmt.Errorf("event %q has incomplete invocation", event.ID)
			}
			if seen[event.ID] {
				return nil, fmt.Errorf("duplicate invocation %q", event.ID)
			}
			seen[event.ID] = true
			open[event.ID] = pending{event.ClientID, *event.Input, event.At}
			continue
		}
		if event.Output == nil {
			return nil, fmt.Errorf("event %q has neither invocation nor response", event.ID)
		}
		p, exists := open[event.ID]
		if !exists {
			return nil, fmt.Errorf("response without invocation %q", event.ID)
		}
		if event.ClientID != p.client {
			return nil, fmt.Errorf("response client differs from invocation %q", event.ID)
		}
		delete(open, event.ID)
		operations = append(operations, porcupine.Operation{ClientId: p.client, Input: p.input, Call: p.call, Output: *event.Output, Return: event.At})
	}
	for _, p := range open {
		operations = append(operations, porcupine.Operation{ClientId: p.client, Input: p.input, Call: p.call, Output: KVOutput{Unknown: true}, Return: end + 1})
	}
	// A client timeout is an observation of uncertainty, not proof that the
	// operation completed then. Preserve the same open interval as a missing
	// response, allowing its effect to be absent from the finite observed prefix.
	for i := range operations {
		if operations[i].Output.(KVOutput).Unknown {
			operations[i].Return = end + 1
		}
	}
	sort.SliceStable(operations, func(i, j int) bool { return operations[i].Call < operations[j].Call })
	return operations, nil
}

// CheckHTTPHistory applies the same sequential model used for simulator
// histories to client-side observations from the HTTP API.
func CheckHTTPHistory(events []HTTPEvent) (porcupine.CheckResult, error) {
	return CheckHTTPHistoryWithin(events, 30*time.Second)
}

// CheckHTTPHistoryWithin bounds search time without changing unknown outcomes.
// Unknown reads have no observed value and never change KVModel state. Removing
// only those no-ops preserves existence of a linearization: they can be inserted
// anywhere in their intervals. Raw events and HTTPHistory retain every attempt.
func CheckHTTPHistoryWithin(events []HTTPEvent, timeout time.Duration) (porcupine.CheckResult, error) {
	history, err := HTTPHistory(events)
	if err != nil {
		return porcupine.Unknown, err
	}
	if timeout <= 0 {
		return porcupine.Unknown, nil
	}
	constraints := make([]porcupine.Operation, 0, len(history))
	for _, op := range history {
		if op.Input.(KVInput).Op == "get" && op.Output.(KVOutput).Unknown {
			continue
		}
		constraints = append(constraints, op)
	}
	// A successful acknowledged-only projection supplies a full-history witness:
	// every unknown operation can be inserted after the final observed return,
	// within its open end+1 interval. This is only a sufficient fast path. If
	// uncertain writes explain an observed read, the projection may be illegal;
	// in that case the complete constraints must still be checked.
	start := time.Now()
	acknowledged := make([]porcupine.Operation, 0, len(constraints))
	for _, op := range constraints {
		if !op.Output.(KVOutput).Unknown {
			acknowledged = append(acknowledged, op)
		}
	}
	if len(acknowledged) < len(constraints) {
		budget := timeout / 4
		if budget <= 0 {
			return porcupine.Unknown, nil
		}
		result, _ := porcupine.CheckOperationsVerbose(KVModel, acknowledged, budget)
		if result == porcupine.Ok {
			return result, nil
		}
		timeout -= time.Since(start)
		if timeout <= 0 {
			return porcupine.Unknown, nil
		}
	}
	result, _ := porcupine.CheckOperationsVerbose(KVModel, constraints, timeout)
	return result, nil
}
