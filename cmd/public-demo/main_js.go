//go:build js && wasm

package main

import (
	"encoding/json"
	"fmt"
	"quorumlog/internal/check"
	"syscall/js"
	"time"
)

func evaluate(raw []byte) (any, error) {
	var events []check.HTTPEvent
	if err := json.Unmarshal(raw, &events); err != nil {
		return nil, err
	}
	if len(events) < 1 || len(events) > 24 {
		return nil, fmt.Errorf("Use 1-24 events")
	}
	for _, e := range events {
		if len(e.ID) > 64 || e.ClientID < 0 || e.ClientID > 16 || e.At > 1000000 {
			return nil, fmt.Errorf("IDs max 64 characters, clients 0-16, time 0-1000000")
		}
		if e.Input != nil && (len(e.Input.Key) > 64 || len(e.Input.Val) > 128) {
			return nil, fmt.Errorf("Key max 64 bytes; value max 128 bytes")
		}
		if e.Output != nil && len(e.Output.Val) > 128 {
			return nil, fmt.Errorf("Output value max 128 bytes")
		}
	}
	history, err := check.HTTPHistory(events)
	if err != nil {
		return nil, err
	}
	unknown := 0
	for _, op := range history {
		if op.Output.(check.KVOutput).Unknown {
			unknown++
		}
	}
	result, err := check.CheckHTTPHistoryWithin(events, 100*time.Millisecond)
	if err != nil {
		return nil, err
	}
	return map[string]any{"result": result, "operations": len(history), "indeterminate": unknown, "budget_ms": 100}, nil
}

func main() {
	callback := js.FuncOf(func(_ js.Value, args []js.Value) any {
		if len(args) != 1 || args[0].Type() != js.TypeString {
			return `{"error":"Expected one JSON input"}`
		}
		raw := args[0].String()
		var result any
		if len(raw) > 32768 {
			result = map[string]string{"error": "Input exceeds 32 KiB"}
		} else {
			value, err := evaluate([]byte(raw))
			if err != nil {
				result = map[string]string{"error": err.Error()}
			} else {
				result = value
			}
		}
		encoded, err := json.Marshal(result)
		if err != nil {
			return fmt.Sprintf(`{"error":%q}`, err.Error())
		}
		return string(encoded)
	})
	js.Global().Set("publicDemo", callback)
	select {}
}
