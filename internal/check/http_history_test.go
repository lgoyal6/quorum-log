package check

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"quorumlog/internal/sm"
)

func TestHTTPHistoryChecksActualInvocationAndResponseIntervals(t *testing.T) {
	events := []HTTPEvent{
		{ID: "put-1", ClientID: 1, At: 10, Input: &KVInput{Op: sm.OpPut, Key: "k", Val: "v"}},
		{ID: "put-1", ClientID: 1, At: 20, Output: &KVOutput{Val: "v", Found: true}},
		{ID: "get-1", ClientID: 2, At: 30, Input: &KVInput{Op: "get", Key: "k"}},
		{ID: "get-1", ClientID: 2, At: 40, Output: &KVOutput{Val: "v", Found: true}},
	}
	result, err := CheckHTTPHistory(events)
	if err != nil || result != porcupine.Ok {
		t.Fatalf("HTTP history = %v, %v; want linearizable", result, err)
	}
}

func TestHTTPHistoryRetainsIndeterminateRequestAndRejectsMissingInvocation(t *testing.T) {
	events := []HTTPEvent{{ID: "timeout", At: 10, Input: &KVInput{Op: sm.OpPut, Key: "k", Val: "v"}}}
	history, err := HTTPHistory(events)
	if err != nil || len(history) != 1 || !history[0].Output.(KVOutput).Unknown {
		t.Fatalf("pending request lost its indeterminate result: %#v, %v", history, err)
	}
	_, err = HTTPHistory([]HTTPEvent{{ID: "orphan", At: 20, Output: &KVOutput{Found: true}}})
	if err == nil {
		t.Fatal("response without invocation must not become a valid history")
	}
}

func TestHTTPHistoryRejectsDuplicateOperationIdentity(t *testing.T) {
	in := &KVInput{Op: sm.OpAppend, Key: "k", Val: "x"}
	_, err := HTTPHistory([]HTTPEvent{
		{ID: "same", At: 1, Input: in},
		{ID: "same", At: 2, Input: in},
	})
	if err == nil {
		t.Fatal("duplicate request identity must be rejected")
	}
}

func TestHTTPHistoryRefusesReusedCompletedIDAndWrongClient(t *testing.T) {
	in := &KVInput{Op: sm.OpAppend, Key: "k", Val: "x"}
	for _, events := range [][]HTTPEvent{
		{{ID: "x", ClientID: 1, At: 1, Input: in}, {ID: "x", ClientID: 1, At: 2, Output: &KVOutput{}}, {ID: "x", ClientID: 1, At: 3, Input: in}},
		{{ID: "x", ClientID: 1, At: 1, Input: in}, {ID: "x", ClientID: 2, At: 2, Output: &KVOutput{}}},
	} {
		if _, err := HTTPHistory(events); err == nil {
			t.Fatal("ambiguous identity must be rejected")
		}
	}
}

func TestHTTPHistoryNegativeControlRejectsLostAcknowledgedWrite(t *testing.T) {
	result, err := CheckHTTPHistory([]HTTPEvent{
		{ID: "put", At: 1, Input: &KVInput{Op: sm.OpPut, Key: "k", Val: "v"}},
		{ID: "put", At: 2, Output: &KVOutput{Val: "v", Found: true}},
		{ID: "read", At: 3, Input: &KVInput{Op: "get", Key: "k"}},
		{ID: "read", At: 4, Output: &KVOutput{Val: "", Found: false}},
	})
	if err != nil || result != porcupine.Illegal {
		t.Fatalf("lost write must fail checker: %v %v", result, err)
	}
}

func TestHTTPTimeoutIsIndeterminateBeyondClientReturn(t *testing.T) {
	result, err := CheckHTTPHistory([]HTTPEvent{
		{ID: "uncertain", At: 1, Input: &KVInput{Op: sm.OpAppend, Key: "k", Val: "x"}},
		{ID: "uncertain", At: 2, Output: &KVOutput{Unknown: true}},
		{ID: "later", ClientID: 2, At: 3, Input: &KVInput{Op: "get", Key: "k"}},
		{ID: "later", ClientID: 2, At: 4, Output: &KVOutput{Found: false}},
	})
	if err != nil || result != porcupine.Ok {
		t.Fatalf("timeout may have no visible effect: %v %v", result, err)
	}
}

func TestUnknownReadsDoNotHideAcknowledgedWriteLoss(t *testing.T) {
	events := []HTTPEvent{
		{ID: "write", At: 1, Input: &KVInput{Op: sm.OpPut, Key: "k", Val: "v"}},
		{ID: "write", At: 2, Output: &KVOutput{Found: true, Val: "v"}},
	}
	for i := 0; i < 20; i++ {
		id := fmt.Sprintf("unknown-read-%d", i)
		events = append(events, HTTPEvent{ID: id, At: int64(3 + i*2), Input: &KVInput{Op: "get", Key: "k"}}, HTTPEvent{ID: id, At: int64(4 + i*2), Output: &KVOutput{Unknown: true}})
	}
	events = append(events, HTTPEvent{ID: "final", At: 50, Input: &KVInput{Op: "get", Key: "k"}}, HTTPEvent{ID: "final", At: 51, Output: &KVOutput{Found: false}})
	result, err := CheckHTTPHistoryWithin(events, time.Second)
	if err != nil || result != porcupine.Illegal {
		t.Fatalf("unknown reads masked loss: %v %v", result, err)
	}
	// Repair the final observation; the valid control must still be accepted.
	events[len(events)-1].Output = &KVOutput{Found: true, Val: "v"}
	result, err = CheckHTTPHistoryWithin(events, time.Second)
	if err != nil || result != porcupine.Ok {
		t.Fatalf("valid history refused: %v %v", result, err)
	}
	history, err := HTTPHistory(events)
	if err != nil || len(history) != 22 {
		t.Fatal("raw conversion dropped unknown attempts")
	}
}

func TestPendingWritesMayFollowLastAcknowledgedRead(t *testing.T) {
	var events []HTTPEvent
	for i := 0; i < 12; i++ {
		id := fmt.Sprintf("pending-%d", i)
		events = append(events, HTTPEvent{ID: id, At: int64(i*2 + 1), Input: &KVInput{Op: sm.OpAppend, Key: "k", Val: fmt.Sprintf("[%d]", i)}}, HTTPEvent{ID: id, At: int64(i*2 + 2), Output: &KVOutput{Unknown: true}})
	}
	events = append(events, HTTPEvent{ID: "final", At: 30, Input: &KVInput{Op: "get", Key: "k"}}, HTTPEvent{ID: "final", At: 31, Output: &KVOutput{Found: false}})
	result, err := CheckHTTPHistoryWithin(events, time.Second)
	if err != nil || result != porcupine.Ok {
		t.Fatalf("pending writes can all linearize after final read: %v %v", result, err)
	}
}

func TestPendingWriteNeededByReadIsCheckedInFullHistory(t *testing.T) {
	events := []HTTPEvent{{ID: "pending", At: 1, Input: &KVInput{Op: sm.OpPut, Key: "k", Val: "v"}}, {ID: "pending", At: 2, Output: &KVOutput{Unknown: true}}, {ID: "read", At: 3, Input: &KVInput{Op: "get", Key: "k"}}, {ID: "read", At: 4, Output: &KVOutput{Found: true, Val: "v"}}}
	result, err := CheckHTTPHistoryWithin(events, time.Second)
	if err != nil || result != porcupine.Ok {
		t.Fatalf("required unknown write lost: %v %v", result, err)
	}
	events[len(events)-1].Output = &KVOutput{Found: true, Val: "different"}
	result, err = CheckHTTPHistoryWithin(events, time.Second)
	if err != nil || result != porcupine.Illegal {
		t.Fatalf("impossible read hidden by projection: %v %v", result, err)
	}
}

func TestHTTPHistoryRejectsUnsupportedUnknownOperation(t *testing.T) {
	_, err := CheckHTTPHistoryWithin([]HTTPEvent{{ID: "invalid", At: 1, Input: &KVInput{Op: "invalid", Key: "k"}}}, time.Second)
	if err == nil {
		t.Fatal("unsupported pending operation must not use successful empty projection")
	}
}

func TestHTTPHistoryRefusesInvalidTimesAndUnboundedBudget(t *testing.T) {
	for _, at := range []int64{-1, math.MaxInt64} {
		if _, err := HTTPHistory([]HTTPEvent{{ID: "bad-time", At: at, Input: &KVInput{Op: "get", Key: "k"}}}); err == nil {
			t.Fatal("invalid timestamp accepted")
		}
	}
	verdict, err := CheckHTTPHistoryWithin([]HTTPEvent{{ID: "open", At: 1, Input: &KVInput{Op: "get", Key: "k"}}}, 0)
	if err != nil || verdict != porcupine.Unknown {
		t.Fatal("nonpositive checker budget must not start unbounded search")
	}
}
