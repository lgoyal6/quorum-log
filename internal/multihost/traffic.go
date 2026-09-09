package multihost

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// trafficClient is one driver-side client. It follows leader redirects and
// retries, which is the documented way to use the store: a retried write
// carries the same client_id and req_id, and the state machine deduplicates
// it, so a retry is the same logical operation.
type trafficClient struct {
	http        *http.Client
	endpoints   []string
	leader      string
	maxAttempts int
	backoff     time.Duration
	rng         *rand.Rand
}

func newTrafficClient(endpoints []string, seed int64) *trafficClient {
	return &trafficClient{
		http: &http.Client{
			Timeout: 3 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        64,
				MaxIdleConnsPerHost: 16,
				IdleConnTimeout:     90 * time.Second,
			},
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse // 307 is handled explicitly
			},
		},
		endpoints:   endpoints,
		maxAttempts: 12,
		backoff:     60 * time.Millisecond,
		rng:         rand.New(rand.NewSource(seed)),
	}
}

func (c *trafficClient) base() string {
	if c.leader != "" {
		return c.leader
	}
	return c.endpoints[c.rng.Intn(len(c.endpoints))]
}

type opOutcome struct {
	ok        bool
	code      int
	attempts  int
	redirects int
	endpoint  string
	val       string
	found     bool
	err       error
}

// put performs one logical write, retrying with the same client_id/req_id.
func (c *trafficClient) put(ctx context.Context, key, value, session string, reqID uint64) opOutcome {
	body, _ := json.Marshal(map[string]interface{}{
		"op": "put", "value": value, "client_id": session, "req_id": reqID,
	})
	var out opOutcome
	for attempt := 0; attempt < c.maxAttempts; attempt++ {
		out.attempts++
		endpoint := c.base()
		out.endpoint = endpoint
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint+"/kv/"+key, bytes.NewReader(body))
		if err != nil {
			out.err = err
			return out
		}
		resp, err := c.http.Do(req)
		if err != nil {
			out.err = err
			c.leader = ""
			if ctx.Err() != nil {
				return out
			}
			time.Sleep(c.backoff)
			continue
		}
		out.code = resp.StatusCode
		location := resp.Header.Get("Location")
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		switch {
		case resp.StatusCode == http.StatusOK:
			out.ok = true
			out.val = value
			out.found = true
			out.err = nil
			return out
		case resp.StatusCode == http.StatusTemporaryRedirect && strings.Contains(location, "/kv/"):
			out.redirects++
			c.leader = leaderFromLocation(location)
		default:
			out.err = fmt.Errorf("http %d", resp.StatusCode)
			c.leader = ""
			time.Sleep(c.backoff)
		}
		if ctx.Err() != nil {
			return out
		}
	}
	return out
}

// get performs one linearizable read, retrying on failure.
func (c *trafficClient) get(ctx context.Context, key string) opOutcome {
	var out opOutcome
	for attempt := 0; attempt < c.maxAttempts; attempt++ {
		out.attempts++
		endpoint := c.base()
		out.endpoint = endpoint
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"/kv/"+key+"?consistency=linearizable", nil)
		if err != nil {
			out.err = err
			return out
		}
		resp, err := c.http.Do(req)
		if err != nil {
			out.err = err
			c.leader = ""
			if ctx.Err() != nil {
				return out
			}
			time.Sleep(c.backoff)
			continue
		}
		out.code = resp.StatusCode
		if resp.StatusCode == http.StatusOK {
			var kv struct {
				Value string `json:"value"`
				Found bool   `json:"found"`
			}
			err := json.NewDecoder(resp.Body).Decode(&kv)
			resp.Body.Close()
			if err != nil {
				out.err = err
				time.Sleep(c.backoff)
				continue
			}
			out.ok = true
			out.val = kv.Value
			out.found = kv.Found
			out.err = nil
			return out
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		out.err = fmt.Errorf("http %d", resp.StatusCode)
		c.leader = ""
		time.Sleep(c.backoff)
		if ctx.Err() != nil {
			return out
		}
	}
	return out
}

func leaderFromLocation(location string) string {
	i := strings.Index(location, "/kv/")
	if i < 0 {
		return ""
	}
	return strings.TrimSuffix(location[:i], "/")
}

// Workload drives the client traffic and records the observed history.
type Workload struct {
	Inv   *Inventory
	Rec   *Recorder
	Acked *AckedWrites

	completed atomic.Int64
	writes    atomic.Int64
	reads     atomic.Int64
	lost      atomic.Int64
	finished  atomic.Bool
}

// NewWorkload prepares the workload.
func NewWorkload(inv *Inventory, rec *Recorder, acked *AckedWrites) *Workload {
	return &Workload{Inv: inv, Rec: rec, Acked: acked}
}

// Completed reports how many operations have finished so far. The fault
// scheduler watches this counter.
func (w *Workload) Completed() int { return int(w.completed.Load()) }

// Finished reports whether the traffic phase has stopped. The fault
// scheduler uses it so a threshold the traffic never reaches fails fast
// instead of waiting for the run deadline.
func (w *Workload) Finished() bool { return w.finished.Load() }

// Endpoints lists the client endpoints (one per host).
func (w *Workload) Endpoints() []string {
	out := make([]string, 0, len(w.Inv.Hosts))
	for _, h := range w.Inv.Hosts {
		out = append(out, h.RaftURL())
	}
	return out
}

// Run drives the configured number of operations. It keeps going until both
// the operation target is reached and the fault plan has finished, so the
// history always covers every fault; the actual counts are reported, never
// assumed.
func (w *Workload) Run(ctx context.Context, faultsDone <-chan struct{}) error {
	defer w.finished.Store(true)
	target := w.Inv.Client.Ops
	hardCap := target * 4
	endpoints := w.Endpoints()
	var wg sync.WaitGroup
	for worker := 0; worker < w.Inv.Client.Concurrency; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			seed := w.Inv.Client.Seed + int64(worker)
			c := newTrafficClient(endpoints, seed)
			session := fmt.Sprintf("qlmultihost-w%d", worker)
			var reqID uint64
			for {
				if ctx.Err() != nil {
					return
				}
				done := int(w.completed.Load())
				if done >= hardCap {
					return
				}
				if done >= target && channelClosed(faultsDone) {
					return
				}
				readNow := c.rng.Float64() < w.Inv.Client.ReadFraction
				key := ""
				if readNow {
					key = w.Acked.Sample(c.rng.Int())
					if key == "" {
						readNow = false
					}
				}
				start := time.Now()
				if readNow {
					out := c.get(ctx, key)
					w.record(worker, session, "get", key, "", 0, start, out)
					w.reads.Add(1)
				} else {
					reqID++
					key = fmt.Sprintf("mh-w%d-%06d", worker, reqID)
					value := fmt.Sprintf("v-w%d-%06d", worker, reqID)
					out := c.put(ctx, key, value, session, reqID)
					w.record(worker, session, "put", key, value, reqID, start, out)
					if out.ok {
						w.Acked.Record(key, value)
					}
					w.writes.Add(1)
				}
				w.completed.Add(1)
			}
		}(worker)
	}
	wg.Wait()
	return nil
}

func (w *Workload) record(worker int, session, op, key, value string, reqID uint64, start time.Time, out opOutcome) {
	rec := OpRecord{
		ClientID:      worker,
		ClientSession: session,
		ReqID:         reqID,
		Op:            op,
		Key:           key,
		Value:         value,
		CallNanos:     w.Rec.Since(start),
		ReturnNanos:   w.Rec.Since(time.Now()),
		HTTPCode:      out.code,
		Attempts:      out.attempts,
		Redirects:     out.redirects,
		Endpoint:      out.endpoint,
		ObservedVal:   out.val,
		ObservedFound: out.found,
	}
	if out.ok {
		rec.Status = StatusOK
	} else {
		rec.Status = StatusIndeterminate
		if out.err != nil {
			rec.Error = out.err.Error()
		}
	}
	w.Rec.Add(rec)
}

func channelClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// Mismatch is an acknowledged write that a later linearizable read did not
// return. Any mismatch is a hard failure of the gate.
type Mismatch struct {
	Key           string `json:"key"`
	Expected      string `json:"expected"`
	ObservedVal   string `json:"observed_val"`
	ObservedFound bool   `json:"observed_found"`
	Status        string `json:"status"`
	HTTPCode      int    `json:"http_code,omitempty"`
	Error         string `json:"error,omitempty"`
}

// VerifyAcked reads every acknowledged write with a linearizable read and
// returns the ones that are missing or changed. The reads are recorded in the
// history too, so the linearizability checker sees them.
func (w *Workload) VerifyAcked(ctx context.Context, phase string) ([]Mismatch, int, error) {
	acked := w.Acked.All()
	client := newTrafficClient(w.Endpoints(), w.Inv.Client.Seed+9999)
	verifierID := w.Inv.Client.Concurrency // a client id no worker used
	var mismatches []Mismatch
	for _, kv := range acked {
		if ctx.Err() != nil {
			return mismatches, len(acked), ctx.Err()
		}
		start := time.Now()
		out := client.get(ctx, kv.Key)
		rec := OpRecord{
			ClientID:      verifierID,
			ClientSession: "qlmultihost-verify",
			Op:            "get",
			Key:           kv.Key,
			CallNanos:     w.Rec.Since(start),
			ReturnNanos:   w.Rec.Since(time.Now()),
			HTTPCode:      out.code,
			Attempts:      out.attempts,
			Redirects:     out.redirects,
			Endpoint:      out.endpoint,
			ObservedVal:   out.val,
			ObservedFound: out.found,
			Phase:         phase,
		}
		if out.ok {
			rec.Status = StatusOK
		} else {
			rec.Status = StatusIndeterminate
			if out.err != nil {
				rec.Error = out.err.Error()
			}
		}
		w.Rec.Add(rec)

		if !out.ok {
			mismatches = append(mismatches, Mismatch{
				Key: kv.Key, Expected: kv.Value, Status: rec.Status, HTTPCode: out.code,
				Error: rec.Error,
			})
			continue
		}
		if !out.found || out.val != kv.Value {
			w.lost.Add(1)
			mismatches = append(mismatches, Mismatch{
				Key: kv.Key, Expected: kv.Value, ObservedVal: out.val,
				ObservedFound: out.found, Status: rec.Status, HTTPCode: out.code,
			})
		}
	}
	return mismatches, len(acked), nil
}
