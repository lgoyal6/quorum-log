// qlchaos records actual HTTP operations on owned local Raft child processes.
// No external endpoints, models, accounts or production data are accepted.
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anishathalye/porcupine"
	"go.etcd.io/raft/v3/raftpb"
	"quorumlog/internal/check"
)

type faults struct {
	sync.RWMutex
	isolated        map[uint64]bool
	delay           time.Duration
	loseResponse    atomic.Bool
	lost            atomic.Int64
	partitioned     atomic.Int64
	delayed         atomic.Int64
	kills           atomic.Int64
	killAfterCommit func()
}
type cluster struct {
	n         int
	bin, dir  string
	targets   []string
	proxy     []*httptest.Server
	peers     string
	mu        sync.Mutex
	processes map[int]*exec.Cmd
	logs      []*os.File
	faults    faults
}

func port() string {
	l, e := net.Listen("tcp", "127.0.0.1:0")
	must(e)
	a := l.Addr().String()
	l.Close()
	return a
}
func must(err error) {
	if err != nil {
		panic(err)
	}
}
func newCluster(n int, bin, dir string) *cluster {
	c := &cluster{n: n, bin: bin, dir: dir, processes: map[int]*exec.Cmd{}}
	c.faults.isolated = map[uint64]bool{}
	must(os.MkdirAll(dir, 0700))
	for i := 0; i < n; i++ {
		c.targets = append(c.targets, "http://"+port())
	}
	for i := 0; i < n; i++ {
		target := c.targets[i]
		id := i + 1
		p := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, e := io.ReadAll(io.LimitReader(r.Body, 8<<20))
			if e != nil {
				http.Error(w, "read", 400)
				return
			}
			if r.URL.Path == "/raft" {
				var m raftpb.Message
				if m.Unmarshal(body) != nil {
					http.Error(w, "message", 400)
					return
				}
				c.faults.RLock()
				blocked := c.faults.isolated[m.From] != c.faults.isolated[uint64(id)]
				delay := c.faults.delay
				c.faults.RUnlock()
				if blocked {
					c.faults.partitioned.Add(1)
					http.Error(w, "injected partition", 503)
					return
				}
				if delay > 0 {
					c.faults.delayed.Add(1)
					select {
					case <-time.After(delay):
					case <-r.Context().Done():
						return
					}
				}
			}
			req, e := http.NewRequestWithContext(r.Context(), r.Method, target+r.URL.RequestURI(), bytes.NewReader(body))
			if e != nil {
				http.Error(w, "request", 500)
				return
			}
			req.Header = r.Header.Clone()
			response, e := http.DefaultTransport.RoundTrip(req)
			if e != nil {
				http.Error(w, "owned node unavailable", 503)
				return
			}
			defer response.Body.Close()
			data, e := io.ReadAll(response.Body)
			if e != nil {
				http.Error(w, "response", 503)
				return
			}
			if r.Method == "PUT" && response.StatusCode == 200 && c.faults.loseResponse.CompareAndSwap(true, false) {
				c.faults.lost.Add(1)
				c.faults.RLock()
				callback := c.faults.killAfterCommit
				c.faults.RUnlock()
				if callback != nil {
					callback()
				}
				<-r.Context().Done()
				return
			}
			for key, values := range response.Header {
				w.Header()[key] = values
			}
			w.WriteHeader(response.StatusCode)
			_, _ = w.Write(data)
		}))
		c.proxy = append(c.proxy, p)
	}
	var parts []string
	for i, p := range c.proxy {
		parts = append(parts, fmt.Sprintf("%d=%s", i+1, p.URL))
	}
	c.peers = strings.Join(parts, ",")
	for i := 1; i <= n; i++ {
		c.start(i)
	}
	return c
}
func (c *cluster) start(id int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	u, _ := url.Parse(c.targets[id-1])
	log, e := os.OpenFile(filepath.Join(c.dir, fmt.Sprintf("node-%d.log", id)), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	must(e)
	command := exec.Command(c.bin, "-id", strconv.Itoa(id), "-listen", u.Host, "-data", filepath.Join(c.dir, fmt.Sprintf("node-%d", id)), "-peers", c.peers, "-tick-ms", "10", "-snapshot-threshold", "8", "-snapshot-trailing", "2")
	command.Stdout = log
	command.Stderr = log
	must(command.Start())
	c.processes[id] = command
	c.logs = append(c.logs, log)
}
func (c *cluster) kill(id int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if p := c.processes[id]; p != nil {
		c.faults.kills.Add(1)
		_ = p.Process.Kill()
		_ = p.Wait()
		delete(c.processes, id)
	}
}
func (c *cluster) close() {
	for i := 1; i <= c.n; i++ {
		c.kill(i)
	}
	for _, p := range c.proxy {
		p.Close()
	}
	for _, l := range c.logs {
		_ = l.Close()
	}
}

type nodeStatus struct {
	ID        uint64 `json:"id"`
	Leader    uint64 `json:"leader"`
	IsLeader  bool   `json:"is_leader"`
	Applied   uint64 `json:"applied"`
	SnapIndex uint64 `json:"snap_index"`
}

func (c *cluster) status(id int) (nodeStatus, bool) {
	var s nodeStatus
	client := http.Client{Timeout: 100 * time.Millisecond}
	r, e := client.Get(c.proxy[id-1].URL + "/status")
	if e != nil {
		return s, false
	}
	defer r.Body.Close()
	return s, json.NewDecoder(r.Body).Decode(&s) == nil
}
func (c *cluster) leader() int {
	for i := 1; i <= c.n; i++ {
		s, ok := c.status(i)
		if ok && s.IsLeader {
			return i
		}
	}
	return 0
}
func (c *cluster) waitLeader() int {
	for i := 0; i < 100; i++ {
		if leader := c.leader(); leader > 0 {
			return leader
		}
		time.Sleep(20 * time.Millisecond)
	}
	return 0
}

type observation struct {
	check.HTTPEvent
	Code      int     `json:"http_code,omitempty"`
	Error     string  `json:"error,omitempty"`
	LatencyMS float64 `json:"latency_ms,omitempty"`
}
type recorder struct {
	mu        sync.Mutex
	start     time.Time
	events    []observation
	next      atomic.Uint64
	namespace string
}

func (rec *recorder) request(ctx context.Context, c *cluster, node, client int, in check.KVInput) {
	number := rec.next.Add(1)
	id := fmt.Sprintf("c%d-op%d", client, number)
	start := time.Now()
	rec.mu.Lock()
	rec.events = append(rec.events, observation{HTTPEvent: check.HTTPEvent{ID: id, ClientID: client, At: time.Since(rec.start).Nanoseconds(), Input: &in}})
	rec.mu.Unlock()
	method := http.MethodGet
	var body []byte
	if in.Op != "get" {
		method = http.MethodPut
		body, _ = json.Marshal(map[string]any{"op": in.Op, "value": in.Val, "client_id": fmt.Sprintf("%s-client-%d", rec.namespace, client), "req_id": number})
	}
	request, _ := http.NewRequestWithContext(ctx, method, c.proxy[node-1].URL+"/kv/"+in.Key, bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	hc := http.Client{Timeout: 180 * time.Millisecond, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	out := check.KVOutput{Unknown: true}
	code := 0
	detail := ""
	response, e := hc.Do(request)
	if e != nil {
		detail = e.Error()
	} else {
		code = response.StatusCode
		var data struct {
			Value string `json:"value"`
			Found bool   `json:"found"`
		}
		e = json.NewDecoder(response.Body).Decode(&data)
		response.Body.Close()
		if code == 200 && e == nil {
			out = check.KVOutput{Val: data.Value, Found: data.Found}
		} else if e != nil {
			detail = e.Error()
		}
	}
	rec.mu.Lock()
	rec.events = append(rec.events, observation{HTTPEvent: check.HTTPEvent{ID: id, ClientID: client, At: time.Since(rec.start).Nanoseconds(), Output: &out}, Code: code, Error: detail, LatencyMS: float64(time.Since(start).Microseconds()) / 1000})
	rec.mu.Unlock()
}

type result struct {
	Nodes, Seed                                           int
	Fault                                                 string
	Verdict                                               string
	Events                                                []observation
	Acknowledged, Unknown                                 int
	P99MS, RecoveryMS                                     float64
	SnapshotsObserved                                     int
	DroppedCommittedResponses                             int64
	PartitionedMessages, DelayedMessages, KilledProcesses int64
	DurationMS                                            float64
	WritesAcknowledged                                    int
	WorkloadDurationMS                                    float64
	SnapshotsAdvanced                                     int
	FinalReadAcknowledged                                 bool
}

func runSchedule(c *cluster, seed int) result {
	random := rand.New(rand.NewSource(int64(seed)))
	leader := c.waitLeader()
	if leader == 0 {
		panic("no initial leader")
	}
	names := []string{"stable", "leader_sigkill", "minority_partition", "delayed_raft", "committed_response_lost_and_sigkill", "follower_restart_during_snapshots"}
	kind := seed % len(names)
	beforeSnapshots := map[int]uint64{}
	for id := 1; id <= c.n; id++ {
		s, ok := c.status(id)
		if ok {
			beforeSnapshots[id] = s.SnapIndex
		}
	}
	record := &recorder{start: time.Now(), namespace: fmt.Sprintf("nodes-%d-seed-%d", c.n, seed)}
	done := make(chan struct{})
	killed := 0
	c.faults.lost.Store(0)
	c.faults.partitioned.Store(0)
	c.faults.delayed.Store(0)
	c.faults.kills.Store(0)
	switch kind {
	case 1:
		killed = leader
		go func() {
			time.Sleep(3 * time.Millisecond)
			c.kill(leader)
			time.Sleep(60 * time.Millisecond)
			c.start(leader)
			close(done)
		}()
	case 2:
		c.faults.Lock()
		for i := 1; i <= c.n/2; i++ {
			id := (leader+i-1)%c.n + 1
			c.faults.isolated[uint64(id)] = true
		}
		c.faults.Unlock()
		close(done)
	case 3:
		c.faults.Lock()
		c.faults.delay = time.Duration(2+random.Intn(5)) * time.Millisecond
		c.faults.Unlock()
		close(done)
	case 4:
		killed = leader
		c.faults.Lock()
		c.faults.killAfterCommit = func() { c.kill(leader) }
		c.faults.Unlock()
		c.faults.loseResponse.Store(true)
		close(done)
	case 5:
		killed = leader%c.n + 1
		go func() {
			time.Sleep(3 * time.Millisecond)
			c.kill(killed)
			time.Sleep(40 * time.Millisecond)
			c.start(killed)
			close(done)
		}()
	default:
		close(done)
	}
	var wg sync.WaitGroup
	for client := 1; client <= 2; client++ {
		wg.Add(1)
		go func(client int) {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				node := c.leader()
				if node == 0 {
					node = leader
				}
				in := check.KVInput{Op: "get", Key: fmt.Sprintf("seed-%d", seed)}
				if i%2 == 0 {
					in.Op = "append"
					in.Val = fmt.Sprintf("[%d:%d]", client, i)
				}
				record.request(context.Background(), c, node, client, in)
				time.Sleep(10 * time.Millisecond)
			}
		}(client)
	}
	wg.Wait()
	<-done
	if kind == 4 {
		c.faults.loseResponse.Store(false)
		c.faults.Lock()
		c.faults.killAfterCommit = nil
		c.faults.Unlock()
		if c.faults.lost.Load() > 0 {
			c.start(killed)
		}
	}
	c.faults.Lock()
	c.faults.isolated = map[uint64]bool{}
	c.faults.delay = 0
	c.faults.Unlock()
	recovery := time.Now()
	postLeader := c.waitLeader()
	if postLeader == 0 {
		panic("cluster did not recover")
	}
	record.request(context.Background(), c, postLeader, 3, check.KVInput{Op: "get", Key: fmt.Sprintf("seed-%d", seed)})
	recovered := float64(time.Since(recovery).Microseconds()) / 1000
	events := make([]check.HTTPEvent, len(record.events))
	latencies := []float64{}
	unknown, ack := 0, 0
	writes := 0
	inputs := map[string]check.KVInput{}
	for i, e := range record.events {
		events[i] = e.HTTPEvent
		if e.Input != nil {
			inputs[e.ID] = *e.Input
		}
		if e.Output != nil {
			latencies = append(latencies, e.LatencyMS)
			if e.Output.Unknown {
				unknown++
			} else {
				ack++
				if inputs[e.ID].Op != "get" {
					writes++
				}
			}
		}
	}
	workloadMS := float64(time.Since(record.start).Microseconds()) / 1000
	verdict, e := verdictFor(events)
	must(e)
	label := "INCONCLUSIVE"
	if verdict == porcupine.Ok {
		label = "LINEARIZABLE"
	} else if verdict == porcupine.Illegal {
		label = "VIOLATION"
	}
	sort.Float64s(latencies)
	snapshots := 0
	advanced := 0
	for id := 1; id <= c.n; id++ {
		s, ok := c.status(id)
		if ok && s.SnapIndex > 0 {
			snapshots++
			if s.SnapIndex > beforeSnapshots[id] {
				advanced++
			}
		}
	}
	return result{Nodes: c.n, Seed: seed, Fault: names[kind], Verdict: label, Events: record.events, Acknowledged: ack, Unknown: unknown, P99MS: latencies[len(latencies)-1], RecoveryMS: recovered, SnapshotsObserved: snapshots, DroppedCommittedResponses: c.faults.lost.Load(), PartitionedMessages: c.faults.partitioned.Load(), DelayedMessages: c.faults.delayed.Load(), KilledProcesses: c.faults.kills.Load(), DurationMS: float64(time.Since(record.start).Microseconds()) / 1000, WritesAcknowledged: writes, WorkloadDurationMS: workloadMS, SnapshotsAdvanced: advanced, FinalReadAcknowledged: record.events[len(record.events)-1].Output != nil && !record.events[len(record.events)-1].Output.Unknown}
}
func hash(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }
func verdictFor(events []check.HTTPEvent) (porcupine.CheckResult, error) {
	return check.CheckHTTPHistoryWithin(events, time.Second)
}

func replay(path string) {
	f, err := os.Open(path)
	must(err)
	defer f.Close()
	scan := bufio.NewScanner(f)
	scan.Buffer(make([]byte, 65536), 8<<20)
	counts := map[string]int{}
	for scan.Scan() {
		var row result
		must(json.Unmarshal(scan.Bytes(), &row))
		events := make([]check.HTTPEvent, len(row.Events))
		for i, e := range row.Events {
			events[i] = e.HTTPEvent
		}
		v, err := verdictFor(events)
		must(err)
		label := "INCONCLUSIVE"
		if v == porcupine.Ok {
			label = "LINEARIZABLE"
		} else if v == porcupine.Illegal {
			label = "VIOLATION"
		}
		counts[label]++
		if label != row.Verdict {
			fmt.Fprintf(os.Stderr, "verdict change nodes=%d seed=%d: %s -> %s\n", row.Nodes, row.Seed, row.Verdict, label)
		}
	}
	must(scan.Err())
	b, _ := json.MarshalIndent(counts, "", "  ")
	fmt.Println(string(b))
}
func main() {
	bin := flag.String("node-bin", "", "owned quorumlogd binary")
	out := flag.String("out", "", "private output directory")
	count := flag.Int("schedules", 100, "seeded schedules per cluster size")
	replayFile := flag.String("replay", "", "recheck saved raw histories without starting processes")
	lock := flag.Bool("lock-only", false, "freeze protocol without running")
	flag.Parse()
	if *replayFile != "" {
		replay(*replayFile)
		return
	}
	if *bin == "" || *out == "" || *count < 1 || *count > 1000 {
		panic("node-bin,out,schedules[1,1000] required")
	}
	must(os.MkdirAll(*out, 0700))
	binary, e := os.ReadFile(*bin)
	must(e)
	protocol := map[string]any{"schema": "quorumlog.http-chaos.v3", "request_cadence_ms": 10, "metric_definitions": "WorkloadDurationMS excludes checker/status collection; DurationMS includes them. SnapshotsAdvanced compares status indices before/after each schedule; FinalReadAcknowledged must hold before post-fault survival is evaluated.", "checker_timeout_ms": 1000, "nodes": []int{3, 5}, "schedules_per_size": *count, "operations_per_schedule": 21, "seed_range": []int{0, *count}, "binary_sha256": hash(binary), "tick_ms": 10, "snapshot_threshold": 8, "snapshot_trailing": 2, "max_client_inflight": 2, "faults": []string{"stable", "leader_sigkill", "minority_partition", "delayed_raft", "committed_response_lost_and_sigkill", "follower_restart_during_snapshots"}, "model": "existing Porcupine per-key KV model; no cross-key transactions", "timeouts": "indeterminate, never definite failure", "limits": "owned local processes; no multi-host/production claim; recovery begins after fault cleanup; p99 over 21 small-run operations is max observed, not a stable tail estimate"}
	data, _ := json.MarshalIndent(protocol, "", "  ")
	p := filepath.Join(*out, "protocol.json")
	if prior, e := os.ReadFile(p); e == nil && !bytes.Equal(prior, data) {
		panic("frozen protocol differs")
	}
	must(os.WriteFile(p, data, 0600))
	if *lock {
		return
	}
	raw, e := os.OpenFile(filepath.Join(*out, "raw.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	must(e)
	defer raw.Close()
	enc := json.NewEncoder(raw)
	summary := map[string]int{}
	for _, nodes := range []int{3, 5} {
		func() {
			c := newCluster(nodes, *bin, filepath.Join(*out, fmt.Sprintf("cluster-%d", nodes)))
			defer c.close()
			for seed := 0; seed < *count; seed++ {
				row := runSchedule(c, seed)
				must(enc.Encode(row))
				must(raw.Sync())
				summary[row.Verdict]++
			}
		}()
	}
	encoded, _ := json.MarshalIndent(summary, "", "  ")
	must(os.WriteFile(filepath.Join(*out, "summary.json"), encoded, 0600))
	fmt.Println(string(encoded))
}
