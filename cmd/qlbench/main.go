// Command qlbench measures quorum-log performance against a running cluster.
//
// Modes:
//
//	qlbench load -endpoints=... -op=put|append|linread|localread -clients=8 -duration=10s
//	qlbench failover -endpoints=... -duration=30s -interval=5ms
//
// All numbers are wall-clock measurements from this single process; run it on
// the same machine as the cluster for the documented single-machine results.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: qlbench <load|failover> [flags]")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "load":
		runLoad(os.Args[2:])
	case "failover":
		runFailover(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "unknown mode %q\n", os.Args[1])
		os.Exit(2)
	}
}

type client struct {
	http      *http.Client
	endpoints []string
	leader    string
	rng       *rand.Rand
}

func newClient(endpoints []string, seed int64) *client {
	return &client{
		http: &http.Client{
			Timeout: 10 * time.Second,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse // handle 307 manually (PUT bodies)
			},
		},
		endpoints: endpoints,
		rng:       rand.New(rand.NewSource(seed)),
	}
}

func (c *client) base() string {
	if c.leader != "" {
		return c.leader
	}
	return c.endpoints[c.rng.Intn(len(c.endpoints))]
}

// write performs one write, following leader redirects; returns success.
func (c *client) write(op, key, val, clientID string, reqID uint64) bool {
	body, _ := json.Marshal(map[string]interface{}{
		"op": op, "value": val, "client_id": clientID, "req_id": reqID,
	})
	for attempt := 0; attempt < 8; attempt++ {
		req, _ := http.NewRequest(http.MethodPut, c.base()+"/kv/"+key, bytes.NewReader(body))
		resp, err := c.http.Do(req)
		if err != nil {
			c.leader = ""
			time.Sleep(50 * time.Millisecond)
			continue
		}
		loc := resp.Header.Get("Location")
		code := resp.StatusCode
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		switch {
		case code == http.StatusOK:
			return true
		case code == http.StatusTemporaryRedirect && strings.Contains(loc, "/kv/"):
			c.leader = strings.TrimSuffix(loc[:strings.Index(loc, "/kv/")], "/")
		default:
			c.leader = ""
			time.Sleep(50 * time.Millisecond)
		}
	}
	return false
}

func (c *client) read(key, consistency string) bool {
	for attempt := 0; attempt < 8; attempt++ {
		resp, err := c.http.Get(c.base() + "/kv/" + key + "?consistency=" + consistency)
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		code := resp.StatusCode
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if code == http.StatusOK {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

func runLoad(args []string) {
	fs := flag.NewFlagSet("load", flag.ExitOnError)
	endpointsStr := fs.String("endpoints", "http://127.0.0.1:9101,http://127.0.0.1:9102,http://127.0.0.1:9103", "comma-separated endpoints")
	op := fs.String("op", "put", "put | append | linread | localread")
	clients := fs.Int("clients", 8, "concurrent workers")
	duration := fs.Duration("duration", 10*time.Second, "measurement duration")
	keys := fs.Int("keys", 64, "key-space size")
	fs.Parse(args)
	endpoints := strings.Split(*endpointsStr, ",")

	// Seed the key space so reads hit existing keys.
	seeder := newClient(endpoints, 0)
	for i := 0; i < *keys; i++ {
		if !seeder.write("put", fmt.Sprintf("bench-%d", i), "seed", "qlbench-seed", uint64(i+1)) {
			fmt.Fprintln(os.Stderr, "seeding failed; is the cluster up?")
			os.Exit(1)
		}
	}

	var mu sync.Mutex
	var lats []time.Duration
	var errs int
	var wg sync.WaitGroup
	deadline := time.Now().Add(*duration)

	for w := 0; w < *clients; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			cl := newClient(endpoints, int64(w+1))
			clientID := fmt.Sprintf("qlbench-%d-%d", os.Getpid(), w)
			var reqID uint64
			var local []time.Duration
			localErrs := 0
			for time.Now().Before(deadline) {
				key := fmt.Sprintf("bench-%d", cl.rng.Intn(*keys))
				start := time.Now()
				ok := false
				switch *op {
				case "put", "append":
					reqID++
					ok = cl.write(*op, key, fmt.Sprintf("v%d", reqID), clientID, reqID)
				case "linread":
					ok = cl.read(key, "linearizable")
				case "localread":
					ok = cl.read(key, "local")
				default:
					fmt.Fprintf(os.Stderr, "unknown op %q\n", *op)
					os.Exit(2)
				}
				if ok {
					local = append(local, time.Since(start))
				} else {
					localErrs++
				}
			}
			mu.Lock()
			lats = append(lats, local...)
			errs += localErrs
			mu.Unlock()
		}(w)
	}
	wg.Wait()

	if len(lats) == 0 {
		fmt.Println("no successful operations")
		os.Exit(1)
	}
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	pct := func(p float64) time.Duration { return lats[int(float64(len(lats)-1)*p)] }
	fmt.Printf("op=%s clients=%d duration=%s keys=%d\n", *op, *clients, *duration, *keys)
	fmt.Printf("ops=%d errs=%d throughput=%.1f ops/s\n", len(lats), errs, float64(len(lats))/duration.Seconds())
	fmt.Printf("latency p50=%.2fms p95=%.2fms p99=%.2fms max=%.2fms\n",
		ms(pct(0.50)), ms(pct(0.95)), ms(pct(0.99)), ms(lats[len(lats)-1]))
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

// runFailover writes continuously at -interval and reports every outage gap
// (time between the last success before failures and the next success).
func runFailover(args []string) {
	fs := flag.NewFlagSet("failover", flag.ExitOnError)
	endpointsStr := fs.String("endpoints", "http://127.0.0.1:9101,http://127.0.0.1:9102,http://127.0.0.1:9103", "comma-separated endpoints")
	duration := fs.Duration("duration", 30*time.Second, "run duration")
	interval := fs.Duration("interval", 5*time.Millisecond, "write interval")
	fs.Parse(args)
	endpoints := strings.Split(*endpointsStr, ",")

	cl := newClient(endpoints, 42)
	cl.http.Timeout = 500 * time.Millisecond
	clientID := fmt.Sprintf("qlbench-failover-%d", os.Getpid())
	var reqID uint64
	deadline := time.Now().Add(*duration)
	lastOK := time.Time{}
	inOutage := false
	var outageStart time.Time
	var maxGap time.Duration

	for time.Now().Before(deadline) {
		reqID++
		ok := cl.writeOnce("put", "failover-key", fmt.Sprintf("v%d", reqID), clientID, reqID)
		now := time.Now()
		if ok {
			if inOutage {
				gap := now.Sub(outageStart)
				fmt.Printf("outage: %.0fms (recovered at %s)\n", float64(gap.Milliseconds()), now.Format("15:04:05.000"))
				if gap > maxGap {
					maxGap = gap
				}
				inOutage = false
			}
			lastOK = now
		} else if !inOutage {
			inOutage = true
			if lastOK.IsZero() {
				outageStart = now
			} else {
				outageStart = lastOK
			}
		}
		time.Sleep(*interval)
	}
	fmt.Printf("max outage gap: %.0fms\n", float64(maxGap.Milliseconds()))
}

// writeOnce is a single non-retrying write attempt (follows one redirect).
func (c *client) writeOnce(op, key, val, clientID string, reqID uint64) bool {
	body, _ := json.Marshal(map[string]interface{}{
		"op": op, "value": val, "client_id": clientID, "req_id": reqID,
	})
	for attempt := 0; attempt < 2; attempt++ {
		req, _ := http.NewRequest(http.MethodPut, c.base()+"/kv/"+key, bytes.NewReader(body))
		resp, err := c.http.Do(req)
		if err != nil {
			c.leader = ""
			return false
		}
		loc := resp.Header.Get("Location")
		code := resp.StatusCode
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if code == http.StatusOK {
			return true
		}
		if code == http.StatusTemporaryRedirect && loc != "" && strings.Contains(loc, "/kv/") {
			c.leader = strings.TrimSuffix(loc[:strings.Index(loc, "/kv/")], "/")
			continue
		}
		c.leader = ""
		return false
	}
	return false
}
