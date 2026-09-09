// Command quorumlogd runs one quorum-log node.
//
// Example 3-node cluster (see scripts/run-cluster.sh):
//
//	quorumlogd -id 1 -listen 127.0.0.1:9101 -data data/n1 \
//	  -peers "1=http://127.0.0.1:9101,2=http://127.0.0.1:9102,3=http://127.0.0.1:9103"
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"quorumlog/internal/engine"
	"quorumlog/internal/server"
)

func main() {
	var (
		id         = flag.Uint64("id", 0, "node ID (required, >= 1)")
		listen     = flag.String("listen", "127.0.0.1:9101", "listen address for client API and raft transport")
		peersStr   = flag.String("peers", "", "comma-separated id=url pairs for the initial cluster, e.g. 1=http://127.0.0.1:9101,2=...")
		dataDir    = flag.String("data", "", "data directory (required)")
		join       = flag.Bool("join", false, "join an existing cluster (do not bootstrap; add via POST /members first)")
		snapThresh = flag.Uint64("snapshot-threshold", 10000, "log entries applied between snapshots")
		snapTrail  = flag.Uint64("snapshot-trailing", 64, "log entries retained behind a snapshot")
		tickMs     = flag.Int("tick-ms", 100, "raft tick interval in milliseconds")
		chaosAddr  = flag.String("chaos-listen", "", "optional loopback host:port for the fault-injection control API (test hook; empty disables it)")
	)
	flag.Parse()
	if *id == 0 || *dataDir == "" {
		flag.Usage()
		os.Exit(2)
	}
	if engine.PlantedApplyBeforeQuorum {
		log.Printf("quorumlogd: PLANTED FAULT BUILD (quorumlog_planted_apply_before_quorum): entries are applied before quorum commit. This build is a negative control, never a release.")
	}
	peers, err := parsePeers(*peersStr)
	if err != nil {
		log.Fatalf("quorumlogd: %v", err)
	}

	srv, err := server.New(server.Config{
		ID:                *id,
		DataDir:           *dataDir,
		ListenAddr:        *listen,
		Peers:             peers,
		Join:              *join,
		SnapshotThreshold: *snapThresh,
		SnapshotTrailing:  *snapTrail,
		TickInterval:      time.Duration(*tickMs) * time.Millisecond,
		ChaosListen:       *chaosAddr,
	})
	if err != nil {
		log.Fatalf("quorumlogd: %v", err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		log.Printf("quorumlogd: node %d shutting down", *id)
		srv.Stop()
		os.Exit(0)
	}()

	if err := srv.Start(); err != nil {
		log.Fatalf("quorumlogd: %v", err)
	}
}

func parsePeers(s string) (map[uint64]string, error) {
	peers := map[uint64]string{}
	if s == "" {
		return peers, nil
	}
	for _, pair := range strings.Split(s, ",") {
		kv := strings.SplitN(strings.TrimSpace(pair), "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("bad peer %q (want id=url)", pair)
		}
		id, err := strconv.ParseUint(kv[0], 10, 64)
		if err != nil || id == 0 {
			return nil, fmt.Errorf("bad peer id %q", kv[0])
		}
		peers[id] = strings.TrimSuffix(kv[1], "/")
	}
	return peers, nil
}
