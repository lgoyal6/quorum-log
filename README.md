# quorum-log

A replicated metadata/lease log in Go: a small key-value state machine
replicated across 3 or 5 nodes with leader election, quorum commit, follower
catch-up, snapshot install, membership changes, graceful restart, exactly-once
client requests, and linearizable reads, verified with a deterministic fault
simulator and a [Porcupine](https://github.com/anishathalye/porcupine)
linearizability checker.

**This project integrates the production-grade Raft implementation from
[go.etcd.io/raft](https://github.com/etcd-io/raft) (etcd/raft). It does not
invent or reimplement the consensus algorithm.** The work here is everything
around the consensus core: durable storage, the deterministic state machine,
request deduplication, the transport and HTTP API, snapshotting, membership,
the fault simulator, and the linearizability checking harness.

## Architecture

```
cmd/quorumlogd        one node: flags, signals, server lifecycle
cmd/qlbench           load and failover measurement client
internal/server       production runtime: wall-clock ticker, HTTP raft
                      transport (one ordered sender goroutine per peer),
                      client API, leader redirects, persisted membership
internal/engine       passive driver around etcd/raft RawNode: Ready
                      processing, persist-before-send, apply, snapshots,
                      proposal/read-index waiters
internal/sm           deterministic KV state machine (put/append/delete)
                      with client sessions for exactly-once retries
internal/storage      disk persistence: fsync'd append-only WAL, hard
                      state, snapshots; rebuilds raft MemoryStorage on boot
internal/sim          deterministic single-goroutine network and process
                      simulator (drop, delay, duplication, reorder,
                      partition, crash/restart) driven by one seeded PRNG
internal/check        history recorder plus the sequential KV model for
                      the Porcupine linearizability checker
```

The engine is deliberately passive: no goroutines, timers, or channels. The
caller decides when to tick, step messages, and process Ready batches. In
production (`internal/server`) that caller is a mutex-serialized wall-clock
loop; in tests it is the simulator, which is what makes every fault scenario
fully deterministic and replayable from a seed. etcd/raft's internal
randomized election timer is neutralized in the simulator and elections are
triggered by the seeded supervisor instead; production nodes use normal raft
election timeouts.

Raft's durability contract is honored: snapshots, entries, and hard state are
persisted (with fsync) before any message from the same Ready batch is handed
to the network.

## Quick start (clean clone, one command)

Requires Go (see `go.mod`) and `curl`.

```sh
git clone https://github.com/lgoyal6/quorum-log.git
cd quorum-log
make run          # builds and starts a local 3-node cluster on 127.0.0.1:9101-9103
```

`make run5` starts 5 nodes instead. `QUORUMLOG_BASE_PORT=9200 make run` moves
the port range. The script polls every node for readiness and fails loudly if
any child process exits during startup. Data lands under `./data/n<i>`; logs
under `./data/n<i>.log`. Ctrl-C stops the cluster. `make clean` removes
binaries and data.

## Client API

Write (put is the default op; `append` and `delete` also exist). `client_id`
plus a monotonically increasing `req_id` give exactly-once semantics across
retries; the state machine applies each `(client_id, req_id)` pair at most
once and returns the cached result for duplicates:

```sh
curl -sL -X PUT http://127.0.0.1:9101/kv/hello \
  -d '{"value":"world","client_id":"me","req_id":1}'
# {"value":"world","found":true}
```

(`-L` follows the 307 redirect when the node you hit is not the leader; see
below.)

Reads default to linearizable (a ReadIndex quorum barrier before serving from
the local state machine). `consistency=local` serves straight from local
state with no consensus round and can return stale data:

```sh
curl -s http://127.0.0.1:9101/kv/hello                      # linearizable
curl -s 'http://127.0.0.1:9102/kv/hello?consistency=local'  # local, may be stale
```

Writes sent to a follower answer `307 Temporary Redirect` with a `Location`
header pointing at the known leader (or `503` when no leader is known); a
proposal that times out answers `503` and must be retried with the same
`client_id`/`req_id`, which dedup makes safe.

Membership and status:

```sh
curl -s -X POST http://127.0.0.1:9101/members \
  -d '{"id":4,"url":"http://127.0.0.1:9104"}'      # add node 4
bin/quorumlogd -id 4 -listen 127.0.0.1:9104 -data data/n4 -join \
  -peers "1=http://127.0.0.1:9101,2=http://127.0.0.1:9102,3=http://127.0.0.1:9103,4=http://127.0.0.1:9104"
curl -s -X DELETE http://127.0.0.1:9101/members/4  # remove node 4
curl -s http://127.0.0.1:9102/status               # leader, applied index, voters, peers
```

## Tests: deterministic faults and linearizability

```sh
go test -race -count=1 ./...
```

Every fault scenario in `internal/sim` runs across the documented seed set
`{1, 7, 42, 1337, 99991}` (declared as `Seeds` in
`internal/sim/scenarios_test.go`) and is fully deterministic per seed:
message drops, delays, duplications, reordering, partitions, crashes, and
elections all derive from one seeded PRNG advanced by a single-threaded step
loop. Scenarios cover leader crash during proposals, minority partitions on
3- and 5-node clusters, an old leader rejoining, follower snapshot catch-up,
membership change, restart from persisted state, duplicate client requests,
and concurrent reads and writes. Client histories from each scenario are
checked for linearizability with Porcupine against a sequential KV model
(`internal/check`).

Run one scenario, or one seed of it:

```sh
go test -count=1 -v -run 'TestOldLeaderRejoins' ./internal/sim
go test -count=1 -v -run 'TestMinorityPartition5/seed=1337' ./internal/sim
go test -count=1 -v -run 'TestDeterministicReplay' ./internal/sim
```

The checker itself is tested to reject planted faults (a lost acknowledged
write and a double-applied append), and the suite has been red-teamed with
three planted implementation faults, each of which makes the relevant tests
fail: applying entries before quorum commit (minority-partition and
old-leader scenarios fail their linearizability check), removing request
deduplication (duplicate-request scenario observes `xx` instead of `x`), and
disabling snapshot persistence (restart scenario fails with a missing
snapshot). These faults are not in the shipped code; they were applied
temporarily to prove the tests can fail.

## Persistence and snapshots

Each node persists under its data directory:

- `raft/wal.log`: append-only entry log, fsync'd on every append; a record
  with an index at or below a previous one marks a raft truncation, and
  replay honors the later record.
- `raft/hardstate.json`: term, vote, and commit index, written atomically.
- `raft/snapshot.json`: the latest state-machine snapshot, written
  atomically.
- `peers.json`: peer URLs learned from membership changes, so a restarted
  node can reach the current cluster even after log compaction.

A snapshot is taken every `-snapshot-threshold` applied entries (default
10000), retaining `-snapshot-trailing` entries (default 64) behind it, after
which the WAL is compacted. Followers that fall behind the retained log
receive a snapshot install instead of entry replay. On restart a node
rebuilds raft's in-memory storage from snapshot plus WAL and rejoins with its
state intact.

## Benchmarks

All numbers below are from one machine (Apple M3 Pro, macOS), with the
cluster and the benchmark client sharing that machine over loopback. They
measure single-machine behavior only: no real network, and client and servers
compete for the same CPU. Do not read them as distributed-deployment numbers.

Exact commands (run each against a cluster started as shown):

```sh
make build
make run                      # 3-node; make run5 for 5-node; single node:
# bin/quorumlogd -id 1 -listen 127.0.0.1:9101 -data data/n1 -peers "1=http://127.0.0.1:9101"

bin/qlbench load -endpoints=http://127.0.0.1:9101 -op=put -clients=8 -duration=10s
bin/qlbench load -endpoints=http://127.0.0.1:9101,http://127.0.0.1:9102,http://127.0.0.1:9103 \
  -op=linread -clients=8 -duration=10s
bin/qlbench failover -endpoints=... -duration=30s -interval=5ms
```

Run A, measured 2026-09-08, 8 clients, 10 s, 64 keys:

| Cluster | Op | Throughput | p50 | p95 | p99 |
|---|---|---|---|---|---|
| 1 node | put | 161.0 ops/s | 49.71 ms | 57.76 ms | 62.30 ms |
| 3 nodes | put | 55.3 ops/s | 141.22 ms | 180.62 ms | 303.78 ms |
| 5 nodes | put | 41.7 ops/s | 195.09 ms | 219.79 ms | 228.94 ms |
| 3 nodes | linearizable read | 9,087.4 ops/s | 0.83 ms | 1.58 ms | 1.91 ms |
| 5 nodes | linearizable read | 6,923.8 ops/s | 1.10 ms | 2.03 ms | 2.64 ms |
| 5 nodes | local read | 8,856.6 ops/s | 0.08 ms | 1.27 ms | 19.65 ms |

Invalid rows, kept for the record: run A's 1-node and 3-node local-read
attempts failed key seeding (`seeding failed; is the cluster up?`) and their
outputs are rejected, not evidence. Run A's 1-node linearizable-read attempt
(13,414.7 ops/s, p50 0.12 ms) logged 20 errors during the same instability
and is likewise rejected.

Run B, the revalidation of exactly those three rows: same machine, later on
2026-09-08, fresh data directories, seeding successful, zero errors:

| Cluster | Op | Throughput | p50 | p95 | p99 |
|---|---|---|---|---|---|
| 1 node | linearizable read | 86,540.8 ops/s | 0.08 ms | 0.16 ms | 0.26 ms |
| 1 node | local read | 90,030.1 ops/s | 0.08 ms | 0.15 ms | 0.25 ms |
| 3 nodes | local read | 83,162.5 ops/s | 0.09 ms | 0.16 ms | 0.28 ms |

The two runs saw very different background load on this shared developer
machine (run A's read tails reach hundreds of milliseconds at max, run B's
stay under 8 ms), so rows are comparable within a run, not across runs. On a
single node a linearizable read needs no remote quorum round, so run B's two
1-node read rows are nearly identical by construction.

Write latency is dominated by the 100 ms raft tick driving batching on this
build, not by disk or network; put throughput drops as cluster size grows
because every commit waits on a larger quorum sharing one machine.
Linearizable reads on multi-node clusters cost a quorum round trip; local
reads skip it and may be stale.

Killing the leader under a continuous 5 ms write stream produced an observed
failover outage of 1,236 ms before writes resumed on the new leader.

## Limitations and evidence boundaries

- All benchmark and failover numbers are single-machine loopback
  measurements; no multi-host deployment has been measured.
- Consensus correctness under faults is verified in the deterministic
  simulator; the production runtime shares the same engine, storage, and
  state machine but its transport and timing paths are exercised by the live
  cluster and benchmarks, not by the simulator.
- The linearizability checker validates recorded histories from the
  simulator's scenarios; it is not wired against the live HTTP cluster.
- Storage uses JSON encodings chosen for inspectability, and the WAL is
  fsync'd per append; the write path is not optimized (no group commit
  tuning, no binary encoding).
- The HTTP API is plaintext with no authentication or TLS; run it on
  loopback or a trusted network only.
- Raft, its correctness proofs, and its edge-case handling come from
  etcd/raft. Claims this project can make are about the integration layer:
  storage, apply, dedup, transport ordering, snapshots, membership, and the
  testing harness around them.

## License

MIT, see [LICENSE](LICENSE).
