# Multi-host gate harness: single-host dry run

- Mode: `single-host-dry-run`
- Executor: `local` (every command ran locally on this one machine)
- Inventory: `inventories/local-dry-run.json` (declared mode `local-dry-run`)
- Scenario: `full`
- Generated: 2026-09-09T22:31:53Z
- Distinct hosts: **false**
- Gate result: **blocked**
- Blocked reason: three distinct authorized hosts required; this was a single-host dry run

## Boundary

Single-host dry run: all three quorum-log node processes, the fault injection, and the client ran on one machine over loopback. This validates the harness only. It is not multi-host evidence, no remote host was contacted, and nothing here measures a network between machines.

## Host inventory

| node | target | hostname | machine identity | boot id | kernel | data device | build target |
|---|---|---|---|---|---|---|---|
| 1 | `local-process-1` | `Mac.lan` | `sha256:2e1c0dfe2090c7ac` | `sha256:ae31e664b1c074dc` | Darwin Mac.lan 25.5.0 Darwin Kernel Version 25.5.0: Tue Jun ... | `/dev/disk3s5` | `darwin/arm64` |
| 2 | `local-process-2` | `Mac.lan` | `sha256:2e1c0dfe2090c7ac` | `sha256:ae31e664b1c074dc` | Darwin Mac.lan 25.5.0 Darwin Kernel Version 25.5.0: Tue Jun ... | `/dev/disk3s5` | `darwin/arm64` |
| 3 | `local-process-3` | `Mac.lan` | `sha256:2e1c0dfe2090c7ac` | `sha256:ae31e664b1c074dc` | Darwin Mac.lan 25.5.0 Darwin Kernel Version 25.5.0: Tue Jun ... | `/dev/disk3s5` | `darwin/arm64` |

Distinctness findings:

- hosts 1 and 2 and 3 share the same machine identity (sha256:2e1c0dfe2090c7ac), so they are the same machine
- hosts 1 and 2 and 3 share the same hostname (Mac.lan), so they are the same machine
- hosts 1 and 2 and 3 share the same boot id (sha256:ae31e664b1c074dc), so they are the same machine

## Operations

- Traffic phase: 1007 operations completed (target 1000, concurrency 8, read fraction 0.50, seed 20260909)
- Recorded history: 1529 operations (526 writes, 1003 reads)
- Indeterminate: 4 writes and 0 reads never learned their outcome and are checked as indeterminate
- Acknowledged writes tracked: 522
- Acknowledged writes re-read after the full restart: 522 (0 missing or changed)

## Fault timeline

| at op | t+ms | event | node | detail | waited ms |
|---|---|---|---|---|---|
| 301 | 2978 | `kill_leader` | 1 | SIGKILL to node 1 on local-process-1 |  |
| 305 | 4799 | `new_leader_elected` | 2 | node 2 took over from killed node 1 | 1821 |
| 414 | 5818 | `restart_killed_node` | 1 | node 1 restarted from its persisted state |  |
| 415 | 5821 | `cluster_settled` |  | all three nodes report the same leader again | 3 |
| 505 | 6612 | `isolate_follower` | 1 | node 1 blackholed from nodes [2 3] in both directions; chaos state: {"node_id":1,"drop_inbound":[2,3],"drop_outbound":[2,3]} |  |
| 704 | 7961 | `clear_follower_isolation` | 1 | node 1 reconnected before the leader is isolated, so a quorum can still exist |  |
| 720 | 8078 | `cluster_settled` |  | all three nodes report the same leader again | 117 |
| 721 | 8104 | `isolate_leader` | 2 | leader node 2 blackholed from nodes [1 3] in both directions; chaos state: {"node_id":2,"drop_inbound":[1,3],"drop_outbound":[1,3]} |  |
| 721 | 9597 | `new_leader_elected` | 1 | node 1 elected while node 2 was isolated | 1493 |
| 859 | 12104 | `restore_network` |  | every chaos isolation cleared on all three nodes |  |
| 862 | 12124 | `cluster_healed` | 1 | all three nodes agree on leader node 1 | 20 |
| 1007 | 14264 | `restart_all_stop` |  | SIGTERM to all three nodes |  |
| 1007 | 17382 | `restart_all_ready` | 1 | all three nodes restarted from disk and agree on leader node 1 | 3118 |
| 1007 | 17474 | `verify_acknowledged_writes` |  | 522 acknowledged writes re-read with linearizable reads; 0 missing or changed |  |

## Measurements

- Write outage after SIGKILL of the leader: **1861 ms** (last acknowledged write at t+2965 ms, first acknowledged write after at t+4826 ms; client-observed: last acknowledged write returning before the event to the first acknowledged write returning after it)
- New leader after the kill: **1821 ms** (driver polled /status until a node other than the killed node 1 reported itself leader)
- Majority progress while one follower was isolated: **103 writes and 96 reads acknowledged** over 1348 ms (76.4 writes/s); progress: **true** (acknowledged client operations returning inside the fault window, measured by the driver)
- Write outage while the leader was isolated from the majority: **3074 ms** (last acknowledged write at t+8069 ms, first acknowledged write after at t+11143 ms; client-observed: last acknowledged write returning before the event to the first acknowledged write returning after it)
- New leader after isolating the leader: **1493 ms** (driver polled /status on the majority until one of them reported itself leader while node 2 was isolated)
- Restart recovery to a ready cluster: **3118 ms** (from the stop signal to all three nodes ready and agreeing on one leader)
- Restart recovery to the first successful linearizable read: **3118 ms** (from the stop signal to the first successful linearizable read after the restart)

## Linearizability

- Checker: github.com/anishathalye/porcupine via internal/check
- Model: internal/check.KVModel (sequential key-value store, partitioned by key)
- Result: **Ok** over 1529 operations in 1 ms
- history recorded by the driver (the client), not by any node; operations whose outcome the client never learned are checked as indeterminate

## Gate criteria

| criterion | met | detail |
|---|---|---|
| `three_distinct_hosts` | no | machine identity, hostname, and boot id distinct across all three hosts: false |
| `at_least_1000_mixed_operations` | yes | 1007 client operations completed during the traffic phase (526 writes, 1003 reads recorded in the history) |
| `majority_progress_while_follower_isolated` | yes | 103 writes and 96 reads acknowledged during the 1348 ms the follower was isolated (76.4 writes/s) |
| `leader_kill_outage_recorded` | yes | write outage after SIGKILL of the leader: 1861 ms |
| `leader_isolation_outage_recorded` | yes | write outage while the leader was isolated from the majority: 3074 ms |
| `new_leader_elected_after_isolation` | yes | new leader observed 1493 ms after the isolation |
| `no_acknowledged_write_lost` | yes | 522 acknowledged writes re-read with linearizable reads after restarting every node from disk; 0 missing or changed |
| `restart_recovery_recorded` | yes | recovery to a ready cluster: 3118 ms; to the first successful linearizable read: 3118 ms |
| `history_linearizable` | yes | porcupine result Ok over 1529 operations in 1 ms |

## Notes and judgment calls

- client traffic and fault injection both ran from this driver; every timestamp is on the driver's clock

## What this run is and is not

Implemented, and measured only as a single-host dry run: the harness ran end to end, so the code paths for deployment, fault injection, history recording, and the linearizability check are exercised and their numbers are real measurements of this machine. They are not multi-host evidence. The gate stays blocked until three distinct authorized hosts are available, and no remote host was contacted by this run.
