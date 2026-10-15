# Local HTTP history verification

`qlchaos` starts only its own loopback `quorumlogd` processes and proxies. It
records monotonic invocation and client-return times against the real HTTP API,
then applies the existing Porcupine per-key KV specification. It does not
implement another consensus protocol. etcd Raft and Porcupine remain pinned in
`go.mod`; their upstream licenses apply.

Build into a disposable directory, then freeze the protocol before collecting
results. Keep results outside the checkout:

```sh
go build -o /tmp/quorumlogd ./cmd/quorumlogd
go build -o /tmp/qlchaos ./cmd/qlchaos
/tmp/qlchaos -node-bin /tmp/quorumlogd -out /tmp/ql-study -schedules 100 -lock-only
/tmp/qlchaos -node-bin /tmp/quorumlogd -out /tmp/ql-study -schedules 100
/tmp/qlchaos -replay /tmp/ql-study/raw.jsonl
```

The runner refuses an existing raw output and a different frozen protocol. It
runs 100 schedules for each of three and five nodes by default. Each schedule
has two clients, ten operations each, and a final recovery read. Seeds identify
repetitions and choose the delay length; cluster elections and OS scheduling
are real and are not deterministic replay. Rechecking recorded histories is
separate from reproducing the exact transport ordering.

The six named cases are stable traffic, leader SIGKILL, minority partition,
delayed Raft messages, a committed write response dropped followed by leader
SIGKILL, and follower restart while snapshot-producing writes continue. Fault
counters prove interception or process termination. `SnapshotsAdvanced` compares
snapshot indices before and after that schedule. A positive snapshot index
alone does not establish advancement in the current schedule.

A client timeout, transport error, or missing response is indeterminate. Its
interval remains open through the end of the observed history. It is never
counted as a definite failed write. Malformed histories, reused operation IDs,
and mismatched client identities are refused. Tests include a known lost
acknowledged write that must be illegal and an uncertain append whose effect
may be absent from a finite observed prefix.

`LINEARIZABLE` is a result under this finite sequential model. `VIOLATION`
requires investigation. `INCONCLUSIVE` is a checker timeout and must remain
inconclusive. The harness uses a one-second per-history checker budget and
retains every operation even when that budget expires. The secondary checker
copy removes only unknown reads: the sequential model treats these as no-ops
with no observed value. Unknown writes and all acknowledged reads remain.
This reduces redundant search without changing the model; raw conversion
retains the omitted no-op attempts. A second sufficient fast path checks only
acknowledged operations. If that projection is linearizable, all valid unknown
operations can be inserted after its last acknowledged observation, within
those unknown operations' open end-of-history intervals. If the projection is
illegal or times out, the complete history is checked using the remaining
budget. A projection failure never rejects a history. Unsupported operations,
invalid identities/timestamps and nonpositive budgets fail closed. Raw events
are preserved. A separately declared larger-budget replay can resolve an
inconclusive result; keep the original budget and verdict visible. No cross-key transaction
claim follows from the per-key API.

Keep availability separate from consistency. Acknowledged and unknown counts,
request latencies, final read acknowledgement, and recovery observations are
recorded. `WorkloadDurationMS` excludes checker/status collection;
`DurationMS` includes those costs. `RecoveryMS` starts after fault removal and
ends after the final read attempt. It does not measure the full failure outage.
`P99MS` is nearest-rank p99 on only 21 attempts, so it is the maximum in that
small sample, not a stable service-tail estimate. Use pooled per-attempt values
and per-schedule units for further descriptive analysis, retaining correlation.
A recovery read must be acknowledged before using that schedule to assess
post-fault acknowledged-data survival.

The normal command owns all child processes. Keep raw JSONL, frozen protocol,
logs and data directories until review. On abnormal runner termination, inspect
its retained node logs and identify owned children before cleanup; never kill
by a broad process-name pattern. There are no external endpoints, production
data, cloud credentials or multi-host reliability results in this harness.
