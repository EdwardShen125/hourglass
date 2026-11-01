# hourglass load-test report

Date: 2026-11-01 · commit `a76f4f8`(+reclaim/handler-inheritance fixes, pre-commit)
Environment: macOS (Apple Silicon), Docker Desktop; MySQL 8.4
(container, 127.0.0.1:33061), etcd v3.5.17 (container, 127.0.0.1:23791).
Single host — absolute numbers are laptop-grade, not fleet-grade; the
shape (scaling, loss, recovery) is the signal. Reproduce:
`go build ./cmd/hourglass-bench && hourglass-bench -s <scenario>`.

## 1. Cluster throughput (submit → fire, single node, N shards)

| jobs | shards | submit rate | fire rate | loss |
| ---- | ------ | ----------- | --------- | ---- |
| 20,000 | 16 | 3,051/s | 3,805/s | 0.00% |
| 50,000 | 8 | 3,242/s | 5,877/s | 0.00% |
| 50,000 | 32 | 3,699/s | 5,553/s | 0.00% |
| 50,000 | 64 | 3,294/s | 5,552/s | 0.00% |
| 100,000 | 32 | 4,061/s | **6,346/s** | 0.00% |

- Fire throughput is flat from 8 to 64 shards on one process — the
  per-process ceiling here is the single host (all engines share one
  MySQL connection pool and one CPU set), not shard contention.
  Multi-process scaling is the failover scenario's domain.
- Submit saturates ≈3.2–4.1K/s: each Submit is one durable INSERT round
  trip (by design — no ack without the row).
- Zero loss at every scale; every job completed exactly once.

## 2. Failover (hard kill mid-flight, 2,000 jobs in the dead node's wheel)

3 runs, all identical outcome:

- takeover (lease expiry → claim → epoch bump → reclaim): **2.57–3.58s**
  (bounded by LeaseTTL=3s)
- delivered by node A before kill: 0 (kill landed before dues)
- delivered by node B after takeover: **2,000/2,000**
- loss/duplication: **0**

The entire in-flight window of the dead node survived: rows stuck in
`acquired` under the old epoch were reclaimed by one statement
(`epoch < newEpoch OR lease aged`) and re-fired exactly once by the new
owner.

## 3. Precision & quiescence (gated test suite, fake clock)

Already enforced as gates in `bench_test.go` (a regression fails CI):

- fire precision, 5,000 jobs / 10 min: p50 **0ms**, p99 **0ms**, max
  **1,000ms** (1s tick) vs poll-5s baseline p50 2,000 / max 5,000ms
- quiescence, 100K pending / 10 min idle: **2** admission reads
  (watermark) vs poller's 121
- hot minute, 100K same-second: 11.1s drain, 0.02 write-stmts/job,
  zero loss/duplication

## 4. Execution face (gRPC executor)

Covered by e2e tests: ack-by-fired-id completes; failed acks consume
attempts and dead-letter after retries; `reschedule_at` self-chains.
Not yet benchmarked at load — queued as follow-up (needs a distributed
executor harness; the single-host numbers would measure gRPC loopback,
not the face).

## Defects the load tests caught (and fixed)

1. **Reclaim was age-only.** `acquired_at` TTL (2min default) meant a
   takeover right after a crash reclaimed nothing — 2,000 jobs sat
   acquired for minutes. Fix: reclaim matches `epoch < newEpoch` OR
   aged lease; epoch fencing makes age a fallback, not the gate.
2. **Engines created after Register started handlerless.** Reclaimed
   jobs fired into "no handler" and dead-lettered 2,000/2,000. Fix:
   Cluster keeps the registry; AddShard-spawned engines inherit it.
3. **KillForTest only killed the lease.** Engines kept firing as a
   "zombie" that happened to be correct only because its wheel entries
   were pre-revoke admissions. Fix: hard-kill stops engines without
   drain, leaving rows acquired for the next owner — the honest
   simulation, which then exposed (1) and (2) within minutes.

Each was invisible to the unit suite and surfaced within one bench run
— the argument for scenario tests against real infra.

## Reading the numbers honestly

- Throughput caps at ~6.3K fires/s are a single-process, single-host
  ceiling with default engine knobs (8 workers, admitLimit 1000,
  dispatch 1024). The design target is horizontal: N processes × M
  shards each.
- The 200K/s ancestor number is not comparable: that system's hot path
  was broker delivery (Pulsar deliverAt), not per-fire SQL round trips.
  hourglass's per-fire receipts+state cost ≈ 3 batched writes per tick
  batch — the honest trade for SQL-auditable execution.
- Takeover time is LeaseTTL-bound by design (3s here; production
  default 15s → 15–20s recovery window). Graceful handover (Handover
  API) drains first and is bounded by wheel size, not TTL.

## Next

- Multi-process bench (2–4 nodes on one host) for true horizontal
  scaling numbers
- Executor-face load harness
- Grafana-able metrics export (fires/s, dispatch depth, reclaim count)
