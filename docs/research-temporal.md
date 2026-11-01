# Temporal vs hourglass — a source-level comparison

Method: read the Temporal server source (sparse clone of temporalio/temporal
@ master, 2026-11) — `service/history/shard/context_impl.go`,
`common/persistence/sql/sqlplugin/{history_timer_tasks,postgresql/shard}.go`,
`common/timer/local_gate.go`, `service/history/queues/{memory_scheduled_queue,rescheduler}.go`,
`common/dynamicconfig/constants.go` — plus the official architecture docs
and two third-party operational write-ups (cited where used). hourglass
facts come from this repo's gated tests and load runs. Every claim below
has a file:line or doc reference; where a number is third-party or
self-measured, it says so.

## The shared ownership model (verified in source, not vibes)

```
Temporal:   hash(workflowID) % numHistoryShards → History Shard
            one owning history host (gossip ring assignment)
            fencing: RangeID — DB-row CAS on every write
            zombie writes: conditional updates carry RangeID and fail
hourglass:  fnv(id) % 256 → shard
            one owning node (etcd lease claim)
            fencing: epoch — stamped on rows, checked on the fire path
            zombie writes: epoch-fenced + lease-expiry reclaim
```

- `renewRangeLocked` (service/history/shard/context_impl.go:1268):
  drains in-flight requests, `RangeId++`, `UpdateShard` with
  `PreviousRangeID` — a compare-and-swap. Stale writes fail.
- Every persistence request carries RangeID
  (common/persistence/persistence_interface.go:234–271).
- hourglass: `registry.Claim` bumps a durable epoch under etcd Txn;
  `Acquire` stamps it on rows; the wheel drops entries whose epoch is
  stale (`wheel.go` collectBatch); reclaim takes rows `epoch < new`.

Two systems arrived at the same shape independently: shard-scoped
ownership + a monotonic fencing generation. Everything below is what
differs *around* that shared core.

## Difference 1 — the timer machinery itself

**Temporal: durable timer table per shard + an in-memory gate.**
Timer tasks are rows: `TimerTasksRow{ShardID, VisibilityTimestamp,
TaskID, Data}` (sqlplugin/history_timer_tasks.go), read via
`RangeSelectFromTimerTasks` on a visibility-timestamp range, batch
`TimerTaskBatchSize` **default 100** (dynamicconfig/constants.go:2291).
In memory, the processor keeps a heap behind a **LocalGate** — a single
`time.Timer` re-aimed at `nextWakeupTime`
(common/timer/local_gate.go); a `TimerProcessorMaxPollInterval`
(**default 5 min**, jitter 0.15, constants.go:2331) is the *fallback*
scan, not the steady state. So Temporal's timers are **not
poller-grade** — the gate wakes at the appointed timestamp.

**hourglass: hierarchical time wheel holding the admitted window.**
60-slot levels with cascade (wheel.go); admission pulls a lookahead
window in CAS batches; a quiescence watermark means an idle store costs
~2 reads per quiet period.

Honest precision statement: both mechanisms wake at the appointed time.
The end-to-end difference is what happens *after the wake*: a Temporal
timer fires a workflow callback that then traverses
history→matching→worker-poll — third-party tuned-cluster measurements
report schedule-to-start ~80–170 ms and ~3K state transitions/s on a
small cluster (piotrmucha.blog, 2025; also the lilatomic incident
write-up) — while hourglass calls a handler (or gRPC executor) in
process, with mechanism-level gates at p50/p99 = 0 ms @ 1 s tick (this
repo's tests). These numbers measure different things (orchestration
pipeline vs scheduling mechanism); neither is an apples-to-apples SLO
against the other.

## Difference 2 — fencing authority: the DB row vs etcd

- **Temporal uses no coordinator service.** Ownership is assigned by a
  gossip membership ring; fencing is the `shards` DB row:
  `lockShardQry = SELECT range_id FROM shards WHERE shard_id = $1 FOR
  UPDATE` (sqlplugin/postgresql/shard.go:22). Grabbing/stealing a shard
  takes that row lock; every write is conditioned on the RangeID read
  under it.
- **hourglass uses etcd as the claim authority.** Claim is an etcd Txn
  (owner key must not exist), leases expire on crash, ownership loss is
  an explicit watch event. The DB never takes an ownership lock;
  reclaim is one epoch-guarded UPDATE.

Operational consequences, both directions:

- Temporal's lock is also its thundering-herd surface: the lilatomic
  incident (single history host, healthy PG) shows shard-acquisition
  `FOR UPDATE` storms with a **2 s context deadline baked in at compile
  time** (`debug.TimeoutMultiplier`) freezing the cluster — fixed by PG
  tuning, not config.
- hourglass's lease is its takeover window: failover waits LeaseTTL
  (3–15 s; measured 2.6–3.6 s at TTL=3 s, docs/loadtest-report.md).
  And etcd becomes a dependency: if etcd is down, existing owners keep
  firing (leases persist server-side until expiry) but no ownership
  change can complete. Temporal has no such external dependency — the
  price is that its coordination layer is the database's lock manager
  plus gossip convergence.

## Difference 3 — what a shard owns (and what that costs to change)

- In Temporal the shard owns **workflow runs**: every operation on a
  run routes (via frontend + ring) to the owning history host, and the
  executions tables are partitioned by shard (1:1 in Cassandra).
  `numHistoryShards` is set at **cluster creation**; changing it means
  rebuilding the cluster — histories are long-lived, so re-hashing is a
  migration, not a config edit.
- In hourglass the shard owns **only scanning**. Submit / Get / Cancel /
  CLI operate on any row from any node (single-row CAS, owner-agnostic);
  the shard decides whose admission loop picks the job up. Shard→node
  assignment changes by editing the etcd directory; data never moves.

Honest nuance both ways: hourglass's `DefaultShards = 256` is also
baked into rows (the `shard` column) — raising it later would re-hash
existing rows. The asymmetry is row lifetime: Temporal histories live
for the workflow's retention (days+), while hourglass terminal rows
expire on TTL (default 7 d) — a shard-count change self-drains within
retention. And physical capacity scaling is decoupled: 256 shards map
onto as many MySQL partitions as the directory says.

## Difference 4 — what the timer wakes up

- Temporal: a timer is a **workflow callback**. `Sleep()` suspends a
  durable execution; the wake replays event history and continues
  program state. Cancel is workflow-level (activity contexts get
  cancelled cooperatively). Rich — and it means every timer drags the
  orchestration pipeline with it.
- hourglass: a timer **fires a payload at a handler** (or pushes to a
  gRPC executor, ack by fired-id, optional reschedule_at self-chaining).
  Cancel wins unconditionally until the fire moment, epoch-fenced.
  No state, no replay, deliberately (DESIGN.md Non-goals: the time
  layer, not the state layer; compose with Temporal when replay is
  needed).

Audit follows the same split: Temporal keeps full event histories (the
strongest possible audit, at the heaviest cost); hourglass keeps
per-fire receipt rows (attempts, errors, cancels — a SELECT).

## Difference 5 — footprint

Temporal server: frontend + history + matching + worker services, a
database, optional Elasticsearch; production-grade DR via active
standby clusters and namespace failover. hourglass: a Go library, the
MySQL you run, etcd for the registry. The fair statement: Temporal is
an application platform that contains a scheduler; hourglass is a
scheduler that fits in your process.

## What hourglass does not attempt (and won't)

Workflow-as-code, replay, activities with heartbeats, multi-language
SDKs, web UI, multi-cluster replication. Those are Temporal's product;
a durable *timer* should not need any of them. If you need durable
execution, run Temporal — and if you also need precise standalone
timers, hourglass can be the alarm clock that starts the workflow.

## Corrections this review made to earlier claims

1. Earlier text said Temporal timers were "an in-memory timer queue" —
   imprecise: durable `timer_tasks` rows per shard, heap + single-timer
   gate in memory, 5-min poll only as fallback. "Poll-based" would be
   flat wrong; corrected wherever it appeared.
2. "shards fixed at boot" as an hourglass differentiator needed the
   nuance in Difference 3 (hourglass's 256 is fixed too; lifetime and
   directory-mapping are the real asymmetries).
3. The quadrant chart's implicit "hourglass strictly more scalable"
   is not defensible from evidence — both are shard-owner models;
   moved to co-located points with Temporal ahead on audit depth.

Sources: temporalio/temporal (files cited inline; Apache-2.0),
docs.temporal.io (task-queue, history-service architecture),
piotrmucha.blog performance series (2025), lilatomic.ca incident
write-up. hourglass numbers: this repo's gated tests and
docs/loadtest-report.md (single-host, laptop-grade — shape, not
fleet-grade).
