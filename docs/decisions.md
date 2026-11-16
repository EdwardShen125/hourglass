# Decision records

The design rationale behind hourglass, extracted from DESIGN.md and the
production system it descends from. Each record states the decision,
what was rejected, and where it lives in code. Written for a reviewer
with 30 minutes: the code proves the engineering, these records prove
the reasoning.

Format per record: Context → Decision → Rejected → Cost accepted →
Landed.

---

## D1 — Split the timing contract: database keeps the long horizon,
the wheel keeps the precision

**Context.** Poll-based schedulers define precision as a polling
interval: to fire closer to the appointed second they query the
database more often, across all pending jobs, forever. Precision and
database load are the same knob.

**Decision.** Admit a lookahead window in batches (CAS), hold admitted
jobs in an in-process hierarchical wheel, fire from memory at the
appointed second. The database is quiescent between admission windows.

**Rejected.**
- *Shorter polling* — precision bought with permanent DB load; a
  1-second poll of 1M pending rows is a full-table interrogation
  every second.
- *Broker delayed messages as the only mechanism* — see D2.

**Cost accepted.** The admitted slice lives in node memory; the
lookahead window is a memory budget, not just a contention knob.
At the ancestor's 200K commands/s with a 30-minute window this is
tens of GB — that scale belongs to cluster mode with short windows
or a broker backend, not embedded mode.

**Landed.** `engine.go` admission loop (`admit`, window floor from the
misfire halo), `wheel.go` hierarchical wheel with O(1) insert and
amortized O(1) tick. Measured: quiescence gate — 100K pending, 10 idle
minutes, 2 admission reads (bench_test.go, gated).

## D2 — The wheel is the core; a broker is a post-1.0 delivery sink

**Context.** The ancestor of this design ran SQL for the long horizon
plus a delayed-delivery broker (Pulsar deliverAt / RocketMQ 5.x timers)
for the near horizon at 200K commands/s. It worked in production.

**Decision.** Rebuild with the wheel as the v1 core and keep the broker
shape as an optional post-1.0 `delayedQueueSink` driver, not a
dependency.

**Rejected.**
- *Broker-first* — three reasons from the ancestor's scars:
  1. **Cancellation boundary**: once published, a delayed message
     cannot be canceled on any broker. Wheel mode cancels until the
     fire moment; broker mode cancels until admission. The ancestor
     removed its Redis-based cancel layer and fixed the boundary at
     admission the hard way.
  2. **Precision sovereignty**: broker deliverAt precision is the
     broker's tracker interval, and the delayed index lives on the
     broker's heap — millions of outstanding delayed messages become
     someone else's memory problem.
  3. **Dependency funnel**: SQL + broker + etcd is three systems
     before the first job fires. It cannot be the 30-second start.

**Cost accepted.** If built, the broker driver inherits two seam
contracts: crash seam is CAS-admit → publish (found by lease-expiry
rescan), and cancellation ends at admission — documented, not hidden.

**Landed.** DESIGN.md "The broker alternative"; no broker code in v1.0.

## D3 — Embedded-first; cluster is the same engine, not a second system

**Context.** Most task systems force a choice: a library that tops out
(one SQLite file) or a service you must operate (a dispatcher,
a database, now a cluster).

**Decision.** One engine behind one API. Embedded mode: SQLite,
in-process workers, zero services. Cluster mode: same data model and
state machine on partitioned MySQL/TiDB with etcd shard ownership.
Point the same code at a different DSN and add etcd.

**Rejected.**
- *Cluster-only* — the 30-second start dies; adoption ladder starts at
  an ops meeting.
- *Two engines* (light embedded + real clustered) — every behavioral
  guarantee would need proving twice; the embedded mode would become
  a toy.

**Cost accepted.** The Store interface carries both dialects'
constraints (scan units, CAS semantics), so neither dialect gets
exotic features the other cannot express.

**Landed.** `store.go` Store interface, `sqlite.go`, `mysql.go`;
cluster assembly in `cluster.go`/`join.go` with no engine changes.

## D4 — Fencing authority is etcd epochs, not database rows

**Context.** After a handoff, a zombie node's late writes must not
resurrect or steal jobs. Something must adjudicate "who is the current
owner" and stale writers must fail against it.

**Decision.** Ownership records in etcd carry a monotonically
increasing epoch. Admission CAS writes the epoch onto the row; the
fire path re-validates the epoch per batch before firing. A zombie's
writes fail the epoch check; a new owner reclaims `acquired` rows with
one statement (`epoch < newEpoch OR lease aged`).

**Rejected.**
- *DB-row fencing as the authority* (the Temporal shape — shard
  ownership lives with the data): workable, but it couples ownership
  truth to every store dialect and makes lease liveness a polling
  question. etcd leases give loss notification as a watch event, not
  a discovery query. (Verified in Temporal source, not vibes:
  docs/research-temporal.md, Difference 2.)

**Cost accepted.** Cluster mode requires etcd — a real dependency, one
more system. Embedded mode never touches it.

**Landed.** `registry.go` (claim, lease, watch, `ForceEpochBump`),
`engine.go` epoch on CAS + batch validation, `wheel.go` `SetEpoch`.
Proven: `TestWheelEpochFencingBlocksZombie`,
`TestEpochFencingEndToEnd`, `TestTwoNodeShardFailoverEndToEnd`,
`TestRegistryWatchDeliversOwnershipLoss`.

## D5 — Cancel of an admitted-but-unfired job is mark, don't remove

**Context.** A wheel entry is reachable by the tick goroutine at any
instant. Removing an entry from inside the wheel races the tick;
locking the wheel for cancel stalls the fire path.

**Decision.** `Cancel` sets a flag on the entry; the fire tick skips
flagged entries. A late mark is a no-op; removal races are structurally
impossible.

**Rejected.**
- *Remove under lock* — correct only if every cancel path takes the
  same lock as the tick; then a slow cancel path stalls firing.
- *Copy-on-write entry swap* — more allocation per cancel for no
  additional guarantee.

**Cost accepted.** Canceled entries occupy wheel slots until their tick
arrives — bounded by the lookahead window, deliberate.

**Landed.** `wheel.go` `Cancel` + tick skip; the store-side transition
(acquired → canceled) is epoch-fenced. Proven:
`TestWheelCancelIsMarkNotRemove`, `TestCancelDuringPumpReReadWindow`.

## D6 — Remote delivery: `fired_record` rows are the durable queue;
memory is a cache

**Context.** Cluster mode pushes fires to gRPC executors. If the push
path were memory-only, a crash between fire and delivery would orphan
the job; if it were synchronous, a slow executor would stall the wheel.

**Decision.** Wheel fire inserts a `fired_record(state=delivering)` row
— the commit point — then enqueues on a per-domain in-process queue.
A reaper re-delivers `delivering` rows when capacity returns; a
takeover reclaims them like any `acquired` row. Fire-side never
blocks: one slow domain backs up only that domain's queue.

**Rejected.**
- *Synchronous push with retry at the fire site* — the hot minute
  becomes a distributed stall.
- *Broker as the delivery queue* — reintroduces D2's dependency funnel
  into the core path.

**Cost accepted.** Two writes per delivered job (insert + update);
justified because "did this side effect happen?" must be a SELECT in
every deployment shape.

**Landed.** `executor.go` delivery + ack-by-fired-id, reaper in
`engine.go`. Proven: `TestExecutorDeliversAndAcks`,
`TestExecutorFailureRetriesThenDeadLetters`, failover ×3 in
docs/loadtest-report.md (2,000 in-flight jobs on the dead node: 0
lost, 0 duplicated).

## D7 — Admission is capacity-bounded; overflow is released, never dropped

**Context.** The wheel has a slot budget; the worker pool has a
concurrency budget. Admitting past either turns precision into OOM or
queue latency.

**Decision.** Admission selects and CAS-acquires only up to available
fire capacity. If the wheel rejects a batch (full), the engine
releases the surplus back to `waiting` and retries at the next tick —
nothing is lost, admission is backpressure.

**Rejected.**
- *Unbounded wheel* — the memory-budget arithmetic of D1 becomes a
  crash instead of a config decision.
- *Block admission on wheel drain* — a hot minute would back up into
  the scan loop and stall every shard sharing the store.

**Landed.** `engine.go` admit limit + release path. Proven:
`TestWheelRejectsWhenFull`, `TestWheelOverflowRelease*`,
`TestBenchmarkHotMinute` (100K jobs due the same second, gated).

## D8 — Fire-path state advances are batched writes

**Context.** A hot minute at 100K jobs with per-job UPDATEs is a write
storm at the exact moment of peak load.

**Decision.** The pump collects the fired batch's terminal states and
flushes one batched transition per target state plus one receipt
batch. Batch width is capped (~32K ids) to stay under MySQL packet
limits — admission batches stay portable across dialects.

**Rejected.**
- *Per-job writes in the worker goroutines* — the lesson the ancestor
  paid for in production; recorded as a load-test finding, not a
  hypothesis.

**Landed.** `engine.go` pump + `attemptFor` batch. Proven:
`TestBenchmarkHotMinute` drain curve; throughput matrix in
docs/loadtest-report.md (6,346 fires/s single process, zero loss).

## D9 — Logical shards, over-allocated, mapped onto partitions by a
directory

**Context.** Partition counts are painful to change in a live MySQL /
TiDB; shard counts that equal partition counts freeze capacity
decisions at boot.

**Decision.** Claim **logical** shards (over-allocated, e.g. 256) and
map them onto physical partitions through an editable directory.
Growing capacity edits the mapping instead of re-splitting partitions;
handoff drains one per-shard wheel.

**Rejected.**
- *Fixed shard count = fixed partition count* — Temporal's shard count
  is permanently fixed at boot (verified in source;
  docs/research-temporal.md, Difference 3). hourglass's 256 is also
  fixed — the real asymmetry is lifetime and directory-mapping, and
  this record does not claim more than that.

**Cost accepted.** An indirect mapping to maintain; rebalance is manual
in v1.0.

**Landed.** `registry.go` directory, `join.go` claims. Proven:
`TestJoinClaimsShardsAndFiresJobs`, `TestGracefulHandoffMovesShardWithoutLoss`,
`TestPartitionedSchemaAndPruning`, `TestPartitionIsolation`.

## D10 — Cancellations persist as receipts; plans leave footprints

**Context.** "Was scheduled, was revoked" is audit data — for humans
debugging a pipeline and, in the agent face, for an agent proving it
kept or revoked a promise.

**Decision.** Canceled rows persist with reason until retention
expires; they are queryable like any receipt.

**Rejected.** *Delete on cancel* — the anti-pattern this replaces;
silently rewriting history is what makes scheduler audits miserable.

**Landed.** State machine (DESIGN.md), `receipts.go`. Proven:
`TestCancel*`, receipts tests.

## D11 — Jitter is drawn once at submit and stored

**Context.** Jitter windows melt hot minutes before they form (the
09:00 cron fleet). Drawing the offset at fire time would make due
times non-reproducible and unauditable.

**Decision.** `due = scheduled + rand(0, jitter)`, drawn once at
submit, stored on the row — durable, decided, visible in SQL. Default
zero, opt-in per schedule.

**Rejected.** *Randomized at admission or fire* — the same dispersion
with none of the auditability; a canceled job's intended slot would be
unknowable.

**Landed.** `jitter.go`. Proven: jitter tests; complemented by the
wheel's catch-up drain (D7) for storms that form anyway.

## D12 — Scope fences are load-bearing

**Context.** Every scheduler graveyard is full of systems that became
workflow engines. The fences below are decisions, not omissions.

**Decision.** Non-goals, enforced by review: no workflow/DAG
orchestration; no exactly-once delivery (idempotent handlers instead);
no step replay or durable-execution journal (compose with Temporal or
Restate when a loop needs replay); no natural-language time parsing;
the engine never calls a model; no required broker; no multi-tenancy
before the core is proven.

**Rejected.** Each fence has a tempting pull-request shape; each was
declined because it couples the timer to a state model, a model
provider, or an ops burden that belongs to a different layer.

**Cost accepted.** Users with DAG needs must compose — deliberately,
with documented seams.

**Landed.** DESIGN.md Non-goals; the v1.0 checklist contains nothing
behind the fence.
