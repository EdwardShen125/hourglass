# hourglass design

This document is the build blueprint: the timing contract, the
components, the state machines, and the failure model that makes the
contracts true. It is written before the code; every deviation should
either fix a mistake in this document or be reverted.

Clean-room provenance: hourglass is rebuilt from a documented
production design
([write-up](https://edwardshen125.github.io/posts/engineering-case-study/building-a-production-go-scheduler-for-200k-delayed-jobs-per-second/)).
No employer code is used.

## Goals

- Delayed jobs and cron, second-level fire precision.
- Durable truth in SQL the user can query, join, and audit.
- Embedded mode: one process, one SQLite file, zero services.
- Cluster mode: same engine on partitioned MySQL/TiDB with etcd shard
  ownership, no code change for the user.
- At-least-once delivery with idempotent handlers; dead letters as
  rows.
- Durable time for agents (v1.x face): declarative jobs, queryable
  receipts, self-chaining — with the engine never calling a model.

## Non-goals

- Workflow/DAG orchestration (dependencies between jobs). That is a
  different problem with different tools.
- Exactly-once delivery.
- Step replay or a durable-execution journal. hourglass is the durable
  time layer, not the state layer; compose it with Temporal or Restate
  when a loop needs replay.
- Natural-language time parsing. The agent converts "tomorrow morning"
  into an exact time; hourglass accepts only exact times.
- Calling models, ever. Execution returns to the scheduling host
  (sampling callback) or a declared target; the engine stays free of
  LLM dependencies.
- Agent memory and agent-to-agent protocols. Watch list, not build
  list.
- A required broker. (A delayed-queue delivery backend may arrive
  post-1.0 for deployments that already run Pulsar or RocketMQ 5.x.)
- Multi-language clients, a web console, or multi-tenancy before the
  core is proven.

## The timing contract

Poll-based schedulers define fire precision as a polling interval: to
fire closer to the appointed second, they query the database more
often, across all pending jobs, forever. hourglass splits the contract:

- The database keeps the **long horizon**: a job due in two days is a
  quiet row. Nothing looks at it until its window approaches.
- The wheel keeps the **precision**: a configurable lookahead window
  ahead of now is admitted in batches (CAS), held in an in-process
  hierarchical time wheel, and fired from memory at the appointed
  second. The database is quiescent between admission windows.

Consequence to plan for: the admitted slice lives in node memory, so
the lookahead window is a memory budget, not just a contention knob.

    wheel memory ≈ admission rate × lookahead window × bytes per timer

At the ancestor system's 200K commands/s with a 30-minute window this
is tens of GB — that scale belongs to cluster mode with short windows
or a broker backend. Embedded mode targets thousands of jobs/s where a
30-minute window is megabytes.

## Components

### Storage driver

    type Store interface {
        // Scan units: one for SQLite; one per partition for MySQL/TiDB.
        Units(ctx) []ScanUnit
        // Select waiting jobs in [from, to] ordered by due time, limit n.
        Select(ctx, unit, from, to, n) []Job
        // CAS waiting -> acquired, only for these ids, under this epoch.
        Acquire(ctx, unit, epoch, ids) []Job
        // Terminal transitions: complete, requeue, dead-letter.
        Complete(ctx, job) / Requeue(ctx, job) / Dead(ctx, job, reason)
        // Reclaim acquired rows whose lease expired (cluster mode).
        Reclaim(ctx, unit, before) []Job
    }

- SQLite: one scan unit, plain index scan, single-writer.
- MySQL/TiDB: explicit partition selection keeps each scan owned by
  exactly one admission loop (see Shard registry). Acquire is a
  conditional UPDATE ... WHERE state='waiting' AND id IN (...), with
  the shard epoch recorded on the row.

### Admission loop

One loop per scan unit. Each tick:

1. Compute the window: from the misfire floor (older of the unit's
   tracked high-water mark and `now - misfire horizon`) to
   `now + lookahead`.
2. Select waiting jobs, ordered by due time, limited by available fire
   capacity — never admit more than can be held or run.
3. CAS-acquire the batch; re-read to confirm winners.
4. Hand winners to the wheel. If the wheel rejects (full), release the
   surplus back to waiting; nothing is lost.

### Hierarchical wheel

Kafka-style hierarchical timing wheel, per scan unit (per-shard wheels
make handoff drain one wheel, not surgery on a shared one).

- Insert O(1), tick advancement O(1) amortized, cascade on wheel
  wraparound.
- The fire path validates the shard epoch **per batch, before firing**:
  a zombie node whose ownership was revoked must not deliver. This is
  load-bearing correctness machinery, not an optimization.
- Cancel of an admitted-but-unfired job is **mark, don't remove**:
  flag the entry, let the fire tick skip it. Removal races the tick;
  a mark is O(1) and a late mark is a no-op.

### Dispersion (jitter) windows

Hot minutes can be melted before they form. A schedule may carry a
`jitter` window; its due time is `scheduled + rand(0, jitter)`, drawn
once at submit and stored — durable, decided, visible in SQL. This
complements the wheel's storm handling: dispersion prevents storms
from forming (the 09:00 cron fleet), the wheel drains the ones that
form anyway (catch-up after downtime, backfill replay). Default zero,
opt-in per schedule.

### Execution

- Embedded mode: fired jobs run on an in-process worker pool with
  per-queue concurrency caps. Cancellation is strong until the fire
  moment.
- Cluster mode: the DeliveryEngine (below) takes over.
- **Declarative targets (v1.x, agent face)**: a job may carry its own
  delivery address instead of referring to a registered handler —
  `target_type` + `target_ref` on the row. Three types: `webhook`
  (HMAC-SHA256-signed POST; the per-job secret ships at submit so the
  target can verify origin), `sampling` (the prompt is handed back to
  the scheduling host's MCP client), `handler` (library-mode
  registered function).
- **The execution log is core, not cluster-only**: every attempt and
  every fire is a receipt row — idempotency key included — in both
  embedded and cluster modes. "Did this side effect happen?" is a
  SELECT on every deployment shape.

### Remote delivery (cluster mode)

The receipt half of `fired_record` is core schema (see Execution);
this section is the remote-push half. The `fired_record` row is the
durable queue; memory is a cache.

    wheel fires at T
      -> INSERT fired_record(state=delivering)      # commit point
      -> enqueue on the per-domain in-process queue  # fast path; may fail
      -> gRPC push to a registered executor -> ack
      -> UPDATE fired_record(state=delivered)

- A domain's slow executor backs up only that domain's queue; when
  full, new commands stay as `delivering` rows and a **reaper**
  re-delivers them when capacity returns. Fire-side never blocks, so
  one slow domain cannot stall shared partitions.
- Executors register in etcd with lease TTL; routing is
  least-outstanding per domain.
- Retry with exponential backoff capped by **both** max attempts and
  max age, whichever comes first — a capped-backoff zombie must not
  retry forever; then `state=dead`. Dead letters are rows:
  `hourglass dlq ls`, `hourglass dlq replay`.

### Shard registry (cluster mode)

- Each scheduling node holds an etcd lease and claims partitions —
  **logical shards**, over-allocated and mapped onto physical
  partitions by a directory. Growing capacity edits the mapping
  instead of re-splitting partitions; Temporal's permanently fixed
  shard count is the cautionary tale.
- Ownership records carry a monotonically increasing **epoch**.
- Handoff, graceful: stop admitting, drain the wheel (fire what is due
  or requeue what is not), release.
- Handoff, ungraceful: lease expires; the new owner bumps the epoch and
  reclaims `acquired` rows whose lease is stale, then rescans the
  misfire horizon. The old owner's late CAS writes fail the epoch check.

## State machines

Job:

    waiting -> acquired (CAS, epoch recorded)
    acquired -> fired (wheel fired, handler dispatched)
    fired -> completed
    fired -> dead (retries exhausted)
    waiting -> canceled (strong cancel, before admission)
    acquired -> canceled (wheel mode, before fire; epoch-fenced)
    completed -> waiting (reschedule: cron next occurrence, or an
    executor ack carrying reschedule_at — one edge, two callers)

    canceled rows persist as receipts (with reason) until retention
    expires — "was planned, was revoked" is audit data.

fired_record (remote delivery): delivering -> delivered | dead;
dead -> delivering (replay).

## Retention and growth

Terminal rows accumulate forever otherwise — the documented pain of
every SQL-truth scheduler (river tells users to archive; an embedded
SQLite file grows silently into gigabytes).

- Terminal rows (`completed`, `dead`) expire on a configurable TTL
  (default 7 days); a background purge loop deletes them in bounded
  batches, never inline with the fire path.
- Purge reports store-size metrics. SQLite runs with auto_vacuum;
  MySQL/TiDB partitions that are fully terminal can be dropped
  wholesale.

## The four load-bearing pieces

These are the parts whose absence silently corrupts the contracts.
Each gets a dedicated test with a fake clock.

1. **Epoch validation on the wheel fire path** — zombie nodes must not
   deliver after handoff.
2. **Acquired-row lease + reclaim** — a new owner distinguishes
   might-still-be-publishing from safely-reclaimable.
3. **Misfire rescan horizon** — the wheel is lossy by design; the
   horizon must cover the admitted-but-undelivered window (lookahead +
   queue depth + retry budget).
4. **fired_record as truth** — in-memory queues are caches; reapers and
   takeovers recover everything the caches dropped.

## Contracts

- Jobs fire at the appointed second — never early, never skipped. The
  wheel holds precision; the database holds truth; memory is a cache,
  never the owner.
- Jobs never orphan. At-least-once delivery with idempotent handlers;
  crashed nodes' in-flight jobs are reclaimed by lease expiry and
  re-fired within the misfire horizon.
- Cancellation actually cancels. Until the fire moment in wheel mode;
  epoch fencing stops a zombie node's late shots from resurrecting a
  canceled job. No cancellation promise after fire. Cancellations
  persist as receipts: plans leave footprints.
- Every fire is a queryable receipt. Idempotency keys are stored,
  attempts are rows; an agent can prove what happened and what never
  did.
- Dead letters are queryable rows, replayable from the CLI.

## Benchmark methodology (v1.0 gate)

The launch claim lives or dies on these, all scripts in-repo and
reproducible:

1. **Fire precision**: p99/p999 of |actual − scheduled| for a
   randomized schedule, hourglass vs a poll-based baseline at default
   settings.
2. **Hot minute**: N=100K jobs due at the same second — time to drain,
   database QPS during the storm, per-second fire curve.
3. **Quiescence**: database query rate while idle with 1M pending jobs
   (wheel admits in windows; pollers never stop asking).
4. **Ceiling**: sustained admission+fire rate, single embedded process;
   cluster scaling is post-1.0.
5. **Sleeping agents**: the same monitor loop as an always-on polling
   process vs an hourglass self-chained wakeup — DB queries per hour,
   wakeups, CPU. The cost chart for "agents that sleep".

Baselines: asynq and river at their defaults, same hardware, same
randomized schedule seed.

## The broker alternative

The ancestor of this design ran a heavier shape in production: SQL for
the long horizon, a delayed-delivery broker (Pulsar deliverAt / RocketMQ
5.x timers) for the near horizon, etcd for sharding — 200K commands/s,
measured. It works. It is not the v1 core, for three reasons:

1. **Cancellation boundary.** Once published, a delayed message cannot
   be canceled — no broker implements it. Wheel mode cancels until the
   fire moment; broker mode cancels until admission. The production
   ancestor removed its Redis-based cancel layer and fixed the boundary
   at admission the hard way.
2. **Precision sovereignty.** Broker deliverAt precision is the
   broker's tracker interval and its delayed index lives on the broker
   heap — millions of outstanding delayed messages become someone
   else's memory problem (and a hot minute becomes a consumer storm).
3. **Dependency funnel.** SQL + broker + etcd is three systems before
   the first job fires. It cannot be the 30-second start.

So the broker shape is kept as a post-1.0 delivery driver
(`delayedQueueSink`), not the core. If built, it inherits two seam
contracts from the ancestor's scars:

- The crash seam is **CAS-admit → publish**: a row acquired but never
  published must be found by lease-expiry rescan. At-least-once makes
  the reverse seam (published, mark lost) a duplicate, not a loss.
- Cancellation ends at admission. Documented, not hidden.

Per-domain mixing stays the end state: wheel domains for precision and
cancellation, broker domains for near-term volume — each domain picks
its failure model.

## The agent face (v1.x)

Agents cannot hold their own futures: they run ephemerally, on
untrusted hosts, inside serverless sandboxes. A promise to act in two
hours is worthless without an external custodian — which is why every
serious agent platform re-implements a durable timer service
(Temporal's per-shard timer queues, Restate's journaled timers,
Durable Object alarms) while the long tail that will not run an
engine polls SQLite every 60 seconds. hourglass's position: the
durable time layer, extracted and standalone — engine-agnostic,
model-free, contract-first.

The face is three pieces, all riding the v1.0 engine unchanged:

1. `hourglass mcp` — an MCP server (stdio locally, streamable HTTP
   remotely) exposing `schedule`, `cancel`, `list`, `receipts`, `dlq`.
   Built on the official modelcontextprotocol/go-sdk. Thin adapter,
   no business logic.
2. Declarative delivery targets (see Execution) — a job carries its
   own address; webhook deliveries are HMAC-signed.
3. Self-chaining via `reschedule_at` in the ack — "call me again in
   30 minutes" turns polling agents into sleeping agents.

Patterns the face enables with zero new engine features: timeout
escorts (a deadline timer beside every human-in-the-loop wait;
arrival of the event cancels the escort), durable monitor loops
(self-chaining), cron briefings. What hourglass deliberately is not:
a step-replay engine, an NL time parser, a model caller, an agent
memory — the fences in Non-goals.

## Deployment modes

| mode | store | coordination | delivery | agent face | target |
| --- | --- | --- | --- | --- | --- |
| embedded | SQLite | none | in-process workers + targets | `hourglass mcp` (stdio) | apps, agents, ≤ thousands of jobs/s |
| cluster | MySQL/TiDB | etcd | remote delivery + gRPC executors | `hourglass mcp` (HTTP) | platforms, agent fleets, hot storms, HA |

## Implementation notes (deferred, recorded)

- Fire-path state advances in **batched** writes (UPDATE by id set),
  symmetric with batched CAS admission — per-job writes turn the hot
  minute into a write storm.
- The executor ack protocol reserves an optional `reschedule_at`
  field from day one (executor self-chaining: drip patterns,
  executor-driven retries); adding a field later costs a protocol
  migration, reserving it costs nothing.
- Webhook targets sign payloads with HMAC-SHA256; the per-job secret
  is generated at submit and returned once — verification is a
  shared-secret check, no PKI.
- The MCP `receipts` tool is first-class, not an afterthought: agent
  self-audit is a headline use of the face.
