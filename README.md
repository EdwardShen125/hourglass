# hourglass

Delayed jobs and cron for Go: start in 30 seconds on SQLite, scale to a
sharded cluster without changing the engine.

```go
h, _ := hourglass.Open("sqlite://./hourglass.db")
defer h.Close()

h.Register("email:welcome", handler)
h.Submit(ctx, "email:welcome", payload, hourglass.Delay(24*time.Hour))
h.Every(ctx, "report:daily", "0 9 * * *")   // cron, registered handler
h.Cancel(ctx, id, "user asked")             // wins until the fire moment

go h.Run(ctx)                               // 1s ticks: admit -> wheel -> fire
```

Inspect and operate from the CLI (reads the same store):

```
$ hourglass stats
jobs by state:
  waiting    1204
  completed  8891
  dead       3
receipts: 17802
oldest terminal kept: 2d18h (ago)

$ hourglass dlq ls --topic report:daily
9f3c...  report:daily  attempts=3  due=2026-10-31T09:00:00Z

$ hourglass dlq replay 9f3c...
replayed 9f3c...
```

No broker, no coordinator, no services to run. When the workload
outgrows one process, point the same code at MySQL and add etcd — the
engine, the data model, and the API stay identical.

## Why

Most Go task systems make you choose: a cache masquerading as a task
store (asynq — Redis, no transactional audit trail), one database
dialect with poll-based precision (river — PostgreSQL), or a central
dispatcher serializing on one task table (xxl-job-style). hourglass
splits the timing contract instead:

- The database keeps the long horizon: every job is auditable,
  cancelable, and recoverable in SQL you can query.
- The time wheel keeps the precision: an in-process hierarchical wheel
  fires jobs at second-level accuracy — no polling interval, no broker
  in the hot path.
- The state machine keeps the honesty: admission is a CAS, delivery is
  at-least-once with idempotent handlers, dead letters are rows you can
  list and replay from the CLI.

The scale story is the same design, not a rewrite: partitioned scans on
MySQL/TiDB, one admission loop per partition, nodes leasing partitions
through etcd with epoch-fenced handoff and misfire rescans. The
architecture was proven in a production system that replaced xxl-job
and peaked above 200K execution commands per second — that story, and
the failures that shaped it, is documented here:
[Building a Production Go Scheduler for 200K+ Delayed Jobs per Second](https://edwardshen125.github.io/posts/engineering-case-study/building-a-production-go-scheduler-for-200k-delayed-jobs-per-second/).

## Measured (v1.0 benchmarks, fake clock, in-repo and reproducible)

| benchmark | hourglass | poll-based baseline |
| --------- | --------- | ------------------- |
| fire precision p50 / p99 / max (5,000 jobs / 10 min) | **0 / 0 / 1000 ms** (1s tick) | 2,000 / 4,000 / 5,000 ms (5s poll) |
| quiescent reads, 10 min idle, 100K pending | **2** (admission watermark) | 121 (one scan per interval, forever) |
| hot minute: 100K jobs due the same second | **11.1s drain, 0.02 write stmts/job, zero loss/duplication** | unbatched path would issue 4+ writes/job |

All three are gated tests (`-run TestBenchmark`), not logged demos:
regressing the wheel to poller-grade precision or losing the quiescence
watermark fails the suite. The 100K hot minute found a real defect on
its first run — every batched id-list statement exceeded SQLite's
placeholder cap above ~8K ids and the batch silently stalled; all
id-list statements now chunk at 500 per round-trip.

## Contracts

- Jobs fire at the appointed second — never early, never skipped. The
  wheel holds the admitted slice in memory; the database holds the
  truth. In-memory state is a cache, never the owner.
- Jobs never orphan. Delivery is at-least-once with idempotent
  handlers; a crashed node's in-flight jobs are reclaimed by lease
  expiry and re-fired within the misfire horizon — no stuck-active
  states, no manual cleanup.
- Cancellation actually cancels. Until the fire moment, cancel wins —
  and epoch fencing stops a zombie node's late shots from resurrecting
  a canceled job.
- Dead letters are queryable rows. `hourglass dlq ls`, `hourglass dlq
  replay` — no lost topics, no console required.

## Status

Pre-1.0, private until launch-ready. The embedded engine is complete
and measured (see above); cluster mode is the remaining v1.0 work. Built
to the enterprise bar (audit, fencing, HA handoff), entered through the
30-second start; the build blueprint is [DESIGN.md](DESIGN.md), the
production lessons behind it are in [docs/lessons.md](docs/lessons.md).
v1.0 scope:

- [x] Embedded core: SQLite storage, single-node admission loop
- [x] Hierarchical time wheel, second-level fire precision
- [x] Delayed jobs + cron, typed handler API
- [x] Dispersion (jitter) windows per schedule
- [x] Retention: terminal-row TTL, background purge, size metrics
- [x] Execution log: attempts, receipts in core schema
- [x] Retry policy: per-job attempts, backoff requeue, dead letters
- [x] Crash recovery: in-flight rows requeued at Open
- [x] CLI: `stats`, `dlq ls`, `dlq replay`
- [x] Benchmarks: fire precision, quiescence, hot minute (gated tests)
- [ ] Cluster storage: MySQL/TiDB partitioned scans, CAS admission
- [ ] Cluster coordination: etcd shard registry, epoch-fenced handoff,
      misfire rescan
- [ ] Executor SDK (gRPC): registration, heartbeat, idempotency

Post-1.0, on demand: an MCP agent face (`hourglass mcp`, declarative
webhook targets, self-chaining acks, event-anchored delays, debounce
windows) — durable time for agents that cannot hold their own
futures — plus delayed-queue delivery backends (Pulsar, RocketMQ 5.x
`deliver-at`) for deployments that already run a broker.

## Comparison

The short table, then the details. The axis that matters most is the
last one: what does adding a process buy you?

|            | durable state    | fire precision      | scaling model          |
| ---------- | ---------------- | ------------------- | ---------------------- |
| asynq      | Redis            | poll interval       | Redis ops ceiling      |
| river      | PostgreSQL       | poll interval       | row locks              |
| xxl-job    | any SQL          | dispatch loop       | lock contention        |
| Temporal   | sharded SQL/NoSQL| per-shard timers    | shards fixed at boot   |
| hourglass  | SQLite → SQL     | time wheel          | single node → sharded  |

### vs asynq

- **Truth lives in Redis** (sorted sets; AOF/RDB are backups): no
  transactional audit trail — "did this side effect happen?" is not a
  query you can run next to your domain tables. hourglass keeps every
  job, attempt, and receipt as SQL rows.
- **Delayed tasks wait on a poller**: asynq's forwarder moves due tasks
  from the scheduled set every 5s (default), so worst-case fire error
  tracks the interval and every server competes on the same zset.
  hourglass admits windows into a wheel: p99 error 0ms at a 1s tick
  (measured above), and the database goes quiet between windows.
- **Scale-out is execution-only.** More asynq servers = more task
  concurrency; scheduling throughput stays a constant of the single
  scheduled-set scan. In hourglass, nodes own disjoint shards — more
  nodes = more scheduling.
- **Redis Cluster sharding exists per-queue but resharding breaks
  multi-key operations** (their wiki says so); hourglass migration
  moves scan ownership only — data never moves, nothing to redirect.
- asynq wins on: a mature Web UI, task aggregation, uniqueness TTLs,
  and raw enqueue latency (Redis writes). If you already run Redis and
  need a queue today, asynq is a fine choice — it just isn't a
  scheduler with an audit trail.

### vs river

- **`FOR UPDATE SKIP LOCKED` over one PostgreSQL table**: claiming is
  row-lock contention under load, and discovery is polling with
  LISTEN/NOTIFY as a doorbell (notifications may drop; the poll
  backstops — their own docs describe the hybrid). The wheel removes
  both the contention and the poll from the hot path.
- **Single-database, single-dialect.** river is PostgreSQL-only, and
  deep backlogs degenerate into increasingly expensive scans. hourglass
  speaks SQLite (embedded) and MySQL/TiDB (sharded scans), and the
  quiescence watermark means a 100K-row idle backlog costs ~2 reads per
  quiet period, not a scan every interval.
- **Same execution-only scale-out**: more river workers claim more
  rows; the table remains one serialized surface.
- river wins on: transactional enqueue inside your own Postgres
  transactions (a genuinely nice property), years of production polish,
  a UI, and no second system to run. If your workload fits one Postgres
  and you want queue+DB atomicity, river is the pragmatic pick.

### vs xxl-job (and its many descendants)

- **A central dispatcher serializing on one task table**: every
  schedule tick competes on the same rows; the DB is the bottleneck by
  construction. hourglass's admission is per-shard CAS batches — no
  central dispatcher exists.
- **No durable audit of executions beyond logs**; misfires are
  operator-visible, not queryable state. hourglass receipts make every
  fire a row with an idempotency key.
- xxl-job wins on: a battle-tested admin console, manual
  run/kill/re-run UX, and Chinese-ecosystem integration. As an
  embeddable engine it was never a candidate — which is exactly the gap
  the ancestor system replaced it in production.

### vs Temporal

- **Same core ownership model** — hash → shard, one owner per shard, a
  monotonic fencing generation (their RangeID is our epoch). What
  Temporal proves is the model; what hourglass changes is the cost of
  admission: Temporal is four services plus Cassandra/MySQL and a
  fixed-at-creation shard count; hourglass is a Go library plus the SQL
  and etcd you may already run, with 256 logical shards remapped
  without moving data.
- **hourglass is a time layer, not a workflow engine.** No event
  history, no replay, no durable closures — deliberately (Non-goals).
  Compose them: use hourglass for the timers that wake a Temporal
  workflow when you need both.
- Temporal wins on: workflows as code, replayable history, multi-SDK
  ecosystem, and operational tooling at a scale few systems reach. If
  you need durable execution, use Temporal; if you need durable *time*,
  that is a smaller, sharper tool.

### The honest summary

- Need a task queue attached to Redis/Postgres with a UI, today:
  **asynq / river** — both are good at what they are.
- Need durable workflows with replay: **Temporal**.
- Need a scheduler that is a library, auditable in SQL, second-precise
  without polling, and whose scheduling throughput grows with nodes:
  that is the gap hourglass occupies — measured, gated, and reproducible
  in this repo.

(Fuller notes with sources: [docs/lessons.md](docs/lessons.md) #11–12.)

## License

MIT
