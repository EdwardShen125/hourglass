# hourglass

Delayed jobs and cron for Go: start in 30 seconds on SQLite, scale to a
sharded cluster without changing the engine.

```go
h, _ := hourglass.Open("sqlite://./hourglass.db")
defer h.Close()

h.Submit(ctx, "email:welcome", payload, hourglass.Delay(24*time.Hour))
h.Every(ctx, "report:daily", "0 9 * * *", handler)
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

Pre-1.0, private until launch-ready. The build blueprint is
[DESIGN.md](DESIGN.md). v1.0 scope:

- [ ] Embedded core: SQLite storage, single-node admission loop
- [ ] Hierarchical time wheel, second-level fire precision
- [ ] Delayed jobs + cron, typed handler API
- [ ] Dispersion (jitter) windows per schedule
- [ ] Retention: terminal-row TTL, background purge, size metrics
- [ ] CLI: `stats`, `dlq ls`, `dlq replay`
- [ ] Cluster storage: MySQL/TiDB partitioned scans, CAS admission
- [ ] Cluster coordination: etcd shard registry, epoch-fenced handoff,
      misfire rescan
- [ ] Executor SDK (gRPC): registration, heartbeat, idempotency
- [ ] Benchmarks: fire-time precision and throughput vs poll-based
      schedulers

Post-1.0, on demand: delayed-queue delivery backends (Pulsar,
RocketMQ 5.x `deliver-at`) for deployments that already run a broker.

## Comparison

|            | durable state    | fire precision      | scaling model          |
| ---------- | ---------------- | ------------------- | ---------------------- |
| asynq      | Redis            | poll interval       | Redis ops ceiling      |
| river      | PostgreSQL       | poll interval       | row locks              |
| xxl-job    | any SQL          | dispatch loop       | lock contention        |
| hourglass  | SQLite → SQL     | time wheel          | single node → sharded  |

## License

MIT
