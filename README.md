# hourglass

Distributed delayed task scheduler in Go: durable state in partitioned
SQL, second-level delivery via per-shard time wheels, and linear scale
through shard ownership.

## Why

Existing Go task systems make different trade-offs: state in Redis with
no transactional audit trail (asynq), a single SQL dialect with
poll-based precision (river), or central dispatch with lock contention
on one task table (xxl-job-style). hourglass splits the timing contract
instead:

- The database keeps the long horizon: every trigger row is auditable,
  cancelable, and recoverable in SQL you can query.
- The time wheel keeps the precision: each scheduling node runs a
  hierarchical timing wheel that fires admitted triggers at second-level
  accuracy, with no broker in the hot path.
- Coordination keeps the scale: nodes lease partitions of the trigger
  table through etcd; ownership handoff is epoch-fenced, and a new owner
  reclaims missed work by rescanning its misfire horizon.

What happens to an admitted trigger is itself a pluggable contract
(`CommandSink`), symmetric with the storage driver:

- `wheel` (built-in): the node holds the admitted slice in its time
  wheel and delivers through the built-in engine — a `fired_record` row
  is the durable delivery queue, per-domain in-process queues are only a
  fast path, a reaper re-delivers anything the fast path dropped, and a
  dead letter is a queryable table row, not a lost topic. Cancellation
  is strong: a trigger can be canceled any time before it fires, and
  epoch fencing stops a zombie node's late shots.
- `delayed-queue` (drivers: Pulsar, RocketMQ 5.x): admission publishes
  with `deliver-at` and the broker holds the admitted slice — for
  deployments that already run one. Cancellation stops at admission;
  that is the broker's contract, not hidden.

Backends can be mixed per domain: point one domain at an existing
Pulsar cluster while another runs broker-free. Kafka is a non-goal for
this driver — it has no native delayed delivery, and a two-topic
workaround would rebuild the wheel against the wrong system.

## Contracts

- Admission is the cancellation boundary for delayed-queue domains. No
  command can be canceled after admission; cancel before the CAS wins.
  Wheel domains cancel until the fire moment; the cancel API reports
  which happened.
- At-least-once delivery, idempotent executors. A node can die between
  admission and delivery; the next owner re-fires within the misfire
  horizon.
- Epoch fencing. CAS writes carry the shard epoch; a previous owner's
  late writes fail instead of corrupting state.
- The fired-record table is the source of truth for delivery.
  In-memory queues cache it; they never own it.

## Status

Design phase. v0.1 scope:

- [ ] Storage driver interface + MySQL/TiDB driver (partitioned scans)
- [ ] CommandSink interface + built-in wheel sink: hierarchical wheel,
      fired-record state machine, per-domain queues, reaper, gRPC push,
      SQL dead letters
- [ ] etcd shard registry: lease, watch, epoch-fenced handoff
- [ ] CAS admission loop with misfire rescan
- [ ] Executor SDK (gRPC): registration, heartbeat, idempotency
- [ ] delayed-queue drivers (Pulsar, RocketMQ 5.x) — after v0.1, on demand

The design is a clean-room rebuild of a production system that peaked
above 200K execution commands per second; its architecture and failure
modes are documented here:
[Building a Production Go Scheduler for 200K+ Delayed Jobs per Second](https://edwardshen125.github.io/posts/engineering-case-study/building-a-production-go-scheduler-for-200k-delayed-jobs-per-second/).

## Comparison

|            | durable state   | delivery precision   | scaling model     |
| ---------- | --------------- | -------------------- | ----------------- |
| asynq      | Redis           | poll                 | Redis ops ceiling |
| river      | PostgreSQL      | poll                 | row locks         |
| xxl-job    | any SQL         | dispatch loop        | lock contention   |
| hourglass  | partitioned SQL | per-shard time wheel | shard ownership   |

## License

MIT
