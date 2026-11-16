# Positioning — and an attempt to knock it down

This document argues against hourglass's own pitch, then reports what
survives. It exists because a repository full of engineering is
common; a repository that shows *how the author tests their own
judgment* is not. The corrections culture started in
docs/research-temporal.md ("Corrections this review made to earlier
claims"); this is its continuation.

## The claim under test

> Agents and ephemeral workloads cannot hold their own futures. Every
> serious platform re-implements durable timers (Temporal's per-shard
> `timer_tasks`, Restate's journaled timers, Durable Object alarms)
> while the long tail polls SQLite every 60 seconds. Therefore a
> standalone, engine-agnostic durable *time layer* — custody without
> the platform — is the missing category, and hourglass is it.

## Challenge 1 — custody may be a feature, not a category

The strongest form: platforms do not extract their timers *because
timer value comes from colocation with the state it guards*.
Temporal's `timer_tasks` sit in the shard next to the history they
wake; DO alarms sit inside the object. Pull the timer out and you
manufacture a distributed seam: the timer fires, but the thing it
wakes has died, moved, or changed its mind — so now you need delivery
retries, dead letters, idempotency… that is, the durable-execution
machinery the pitch just said you don't need. On this reading,
"standalone durable timer" is not "custody without the platform", it
is "a platform, disassembled", and the seams are the price.

Status: **partially sustained.** hourglass's answer is that the seam
contracts are explicit and small (D2's CAS-admit → publish crash seam;
cancellation boundary documented, not hidden) — but the challenge is
correct that the seams exist and are the real cost of extraction.

## Challenge 2 — "firing is solved" is a speaker's assertion

Every generation claims the previous one solved firing. Our own
failover measurements say otherwise: takeover is 2.57–3.58 s, bounded
by LeaseTTL. During that window nothing fires — the misfire floor
means every ownership change is a precision cliff. If "precision
sovereignty" is the pitch, then the frontier is *precision continuity
across handoff*, and custody is a secondary story.

Status: **sustained against the slogan; mitigated in the design.** The
misfire horizon rescans and re-fires within the window (no loss), but
latency during handover is real and should be presented as a
measured limitation, not hidden behind "second-level precision".

## Challenge 3 — the embedded open-source position is squeezed from
both sides

Below: cloud schedulers are free, managed, and already carry
cancellation and signed delivery (EventBridge Scheduler, Cloud Tasks,
DO alarms). Above: agent platforms absorb scheduling the way platforms
absorbed queues. QStash proves people pay for "durable wake as a
service" — but it is a hosted broker with a face, not an embedded
engine; there is no evidence anyone pays for *embedded* durable
timers. The engine-agnostic, model-free middle is glue that neither
side owns end-to-end.

Status: **sustained as a commercial claim.** This project does not
contest it: see "Consequence" below.

## Challenge 4 — of the "six needs", five are furniture

Cancellation to the last moment, receipts, signed wakes, wake packs —
each is a quarter of work for an incumbent to copy. Only custody is
load-bearing, and challenge 1 already dents custody. A category
defined by copyable features is a roadmap, not a category. And the
generational ladder whose top rung is "the speaker's product" is the
same narrative bias this project already caught itself once: the
quadrant chart's "hourglass strictly more scalable" was retracted in
docs/research-temporal.md for exactly this reason.

Status: **sustained as a critique of category claims.** What survives
is narrower: incumbents structurally will not carry *your*
authorization context or make *their* timers queryable in *your* SQL.
The trust and evidence layer — receipts in the user's database, an
authorization envelope that travels with the wake — is the only part
with structural, not incremental, defensibility.

## What survives the challenge

1. The **need** is real: ephemeral requesters require external custody
   of futures. Every platform's re-implementation is evidence.
2. The **category claim** is unproven, and this repository should stop
   making it. hourglass is a *reference implementation with an honest
   measurement story*, not a category founder.
3. The **defensible core** is the evidence layer (queryable receipts,
   durable jitter, cancellation footprints, authz envelope if ever
   built) — the parts that are structural for incumbents to copy.
4. The **engineering** is the point, not the market: correctness
   machinery (fencing, misfire horizon, batched fire path), measured
   claims, and decision records (docs/decisions.md).

## Consequence for this project

The goal of hourglass is to demonstrate engineering and judgment, not
to win a market. Therefore:

- Scope is frozen at the v1.0 that exists: embedded + cluster + gRPC
  executor. No v1.x agent-face features will be added to chase the
  category story (the earlier MCP/event-anchor/debounce roadmap is
  shelved for this reason).
- Investment goes to the artifacts a serious reviewer reads:
  decisions.md, measurement rigor (benchstat/HDR upgrade), this
  document, and the load-test report.
- Known limitations stay stated, not marketed: single-host benchmark
  numbers, takeover latency cliff, fixed logical shard count, manual
  rebalance.

## Corrections log

1. research-temporal.md: "Temporal timers are an in-memory queue" →
   corrected to durable `timer_tasks` rows per shard; "poll-based" was
   flat wrong.
2. research-temporal.md: "hourglass strictly more scalable" quadrant
   claim → retracted; both are shard-owner models; Temporal ahead on
   audit depth.
3. This document: the "custody is the frontier" pitch → downgraded to
   "the need is real; the category is unproven; the evidence layer is
   the defensible part" (2026-11-16).
