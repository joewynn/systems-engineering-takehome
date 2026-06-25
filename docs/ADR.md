# ADR-001: Production-grade order→payment pipeline

## Status
Accepted

## Context
The pipeline accepts orders via `POST /orders`, publishes to a Redis Stream, and a
worker charges a flaky payments service, writing results to a Redis ledger. The
correctness contract is strict: every order charged exactly once, overcharge fails
immediately, undercharge after 120 s fails.

The prototype had three critical flaws: `xread` with `last_id="$"` silently drops
all in-flight messages on worker restart; no idempotency gate allows the same
`order_id` to be charged multiple times; and unhandled 500s or hangs crash or
stall the processing loop permanently.

## Decision

### Delivery & consistency semantics
**Effectively-once.** At-least-once delivery via Redis consumer groups (`XREADGROUP`
+ `XACK`) ensures no message is lost on restart — unacked messages are reclaimed via
`XAUTOCLAIM`. Delivery alone is not enough; the consumer must also be idempotent so
that redelivered messages produce no additional side effects. The combination yields
an effectively-once outcome without a distributed transaction.

### Idempotency
Before calling `/charge`, the worker sets `SET processed:{order_id} 1 NX`. If the
key already exists, the message is acknowledged and skipped — no charge is issued.
The state lives in Redis (same store as the stream) so no external coordination is
needed. `SET NX` is atomic, eliminating the check-then-act race that would otherwise
allow two concurrent workers to both pass the gate and double-charge.

### Failure handling
All calls to `/charge` carry a per-request timeout. On `5xx` or timeout the worker
retries with exponential backoff and jitter. Transient failures (network blip, 500)
are retried; a message is classified as permanently failed only after exceeding the
max retry budget. Poison messages are moved to a `orders:dead-letter` stream so one
bad message cannot stall the consumer group. The processing loop continues with the
next pending message regardless.

## Tradeoffs & alternatives

### Build vs adopt: Redis Streams vs Kafka / SQS / managed broker
Keep Redis Streams for this workload — single consumer group, low volume,
sub-millisecond latency requirement. Switch when:

| Trigger | Reach for |
|---|---|
| Stream size approaches RAM / zero-data-loss compliance | Kafka / Confluent Cloud |
| Already on AWS, want zero-ops | SQS FIFO (note: 3k msg/s cap) |
| Complex dynamic routing / fan-out | RabbitMQ |

Redis cannot scale horizontally without cluster sharding, has no infinite retention,
and AOF/RDB durability trade-offs are not acceptable under payment compliance audits.

### From CI to CD
Build once in CI, tag with the git commit SHA. Promote that immutable artifact
through environments by updating manifests — never rebuild, never deploy `:latest`.

- **Dev** — auto-deploy on every merge to main
- **Staging** — auto-deploy after acceptance tests pass
- **Prod** — manual approval gate (audit trail, human sign-off required for payments)

**Rollout:** Canary over blue-green. A payment failure is a revenue event; canary
bounds blast radius to 1 % of live traffic with auto-rollback if the error rate
spikes within a 10-minute soak window. Use Argo Rollouts or Flagger.

**GitOps:** Start with `kubectl apply` / `kustomize edit set image` via CI runner.
Graduate to ArgoCD when managing multiple clusters or when the ops team needs drift
detection and a GUI for prod approvals.

### Scaling to 100×
1. **First to break — the worker.** A single process is I/O-blocked on the flaky
   payments API; consumer lag grows unbounded. Fix: scale the consumer group
   horizontally (more pods, same group name) and add async I/O within each worker.
2. **Next bottleneck — the payments service.** 100× outbound volume hits provider
   rate limits and amplifies existing flakiness. Fix: distributed token-bucket rate
   limiter (Redis), circuit breaker to shed load fast, backoff with jitter to avoid
   thundering herd on retry storms.
3. **Final bottleneck — Redis.** Memory fills as backed-up messages accumulate;
   single-threaded command processor saturates under concurrent `XREADGROUP`/`XACK`.
   Fix: `XADD MAXLEN ~` trimming and Redis Cluster sharding.

## Consequences
_To be completed after implementation._
