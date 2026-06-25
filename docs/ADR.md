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
The worker checks `EXISTS processed:{order_id}` before processing. If present, the
message is ACKed and skipped — no charge is issued. After a successful charge, the
ledger write and the idempotency key are set together in a single `MULTI/EXEC`
pipeline, making them atomic: key exists ↔ ledger was incremented. Setting the key
only after the ledger write (not before the charge) is the critical invariant — it
prevents a crash between SET NX and the ledger write from silently losing an order
on restart.

### Failure handling
All calls to `/charge` carry a per-request timeout (`CHARGE_TIMEOUT_S=6`, above the
service's `SLOW_SECONDS=5`). Failures are classified by type:

- **Transient (5xx, timeout)** — retried with exponential backoff and jitter
  (`BASE_BACKOFF_S=0.25`, `MAX_BACKOFF_S=5`). `MAX_RETRIES=20` because the payments
  service failures are stateless and random (30 % 5xx, 10 % hang, independent per
  call). There is no such thing as a permanently failed order from this service — every
  call has a fresh chance of success. Dead-lettering after only a few retries would
  silently drop retrievable work. P(all 20 attempts fail) ≈ 0.0001 % per order.
- **Permanent (4xx)** — dead-lettered immediately; a malformed request will not
  fix itself on retry.

Poison messages are written to `orders:dead-letter` and ACKed so they leave the PEL.
The processing loop continues with the next message regardless.

### CI/CD pipeline hardening
Three jobs added to `.github/workflows/ci.yml`, chained so each gates the next:

1. **Hadolint** (added to `lint` job) — static lint of all Dockerfiles before any
   build. Catches unpinned base tags, missing `--no-cache-dir`, and layer ordering
   issues at zero cost. Fails fast before wasting build minutes.

2. **Trivy** (new `security` job, runs after `integration`) — scans the built worker
   image for CVEs in the base image and Python deps. Fails on `CRITICAL` severity.
   Scoped to the worker only (the one service changed) to keep scan time under 60 s.

3. **GHCR push** (new `publish` job, `main` branch only, runs after `security`) —
   builds the worker image and pushes to GitHub Container Registry tagged with the
   git commit SHA and `latest`. Uses `GITHUB_TOKEN` only — no external secrets.
   SHA tag is the promotable artifact; `latest` is a dev convenience, never deployed
   to staging or prod per the image promotion strategy above.

Gate order: `lint` → `integration` → `security` → `publish`. A CVE or lint failure
blocks the image from being published.

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
