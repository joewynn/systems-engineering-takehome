# ADR-001: <short title for the decision you made>

> Architecture Decision Record. Keep it tight — one to two pages. We're reading for
> *decisions and tradeoffs*, not prose. Delete this quote block and the prompts below
> as you fill it in.

## Status
Proposed

## Context
What is the system, and what conditions does it actually have to survive? (Duplicate
deliveries, worker restarts, a flaky downstream...). What was wrong with the prototype?

## Decision

### Delivery & consistency semantics
Which did you choose — at-most-once / at-least-once / effectively-once — and **why**?
What does that imply the consumer must guarantee?

### Idempotency
How do you make re-processing the same order a no-op? What's the key, where does the
state live, and what's the race you had to avoid?

### Failure handling
Retries, backoff, timeouts. How do you tell a *transient* failure from a *permanent*
one? Where do poison messages go? How do you keep one bad message from halting everything?

## Tradeoffs & alternatives

### Build vs adopt: Redis Streams vs Kafka / SQS / managed broker
We used Redis Streams to keep setup light. Would you keep it? At what point (throughput,
durability, team, ordering, retention needs) would you switch, and to what?

### From CI to CD
This repo stops at CI. How would you take it to continuous delivery — image promotion,
environments (dev/stage/prod), rollout strategy (blue-green / canary), and would you run
GitOps (ArgoCD / Flux)? Reason about it; don't build it.

### Scaling to 100×
What breaks first at 100× throughput, and what would you change? Name the next bottleneck.

## Consequences
What's better now. What's still weak / what you'd do next with more time.
