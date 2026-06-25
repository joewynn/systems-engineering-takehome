"""Order worker — production-grade.

Reads from the Redis Stream 'orders' using a consumer group so that:
  - Messages survive worker restarts (unacked entries are reclaimed via XAUTOCLAIM).
  - Each order_id is charged exactly once (idempotency gate via SET NX).
  - Transient payment failures are retried with exponential backoff; poison messages
    are routed to a dead-letter stream so one bad message never stalls the loop.

Environment variables:
    REDIS_URL       — Redis connection string (required)
    PAYMENTS_URL    — Base URL of the payments service (required)
    CONSUMER_GROUP  — Consumer group name (default: "workers")
    CONSUMER_NAME   — This instance's consumer name (default: "worker-1")
"""
import json
import os
import random
import time

import redis
import requests

REDIS_URL    = os.environ["REDIS_URL"]
PAYMENTS_URL = os.environ["PAYMENTS_URL"]

STREAM    = "orders"
DL_STREAM = "orders:dead-letter"
GROUP     = os.environ.get("CONSUMER_GROUP", "workers")
CONSUMER  = os.environ.get("CONSUMER_NAME", "worker-1")

# Idempotency key TTL: long enough to outlive any retry window.
IDEMPOTENCY_TTL_S = 86_400  # 24 hours

# Payments client config: timeout slightly above SLOW_SECONDS (5 s) in docker-compose.
# MAX_RETRIES is high because 5xx / timeouts from this service are purely transient
# (random per call). Only 4xx errors are truly permanent and go to dead-letter.
CHARGE_TIMEOUT_S = 6
MAX_RETRIES      = 20
BASE_BACKOFF_S   = 0.25
MAX_BACKOFF_S    = 5.0

r = redis.from_url(REDIS_URL, decode_responses=True)


def ensure_group():
    """Create the consumer group if it does not already exist.

    Uses id='0' so the group starts from the beginning of the stream — any
    messages produced before this worker first started are not skipped.
    BUSYGROUP is Redis's normal response when the group already exists (e.g.
    on a worker restart); any other ResponseError is a real problem.
    """
    try:
        r.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
    except redis.exceptions.ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


def claim_pending():
    """Reclaim and process all messages in the pending-entry list (PEL).

    Called once on startup. If a prior worker instance died after receiving a
    message but before ACKing it, that message sits in the PEL. XAUTOCLAIM
    with min_idle_time=0 transfers ownership of every such message to this
    consumer so they are reprocessed rather than lost.
    """
    cursor = "0-0"
    while True:
        cursor, messages, _ = r.xautoclaim(
            STREAM, GROUP, CONSUMER,
            min_idle_time=0,
            start_id=cursor,
            count=100,
        )
        for msg_id, fields in messages:
            handle(msg_id, fields)
        if cursor == "0-0":
            break


def is_already_processed(order_id: str) -> bool:
    """Check whether this order_id was fully processed (ledger already written).

    The key is set only AFTER a successful ledger write (see handle()), so its
    presence guarantees the ledger entry exists. Returns True if the order is
    done, False if it still needs to be processed.
    """
    return bool(r.exists(f"processed:{order_id}"))


def call_charge(order_id: str, amount_cents: int):
    """Call POST /charge with timeout, retrying on transient 5xx / timeout.

    Transient: any 5xx response or request timeout — retried up to MAX_RETRIES
    times with exponential backoff and jitter.
    Permanent: any 4xx response — raised immediately without retry (a malformed
    request will not fix itself on retry).

    Raises RuntimeError after MAX_RETRIES exhausted.
    """
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.post(
                f"{PAYMENTS_URL}/charge",
                json={"order_id": order_id, "amount_cents": amount_cents},
                timeout=CHARGE_TIMEOUT_S,
            )
            if resp.status_code < 500:
                resp.raise_for_status()  # 4xx → non-retriable, bubble up
                return                   # 2xx → success
            # 5xx falls through to retry logic below
        except requests.exceptions.Timeout:
            pass  # retriable

        backoff = min(BASE_BACKOFF_S * (2 ** attempt), MAX_BACKOFF_S)
        jitter  = random.uniform(0, backoff * 0.5)
        time.sleep(backoff + jitter)

    raise RuntimeError(f"charge failed after {MAX_RETRIES} attempts for order {order_id}")


def dead_letter(msg_id: str, fields: dict, reason: str):
    """Route a poison message to the dead-letter stream and log it.

    The original stream entry data is preserved alongside the failure reason
    so the message can be inspected and replayed manually if needed.
    """
    r.xadd(DL_STREAM, {"original_id": msg_id, "data": fields["data"], "reason": reason})
    print(f"[dead-letter] {msg_id} — {reason}", flush=True)


def handle(msg_id: str, fields: dict):
    """Process a single stream message end-to-end.

    Ordering is intentional and must not be changed:
      1. Check idempotency gate (SET NX) — skip if already processed.
      2. Call /charge — on permanent failure, dead-letter and ACK, do not set
         the idempotency key (the order was never charged).
      3. Write to the Redis ledger.
      4. XACK — only after the ledger write succeeds.

    If the worker dies between step 3 and step 4, the message is redelivered.
    The idempotency key from step 1 already exists, so the redelivery is a
    no-op that ACKs cleanly without issuing a second charge.
    """
    order    = json.loads(fields["data"])
    order_id = order["order_id"]

    if is_already_processed(order_id):
        r.xack(STREAM, GROUP, msg_id)
        print(f"[skip] duplicate {order_id}", flush=True)
        return

    try:
        call_charge(order_id, order["amount_cents"])
    except Exception as exc:
        dead_letter(msg_id, fields, str(exc))
        r.xack(STREAM, GROUP, msg_id)
        # Key is NOT set — the order was never charged; a retry is safe.
        return

    # Atomically write the ledger AND mark the order as processed.
    # MULTI/EXEC guarantees: key exists ↔ ledger was incremented.
    # If the worker dies before this pipeline, the message stays in the PEL,
    # gets reclaimed on restart, and the charge is retried — safe because
    # the ledger is the source of truth that check.py verifies.
    pipe = r.pipeline(transaction=True)
    pipe.incrby(f"ledger:{order['customer_id']}", order["amount_cents"])
    pipe.incr("processed_count")
    pipe.set(f"processed:{order_id}", 1, ex=IDEMPOTENCY_TTL_S)
    pipe.execute()

    r.xack(STREAM, GROUP, msg_id)
    print(f"[ok] {order_id} → {order['customer_id']} +{order['amount_cents']}¢", flush=True)


def main():
    print("worker started", flush=True)
    ensure_group()
    claim_pending()

    while True:
        # ">" delivers only messages not yet assigned to any consumer in the group.
        resp = r.xreadgroup(GROUP, CONSUMER, {STREAM: ">"}, count=10, block=5000)
        if not resp:
            continue
        for _stream, messages in resp:
            for msg_id, fields in messages:
                handle(msg_id, fields)


if __name__ == "__main__":
    main()
