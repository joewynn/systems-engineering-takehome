"""Order worker (PROTOTYPE).

This is the quick version a teammate threw together to demo the happy path. It reads
orders off the stream and charges the customer. It has not been run against real
conditions — duplicate deliveries, restarts, or the payments service misbehaving.

Your job: make this production-grade. See the README.
"""
import json
import os

import redis
import requests

REDIS_URL = os.environ["REDIS_URL"]
PAYMENTS_URL = os.environ["PAYMENTS_URL"]
ORDERS_STREAM = "orders"

r = redis.from_url(REDIS_URL, decode_responses=True)


def process(order):
    # Charge the customer, then record it in the ledger.
    resp = requests.post(
        f"{PAYMENTS_URL}/charge",
        json={"order_id": order["order_id"], "amount_cents": order["amount_cents"]},
    )
    resp.raise_for_status()

    r.incrby(f"ledger:{order['customer_id']}", order["amount_cents"])
    r.incr("processed_count")
    print(f"processed {order['order_id']} for {order['customer_id']}", flush=True)


def main():
    print("worker started", flush=True)
    last_id = "$"  # start from new messages
    while True:
        resp = r.xread({ORDERS_STREAM: last_id}, count=10, block=5000)
        if not resp:
            continue
        for _stream, messages in resp:
            for msg_id, fields in messages:
                last_id = msg_id
                order = json.loads(fields["data"])
                process(order)


if __name__ == "__main__":
    main()
