# AI Notes

This role is partly about *correcting* AI output, so we want to see how you use it — and
where your judgment overrode it. Keep this short.

## A prompt I used

```prompt
❯ /plan the coding considering the architecture decision record
```

This original planned way to deal with idempotency was 

### Idempotency

Before calling `/charge`, the worker sets `SET processed:{order_id} 1 NX`. If the
key already exists, the message is acknowledged and skipped — no charge is issued.
The state lives in Redis (same store as the stream) so no external coordination is
needed. `SET NX` is atomic, eliminating the check-then-act race that would otherwise
allow two concurrent workers to both pass the gate and double-charge.

## Something the AI got wrong or oversimplified — and how I caught it

The AI oversimplified the idepodency logic and we cought the bug during testing when the undercharging was failing. The idempotency key was set before the charge succeeds, so if the worker dies mid-retry the key already exists on restart, the message is skipped, and the ledger is never written. Fixing the ordering — set the key atomically with the ledger write, after the charge.

then update the ADR accordingly

### Idempotency


The worker checks `EXISTS processed:{order_id}` before processing. If present, the
message is ACKed and skipped — no charge is issued. After a successful charge, the
ledger write and the idempotency key are set together in a single `MULTI/EXEC`
pipeline, making them atomic: key exists ↔ ledger was incremented. Setting the key
only after the ledger write (not before the charge) is the critical invariant — it
prevents a crash between SET NX and the ledger write from silently losing an order
on restart.
