# ADR-0364. Direct committed queue hints to available segmented Workers

The #52 Session runtime owns one PostgreSQL notification listener for its queue and routes hints over the existing prototype Worker wake transport. A Worker advertises availability on joining and on freeing a slot. One queued Turn can wake one waiting Worker. A bounded central reconciliation tick covers missed notifications. PostgreSQL still decides readiness, Worker capacity, exclusive ownership and the fencing epoch at claim time.

Reuse the FIFO readiness router and reconnecting claim client from the [directed wake study](../reports/directed-worker-wake-20261119.md). Keep each segmented lease acquisition in its existing transaction. Limit buffered hints and discard them on router restart. A missed hint can delay a claim and has no effect on durable queue contents. Apply this topology to the candidate branch of the runtime.
