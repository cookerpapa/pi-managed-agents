# ADR-0377. Project ready native commits on the independent Session consumer

The independent Session image is the integration target for #52. Its partition
consumer may fold consecutive warm, admitted native commits in one private
draft and apply the draft in a single PostgreSQL transaction. Batch only
records already available from Kafka, up to sixteen. Do not create a timer to
fill it. Resolve each live event against its own intermediate native state.
Release events, update the recovery witness and acknowledge records in order
after successful admission. A revoked draft cannot deliver events or effects.

An unknown writer, a replay, an external operation, a control boundary or a
different physical Session scope breaks the fast path. The ordinary handler
continues to own those operations, including dispatch permission. A failed
batch retries through the consumer's existing single-record isolation. Batching
does not change the transaction durability or authorize a new effect.

Compare the same image entry, trace, offered load, PG budget and partition
count. Report visible call/result/answer records separately from native Kafka
frames, as well as end-of-input backlog and time to durable settlement.

Use the same serving roles for integration checks on the rest of the Session
path. The Worker restores its own reply mailbox and the scoped Kafka tool
catalog before admitting work. It imports definitions and read-only release
metadata. The independent delegation executor registers children and returns
the creation result. The retained Tool host owns HTTP MCP effects. Neither
executor transfers its implementation into the Worker. Projection reads do not
settle model charges. The API selects the current context through frozen Flow
ranges and computes usage from provider receipts using management ancestry.

The [reconciliation record](../reports/independent-session-reconciliation-20261128.md)
tracks earlier decisions, serving verification and outstanding storage conflicts.
