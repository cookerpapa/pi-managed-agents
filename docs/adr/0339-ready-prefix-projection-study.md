# ADR-0339. Study synchronous projection of a ready partition prefix

The performance branch implements this experiment. [Measured results](../reports/synchronous-ready-prefix-20261120.md).

Use the existing partition consumer and its bounded prefetch. Give an opt-in
projection handler up to 16 already available records from the same assignment,
partition and seek epoch, with a 256 KiB soft byte budget. One oversized valid
first record may proceed. Start a sparse record immediately. Never keep a PG
connection open to collect messages.

The experiment applies stable mutations and the existing kind of PG partition
checkpoint in one ordinary synchronous transaction. Keep each Pi submission's
original sequence and ordinal. Only the contiguous accepted prefix can advance.
Human confirmations, Run settlement and tool dispatch become actionable after
the corresponding commit. End the prefix at a confirmation or effect boundary.
Tool dispatch additionally requires the original monotonic consumer-group
confirmation. Rewinding a PG projection does not grant another dispatch.
Memory-only progress continues to require its protected replay suffix.

Compare the original projection, one record with checkpoint, and a bounded
ready prefix with the same checkpoint, SQL ownership checks and PG durability.
Verify same final content, low-load delivery and batch occupancy as well as
throughput, ACK latency, backlog and drainage. Exercise transaction rollback,
PG success before delivery, and replay across a committed effect boundary.
Only the prototype opts into the candidate. Moving the maintained native
recovery adapter requires separate review and acceptance.

This reuses Kafka's ordered partitions and PG's transactions and external
offset storage. Existing outbox and native replay rules still apply.
[Kafka consumer offsets](https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)
and [PostgreSQL transactions](https://www.postgresql.org/docs/17/tutorial-transactions.html)
describe the underlying contracts.
