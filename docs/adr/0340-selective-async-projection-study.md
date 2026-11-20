# ADR-0340. Experiment with transaction-local durability and stable recovery positions

This decision applies to the minimal storage performance branch. Keep PostgreSQL's
default synchronous commit and fsync enabled. Compare the same moving recovery
checkpoint under four combinations of single/ready-prefix transactions and
synchronous/selective acknowledgements. Use independent real Kafka and PG
fixtures, deterministic model replies and serial workloads.

In this experiment, a batch can use transaction-local asynchronous commit for
complete text answers, a copy of the already-admitted root input, initial empty
Inbox materialization, the first replay pin, and validated sandbox intents with an
original reply position and deadline. Persist the native task reference and the
projection cursor atomically. Kafka's group grant independently protects external
execution. Keep the original PG Run admission synchronous. Use synchronous
acknowledgement for human input mutation receipts, cancellation, native Tool
settlement, accounting evidence, ownership changes, and Run completion as a
conservative control. Kafka does not prescribe any of these PG commit policies.
A separate experiment also relaxes the synthetic normal Run completion, whose
only database effect here is reconstructible from the ordered Session log. Keep
Run admission/claims synchronous. After PG crashes, a scheduler-side read must
wait for the relevant partition to catch up before acting on a Run's state.
Exercise loss and replay of a previously acknowledged completion before
interpreting this comparison. Production-wide scheduler gating is outside the fixture.
A mixed batch uses the stronger contract. A sparse record starts immediately. Use one SQL call per
ready prefix. Have PG report the setting observed inside the mutation and check
the pool default separately.

Keep one current recovery position on each active Run. Advance it in the same
transaction as a stable mutation when the Projector can encode its complete
restorable state in small metadata. An idle, fully checkpointed Run pins no Kafka
records. Pin the first subsequent uncovered record once; other Runs may advance
the partition's PG position. Native Inbox content remains in its existing
storage. Partial live documents stay in Kafka. A pending Inbox, open generation,
unsettled child display or unsupported checkpoint shape retains the earlier
safe position. Retain the distinction between a Kafka position and a native
sequence. Restore the active states and the partition head from one PG snapshot.
The consumer group keeps its independent dispatch boundary. A rewind never
reissues a granted external command. Use an explicit seven-day retention on
isolated study topics, and surface a missing required suffix before delivery.
The serving cluster's current retention policy remains a separate decision.

On DB connection loss or a regressed durable head, discard the in-memory
assignment and reload the recovered PG state before accepting further effects.
Exercise an actual PG crash with Kafka ahead of PG, loss of a committed reply,
overlapping live conversations, expired retention, mixed batches and the windows
on both sides of a Kafka dispatch grant. Show the recovered content and compare
the number of records needed at cold start as history grows.

PostgreSQL supports a per-transaction setting, and recovery preserves a
consistent prefix of committed WAL. Kafka exposes the independent committed
consumer position and permits a seek to an earlier available offset.
[PostgreSQL asynchronous commit](https://www.postgresql.org/docs/17/wal-async-commit.html)
and [Kafka consumer positions](https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html).
