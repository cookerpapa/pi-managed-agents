# Fencing and Kafka publication: established patterns

Date: 2026-11-17. Research only; no runtime, dependency, ownership or deployment
change. The maintained implementation remains [ADR-0247](../adr/0247-session-work-and-tool-dispatch.md).
Kafka source was inspected at tag `4.1.0` and the corresponding current trunk
methods. The comparison uses official designs, implementation and documentation;
it is not a survey of market share or a claim that another framework proves our
protocol correct.

The owner subsequently clarified a preference to remove only the Worker's
write-authorization queries. The follow-up
[producer-reuse and Raft assessment](producer-reuse-and-raft-20261117.md) records
that scope and evaluates placement alternatives; no deployed check was removed.

## Main finding

External task ownership plus Kafka producer fencing is an established combination.
Kafka Connect provides a particularly close precedent for checking ownership
after transactional-producer initialization. Two different generations are not
automatically a design defect: one identifies permission to execute, the other
identifies a Kafka producer. The required link between them must be explicit.

The separate performance concern is producer granularity and lifecycle. Our
[measurements](../reports/terminal-history-query-repair-20261117.md) show material
startup cost when many cold Sessions activate together. That does not establish
that the PG checks themselves account for the cost, nor that restoring a log
gateway would be faster overall.

## 1. External source ownership: Kafka Connect

Kafka's own [ExactlyOnceWorkerSourceTask](https://github.com/apache/kafka/blob/4.1.0/connect/runtime/src/main/java/org/apache/kafka/connect/runtime/ExactlyOnceWorkerSourceTask.java#L131)
performs these calls around initialization, with an intervening stop check:

```java
preProducerCheck.run();
producer.initTransactions();
postProducerCheck.run();
```

The [DistributedHerder](https://github.com/apache/kafka/blob/4.1.0/connect/runtime/src/main/java/org/apache/kafka/connect/runtime/distributed/DistributedHerder.java#L2027)
supplies a prior-generation fencing operation as the pre-check. Its post-check
refreshes the configuration snapshot, then verifies both the task configuration
generation and the current assignment. It does not assume producer initialization
alone grants authority over an external source.

[KIP-618](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/153816406/KIP-618+Exactly-Once+Support+for+Source+Connectors),
in its delayed-worker startup scenario, describes an old assigned Worker resuming
after a newer task has started. The required post-initialization configuration
refresh rejects obsolete work. The source code also notes that late initialization
can fence a newer task. This is the same category of race as the existing
[PiCloud experiment](../reports/kafka-native-fencing-20261013.md).

**Application to Pi Managed Agents:** PG replaces Connect's task-generation and
assignment authority; our post-init query checks the current Session grant.
Connect's pre-check is not literally a PG query and its source-offset protocol
differs from native Pi Storage. The precedent supports the handoff pattern, not
equivalence of the implementations or exactly-once execution of shell commands.

## 2. Kafka input ownership: Kafka Streams

[KIP-447](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/103093950/KIP-447+Producer+scalability+for+exactly+once+semantics)
explicitly identifies the memory, thread, connection and batching costs of
per-task/per-input-partition producers. It extends transactional offset commits
with consumer-group metadata, allowing the coordinator to reject obsolete group
members and enabling producer reuse across tasks. Its ownership proof comes
from Kafka's consumer group, not an unrelated application's lease table.

Kafka 4.1.0's [ActiveTaskCreator](https://github.com/apache/kafka/blob/4.1.0/streams/src/main/java/org/apache/kafka/streams/processor/internals/ActiveTaskCreator.java#L103)
creates a thread-associated producer whose transaction ID includes application,
process and thread identity. [StreamsProducer](https://github.com/apache/kafka/blob/4.1.0/streams/src/main/java/org/apache/kafka/streams/processor/internals/StreamsProducer.java#L249)
passes consumer-group metadata with offsets before committing its transaction.

**Application to Pi Managed Agents:** our Worker receives work through PG claims,
not Kafka input-partition assignments. Sharing one producer across those Sessions
does not independently fence an obsolete Session grant. Borrowing the Projector's
membership for an unrelated Worker is not a valid substitute. The existing
Projector publication guard already uses group-bound transactions for its own
partition ownership. Moving Agent scheduling to consumer groups would change
placement, recovery and the authority boundary and requires a new decision.

## 3. Database-owned events: transactional outbox and Debezium

The [Debezium outbox pattern](https://debezium.io/blog/2019/02/19/reliable-microservices-data-exchange-with-the-outbox-pattern/)
commits the business update and an outgoing event in one database transaction.
CDC later publishes committed changes to Kafka. The maintained
[Outbox Event Router](https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html)
provides event identity and aggregate-based routing; it does not itself validate
an application's Worker fencing token.

**Application to Pi Managed Agents:** a PG row lock/conditional mutation can make
the execution-authority check and outgoing-event registration atomic. The relay
then transmits already accepted facts. We already use an Outbox for control
changes. Applying this to every native/streaming submission would add PG writes
and WAL traffic and change the Kafka-first acceptance contract. It would conflict
with the current choice to keep native live progress out of PG. Storing references
instead moves payload durability elsewhere; it does not eliminate that decision.

## 4. Framework-owned checkpoints: Flink KafkaSink

[Flink's KafkaSink](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/kafka/)
coordinates exactly-once Kafka transaction commits with checkpoints. Committed
visibility depends on checkpoint completion, and transaction timeout must account
for checkpoint/restart duration. Ownership and recovery are framework concerns,
not a free interpretation of a business epoch placed in a Kafka record.

**Application to Pi Managed Agents:** this is a mature coordinated-publication
pattern, but installing a second execution/checkpoint framework merely to publish
Pi Storage batches would duplicate authority and change the latency/ACK model.
It is useful evidence, not a recommended dependency addition.

## What this changes in our recommendation

The earlier conversational preference for restoring a log gateway was premature.
Direct publication with a startup authorization bridge has a strong official
precedent. Keep the existing pre/post authorization checks and ordered closure
while evaluating client initialization and producer lifetime costs.

There are two credible performance directions to compare, with unchanged safety
requirements:

| Candidate | Benefit to investigate | Cost or semantic work |
| --- | --- | --- |
| Keep Session-owned direct writers | No extra service hop; preserve current recovery and acknowledgement | Per-Session producer initialization, connections and client scheduling remain measurable costs |
| Reuse a bounded set of producers behind a log-admission service | Amortize transport setup; centralize business-grant validation | Restore an RPC dependency; prove grant freshness, append/seal ordering, lost ACK handling and stale service-instance isolation |

The second row is a design candidate inferred from these mechanisms, not a claim
that Kafka Connect, Streams or Debezium is a drop-in HTTP log gateway. It must
use one durable business authority and the existing ordered-control boundary,
not an unsynchronized in-memory epoch map or another scheduler.

Producer pooling without changing the fencing scope is not just a connection-pool
setting. Retaining a producer after a Run has released its Session lease likewise
needs an ownership/lifecycle design; it cannot silently reuse an old grant.

The decision criteria are measured startup/commit/tail latency, active producer
count, memory/FD/thread use, isolation when one Session is closed, delayed old
initializers, lost acknowledgements and the operation of unaffected Sessions
during component replacement. Compare those costs before changing architecture.

## Guarantee terminology

Kafka transactions may physically append records that remain uncommitted or are
later aborted. Preventing obsolete work from becoming accepted committed history
is different from proving no stale bytes ever reached a broker. Likewise, a record
key or ordinary payload field called `fencingToken` is not a conditional Kafka
write. These distinctions matter when evaluating a consumer-side filtering
alternative: it would change the current acknowledgement/admission contract.

No new experiments or live changes were made for this survey. Existing measured
initialization costs and the allocator-abort limitation remain open; the presence
of a mature protocol analogue does not close performance or recovery testing.
