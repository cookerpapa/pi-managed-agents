# Producer reuse and the Raft boundary

Date: 2026-11-17. Research and proposal only. The owner clarified that the immediate
goal is removing **write-authorization queries from the Worker**, not removing its
PG claims, renewals or cold-context reads. No source, protocol, dependency or live
configuration changed. Current authorization checks remain until a replacement
handoff contract is designed and verified.

Subsequent owner clarification: an extra log-publication hop is acceptable, but
stale publication-service instances must be fenced. Do not introduce a separate
Raft service; evaluate Kafka's existing ownership/transaction mechanisms. KRaft
metadata replication is not itself a business-grant validation API.

This follows the [fencing-pattern survey](kafka-fencing-patterns-20261117.md).
Its Connect precedent remains valid; the new placement preference changes which
design candidates deserve evaluation, not the need to establish write authority.

## Separate three kinds of reuse

1. **Transport reuse:** share sockets, metadata or client execution resources while
   retaining independent logical producer identities. Whether the selected SDK
   can do this is a separate API/performance question; it does not remove business
   authorization or transaction initialization automatically.
2. **Producer reuse:** several Sessions publish through one transactional identity.
   Native producer fencing then applies to that shared identity, not an individual
   Session. Independent Session fencing must be enforced elsewhere.
3. **Longer owner lifetime:** retain a Session producer across otherwise separate
   Runs. Current normal completion releases the Session owner; keeping that producer
   would require an explicit ownership/lifecycle decision. A cached producer does
   not make a released grant reusable.

| Producer scope | Steady-state count for Session-log publication | Consequence |
| --- | --- | --- |
| Active physical Session, current design | Proportional to active Session writers | Native fencing isolates one Session; cold activation pays initialization |
| Worker process | Proportional to Workers | Native fencing affects every Session sharing that producer; PG Session authority does not become Kafka group authority |
| Owned log partition | Proportional to assigned partitions | Many Sessions share transport, but a log-admission component must validate each Session's grant and serialize its closure |
| One global writer | One effective writer | Broad queue/failure scope; no demonstrated requirement for this centralization |

The configured Session topic has 32 partitions. A partition-owned design could
reuse roughly one effective publication writer per partition in steady state,
instead of one per active Session. This is not a limit on all Kafka clients, and
handoff can temporarily overlap old/new client instances. Replies and other topics
retain their separate transports.

Kafka Streams' [KIP-447](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/103093950/KIP-447+Producer+scalability+for+exactly+once+semantics)
provides a precedent for amortizing producers while using consumer-group metadata
for fencing. It does not make a shared Worker producer understand independently
issued PG Session grants.

## Two candidates satisfying the clarified placement preference

### Narrow authorization port

Keep direct per-Session producers, but invoke an existing management-role API
for pre/post initialization authorization. That role queries PG and validates the
complete current grant. The Worker no longer embeds the authorization SQL.

This is a real dependency-boundary change but does not eliminate the system's
authorization reads or producer cold-start cost. It adds management RPC dependency
at startup. It is the small candidate if moving the SQL responsibility is the
only requirement; it should not be advertised as a throughput fix.

### Partition-owned log admission

Evaluate this candidate if both query placement and producer reuse matter:

```text
PG control authority -- validated grant/closure --> partition publication owner
Worker -- append(Session, grant, native batch) --> publication owner --> Kafka
Kafka --> existing Session Projector --> PG/history/execution
```

The publication owner validates business grants; Worker code sees a PiCloud-owned
append port. Transport producers outlive individual Runs. Closing one Session
changes that Session's admission, without intentionally fencing every Session on
the shared producer. The Worker still receives success only after the accepted
batch has its required Kafka durability acknowledgement.

PG remains the execution authority. Bootstrap/open may read PG on the service
side. A grant view cannot be treated as fresh merely because it is in memory or
updated by a subscription. The protocol must establish readiness and order for
grant opening, appends, revocation and sealing, including closure without any
successor epoch. A sealed epoch cannot reopen through a delayed control message.
No per-commit PG lease polling or second execution scheduler is proposed.

The repository already has useful primitives, currently **for control publication**:
[partition writers](../../packages/event-log/src/kafka-publication-writer.ts),
[Session publication queues](../../packages/runtime-core/src/session-publication-ingress.ts)
and [group-bound transaction commits](../../packages/event-log/src/kafka-partition-reader.ts).
They are not already a Worker log-admission service. Worker grant management,
native ACK/uncertainty handling and closure would need an explicit reviewed
contract rather than resurrecting removed ingress code as compatibility support.

Keeping the existing partition owner is the starting candidate. Separating a new
service that independently acquires those partitions would change the current
single-group boundary and needs its own decision. Co-locating admission with
projection also creates an availability dependency which the current direct path
avoids. Producer reuse must be compared against that dependency and extra hop.

### Fencing the publication service itself

Moving the fixed transactional ID from Session to partition is insufficient on
its own: a delayed old service initializer can still fence its replacement.
The current control publisher uses a fixed partition ID plus a group transaction
guard. The proposed Worker-log path must separately address both stale commits
and that availability interference.

The stronger candidate is a producer identity specific to the publication-owner
incarnation, combined with Kafka consumer-group fencing. Kafka Streams provides
the relevant combination: its
[producer identity includes process/thread identity](https://github.com/apache/kafka/blob/4.1.0/streams/src/main/java/org/apache/kafka/streams/processor/internals/ActiveTaskCreator.java#L111),
and its [transaction commit supplies consumer-group metadata](https://github.com/apache/kafka/blob/4.1.0/streams/src/main/java/org/apache/kafka/streams/processor/internals/StreamsProducer.java#L249).
Different producer IDs alone do not fence each other; the group-bound transaction
is essential. A late initializer then does not deliberately take over the new
instance's producer identity, while its obsolete group membership cannot authorize
a new transaction through the guard.

For example, A's partition assignment belongs to group generation 20; B takes
over in generation 21. A cannot turn a generation-20 request into valid generation-21
work just by reconnecting or creating a producer. Local request/assignment handles
must also be invalidated on revocation and never silently upgraded after rejoin.
Group-member validation does not replace the application's partition and Session
grant checks.

Already admitted transactions need a handoff barrier. A transaction whose group
check succeeded before revocation may complete afterward; the successor must
settle or wait out the prior transaction boundary and restore its accepted prefix
before becoming ready. It is inaccurate to promise that every old in-flight
commit instantly becomes impossible at a rebalance.
[KIP-447](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/103093950/KIP-447+Producer+scalability+for+exactly+once+semantics)
discusses this stable-offset/pending-transaction boundary.

Publication must not advance the Projector's effect-dispatch position merely to
pass the group check. The existing implementation submits its already confirmed
position under a commit lock. Real-broker tests must validate this usage, including
the client metadata captured during rebalance, successor startup, delayed producer
initialization and an ACK lost after commit. Current mocked reader/publication
tests establish local lifetime guards, not this proposed end-to-end protocol.

The required guarantee concerns accepted committed history. Aborted or uncommitted
bytes may reach brokers; downstream readers must use committed visibility. This
is broker transaction admission, not retrospective business-epoch filtering by
the Projector.

### OpenMA comparison

Inspected `openma-ai/open-managed-agents` at commit
`c60476ecf1c88afc9cd1ee2cf0ce11d5165c7322`, including its managed Node and Cloudflare
execution paths. This was source inspection, not a deployment or fault audit.

OpenMA has the same general stale-execution problem. Its
[execution authority ADR](https://github.com/openma-ai/open-managed-agents/blob/c60476ecf1c88afc9cd1ee2cf0ce11d5165c7322/docs/adr/0006-session-execution-authority.md)
uses a durable execution row with attempt, owner, generation and renewable expiry.
Its [execution host](https://github.com/openma-ai/open-managed-agents/blob/c60476ecf1c88afc9cd1ee2cf0ce11d5165c7322/packages/session-runtime/src/execution-host.ts)
aborts on renewal loss and performs a final renewal before settlement. Recovery
does not claim exactly-once arbitrary external effects.

The managed Node publication path uses
[conditional SQL projection](https://github.com/openma-ai/open-managed-agents/blob/c60476ecf1c88afc9cd1ee2cf0ce11d5165c7322/packages/managed-agents-adapters-sql/src/session-runtime-projection-sql-persistence.ts#L274):
event inserts and Session updates carry predicates over the expected revision,
execution state, attempt, owner, generation and lease expiry. The implementation
uses a revision guard and checked batches to reject/roll back rejected publication;
an obsolete execution returns `execution_fence_lost`. Thus the SQL acceptance
boundary can examine the business identity directly. It is not Kafka producer
initialization under another name.

The Cloudflare path hosts the Session in a Durable Object, but still uses the
execution authority: its
[local history wrapper](https://github.com/openma-ai/open-managed-agents/blob/c60476ecf1c88afc9cd1ee2cf0ce11d5165c7322/apps/agent/src/runtime/session-do.ts#L3801)
checks the current execution/attempt/generation and expiry before append, while
managed projection receives the fence explicitly. The
[shared lease reader](https://github.com/openma-ai/open-managed-agents/blob/c60476ecf1c88afc9cd1ee2cf0ce11d5165c7322/packages/session-runtime-sql/src/execution-lease.ts)
reads the authoritative SQL execution row. Durable Objects do not justify
describing all these checks as unnecessary, and D1/local-DO persistence still has
its own cross-boundary work.

The inspected built-in Session paths use SQL/DO event storage rather than the
PiCloud Kafka-first log. They therefore do not have the particular PG-grant to
Kafka-producer-epoch handoff. OpenMA also consults execution authority at several
side-effect/publication ports; it is not evidence for removing lease checks
everywhere or for lower database load. The reusable lesson is to enforce the
business fence where an event can actually be accepted or rejected. Borrowing its
SQL predicates wholesale would change our streaming/durability architecture.

## What Raft can solve

[Raft](https://raft.github.io/) replicates an ordered sequence of commands so a
state machine can remain consistent across replica failures. If the **same**
state machine owns current grant state and an authoritative conditional append,
it can process opening, append and closure under one agreed order. That is a
valid foundation for a replicated log service.

Merely storing the current grant in Raft does not atomically couple it to a later
Kafka RPC. A successful authorization read can be followed by revocation or a
process pause; Kafka still evaluates its own producer identity. An old service
leader also cannot safely publish arbitrary external effects just because it
used to lead a Raft group. These are deductions from the boundary between the
replicated state machine and the external Kafka write.

There are three materially different uses:

| Raft use | What it adds | What remains |
| --- | --- | --- |
| Replicate only grants or elect a service leader | Consistent coordination state | Kafka admission, stale publisher isolation and cross-system handoff |
| Persist accepted log batches in Raft, export to Kafka | Recoverable accepted work before export | A second log/durability boundary; export ordering, duplicate/uncertain delivery and ACK semantics |
| Use a Raft-backed conditional-append service as the authoritative Session log | Grant check and log acceptance can share a state machine | A replacement storage/transport architecture and its recovery/integration work |

[etcd guarantees](https://etcd.io/docs/v3.6/learning/api_guarantees/) are instructive:
KV transactions are strictly serializable, but Watches are asynchronous and not
linearizable; their delivery delay has no guaranteed bound. A watch-fed cache of
authorization is therefore not, by itself, proof of current authority. etcd also
documents ambiguous outcomes after connection loss. Adding a consensus service
does not remove the need to define uncertainty handling.

[Redpanda's architecture](https://docs.redpanda.com/streaming/current/get-started/architecture/)
uses Raft for partition replication, while its
[transaction interface](https://docs.redpanda.com/streaming/current/reference/rpk/rpk-cluster/rpk-cluster-txn/)
still fences using broker-managed producer IDs/epochs. This is a concrete example
of why changing replication algorithms does not give the Kafka API a conditional
write against a PG-issued business token. Kafka's own
[KRaft](https://kafka.apache.org/41/operations/kraft/) manages cluster metadata;
it does not expose a user-defined Session-grant state machine.

## Recommendation and decision gates

The owner also reaffirmed that interrupted user-facing output is part of shared
conversation history, especially when a future voice client lets the user interrupt
speech and correct it. Preserve the
[partial-output durability requirement](../STREAM_DURABILITY.md#why-partial-assistant-output-is-durable)
in every candidate. A lower initialization cost is not a reason to persist only
completed responses or treat spoken/displayed prefixes as disposable progress.

Prioritize a bounded partition-publication prototype and compare it with the
smaller authorization-port change. Do not add a Raft/etcd deployment just to move
two Worker queries. If a new authoritative replicated log is later justified,
evaluate established implementations before building an algorithm or broker fork.

Before implementation, record the exact authority/ACK/migration decision in an
ADR. Required comparisons include:

- Old grant arriving after closure, with and without a newer Worker.
- Revocation during grant opening, queued append or transaction commit.
- Publication-owner rebalance/restart, including an old initializer resuming late.
- Lost Kafka ACK versus a request definitely rejected before send; preserve
  uncertainty and never replay an unknown external effect to hide it.
- One Session closing while others share the partition, and a shared commit with
  an uncertain outcome; record the precise affected set.
- Cold 64/192 workload, a long active Session, producer/thread/FD/memory counts,
  added RPC latency and operation while the publication role is replaced.

No candidate has passed those new protocol/measurement gates in this study.
The owner preference is recorded; this note is not authorization to remove the
currently deployed checks before a replacement is agreed and validated.
