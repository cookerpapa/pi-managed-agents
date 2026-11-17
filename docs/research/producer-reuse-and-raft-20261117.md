# Producer reuse and the Raft boundary

Date: 2026-11-17. Research and proposal only. The owner clarified that the immediate
goal is removing **write-authorization queries from the Worker**, not removing its
PG claims, renewals or cold-context reads. No source, protocol, dependency or live
configuration changed. Current authorization checks remain until a replacement
handoff contract is designed and verified.

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
