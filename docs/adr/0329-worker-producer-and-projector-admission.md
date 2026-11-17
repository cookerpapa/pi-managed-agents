# ADR-0329: Shared Worker producer and Projector admission

Status: accepted for implementation on 2026-11-17; not yet deployed (#43).
Supersedes ADR-0247's per-Session Kafka producer identity and initialization
authorization, preserving PG ownership, native continuation and effect boundaries.

## Decision and adoption

Use one long-lived idempotent Confluent/librdkafka producer per Worker boot for
Session records. Each native commit and its effect references already fit in one
Kafka record. Ordinary Worker publication therefore needs no multi-record Kafka
transaction. Keep RF=3 topic configuration, required ISR acknowledgements,
per-Session ordering and immutable record identities. The SDK owns safe transport
retries; the application never resubmits an uncertain native batch.

Reuse the existing native client and Kafka delivery reports rather than adding a
gateway, producer pool, Raft service or broker patch. The Kafka source/Streams
survey and producer-lifetime comparison are recorded in the November 17 research.
Kafka key/partition remains the physical Session. A shared transport never shares
native state, sequence allocation, grants or leases across Sessions.

Kafka success means durable candidate publication. The sole Projector admits the
PG-authorized Session incarnation and valid ordered prefix before all effects:
native state, visible text, Tools, cancellation, work completion and resource
activity/probes. Closed writers cannot reopen merely by publishing a higher field
value. Cached open identities include complete scope; cold/rebalanced state reads
PG authority and immutable writer closure positions. Do not poll execution leases
per Tool or per text fragment. Existing PG projection transactions retain their
atomic materialization/identity safeguards.

Control closure uses the existing partition publication queue and group-guarded
transactional control producer. It no longer initializes/takes over a Session
producer. PG closure authorization and synchronous external routing revocation
still precede seal publication. Duplicate closure is idempotent. A replacement
waits for the seal and PG checkpoint; pre-seal valid output remains recoverable,
post-seal candidates cannot affect native positions. Accepted original Tool
replies and deadlines remain recoverable without redispatching effects.

Keep Worker claims/direct renewal/cold reads and monotonic local expiry. Remove
only writer-opening PG checks. Known local revocation stops new publication and
model admission. A definite SDK enqueue rejection affects that operation only.
An uncertain shared transport failure fails the producer, revokes its local
families and retires the Worker boot; Kubernetes starts a fresh boot. No uncertain
batch is replayed and no broken producer is silently reused. Normal Session closure
does not close the shared producer or stop unrelated families.

ADR-0328 exact operation confirmation must be active at cutover. Kafka publication
alone cannot acknowledge a human input. Normal model progress remains asynchronous
with respect to PG. Retain the partial-output and native live-suffix policies;
neither logs nor screenshots gain a second persistence authority.

## Matched cutover and acceptance

Drain and verify old writers, checkpoints and consumer recovery references before
switching all affected roles. Use a new code-owned log Topic version rather than
mixed admission semantics. Preserve canonical history, user accounts, credentials,
model/provider configuration and Workspace bytes. Remove the obsolete Session
transaction writer implementation and its protocol tests; replace them with
shared-transport and downstream admission tests, not compatibility branches.

Verify cold/warm publication, independent Session closure, old/new interleavings,
seal without successor, malformed identity, early/lost/late ACK, process suspension,
consumer reassignment, valid interrupted text and original Tool/image recovery.
An obsolete terminal/cancellation/activity record must have zero external effect.
Re-run #42 pressure/failure workloads serially with fake providers and measured
producer initialization, queueing, ACK, projection, PG/history and settlement
timings. Report the previous allocator abort separately unless reproduced and
explained; a passing new benchmark does not diagnose it.
