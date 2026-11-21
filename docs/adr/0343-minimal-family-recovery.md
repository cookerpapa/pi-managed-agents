# ADR-0343. Preserve Conversation ownership in minimal Session recovery

The #52 performance candidate projects a native Inbox at its own Conversation
address. One physical Session writer can update several Conversations in one
native batch. Admit the whole batch under its existing Run and writer, persist
each changed Inbox once at its final ordinal, and publish acknowledgements only
after the transaction. A bounded moving checkpoint may represent empty Inboxes
by document identity and Conversation only. A pending Inbox in any lane keeps
its reconstruction suffix pinned. The single-root continuation benchmark's
history head measures root entries only; child answers and compaction markers
cannot change its count, cutoff or family completion signal.

Run the cold handoff acceptance on isolated resources before changing serving
storage or the lease model. The Kubernetes release hold in #48 still applies.

The Inbox row keeps its native creation sequence. After a settled predecessor,
a cold Projector loads prior document routes through that predecessor's sequence
in its opening query. The settled Run head does not copy the child route list.
Native Submission request identities are scoped to the physical Session and
Conversation. The cloud API's public idempotency UUIDs are unchanged.

This decision covers the Inbox and root continuation head. A Projector checkpoint
is not yet a complete Worker checkpoint. The new storage still needs a strategy
for Conversation ownership/ancestry, outstanding Tasks and Submissions, mounted
documents and original Tool/host-call bindings at takeover.

## Retained-log read experiment

For #52, first try the existing settled root's PG history, the accepted native
commits of the interrupted Run up to its recoverable seal, and the original
protected reply position. The experiment adds no serving schema, checkpoint or
per-message projection write. The incoming actor uses a read-only PG connection.
Only normal, newly committed Harness transitions after recovery publish again.

The comparison is scoped to one interrupted writer epoch. A settled ordinary
root predecessor can supply its existing history head and messages. Verify
message bodies/positions, Inbox state, pending Tool identity and an accepted
native-sequence witness in PG before resuming. Reject an incomplete native
prefix or an ambiguous boundary. The reader never issues Tool effects or
commits the Projector group's Kafka offsets. Its temporary consumer setup
latency is part of the measurement. This experiment does not change the
runtime's normal Worker admission or establish a permanent replay service.

[Cube/Kafka handoff and retention results](../reports/retained-family-handoff-20261121.md).

## Isolated PG-only falsification test

Compare the existing candidate rows with a control that folds the same accepted
native changes in Projector memory and saves their current non-Entry state once
at the old writer's seal. Use the existing Entry and Inbox rows for their bodies.
Run the candidate-only read before giving the replacement access to the control
snapshot. Use two otherwise identical native queued submissions with different
opaque request IDs to test whether the original state is uniquely recoverable.
This study adds no serving migration. Its scratch snapshot, memory retention and
single-epoch readiness boundary are experimental, not a choice of production format.
