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

## Atomic recoverable seal in the candidate composition

A recoverable seal may publish a new-storage handoff inside the existing PG
transaction that seals the old writer and returns the same Run to the claim
queue. The replacement may observe either the unclaimed old state or the
committed handoff and a claimable Run. A checkpoint write failure aborts the
transition. No additional PG write is required for an ordinary native commit.
The steady serving composition retains its existing recovery store until cutover.

Exercise a cold Session Projector before the seal, including the loss of its
candidate native fold, route cache and Kafka assignment. Rebuild the admitted
native prefix using the existing partition consumer. If the native prefix is
incomplete the handoff fails closed. For the first fresh-Session slice the
checkpoint contains current non-Entry family records and display sequences.
PG messages and current Inboxes supply their own bodies and the original tool
reply reference remains in the Run. The Worker reads under the new lease;
sandbox target selection stays private to execution. Verify the original reply
after the sealed Session log has been rotated. Later epochs, arbitrary
point-in-time reads and the final storage cutover require separate coverage.

The candidate stores small native head-marker coordinates alongside the
current family records. On replacement, compute each active Conversation's
nearest visible head using its own ancestry cutoff. Read the kept suffix from
PG plus direct Entry references of outstanding native work. Reconstruct
current Inbox documents from their existing rows; a missing pinned entry or
original tool task/reply match rejects recovery. Preserve otherwise unallocated
native IDs and the commit head through Pi's public embedded storage methods.
The recovery seal forces synchronous commit for this transaction. Historical
read APIs and further handoffs of the same Run are outside this first slice.

## Repeated handoff in the same root Run

A replacement continues the native sequence of its immediate, PG-sealed predecessor.
When the candidate Projector folds its first successor commit, seed the fold from
the existing Run handoff and the predecessor's Inbox document identities. Apply
only the contiguous admitted successor suffix. A claimant can also die before
publishing: the ordered seal may carry forward a verified predecessor when no
output prefix exists for this incarnation. Verify tenant, physical Session, Run,
turn, adjacent lease epoch, original partition and earlier seal position. Reuse
the single Run field and its atomic seal/requeue transaction. Ordinary publication
and the tool reply channel keep their existing authorities and absolute deadlines.

## Handoff after a settled root Run

For a fresh writer starting the next Run, the checkpoint fold starts at the
same settled root as the Worker. Its existing PG header supplies the native
sequence, next ID and kept floor. Seed the surviving document identities and
configuration. No earlier Entry bodies are needed to start the Projector fold.
For a replacement within the same Run, require the matching ordered seal.
A cold Worker opens one sealed family in empty native storage. The bounded root
prefix belonging to earlier completed work and this Run's selected history come
from the same physical Session and sealed sequence, in one PG snapshot. Keep
ordinary admissions, tool-effect permissions, reply floors and deadlines intact.
