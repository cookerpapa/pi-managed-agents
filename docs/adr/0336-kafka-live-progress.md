# ADR-0336. Project ordinary live progress from the Session log

Accepted for the controlled performance candidate in [#50](https://github.com/cookerpapa/pi-managed-agents/issues/50), following [ADR-0244](0244-kafka-backed-native-live-state.md), [ADR-0245](0245-native-commit-text-presentation.md) and [ADR-0334](0334-ordered-projection-round-trips.md).

## Decision

A writer's first uncheckpointed change establishes the existing transactional per-document replay floor. The ordered partition owner can then fold successive pure changes to those pending native live documents, validate the writer, sequence, document version and display destination in memory, and publish the existing SSE events. Kafka already has the complete original commit before this happens. A pure record with no external or stable work needs no PG operation. Cold identity or document state requires bootstrap. A change introducing an attachment, settling a document, advancing stable presentation, acknowledging human input or requesting a Tool takes the transactional path.

Only the current materialized live values and small integrity receipts for deferred sequences are held in the owner. At the next stable commit or writer handoff, insert the outstanding receipt metadata in bulk inside the guarded boundary transaction. This preserves native contiguous cold reads, equality checks on duplicate sequences and exact handoff verification with the existing schema. PG receives no deferred streaming bodies. The first replay position remains protected while another Session advances the partition. Active proof memory scales with the uncheckpointed suffix and is included in the owner memory estimate. Rebalance discards it and replays the protected log. A failed PG commit cannot discard an outstanding proof or expose an uncommitted snapshot.

On partition assignment keep the PG checkpoint as well as the earliest protected replay position. During rewind, a fully covered root commit with the same fingerprint and no outstanding native live document needing that sequence can reconstruct the view using a read-only query. An absent Session receipt still requires processing even below the partition checkpoint. A recovered complete message never requests another shell execution.

The Worker keeps Pi's Kafka-acknowledged sequence and the latest sequence requiring PG materialization separately. Overlay pressure/explicit flush waits for the latter and keeps the live value in memory. The native commit transport, Pi core, Kafka group, session fencing and effect dispatch use their existing contracts.

## Alternatives and acceptance

Combining a few SQL statements retains a transaction on every text fragment. A second consumer or text Topic duplicates ordering and creates another replay boundary. Individual PG receipts written at the next stable boundary retain the existing integrity and rebuild semantics without a new range ledger or new infrastructure. The original complete Kafka commit and the existing PG recovery floor are sufficient for the in-flight payload.

Verify the SQL count, lost/replayed commits, invalid identities, cold recovery, Stop, mixed inputs and Tools, and compare synchronized 192-client measurements using the same owned synthetic workload as #48. The deployment gate in #48 still applies.

[Controlled results](../reports/kafka-live-progress-20261118.md) include the original and repeated controls as well as post-change bursts.
