# ADR-0331: Bounded partition prefetch and coalesced progress

Status: implemented and deployed on 2026-11-18 (#47).

[Delivery and measurements](../reports/projector-prefetch-memory-delivery-20261118.md)
record the deployed reader, serial recovery acceptance and remaining throughput limits.

## Evidence and adoption

The November 17 consumer diagnosis reproduces native prefetch purging on every
pause: 2,048 records generate approximately 66,560 receives, with long delivery
gaps. Per-record pause must no longer implement partition serialization.

The pinned Confluent KafkaJS API has a per-partition cache/concurrent handlers,
but does not expose the processed EOF/transaction-gap evidence required by
ADR-0209. Its native-handle getter is explicitly private. Retain the pinned public
native API and existing small adapter; add bounded process-local buffering, not
a second broker, durable queue or scheduler. Do not patch librdkafka.

## Delivery and memory

The poller retains fetched records and each partition runs one handler at a time.
Independent partitions remain concurrent. Count and estimated-byte budgets cover
queued and active records, both per partition and in total. Native prefetch keeps
its own bound. A single newly returned record may exceed the soft byte threshold;
do not reject an otherwise valid large record or silently discard it.

Native pause applies only to explicit pause/failure, pending seek or high-water
pressure. Resume uses a lower watermark to avoid per-record oscillation. Pressure
pauses fetching, not processing of already buffered records. Queued records are
discardable only after generation/seek invalidation with a verified replay start.

EOF advances only after preceding queued and active records settle. Polls crossing
assignment or seek cannot install an EOF or skip a returned record: discard stale
delivery and rewind the verified start. Old completions cannot alter a successor's
queue, pause state, offsets or publication authority. Reader failure retires its
generation; native handles outlive every borrowed transaction and handler.

Asynchronous assignment may finish inside a poll whose generation is already
current but whose partition snapshot is still empty. Re-observe newly assigned
partitions from their verified start even if that poll returned only EOF and no
data. Dropping such an EOF without replay can leave an idle partition waiting
forever for a notification that only new data would otherwise retrigger. The
November 18 SQL investigation adds deterministic and RF3 coverage of this edge.

## Offset confirmation

Ordinary completed progress is coalesced by the existing periodic committer,
without holding each partition's handler open for a periodic checkpoint. Required
Tool dispatch still waits for its monotonic durable offset confirmation.

Offset requests acquire their partition's existing publication/commit guard before
joining a shared native commit batch. A partition blocked on a control transaction
must not hold healthy partitions. Compatible ready requests use one native offset
commit and exact callback coverage; no caller succeeds before confirmation.
Control transactions retain current-group fencing and cannot regress a committed
offset. Failed/uncertain confirmation admits no new external effect.

## Acceptance and release

Keep Kafka RF3/read_committed, PG ownership, Session identity, current wire format,
effect admission and native recovery unchanged. Cover buffer pressure, ordering,
seek, EOF with buffered records, overlapping assignment, stale completion,
partition failure isolation and publication disposal in source and real RF3 tests.
Deploy through the reviewed release plan, then run simulated-provider load and
failure checks serially. Compare actual reader receive amplification and Run
throughput; the diagnostic buffered control is not release acceptance.
