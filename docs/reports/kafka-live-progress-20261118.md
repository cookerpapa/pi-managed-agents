# Kafka-backed live progress and recovery

[Issue #50](https://github.com/cookerpapa/pi-managed-agents/issues/50) implements [ADR-0336](../adr/0336-kafka-live-progress.md) on the #48 performance branch. An admitted, already-pinned live-only change produces its normal web event without a PG transaction. The first outstanding change establishes a durable per-document replay reference. Semantic or mixed changes and handoff still pass the existing PG ownership and integrity checks.

## Recovery

At assignment, PG returns the partition materialization checkpoint and the earliest outstanding positions for open Runs or live documents. Take the earlier start required by this evidence and by the Kafka group. Seek inclusively, load needed native document checkpoints and apply each ordered suffix. For example, a document needing offset 120 can coexist with a partition checkpoint of 160. A materialized root message with matching native bytes, covered presentation and no document needing that position can be verified with a read-only query. A missing Session receipt still requires processing, even below the partition checkpoint.

An ordinary text response can generate several native document updates followed by a mixed commit containing the complete assistant Entry, Submission/Task changes and live settlement. Different Conversations and non-text work can interleave. A receipt records the original native sequence, ID watermark, commit fingerprint, write count and digests of touched current values. Deferred receipts are inserted together at the next stable boundary. This retains per-sequence integrity metadata in the existing PG table. The streaming bodies stay in Kafka. A cold Worker opens after complete handoff, and an active Worker can evict its ordinary record overlay while keeping Kafka-acknowledged live text.

## Synchronized short Runs

Each wave used 192 prepared SSE subscribers and separate Sessions, two Workers, one Projector, 32 Session partitions, 12 projection connections and a deterministic 4 KiB answer with zero configured delay. The experimental native reader returned up to 32 records per poll. No paid provider was used. The observer joined each provider completion to the successful PG COMMIT callback for that very Run. Queue time measures the interval between local record receipt and the start of its partition handler. Lag is the sampled sum of Kafka offset distances.

| Wave | First pins at eight calls | Later updates and pg calls per update | Native calls per Run | Model finish to PG P50 / P95 | Mean local terminal queue | Sampled peak lag | End lag |
|---|---:|---|---:|---:|---:|---:|---:|
| Original candidate, earlier burst | 192 | 373 at 7 | 88.60 | 10.83 s / 16.93 s | 9.80 s | 1827 | 0 |
| Kafka-backed live progress, first repetition | 192 | 189 at 0 | 75.67 | 2.74 s / 4.29 s | 2.32 s | 1285 | 0 |
| Kafka-backed live progress, second repetition | 192 | 131 at 0 | 75.57 | 2.15 s / 3.97 s | 1.83 s | 665 | 0 |
| Original candidate, paired control | 192 | 159 at 7 | 80.79 | 2.96 s / 4.97 s | 2.48 s | 1429 | 0 |
| Kafka-backed live progress, paired repeat | 192 | 178 at 0 | 75.81 | 3.22 s / 6.49 s | 2.89 s | 1251 | 0 |

In the paired measurements the baseline completed 15.10 Runs/s with a 4.97 s P95 and the candidate completed 12.23 Runs/s with a 6.49 s P95. Native SQL fell from 80.79 to 75.81 calls per Run. The three candidate bursts recorded 498 eligible subsequent updates and zero SQL calls in all 498. Each of their 576 initial pins still used the transactional path. All five bursts drained to zero lag. These results support the storage-boundary change and do not establish a throughput improvement. The first two candidate repetitions preceded the final recovery/attachment checks.

The term native calls counts queries inside durable commit handlers, including transaction commands. Live commit frequency changes with scheduling even for the same synthetic answer. All accepted waves completed 192/192 Runs. The paired control used pre-change source bfb5ee1e on the same host, with the same temporary Worker restart and measurement hooks as the paired candidate. The earlier control was materially slower than the later control. Exact hot-path call counts are reproducible. The ratio between any two latency measurements is workload- and host-dependent. No sustained fleet capacity follows from these bursts. [Content-free measurements](kafka-live-progress-20261118.json).

## Fault checks and disposition

Sixteen serial regression suites passed 217 tests on the final source. They include stable versus live Worker flush under pressure, an attachment-bearing update, the zero-database live path, conflicting and duplicate sequences, rollback, a read-only history rewind, an unfinished sibling Conversation, account eligibility, original Tool replies and partition retention.

The serial fault suite passed seven scenarios. After a Projector restart following visible partial output, four canonical answers matched the complete expected text exactly once, existing Worker leases survived, and all 32 idle partitions served initial snapshots. Two user Stops retained their measured 192-byte visible prefixes in the aborted native answer. Worker replacement recovered four original Tool calls, and the lost-reply scenario produced four physical writes with no duplicate. Provider rate-limit/disconnect and Stop with a late Tool result also passed. The paired candidate burst and the final storage regression additionally include the attachment-aware Worker flush check.

The controlled workload templates, provider route and test tenants were restored. The first instrumentation setup hit the client-side ConfigMap annotation limit before applying a workload. A preliminary 192/192 wave returned an unsuccessful driver status because its expected stage count was wrong. Both were corrected and these attempts are excluded from the accepted comparison. The normal release remains under the #48 rollout gate.

See the [capacity procedure](../RUNTIME_CAPACITY.md) and [SQL diagnostic procedure](../../scripts/experiments/projector-sql-20261118/README.md) to repeat an owned development measurement.
