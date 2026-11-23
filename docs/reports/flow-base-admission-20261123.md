# Fork history with a fixed Session base

A Session can store its writable `flow_id` and the sealed `base_run_id` inherited at Fork or prune. A new root Run links to the latest local root, or to the base if it is the first in this flow. An ordinary admission inserts one Run and locks the Session to serialize concurrent submissions. The base stays fixed. An empty fork already protects its ancestry. This is a candidate for the serving integration.

## Run admission and recent reads

Three paired trials ran sequentially on fresh PG 17.6 containers limited to two CPUs and 768 MiB. Each trial used two schemas with the same UUIDs, 100,000 completed predecessors, 1,024 empty forks and a warmed index. Each scenario issued 1,024 operations per layout. Layouts alternated in counterbalanced blocks of 32. The latency includes client-to-PG round trips. WAL is the difference in the instance insert LSN across each block; only one schema was active. Fsync and synchronous commit were on. A checkpoint after seeding means the byte counts also include full-page images. They are workload measurements.

| Operation | Moving head P95, ms (trials 1 / 2 / 3) | Fixed base P95, ms (trials 1 / 2 / 3) | Fixed-base WAL saving per admission |
|---|---:|---:|---:|
| First Run in an empty fork | 4.89 / 4.60 / 4.27 | 4.07 / 3.92 / 4.11 | 8.9–9.2% |
| Next Run on a 100,000-Run flow | 4.30 / 4.10 / 4.45 | 4.18 / 4.98 / 4.91 | 4.0–5.6% |
| Eight clients submitting to one window | 91.74 / 92.79 / 90.79 | 28.85 / 29.23 / 30.10 | 11.9–19.2% |
| Eight clients submitting across eight windows | 8.01 / 7.13 / 8.01 | 8.66 / 6.83 / 7.83 | 25.0–26.8% |
| Resolve the tip of an empty fork | 0.85 / 0.64 / 0.92 | 1.16 / 0.78 / 1.22 | — |
| Resolve the tip of the long flow | 0.97 / 0.58 / 0.94 | 1.46 / 0.77 / 1.32 | — |
| Read 24 recent 1 KiB previews | 2.21 / 1.49 / 2.67 | 2.47 / 1.65 / 2.98 | — |
| Eight clients reading 24 previews | 3.70 / 2.66 / 3.85 | 56.64 / 2.60 / 57.98 | — |

PG reported one Run insert per admission under both layouts. Across all four write scenarios and three trials it counted 12,288 updates to the Session row with the moving head and zero with the fixed base. The one-window contention case has a different latency distribution as well as a different P95. The raw P50, P99 and operation rates are in the evidence file. The measurements cover admission and reads. Capacity of the Kafka Projector is a separate workload.

For each trial, the actual fixed-base tip plan did a backward scan of the existing unique tenant/flow/mailbox index and returned zero rows on the empty fork and one on the long flow. A full 24-preview page on the 100,000-Run corpus visited one Session, 25 Run rows (including the tip lookup) and 24 message rows. The head layout visited 24 Run rows.

## What deletion waits for

For the history `A → B → C → D → E` with another window forked at B, the source deletion transaction locks the Session, resolves E and enqueues E for cleanup in the same transaction that removes the window. The caller can return after commit. The collector locks E, checks the indexed references, deletes E and its messages, and continues through D and C. B remains while the other window references it. If the fork has appended G, the final deletion follows G, B and A. If G never existed, its fixed base points directly at B. Reversing the order of deletion also works: the source window still protects its current flow.

| Completed Runs with one 1 KiB contribution each | Foreground detach, head / base | Reclaim exclusive half, head / base | Reclaim last shared half, head / base | Transactions per half |
|---:|---:|---:|---:|---:|
| 1,000 | 2.87 / 3.04 ms | 0.11 / 0.12 s | 0.10 / 0.13 s | 2 |
| 10,000 | 3.02 / 3.03 ms | 1.11 / 1.24 s | 1.11 / 1.24 s | 20 |
| 100,000 | 3.92 / 2.88 ms | 15.51 / 13.46 s | 14.92 / 10.28 s | 196 |

Each deep detach is a single observation per layout and size. For 100 empty-fork detachments on the largest corpus, P95 was 4.27 ms for head and 4.28 ms for base. Each collection transaction handles at most 256 root Runs. Background work grows with the unreachable suffix and the amount of dependent data. FK cascades and message deletes in heavier histories can cost more. PG vacuum and filesystem space reclamation are outside these timings.

Both layouts passed the combined delegated-Run schema checks, source-first and fork-first deletion with empty and populated forks, actual lock contention between distinct admissions, idempotent retries, concurrent Fork and deletion, queue reordering, prune/continue, stale admission rejection and resumption of a partially completed cleanup job. A separate nested fixture crossed 1,000 flows to verify the oldest anchor.

## Concurrent page reads

With eight readers the fixed-base page showed a 56–58 ms P95 in two of the three trials. On an idle database, timed plans visited a bounded number of rows. A focused comparison sampled the Linux container CPU accounting and reran the same SQL using a named statement on each connection. See the recorded CPU usage, throttled periods, server planning/execution times and client P99 in the [evidence](flow-base-admission-20261123.json). The aggregate CPU counters are scoped to the whole container during each block.

In two diagnostic trials, the fixed-base query used 1.33–1.36 ms of database-container CPU per ordinary read. Named statements reduced this to 0.64 ms per read. Fixed-base P95 reached 1.79–1.82 ms with eight clients, while P99 varied from 2.63 to 19.78 ms. Both layouts recorded CPU throttling during the ordinary reads. CPU pressure and repeated statement preparation are consistent with the measured tail. Pinning the cause of every slow request would require per-request scheduling traces.

## Integration recommendation

Use the fixed base for the next isolated serving integration. It removes the per-admission Session update, allows an empty fork without a synthetic Run and keeps the user-visible delete short. Use the indexed latest-page query as a named statement on long-lived API connections and measure it again with real paging, different Session sizes and competing projection traffic. The release gate still includes public Fork/prune, cold handoff, retained attachments and ordered writer routing. [Experiment commands](../../scripts/experiments/minimal-session-storage/README.md#run-boundary-flow-and-directory-recovery) and [ADR-0357](../adr/0357-flow-base-admission-study.md).
