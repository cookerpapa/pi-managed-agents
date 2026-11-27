# Independent Session claim cost under load

The candidate grants physical leases in one PG transaction per selected batch. Each Session still gets an individual UUID, a strictly newer epoch and an expiry. The Worker locks its boot registration and accounts for outstanding PG leases before granting capacity. Within the transaction it now initializes the set of physical Sessions, advances eligible epochs and inserts leases, attaches the Turns, and removes their ready rows in four SQL round trips. PG enforces the unique unreleased owner and the Turn owner trigger; a missing management connection, exhausted epoch or incomplete grant rolls the batch back. The warm-history hint and candidate order are preserved. [Claim implementation](../../scripts/experiments/minimal-session-storage/independent-session-leases.mjs) · [Physical lease grant](../../packages/runtime-core/src/session-lease-coordinator.ts).

## Cost of the claim itself

Both Worker implementations used the same disposable PG instance with a two-CPU quota. Each arm claimed the same number of fresh, already queued Turns and waited for its confirmed authorities and input bootstrap. Queue population and settlement were outside the timer. No model, Kafka or Projector competed for the machine. After one warm-up, the test alternated arm and batch order over five samples per cell. SQL counts include transaction control and post-commit reads. [Reproducible claim study](../../scripts/experiments/minimal-session-storage/claim-only-study.mjs).

| Sessions | Baseline SQL | Candidate SQL | Baseline median | Candidate median |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 15 | 12 | 17.5 ms | 16.4 ms |
| 8 | 64 | 12 | 60.9 ms | 19.9 ms |
| 32 | 232 | 12 | 220.65 ms | 28.1 ms |

These counts are for successful, unhinted claims. Looking up a preferred warm Session before filling the remaining slots can add one selection query. The twelve calls consist of BEGIN and the durability setting, one candidate selection, one Worker lock, one capacity count, four batch writes, COMMIT and two post-commit reads. At 32 Sessions the old loop executed 224 serial per-Session SQL calls. On the candidate, all four batch writes finish before any of the claimed Sessions starts execution.

## Where a long claim spent time

The synthetic serving workload used 192 fresh Sessions, 64 HTTP clients, a Worker with 64 slots, four Kafka partitions, three brokers and one short simulated answer per Turn. It runs in separate images on one WSL host. With the opt-in timing probe, each SQL query is correlated with the parse, bind and execution durations emitted by its PG backend. Worker and PG run on the same host clock. The PG log has millisecond resolution, so request and return travel estimates are useful in aggregate. Those intervals include process scheduling, socket delivery and driver work. PG durations include time waiting inside PG. A 10 ms lock-wait logging threshold captured no reported heavyweight lock acquisition waits in the four profiled runs. This does not measure every possible short lock or internal PG wait. [Timing probe](../../scripts/experiments/minimal-session-storage/claim-timing-probe.mjs) · [Serving fixture](../../scripts/run-independent-session-serving-check.mjs).

The largest claim delay in one baseline run at limit 32 was **652 ms** for 32 Sessions. The following intervals partition that wall time. The PG span includes gaps between wire-protocol messages. The 239 ms of reported PG parse/bind/execute duration is a subset of the 258 ms span.

| Interval | Wall time |
| --- | ---: |
| Call entry to first SQL submission | 71 ms |
| Client SQL submission to first PG processing, summed | 109 ms |
| First through last PG processing for each query, summed | 258 ms |
| Last PG processing to resolved client query, summed | 199 ms |
| Time between query calls and remaining Worker bookkeeping | 15 ms |

In this **same batch**, grouped by business step, candidate selection, Worker admission and transaction setup took 104 ms of client SQL time. Initializing, checking and signing leases took 233 ms. Attaching Turns and dequeuing them took 222 ms. COMMIT took 4 ms, and the two post-commit reads took 3 ms. These are another breakdown of the same 566 ms spent in client query calls. A separate baseline repetition reached **1,070 ms** for 32, with **998 ms** in client query calls and **288 ms** of reported PG processing. The prior 1.16 s figure measured the spread between Turn start timestamps inside one transaction; the claim timer here includes checkout, commit and post-commit work.

The candidate can still exhibit long claims when the rest of the host is busy. In the limit-32 profile its slowest claim processed just **three** Sessions in **1,236 ms** and executed 12 SQL calls. It spent 356 ms before the first SQL and 181 ms after commit. PG pool checkout totaled 366 ms across the claim, overlapping those intervals. Within the SQL calls, approximately 268 ms preceded PG processing, the PG processing spans totaled 460 ms, and 136 ms elapsed on the return side. Worker event-loop active time during the claim was 1,028 ms, including other running Sessions. These figures locate the delay without treating event-loop activity as CPU time attributable to the claiming task.

## End-to-end observations

The table uses uninstrumented serving images. All arms sealed 192 Turns, received 192 upstream results, passed the active Stop test and ended with zero lag. Each trial starts a fresh disposable stack and a fresh workload. The two default-limit pairs were run in opposite version order. The plain harness samples raw Kafka lag every 250 ms. Terminal p99 starts at Worker terminal publication time and ends at PG settlement. It covers a later stage than the claim.

| Batch limit | Grant implementation | Answers/s | Peak sampled raw lag | Terminal to PG p99 |
| ---: | --- | --- | --- | --- |
| 8 | Baseline | 53.2, 17.8 | 39, 75 | 301, 672 ms |
| 8 | Set-based | 21.7, 35.0 | 185, 171 | 1,596, 894 ms |
| 32 | Baseline | 18.9 | 65 | 702 ms |
| 32 | Set-based | 52.5 | 165 | 447 ms |

The claim-only improvement is reproducible. Whole-service throughput varies substantially on this host, and the new path produced greater transient lag in these controls. The default batch limit remains eight in the isolated candidate. Keep the production cutover and Projector changes separate.

## Worker concurrency and PG CPU controls

These unprofiled candidate runs keep 64 submitting HTTP clients, max batch eight and zero artificial provider delay. The slot limit controls concurrent Session execution on the single Worker. The last two trials give the disposable PG four CPUs. The mean CPU column divides the cgroup CPU time by elapsed wall time. Throttled periods count quota periods with throttling; they are not milliseconds of claim delay.

| Worker slots | PG CPU quota | Mean PG cores | Throttled periods | Answers/s | Peak raw lag | Terminal to PG p99 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 16 | 2 | 1.39 | 11 | 35.4 | 51 | 298 ms |
| 64 | 2 | 1.91 | 39 | 35.0 | 171 | 894 ms |
| 64 | 2 | 1.87 | 20 | 53.5 | 156 | 555 ms |
| 64 | 4 | 2.31 | 10 | 23.5 | 84 | 791 ms |
| 64 | 4 | 2.45 | 11 | 24.1 | 118 | 1436 ms |

At 64 slots the two-CPU PG consumed 1.87–1.91 cores on average. Raising the PG quota reduced throttling and allowed PG to use more than two cores, but the two four-CPU runs did not deliver a throughput gain. At 16 slots, PG consumed fewer cores, and peak lag and terminal p99 were lower in the measured run. Time spent waiting for a slot rose relative to the faster 64-slot run. These short trials support retaining headroom for projection; they do not determine an optimal production slot count or isolate which other PG statements consumed the shared CPU. A further whole-service optimization needs a fixed input rate, sustained lag, and the separate claim and projection latency measurements.

## Correctness checks

Two eight-slot Workers raced for thirteen independent Sessions with different initial epochs. They claimed eight and five distinct leases and each advanced its own Session’s epoch by exactly one. Both an expired management connection and an exhausted epoch rolled back all grants, Turn changes and queue removals. Sixteen focused PG/Kafka and authority tests passed, including a successor waiting for the predecessor’s ordered seal, independent parent/child ownership, direct renewal, Stop, and refusal of obsolete writers. This slice changes no durable schema. [Concurrent and rollback checks](../../scripts/experiments/minimal-session-storage/segment-wake.integration.spec.mjs) · [Handoff suite](../../scripts/experiments/minimal-session-storage/independent-session-leases.integration.spec.mjs).

## Reproduction

Run the following in a candidate checkout with installed dependencies. Set IMAGE_TAG to the role image tag to measure. The optional baseline directory is another checkout at dd4c0e58 with the matching dependencies. The database, brokers, synthetic data and Worker images are isolated from the live service.

```sh
export TSX_TSCONFIG_PATH="$PWD/packages/control-plane/tsconfig.json"
node --import tsx scripts/experiments/minimal-session-storage/claim-only-study.mjs /path/to/baseline
PI_CLOUD_SEGMENT_CLAIM_BATCH=32 node --import tsx scripts/run-independent-session-serving-check.mjs "$IMAGE_TAG" --load --profile-claims
PI_CLOUD_SEGMENT_CLAIM_BATCH=8 node --import tsx scripts/run-independent-session-serving-check.mjs "$IMAGE_TAG" --load
```

For the resource controls, set PI_CLOUD_CHECK_WORKER_SLOTS and PI_CLOUD_CHECK_PG_CPUS on the synthetic load command. These options apply to the disposable stack.

The diagnostic entry is mounted only on the synthetic Worker. Normal claims do not enable duration logging or emit per-query metrics. Only timings, counts and workload parameters enter this report.
