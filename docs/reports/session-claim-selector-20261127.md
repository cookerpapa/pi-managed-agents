# What the independent Session Worker reads when claiming work

Issue #52’s candidate Worker selects eligible Turns, grants their leases in the same transaction, then confirms authority and bootstraps its inbox. These are separate operations. This study measures the first SELECT, the Worker’s local connection checkout and the end-to-end response under controlled arrival rates. All measurements used isolated synthetic data. [Selector](../../scripts/experiments/minimal-session-storage/independent-session-leases.mjs) · [Previous lease measurements](session-claim-cost-20261127.md).

The [subsequent candidate integration](session-claim-selector-integration-20261127.md) implements both measured changes. The comparisons below describe the preceding revision.

## Why the SELECT has several joins

The FIFO path starts from a ready Session, finds its writable Flow and a queued Turn, checks that an unreleased physical owner or an earlier unfinished Turn cannot be bypassed, and locks the Session and Turn with SKIP LOCKED. A cache-holder hint also validates the prior Turn’s history version. The query returns Turn settings, prior display position and Workspace grant, and the first eligible input ID and presence of input. It does not scan the transcript body table. Boot registration, remaining Worker capacity and the grant itself are checked later in the transaction.

For example, to start Turn 4,097, it is sufficient to rule out an **unfinished** predecessor. The existing sequence index can find earlier positions, but depending on the plan PG still reads each earlier Turn to test whether it has settled. The study evaluates a partial index on tenant, Flow and mailbox position containing only nonterminal Turns and Turns whose seal has yet to be projected. A queued Turn is itself covered by that predicate. The index changes no eligibility rule. The same SQL runs in both arms. [Index tested only in the fixture](https://github.com/cookerpapa/pi-managed-agents/blob/881cd3f1/scripts/experiments/minimal-session-storage/claim-selector-index.sql).

## What the plans actually read

The exact selector returned the same IDs in the same order before and after the index. An eight-row claim took 0.85 ms of PG execution on 192 fresh Sessions after explicit ANALYZE, plus 1.36 ms of planning. Before explicit ANALYZE on the same fixture it took 10.0 ms of execution. PG selected a join order starting at queued Turns and filtered other Sessions repeatedly. Statistics used by the planner matter even with the same SQL and available indexes. With the first eight Sessions locked by another transaction, the analyzed selector skipped them and returned eight different Sessions in 1.08 ms.

Nine Sessions with 4,096 completed Turns each and one queued Turn each reproduced a different cost. The preceding-Turn index scan discarded 4,096 rows on each of nine loops to prove that none blocked admission. The partial index reduced execution from 11.23 to 0.68 ms on exactly the same data. A targeted warm claim dropped from 1.35 to 0.48 ms. When a completed predecessor acquired an unconfirmed seal, both arms returned zero rows for the target. Projecting its seal made it claimable again.

Adding 4,096 newly queued Sessions exposed an independent issue in the SELECT list. PG chose a sequential scan over **41,160** input rows to answer the projected has-input boolean, even though only eight Turn results were returned. The alternate scalar subquery uses the existing per-Turn input index for an exact lookup, preserving true for withdrawn-only input and false for an empty mailbox. The first eligible input ID can be null in both cases. Direct comparisons confirmed identical results, including this edge. These are warm-buffer PG execution medians, excluding planning, lock grant, network and post-commit reads. Shared hits count buffer accesses and can visit the same page many times.

| Same 4,105 ready Sessions, return eight | PG execution | Shared buffer hits | Physical reads |
| --- | ---: | ---: | ---: |
| Current SELECT and indexes | 36.03 ms | 41,738 | 0 |
| Point lookup for the input boolean only | 20.48 ms | 40,595 | 0 |
| Partial index for unfinished Turns only | 10.67 ms | 1,594 | 0 |
| Both | 0.79 ms | 451 | 0 |

On a single warm connection the named and unnamed SELECTs took 3.53 and 3.77 ms at 192 fresh Sessions; at 4,105 ready Sessions with completed history they took 33.11 and 32.71 ms with the original indexes. PG counted 18 custom plans and zero generic plans for each named statement in these arms. Reusing the parsed statement can still require planning at Bind. [Plan fixture and comparison SQL](../../scripts/experiments/minimal-session-storage/claim-selector-study.mjs).

## Live query timing and pool contention

In a 192-Turn burst at the default batch limit of eight, the median candidate SELECT took 6.0 ms as measured by the Worker (p95 33.4 ms). Its median PG Parse, Bind and Execute times were 0.34, 2.63 and 1.16 ms respectively. The percentiles of different phases need not belong to the same query. The slowest complete claim took 244 ms. Its SELECT took 52.6 ms at the client, including 36.3 ms Parse, 13.2 ms Bind and 1.0 ms Execute on PG. Parse and Bind durations are elapsed time on the server and include scheduling delays.

With a batch cap of 32, the slowest complete claim took 1,047 ms in this profile. It spent 432 ms obtaining its initial connection: the Worker pool had reached its limit of 12, with zero idle connections and 16 requests already waiting. The selector then took 279 ms at the client. PG reported 77 ms Parse, 192 ms Bind and 7.3 ms Execute. No heavyweight lock acquisition over the probe’s 10 ms reporting threshold was reported for that SELECT. The durations are from the same claim. In the cap-eight run, the longest checkout (69 ms) began with only two connections in the pool and zero idle, so a slow checkout alone does not establish pool saturation. A different, full-pool checkout (53 ms) saw four waiting requests. The last classified work on seven held clients was history and on two was inbox. The API and Projector have separate client pools and share the PG server.

A second burst enabled PG's per-backend resource accounting only for the selector. In its slowest SELECT, Bind took 90.7 ms of elapsed time and approximately 13.0 ms of process CPU (5.6 ms user, 7.3 ms system). Execute took about 1.1 ms of CPU. PG used an average of 1.78 of its two allocated CPU cores, and the cgroup was throttled in 23 quota periods during the run. Those are instance-wide figures. They cannot identify which scheduling or wait event accounts for every remaining millisecond of this one Bind. In this profile a 70 ms transaction checkout found all 12 connections held by history loads and 12 earlier requests waiting. A post-commit checkout also encountered all 12 held by inbox operations.

The opt-in named-statement variant parsed 11 of the 39 SELECTs made by successful claims in one burst. The other 28 used an existing statement on their respective connections. Pooling and PG’s choice of custom or generic plan both matter. This trial did not establish an end-to-end throughput improvement. [Worker-only diagnostic](../../scripts/experiments/minimal-session-storage/claim-timing-probe.mjs).

## Does the lag grow at a fixed input rate?

Each trial created its Sessions before timing began, then launched one fresh, single-answer Turn per scheduled arrival using at most 64 HTTP clients. At 20/s it submitted 400 Turns over 20 seconds; at 60/s it submitted 1,200. One 64-slot Worker, a two-CPU PG, one Projector and four RF3 partitions served each trial. The upstream added zero artificial delay. The driver records launch lateness, admitted inputs and per-partition lag summed at 250 ms intervals. All trials completed the expected number of canonical answers, reached zero final Kafka lag and passed the subsequent Stop check. Raw lag counts all Kafka records, not just visible answers. Drain time ends at the harness's next observed settlement check (200 ms polling interval).

| Target/s | Selector and indexes | Observed admissions/s | Complete answers/s incl. drain | Peak raw lag | Drain after last admission | Terminal to PG p99 |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 20 | Current | 20.0 | 19.8 | 3 | 0.21 s | 51 ms |
| 20 | Unfinished index | 20.0 | 19.8 | 22 | 0.22 s | 308 ms |
| 60 | Current | 59.8 | 29.4 | 452 | 20.79 s | 5,208 ms |
| 60 | Unfinished index | 59.8 | 26.0 | 443 | 26.08 s | 3,695 ms |
| 60 | Current, repeat | 60.0 | 59.4 | 147 | 0.22 s | 776 ms |
| 60 | Both, repeat | 60.0 | 58.8 | 121 | 0.42 s | 563 ms |

In the first current-selector 60/s trial, sampled raw lag was 8, 395, 430 and 424 near seconds 5, 10, 15 and 20. Admission-to-Turn-start p95 reached 18.4 s, which includes waiting for a Worker slot. In the current-selector repeat, the respective lag values were 131, 18, 7 and 7. At the end of the input window PG timestamps put 0 Turns waiting for a claim, 8 from grant to Worker terminal, 1 terminal awaiting PG and 1,191 settled. The repeat with both changes had 0, 5, 7 and 1,188 respectively. Both repeats used the same workload; the combined arm substituted only the boolean expression in the otherwise identical Worker image and added the disposable index. The two slow and two fast runs show why a short, single-host throughput figure cannot certify steady capacity. The selection improvements remove measured unnecessary reads for long-lived Sessions. The fresh-Session mixed serving workload supplies no reliable overall throughput multiplier.

## Recommendation

Keep the existing eligibility checks and independent physical leases. The targeted boolean expression and unfinished-Turn index deserve a candidate change for long histories. Before installing the index on an existing serving database, measure its write cost under repeated Turn state changes and plan installation and rollback. The named statement provided no consistent improvement. Pool sizes and production capacity are separate decisions: the experiment verified actual Worker pool waits and non-CPU time on the PG backend, but increasing connection counts could also add PG contention.

## Reproduction

The plan study captures the SQL and parameters emitted by the current Worker. It registers synthetic Sessions and real Turn settings on a two-CPU disposable PG, creates history and mailbox rows, and runs EXPLAIN ANALYZE with buffers and node timing disabled. The initial run warms each plan test and the next five supply the medians. The lock check holds eight candidate rows using an independent connection. The named-statement test alternates named and unnamed execution on one connection and reports PG’s actual plan reuse counters. Normal service images receive no instrumentation.

```sh
export TSX_TSCONFIG_PATH="$PWD/packages/control-plane/tsconfig.json"
node --import tsx scripts/experiments/minimal-session-storage/claim-selector-study.mjs
PI_CLOUD_SEGMENT_CLAIM_BATCH=32 node --import tsx scripts/run-independent-session-serving-check.mjs "$IMAGE_TAG" --load --profile-claims --selector-cpu
node --import tsx scripts/run-independent-session-serving-check.mjs "$IMAGE_TAG" --load --profile-claims --prepare-selector
PI_CLOUD_CHECK_ARRIVALS_PER_SECOND=20 PI_CLOUD_CHECK_TURNS=400 node --import tsx scripts/run-independent-session-serving-check.mjs "$IMAGE_TAG" --load
```

Add --index-unsettled to a synthetic load run to install the experimental index on its disposable instance. Use the segmented role images from the previous claim gate. The test fixture removes its databases, brokers and synthetic users after each trial.
