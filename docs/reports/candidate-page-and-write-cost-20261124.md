# Candidate history pages and Projector writes

The #52 primary-only reader uses the existing tenant, physical Session and Conversation Entry index. The candidate Projector combines Run changes that target the same admitted owner in one native frame. Neither change alters Pi commits, Kafka order, the read protocol, the Worker lease model or the deployed storage. See the [decision](../adr/0358-candidate-projection-cost.md) and [measurements](candidate-page-and-write-cost-20261124.json).

## Reading tool history

Seek by Entry and retain the Run membership predicate on the resulting row. Skip the dependency seek when the assistant pointer is null. The same restriction applies to pending Tools and named child histories. Cross-Run, sibling-lane and missing proposal cases preserved the original reader's error or absent-input behavior. An inherited branch passed an HTTP result/argument comparison. The long-Run study compared 64 equivalent older pages per arm, alternating the earlier join and the installed reader. Both arms used the same two-CPU PostgreSQL primary.

| One Run with 1,024 completed Bash calls | Earlier lookup | Exact Entry |
|---|---:|---:|
| Mean PG CPU per older page under eight concurrent reads | 257.34 ms | 9.53 ms |
| Buffer hits to locate the page | 8,393 | 139 |
| Buffer hits for bodies and proposals | 16,512 | 640 |

The exact plans returned one indexed row per requested body or proposal. There were 64 body seeks and 32 proposal seeks on the sampled body query. The final predicate kept a valid Entry in a different Run from exposing its parameters. The separate delegated reader was regression-tested across invocations and sibling lanes. Its CPU was not measured separately.

## What can be removed from the write path

- A native message with a matching display boundary can use the already-required conditional Run UPDATE as its admission proof. The immutable INSERT consumes the row returned by this UPDATE. Missing or different owners cannot supply input to the INSERT. An immutable-content conflict rolls back coverage and the partition position with the message.
- A Tool execution or settlement with matching root display coverage can update its waiter and coverage together, after the existing Tool admission check. Child display, unmatched scope, other frame shapes and cancellation retain their own rules. A result with a waiter settlement takes this path in preference to the message/display path.
- In the matched 64-Run tape, these two changes saved 2,560 Run row updates, from 12,096 to 9,536. All 1,280 dispatches and results retained their wait state semantics. The standalone message guard was replaced by the conditional display UPDATE 1,472 times. Other frames still verify ownership.
- An isolated cursor-only SQL variant preserved a valid pre-seal write, suppressed a delayed already-covered transaction and rejected a transaction spanning the changed partition head. The live fencing view and a freshly bootstrapped view rejected the old writer. Injecting an old positive admission at the **current** offset admitted a wrong-writer message at the SQL boundary in the cursor-only variant. This injects stale admission deliberately; it does not demonstrate that the normal Kafka consumer handed a post-seal record to a revoked owner. A position by itself cannot identify the writer. The combined variant retains a transaction-local identity proof.
- A separate memory-only waiter variant successfully reconstructed the original mailbox position, operation and deadline from retained native records after discarding the Worker/Projector state. Writing the recovered wait references and using the real seal operation in one transaction survived a rollback and supplied the successor's real cold-read interface. Before sealing, however, the actual page reader reported zero pending tools and the actual retention reader returned zero reply floors while Bash was active. The original resource span reference also differed after replay. Both variants refused replay when the required offset preceded the broker low watermark. The test advanced the watermark rather than waiting seven days. No real broker reply was deleted to produce this finding. The candidate keeps durable pending references.

## Load and accumulation

The disposable primary had two CPU equivalents, 768 MiB and a 12-connection projection pool. Three Kafka brokers replicated the same fixed Pi tape for each arm of a suite. There were 32 partitions, 64 Runs, 20 tool calls per Run, 11,904 raw and 2,624 visible records. The admission/cancellation boundary and Kafka commit before external dispatch remained in place. One process simulated a receiver for each target with a 50 ms asynchronous acknowledgement timer. The recorded tool results were offered independently of that timer; these are Projector measurements, not interactive model or VM throughput. All trials verified all dispatches, acknowledgements, results and final PG partition positions.

One real Chrome client watched one live Run through the Session API. Each trial measured 20 result latencies from offering the result to the Kafka producer until its unique text was found inside the chat viewport in an animation frame. The fixture supplied a retained Run with an actual SSE subscription and published the admitted presentation events to the Session live service. Its local observation relay is included. With 20 samples, nearest-rank p99 is the maximum. The values are useful for a paired test, not a fleet percentile.

The final confirmation included a warm-up and two ordered, reversed passes per offered rate. Each history-read arm used the baseline writer plus one serial reader targeting two older pages/s. Slow responses reduce that reader's achieved request rate. Other arms had no concurrent older-page load. The broker lag poll sampled actual high watermarks relative to processed offsets about every 400 ms and converted them to visible record counts with the tape's per-partition positions. All arms drained after the finite input stopped.

| Target visible/s | Write/read path | Aggregate visible/s | Result-to-viewport p99, ms by pass | Peak sampled visible backlog by pass |
|---:|---|---:|---:|---:|
| 475 | Baseline writer | 225 | 6,210 / 5,447 | 1,683 / 1,733 |
| 475 | Combined writer | 344 | 4,998 / 223 | 1,396 / 99 |
| 475 | Baseline with earlier page query | 304 | 7,487 / 1,335 | 1,775 / 624 |
| 475 | Baseline with exact page query | 474 | 389 / 184 | 129 / 107 |
| 1,200 | Baseline writer | 381 | 6,758 / 2,201 | 2,265 / 1,454 |
| 1,200 | Combined writer | 372 | 6,488 / 2,483 | 2,215 / 2,044 |
| 1,200 | Baseline with earlier page query | 308 | 9,739 / 2,944 | 2,502 / 1,669 |
| 1,200 | Baseline with exact page query | 340 | 8,604 / 2,426 | 2,454 / 1,501 |

Aggregate rates divide total visible records by total processing time. Reading the old page took 842 / 304 ms on average in the 475 target arms; the new page took 21 / 22 ms. Write throughput varied materially even with identical code. For the 1,200 baseline pair, mean time holding a projection connection changed from 13.7 to 6.0 ms and PG CPU consumed for the same tape changed from 17.5 to 7.8 CPU-seconds. The 2-CPU quota reported more throttling in the slow pass. Mean Kafka dispatch-grant latency was about 24 ms in both, and the simulated receiver stayed near 50 ms. These measurements localize substantial variation to the database path but do not isolate the underlying host cause. The results establish fewer Run updates and cheaper point reads. They do not establish a stable increase in maximum write throughput or the ability to sustain 1,200 visible records/s.

## Reproduction

Run these serially from the experiment checkout. The browser fixture uses the Web build in that checkout.

```sh
npm run build --workspace @pi-cloud/web-ui
export TSX_TSCONFIG_PATH=packages/control-plane/tsconfig.json
node scripts/run-with-test-postgres.mjs -- node --import tsx scripts/experiments/minimal-session-storage/scroll-demand-study.mjs --long-plans
node --import tsx scripts/experiments/minimal-session-storage/write-load.mjs --faults-only
node --import tsx scripts/experiments/minimal-session-storage/write-load.mjs
node --import tsx scripts/experiments/minimal-session-storage/write-load.mjs --confirm
npm run test:transport
```
