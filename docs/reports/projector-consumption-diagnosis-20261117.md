# Projector consumption diagnosis — November 17, 2026

The investigation confirms a consumer scheduling defect: the maintained reader
uses native `pause()`/`resume()` for every record to enforce partition ordering.
The pinned librdkafka implementation purges prefetched records on pause. Subsequent
resumption fetches those records again and can leave substantial gaps before the
next delivery. Repetition amplifies both transport work and partition waiting.

This is a diagnosis, not a deployed performance fix. The serving application
remains `projector-admission-20261117-r1`, source `692c1992`, schema 226 / Topic v22.
Both temporary observation overlays were removed and the original Deployment
template restored. [Aggregate evidence](projector-consumption-diagnosis-20261117.json)
contains timings/counters without conversation content or test identities.

## The specific behavior

`KafkaPartitionReader.#deliver()` pauses a partition before calling its handler
and resumes it in the handler's finalizer. The poller asks for one record at a
time. Native pause is being used to serialize application processing, although
it also changes transport fetch state.

The installed `@confluentinc/kafka-javascript` is 1.10.1; the deployed native library
reports librdkafka 2.15.1. Its `rd_kafka_toppar_pause_resume()` sets the next fetch
position to the last application-delivered position and purges that partition's
fetch queue. The fetcher also invalidates data across the changed version barrier.
The [upstream source](https://github.com/confluentinc/librdkafka/blob/master/src/rdkafka_partition.c)
contains this behavior, and [upstream issue #5343](https://github.com/confluentinc/librdkafka/issues/5343)
describes the same rapid pause/resume performance problem. At inspection that issue
was open; there is no verified upstream fix to claim as an immediate upgrade path.

For one partition containing A, B, C and D:

1. The client fetches A–D and delivers A.
2. Pi Managed Agents pauses the partition while processing A; native pause drops
   the prefetched B–D.
3. Processing finishes and the partition resumes, fetching B–D again.
4. Delivering B repeats the cycle for C–D.

Application processing still sees each record once and in order. The waste is in
prefetch/transport, not duplicate Tool execution. For 64 prefetched records per
partition, this pattern can fetch `64 + 63 + ... + 1 = 2,080` records instead of
64. Across 32 partitions that predicts 66,560 fetches for 2,048 application records.
The observed native counters closely match this pattern.

## Controlled RF3 experiment

All cases used one owned Topic with 32 partitions, 64 records per partition,
2 KiB padding, RF3 and minimum ISR 2. No PG, model or Tool execution was involved.
The sequence and total count were asserted. Setup/group assignment is excluded
from delivery time. Cases ran serially against the existing private brokers.

The untouched deployed reader took 10.46 seconds with a 10 ms asynchronous
handler; the maintained reproduction measured 10.23 seconds. With a zero-delay
handler it took 1.13–1.15 seconds, versus 0.31 seconds for a plain reader without
per-record pause. Thus waiting work substantially amplifies the problem.

The matched controlled cases keep the same client configuration and per-partition
serial handler. Only the per-record native pause policy changes. The buffered
control keeps this fixed experiment's already fetched records in memory; it is
not a production implementation or a rebalance/recovery acceptance claim.

| Handler delay | Per-record pause, delivery time | Buffered control, delivery time |
| --- | ---: | ---: |
| 10 ms | 6.52–11.62 s | 0.93–1.00 s |
| 50 ms | 63.17–63.19 s | 3.51–3.55 s |

In the maintained reproduction's 50 ms pair, native receive counts were 66,528
versus 2,048; average processing concurrency was 1.63 versus 28.92. Although all
32 partitions could become active, the paused case spent most of its time waiting
between deliveries. Its mean gap between one handler ending and the next starting
on that partition was about 951 ms. Periodic native counters are approximate and
can lag the final few records; they are not a loss or duplicate-processing count.

The configured `fetch.queue.backoff.ms` was already 5 ms and
`fetch.wait.max.ms` 25 ms. These results do not justify a claim that lowering the
default queue-backoff setting alone fixes the problem. Source inspection also
shows the broker I/O loop can idle for up to one second; exact attribution of
every gap to that internal wakeup path would require native tracing.

Reproduce the bounded experiment on the maintained idle deployment:

```sh
PI_CLOUD_READER_DIAGNOSTIC=1 node --import tsx \
  scripts/run-projector-reader-diagnostic.mjs /private/reader-diagnostic.json
```

The driver holds the release lock, refuses unrelated active Runs, uses a unique
Topic and confirms its removal. It never joins or advances the production Session
consumer group. The embedded buffered control has no production EOF, replay,
fencing or failure-handling contract and must not be installed as the replacement.

## Correlation with actual Runs

Two 64-concurrency / 192-Run simulated-provider repetitions added observation-only
source overlays to the Session runtime. They timed native delivery/handler/resume,
progress commits and Projector processing. An independent, read-only consumer
recorded when each record was already readable under `read_committed`, without
committing offsets. Existing Worker model-response timings supplied the other end.

The first repetition also sampled PG commit metadata; the second omitted this
sampling. Every Run completed, and both owned fixtures/configurations were cleaned.

| Mean interval per terminal | With PG observation | Without PG observation |
| --- | ---: | ---: |
| Harness response → terminal record creation | 67 ms | 69 ms |
| Record creation → independent reader observation | 250 ms | 240 ms |
| Already observed in Kafka → Projector delivery | 11.02 s | 14.71 s |
| Projector handling of the terminal | 486 ms | 496 ms |

The independent-reader interval includes that reader's own fetch/scheduling time;
it is not a broker append-ACK latency measurement. These instrumented workloads
ran at 4.15/3.00 Runs/s, slower than some clean samples. Their measurements locate
waiting and correlate repeated fetches; they are not treatment speedups or new
uninstrumented capacity baselines. The prior clean 4.57/5.93/8.50 Runs/s evidence
remains in the [delivery report](projector-admission-delivery-20261117.md).

The actual Session consumer received approximately 90,000/97,000 messages while
delivering approximately 3,200 records in each observed workload window: roughly
28–30 times as many receives. This excludes the independent reader's counters.
It directly connects the isolated prefetch defect to the actual execution path.

In the second trace, terminal waiting averaged 14.71 seconds. Approximately
7.42 seconds overlapped earlier handlers on the same partition, and 7.28 seconds
fell outside any record handler even though the terminal was already readable.
One observed terminal waited 50.48 seconds: 17.28 seconds overlapped earlier
handlers and 33.20 seconds were delivery gaps. These are partition intervals,
not CPU time. Earlier-handler cost includes PG/pool/application waits; it must
not all be relabelled as native fetch delay.

There is a second measured cost: progress commits serialize through the reader's
shared callback tail. Mean wait before the native commit callback began was
205–231 ms, while that callback itself averaged 21–23 ms. This belongs in the
next design review; its contribution is not additive to every Run, because many
partitions and requests overlap.

## Repair boundary to discuss

The next change should separate transport fetching from ordered record handling:

- Retain prefetched records in a bounded buffer and serialize handlers per
  partition; use native pause for actual sustained pressure or fault isolation.
- Preserve native EOF/transaction-gap evidence, generation checks, seek/retry,
  stopped-partition isolation and the distinction between fetched and processed
  offsets. Prefetching must never advance effect-dispatch or recovery authority.
- Keep confirmed dispatch before external Tool effects and all Projector fencing.
  Simply deleting `pause()` breaks the current single-in-flight-partition contract.
- Review batching/coalescing progress commits without delaying required effect
  admission or regressing offsets during control-publication transactions.

Adopt before build: the pinned Confluent KafkaJS API already implements a
per-partition message cache and concurrent handlers. Evaluate that public API
first, but do not bypass the processed EOF/position requirements documented in
ADR-0209. Its private `_getInternalClient()` is not an acceptable integration
contract. Reusing native APIs with a small bounded adapter remains an option if
the higher-level public API cannot preserve those requirements.

This issue does not require replacing Kafka/PG, adding a broker or changing Pi's
agent loop. The Projector metadata-cache review in #46 remains separate. No
optimized reader has been deployed, and the microbenchmark ratio is not a promise
of equivalent end-to-end throughput improvement.

## Restoration

Both original runtime templates were restored and diagnostic ConfigMaps removed.
All serving Pods were Ready, original machine intents and 393 sampled Workspace
files were preserved, and no owned load fixtures remained. The isolated Topics
were deleted and the maintained reproduction confirmed metadata absence.
All model requests in this investigation used the simulated provider.
