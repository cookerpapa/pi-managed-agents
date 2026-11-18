# Projector SQL cost and idle startup — November 18, 2026

The remaining short-Run limit is not explained by a twelve-connection pool alone.
The projection path performs approximately **155–159 pg query calls per short
Run**, including transaction commands. Increasing the pool to thirty-two removes
acquisition waiting but does not improve the observed throughput: time moves into
the query call-to-callback interval. Main-thread samples show substantial database
protocol send/receive work, while PG has real but more limited server-side costs.

This investigation also found an idle-partition EOF race that can prevent a new
web stream from obtaining its initial snapshot. Its correction is deployed as
`projector-idle-eof-20261118-r1`, application `cd1624b4`, with matched live acceptance.
SQL batching/restructuring is a proposed next slice, not an implemented change.

[Aggregate evidence](projector-sql-diagnosis-20261118.json) excludes test identities,
query parameters, model content and credentials. The baseline is
`prefetch-cache-20261118-r1`, application `4ce1bddc`, schema 226 / Session Topic v22.
All temporary observation mounts and pool changes were restored.

## Boundaries and measurements

The host remains 24 GiB WSL with two Workers, one Projector, thirty-two Session
partitions and PostgreSQL 17.6. Each workload offers 192 fresh Runs at concurrency
64, with one 4 KiB simulated answer and zero provider delay. No external model
request is involved. These are instrumented comparisons, not new clean production
capacity claims.

Public pg Pool/Client wrappers measure acquisition and query intervals inside each
projection. They preserve Promise/callback results and exceptions and never
record parameters or returned bodies. Overlapping intervals are unioned when
partitioning wall time. PG activity is read at 100 ms intervals. Optional V8
sampling opens no Inspector network port. The observer is bounded and temporary;
it is not another telemetry pipeline or native log producer.

| Valid observed case | Completed Runs/s | Query calls | Mean record projection | Waiting for pool | Query call-to-callback | Other |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 12 connections + V8 sampling | 7.00 | 30,316 | 198.4 ms | 80.2 ms | 116.0 ms | 2.1 ms |
| 32 connections + V8 sampling | 6.49 | 30,576 | 163.3 ms | 0.2 ms | 160.9 ms | 2.1 ms |
| 12 connections + server duration logging | 7.37 | 29,786 | 185.5 ms | 72.3 ms | 111.1 ms | 2.1 ms |

Every admitted Run in these three cases completed, with no SSE setup failure.
Native streaming commit grouping varies with scheduling, hence the small change
in statement/record counts. The pool comparison does not prove twelve is optimal;
it disproves treating its waiting time as automatically recoverable throughput.

Across these cases a Run caused approximately twelve to thirteen timed records
and nine to ten explicit PG transactions. BEGIN, COMMIT and per-transaction timeout
configuration are included in the query count. Each native commit also reads
authority/native-head/current-state metadata, applies retained/current records,
maintains history/presentation references and advances recovery positions.
The number is not a count of 159 distinct business decisions, nor evidence that
every streamed text fragment is stored as PG message history.

## Server work versus client waiting

In the duration-observed case, query call-to-callback intervals accumulated to
260.4 seconds across concurrent work. PostgreSQL recorded approximately 96.6
seconds across the same projection-pool PIDs and workload window, including a few
snapshot metadata queries. These are accumulated concurrent intervals, not the
26-second elapsed workload time.

PG's Parse, Bind and Execute duration events are separately summed. The total is
not a perfect one-to-one subtraction from client intervals: millisecond timestamp
matching leaves ambiguous events, excluded from per-query attribution. Nevertheless,
server execution alone does not account for all the client wait. The remainder
includes protocol encoding, socket/network work, callback scheduling and result
processing; it must not all be called disk or network latency.

Read-only activity samples frequently found projection connections in `idle in
transaction / ClientRead`: the backend was waiting for its client. There was no
sampled broad lock pileup. Idle same-Pod testing of a warmed prepared scalar
query averaged 0.347 ms over 500 calls, much less than loaded call intervals; this
is an idle transport control, not a substitute for real native writes.

For the twelve-connection V8 case, about 8% of main-thread samples were idle.
The pg query-send path accounted for about 31% inclusively; socket-write frames
under pg accounted for about 24% and are a subset of that path. The observer's
report-emission path accounted for about 1.3%. These are main-thread stack samples,
not whole-system CPU time. During a live observation both Projector and PG had
unlimited cgroup CPU quota and zero recorded quota throttling. This excludes a
container CPU cap, not physical-host limits or single-thread contention.

There are also concrete server-side costs. The query maintaining `pi_history_blocks`
ran three times per Run in this workload and averaged about 38–47 ms on the client.
Native head/current-state reads, owned-record writes and presentation selection
also recur. No single seconds-long query dominated; the expense is distributed
across many operations. Enlarging the pool does not remove that work.

## The initial snapshot regression

The first uncorrected observation completed 168 Runs while 24 requests timed out
before submitting a Run, waiting for their initial SSE snapshot. The next case
submitted none: all 192 requests remained in the initial connecting phase.
Those cases are retained as failed setup experiments and excluded from the valid
pool comparison. They explain why the load driver now records failed-stream status
instead of reporting only a generic Tool-admission timeout.

The deterministic reproduction establishes this sequence:

1. Kafka starts a new assignment; its PG/bootstrap work is asynchronous.
2. A poll starts in the new generation before the partitions are installed.
3. Assignment finishes during that poll, which receives an EOF-only tail.
4. Its partition was absent from the poll snapshot, so the EOF is discarded.
5. There is no data message to enter the existing rewind branch. Native EOF need
   not repeat until new data arrives, leaving the initial snapshot gate waiting.

The correction re-observes newly installed partitions from their verified start
at the poll boundary, including empty batches. It does not accept a stale EOF or
substitute a fetched/broker end offset for processed evidence. The failing unit
case passes after correction; real RF3 tests cover delayed assignment onto an
idle transaction-marker tail with no new data. Three observed corrected workloads
then completed all 192 Runs each without the setup timeout.

This explains the reproduced idle-startup failure. The earlier four-Tool admission
anomaly lacked the fourth operation's evidence, so its exact historical cause
cannot be asserted retroactively.

## Next implementation boundary

Keep Pi's native commit, PG ownership, idempotency, recovery and effect-admission
contracts. Review the implementation inside a single native PG commit:

- Combine related keyed authority/head/current-state/origin reads where the same
  lock order and snapshot can be preserved.
- Batch related writes without widening a transaction across unrelated Sessions
  or discarding native/recovery records.
- Inspect the presentation-block query's use of the general `conversation_turns`
  view when the caller already has admitted live Run/Conversation identities.
  Prove the required active-child/Fork semantics before narrowing that path.
- Measure query calls, server work, client CPU and end-to-end completion together.
  Do not promise a particular speedup from query-count reduction alone.

There is no proposal here to drop partial text, remove fencing, replace Kafka/PG,
or add another cache or scheduler. Architectural/storage-boundary changes still
require a separate reviewed decision.

## Reproduction and restoration

See the [operator instructions](../../scripts/experiments/projector-sql-20261118/README.md).
All cases held the release lock, used owned tenants/fixtures and restored their
configuration. Original accounts, model/provider configuration, credentials, two
machine intents and 393 sampled Workspace files passed preservation checks.

## Publication and profiling calibration

The matched release built only the control-plane and Worker images and reused
twelve artifacts. Schema, native compute, Workspace storage, model/provider
configuration and the production twelve-connection pool were preserved. Cached
vulnerability evidence passed; no dependency or database upgrade was introduced.

After publication, the clean 64-concurrency / 192-Run case completed every Run at
15.06 Runs/s, with settlement p95 6.73 seconds and no initial-stream failure.
That is one short burst, not a new sustained capacity guarantee. All six deployed
failure scenarios passed. The Projector-restart case now opens an idle Session on
every partition without submitting a new Run: all thirty-two initial snapshots
arrived, with a maximum of 349 ms. Worker replacement retained original replies,
Stop ignored late replies and lost external replies produced UNKNOWN without
repeating the four physical writes.

Five bounded GPT-6 Luna/Cube cases passed, including native Inbox editing/Stop,
Worker recovery, interrupted visible text and simultaneous Worker/Projector kill.
No PG streaming-body history was added. The container-inspection race during the
intentional kill remained visible; independent exit/recovery assertions passed.

A final calibration removed pg wrappers, per-record async context, SQL timing
logs and PG activity sampling, retaining only V8 profiling. It completed all
192 Runs at 5.63 Runs/s. Main-thread samples were about 8% idle, with about 32%
in the pg query-send path inclusively and 26% in its socket-write subset. Thus
the stack pattern is not produced solely by the detailed SQL wrappers. The large
throughput variation between clean and profiled runs also means profiler effects,
cold state and background work have not been isolated; do not use these profiles
to claim an exact uninstrumented CPU budget or fixed throughput ceiling.

The first CPU-only setup did not become ready and ran no workload; its template
was restored. The revised operator activates profiling only after normal service
readiness and confirms its signal handler before collection. The revised setup
passed the same-image startup smoke and the full calibration. All observation
mounts were removed afterward, all 37 running Pods were Ready, original asset
preservation passed again, and active Runs/leases plus pending live/prefix/prefetch
state were zero at final verification.

The default event-log suite passed 61 tests; the release-helper suite passed 355.
The existing RF3 cases passed, and the new EOF-only test passed after correcting
its fixture to wait for all replicated transaction markers to become visible.
The reader/publication unit selection passed eleven tests, including the new
previously failing case. No database or dependency upgrade was required.

The tools use documented [pg pooling](https://node-postgres.com/apis/pool),
[PG activity states](https://www.postgresql.org/docs/17/monitoring-stats.html),
[session duration logging](https://www.postgresql.org/docs/17/runtime-config-logging.html)
and [Node Inspector profiling](https://nodejs.org/docs/latest-v24.x/api/inspector.html).
`pg_stat_statements` was not preloaded; installing its shared-memory facility
would require a PG restart. This bounded investigation used existing facilities
instead of changing production database startup or deploying a tracing service.
