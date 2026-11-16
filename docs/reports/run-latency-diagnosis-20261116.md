# Cold Run latency diagnosis — November 16, 2026

Investigation after the [claim-query repair](run-claim-query-repair-20261116.md).
The serving source remains `68dd3e55`, release `claim-keyed-20261116-r1`.
No production query, ownership rule, schema, Kafka durability setting or permanent
runtime configuration changed. [Aggregate evidence](run-latency-diagnosis-20261116.json)
excludes request bodies, credentials and Session/Run identities.

The bounded [terminal-query repair](terminal-history-query-repair-20261117.md) is
subsequently deployed and verified. Client initialization remains separate work.

## What the seconds contain

The original two workloads each completed 192 cold, single-request text Runs at
concurrency 64. Pairing each Run with its own provider request and Worker logs gives:

| Mean per Run | Original first sample | Original repeat | New clean repetition |
| --- | ---: | ---: | ---: |
| Submission to provider | 4,618 ms | 2,822 ms | 2,925 ms |
| Simulated provider | 3 ms | 6 ms | 12 ms |
| Provider completion to settlement | 1,320 ms | 5,209 ms | 9,086 ms |
| Total submission to settlement | 5,942 ms | 8,037 ms | 12,022 ms |
| Writer preparation, within the first interval | 2,008 ms | 1,029 ms | 861 ms |
| Entire claim transaction, within the first interval | 30 ms | 36 ms | 107 ms |
| Native Harness open | 0.67 ms | 1.12 ms | 2.88 ms |
| Model concurrency permit wait | 0.07 ms | 0.11 ms | 0.27 ms |

The clean repetition used the restored executable and no injected application
hooks. Read-only PG activity sampling and existing logs/metrics remained enabled.
All 192 Runs completed, each with one claim; throughput was 4.41/s and settlement
p95 26.47 seconds. This confirms variability, not a new capacity guarantee.
Requested first-byte/chunk delays were zero; the provider still sends real
protocol frames. Counts include neither Tool execution nor paid model work.
Settlement here uses the recorded `runs.settled_at` timestamp, not the later
client observation or a separately measured database COMMIT acknowledgement.

Opening the Harness and reading its immediate context are not the main costs.
Other startup stages include native commits: for example, `world_state_prepare`
creates/captures the native root and waits for publication. Its original means
were 728/351 ms despite less than one millisecond of recorded storage reads.
That stage must not be described as spending its whole duration assembling text.

## Kafka client initialization and shared background work

Each newly active physical Session opens its own stable transactional producer.
`KafkaSessionWriter.start()` performs PG authorization, native connect/metadata,
transaction initialization and post-initialization authorization. The native
connect/init/begin/commit methods use `Nan::AsyncQueueWorker` in the pinned
Confluent client. These calls share process-wide libuv workers with asynchronous
file operations and the local Model Gateway's Zstandard request decompression.

One temporary diagnostic repetition timed these public callbacks, without changing
their options or return semantics. Before an unrelated-to-the-test-plan allocator
abort described below, connect averaged approximately 0.60/1.69 seconds on the
two Workers; transaction initialization averaged 0.61/1.06 seconds. A small local
`fs.stat` probe also stalled: ten samples above 100 ms on each Worker averaged
441/834 ms, of which the JS event loop spent 396/700 ms idle. This identifies
background-work contention, not seconds of synchronous context processing.

The callback timings include queueing and native/network work; they do not prove
all initialization time is libuv queueing. The exact broker/coordinator share
remains unmeasured. Earlier isolated 4-versus-16-thread tests did not establish a
consistent throughput improvement, so increasing `UV_THREADPOOL_SIZE` alone is
not a demonstrated repair. Per-Session producer identity and pre/post-init
authorization cannot be removed as a performance shortcut.

## Completion waits behind projection, including expensive history selection

In the original two samples, the interval from the Harness receiving the complete
model response to PG settlement averaged 775 ms and 4,712 ms. In the clean
repetition it averaged 8,521 ms. The visible result still has to pass native
settlement, terminal publication and ordered projection; receiving model bytes
is not the Run's completion boundary.

The diagnostic repetition paired terminal publication with consumer delivery.
For 118 terminals consumed before the allocator abort, Kafka ACK to consumer
delivery averaged 1,070 ms, p95 3,248 ms. This separates waiting after durability
acknowledgement from the send itself. Consumer delivery is not a broker latency
measurement: it includes partition visibility, earlier records, pauses and polling.

The clean repetition recorded 28 `work_finished` records taking over one second.
Their slowest record took 1,536 ms, including 1,480 ms in PG projection and less
than one millisecond notifying the live view. PG sampling repeatedly found
`settleFamilyHistory()` querying `pi_history_live_turns`; sampled statement ages
reached 459 ms. That view expands the root/child conversation union and shared
history-range checks, even though this path already knows the particular Run.

A read-only, five-second-bounded EXPLAIN on one existing completed Run found:

| Query | Planning | Execution | Buffer hits |
| --- | ---: | ---: | ---: |
| Actual family-settlement selection | 56.47 ms | 4.34 ms | 35 |
| Same selection repeated | 40.45 ms | 3.70 ms | 32 |
| Identity-only conversation control | 5.65 ms | 0.34 ms | 5 |

The identity-only control omits history eligibility and is **not** a replacement
query or an equivalence claim. The actual selection returned zero pending rows,
yet retained substantial planning cost. Loaded sampling and this idle plan are
different observations; no complete attribution of the loaded statement to
planning versus execution is claimed.

The Projector has 32 ordered partitions and a separate 12-connection projection
pool. Each record includes multiple awaited PG operations. Costly terminal
transactions occupy both a partition and a connection while later records wait.
Observed Kafka lag reached 1,563 in the clean repetition. The evidence identifies
this as a contributor, not proof that one SQL accounts for the entire 8.5 seconds.
Neither enlarging the pool nor adding consumers has been validated as the fix.

## Diagnostic limits and restoration

The injected repetition completed all 192 Runs but one Worker aborted with
`corrupted double-linked list`; the kernel recorded SIGABRT in a libuv worker.
Nine Runs were reclaimed. Its 4.09/s aggregate and post-abort long tails are
confounded by recovery and instrumentation and are not normal-path capacity
evidence. The cause is unresolved: do not attribute it conclusively to either
the instrumentation or a production-client defect. The subsequent clean
repetition had no repeated claims and reproduced slow terminal projection.

An initial preload attempt also affected the tsx launcher/loader context and did
not start the replacement service. The old service stayed ready and the original
template was restored. Limiting instrumentation to the application main
thread/process allowed the subsequent attempt to start. Both original templates
and all model/provider configuration were restored after measurement. Temporary
ConfigMaps and owned load resources were removed. Original accounts, credentials,
393 Workspace files and two development machines were preserved. No paid-provider
requests, foreign-model calls or charges occurred.

## Proposed next slices

1. Narrow family/terminal history selection to the known Run and native child
   identities before evaluating shared-history eligibility. Preserve Fork/prune,
   child settlement and atomic terminal semantics. Measure planning as well as
   execution/buffer work; do not simply remove the eligibility rules.
2. Evaluate the per-Session Kafka client's initialization and shared background
   scheduling separately, including the allocator abort. Keep stable producer
   fencing and R=3 acknowledgement. A change of client/process ownership requires
   discussion and an ADR before implementation.
3. Recheck terminal ACK-to-projection delay after those bounded changes. Keep
   native commit semantics and distinguish per-record work from backlog; do not
   mask it by raising timeouts or releasing Runs before terminal projection.
