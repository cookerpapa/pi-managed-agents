# Projector consumption and memory — November 18, 2026

Issues #46 and #47 are deployed as `prefetch-cache-20261118-r1`, application
revision `4ce1bddcf8507f281f33adeaf46a2f38c91a2fdc`, schema 226 and Session Topic v22.
The release built three artifacts and reused eleven, including the unchanged
provider, Web, Cube and Guest images. Native compute and Workspace storage were
preserved. [Aggregate evidence](projector-prefetch-memory-delivery-20261118.json)
records measured results without conversation bodies or test resource identities.

## Delivered behavior

- The poller retains fetched records; each partition processes them in order while
  other partitions progress independently. Native pause now handles real buffer
  pressure, explicit suspension, seek and failure. It is no longer a per-record
  execution lock. Limits cover queued and active records: 4,096 records / 32 MiB
  estimated bytes globally and 256 / 4 MiB per partition. A valid oversized record
  may cross the soft byte threshold once. Native buffering has its separate limit.
- Ordinary progress uses the existing periodic committer. Ready partition requests
  share native offset confirmations; a partition waiting on its publication guard
  does not hold healthy partitions. Tool dispatch still waits for its confirmed
  monotonic offset. EOF, seek, stale callbacks and reassignment preserve the
  distinction between fetched, processed and committed positions.
- One coherent admission snapshot replaces independently retained open/closed
  identity sets. LRU eviction removes its associated writer index. Misses reload
  the exact PG authority, identical loads coalesce, and closure/reset prevents
  stale asynchronous fills. There is no global permission-cache clearing.
- Native live documents, display text and recovery prefixes retain their existing
  durable coverage/checkpoint boundaries. They are not ordinary LRU entries.
  Empty settled holders are bounded; rejected records and sealed history replay
  cannot refill live presentation or authorize effects. Active text remains
  workload-sized rather than being silently discarded to satisfy a memory cap.

[ADR-0331](../adr/0331-bounded-partition-prefetch.md) and
[ADR-0332](../adr/0332-projector-memory-lifetimes.md) record the contracts. The
pinned Confluent public native API remains: its KafkaJS wrapper does not expose
the processed EOF evidence required here. No Kafka fork, new broker, ingress,
storage authority or Pi-core patch was introduced.

## Consumer measurement

The maintained RF3 reproduction uses 32 partitions, 2,048 records and a 10 ms
asynchronous handler. Setup/group assignment is excluded from delivery time.
The actual deployed reader completed delivery in **0.930 seconds**, versus
10.23 seconds in the preceding maintained-reader diagnosis.

In the same new experiment, the controlled per-record-pause implementation
received approximately **66,557 records** and took 14.30 seconds. The deployed
reader received **2,112** for 2,048 unique ordered deliveries, approximately
1.03 times the logical count. Native counters include fetch/setup behavior and
are sampled; they are not a duplicate execution count. The final counter sample
was taken outside the deployed reader's timed interval.

The deployed buffer peaked at 1,601 records / 4,234,144 estimated bytes and ended
empty. The diagnostic buffered control remains only a fixed-size comparison;
production acceptance uses the maintained reader with its recovery semantics.

## End-to-end capacity and remaining limit

All load used the same 24 GiB WSL host, two Workers and 32 Session partitions.
The short cases run 192 fresh Runs at concurrency 64, each requesting one 4 KiB
answer with zero simulated model delay. Throughput includes workload drainage.
This is a historical comparison, not simultaneous A/B deployment; background
work and cold-process effects are not eliminated.

| Sample | Previous completed Runs/s | New completed Runs/s | Previous settlement p95 | New settlement p95 |
| --- | ---: | ---: | ---: | ---: |
| First separate repetition | 4.57 | 8.29 | 32.66 s | 12.62 s |
| Second separate repetition | 5.93 | 18.48 | 18.67 s | 5.25 s |
| Later capacity sequence | 8.50 | 8.64 | 16.76 s | 12.04 s |

Every one of the 192 Runs completed in each sample. For the first two new
repetitions, mean provider-finish-to-PG-settlement time was 5.43 / 2.20 seconds,
versus 9.95 / 7.99 seconds previously. These paired per-Run intervals include
transport, Harness and downstream work; they are not pure Projector timings.

The sustained throughput ceiling is **not resolved**. Offered rates of 1, 2, 4
and 8 Runs/s completed all submitted work. At higher load:

| Offered rate for 30 s | Offered | Submitted and completed | Driver skipped before submission | Completed/s including drain |
| --- | ---: | ---: | ---: | ---: |
| 16/s | 480 | 352 | 128 | 10.26 |
| 32/s | 960 | 295 | 665 | 8.74 |

The driver limits in-flight requests to 64; skipped arrivals never reached the
API. No admitted Run failed in these capacity stages. At 16/32 offered Runs/s,
the mean timed record handler was about 114/189 ms, of which the `projection`
stage accounted for 112/186 ms. Projection includes internal authority checks,
PG work, connection waits and application processing. The separate post-projection
`boundary` check was mostly cached and must not be mistaken for all authorization
cost. Profile the projection stage before selecting another optimization or
changing transaction boundaries. A consumer microbenchmark speedup does not imply
the same end-to-end multiplier.

## Memory and cleanup

Identity metadata defaults to 32 MiB estimated bytes or 65,536 entries, whichever
comes first. In the isolated real-cache/mock-PG experiment, 50,000 and 100,000
observed UUID identities both plateaued at 28,976 retained entries. Forced-GC heap
growth was approximately 17.2 MiB; reset removed the entries. One thousand hot
reads issued zero PG queries. This measures the cache implementation, not 100,000
concurrent live Agents or a total-process RSS guarantee. A real-PG test separately
forces active identity eviction and verifies valid-prefix reload and cold replay.

Empty presentation metadata is capped at 4,096 holders / 4 MiB estimated bytes,
with idle expiry. The isolated 20,000-completion check preserved an unfinished
1 MiB text tail while removing excess empty holders, then released the text at
coverage. Uncheckpointed native progress is never discarded by this policy.

After the initial smoke and two cold repetitions, 388 identities required 388 PG
loads and produced 9,555 cache hits. Native live views, display tails, event-ID
prefixes and prefetch all returned to zero. After the capacity sequence, a 1 Hz
read-only scraper observed fresh all-zero values **1.396 seconds** after the last
Run's settlement. The underlying sampler refreshes every ten seconds; this is an
observed upper bound for that run, not a cleanup SLA. The final post-recovery
snapshot again shows zero active Runs/leases and zero pending live/prefix/buffer
state. Disposable identity/empty-holder metadata remains within its documented
budget. Short-stage metric peaks can be undersampled.

## Verification and preservation

- Workspace checks were followed by successful targeted reruns of corrected
  metric/periodic-commit assertions. TypeScript, build, formatting, documentation,
  Helm, installation, observability and image boundaries passed.
- All 461 deterministic fault assertions passed; the final RF3 suite passed
  twelve tests, including pressure/reassignment, buffered EOF, aborted tails,
  partition isolation and commit-before-effect. Release helpers passed 352 tests,
  Guest admission eleven, and the final real-PG/audit additions seven.
- Serial deployed load covered 32 acceptance stages across six groups: text,
  continuous Tools, real Guest execution, native children, images, SSE/history,
  provider-context expiry, compaction, high arrival rates and injected failures.
  The concurrent history-page sample had no failed pages; maximum observed page
  latency was 1.70 seconds. These finite tests do not establish fleet-scale HA.
- Worker handoff preserved the same Run and original replies, with four physical
  Tool calls rather than replay. Projector restart preserved healthy writers;
  Stop ignored late completion. The final lost-reply test observed four physical
  writes, each once, and canonical UNKNOWN outcomes. Synthetic metering recorded
  58 usage receipts: 42 eligible and sixteen platform-cost, with zero charges.
- Five GPT-6 Luna/Cube cases passed: Inbox editing/reordering/promotion, Stop,
  Worker recovery, streamed-prefix Stop and simultaneous Worker/Projector kill.
  Visible partial text survived without adding PG streaming-body history. One
  inspection raced the intentionally removed container and logged `container not
  found`; the independent exit/replacement and recovery assertions passed.

The first live fault group did not pass: its lost-reply setup expected four Tool
admissions, but only three corresponding Worker claims were observed. The original
driver discarded the fourth operation's outcome during cleanup, so its exact
pre-admission cause remains unconfirmed. Commit `450fe3d6` retains such diagnostic
evidence before cleanup. The complete rerun passed with unchanged admission counts
and assertions; this is not proof that the first anomaly's cause was fixed.

Image evidence used cached vulnerability databases. Root npm audit separately
retains six HIGH findings derived from the exact unpatched development-only
picomatch advisory reviewed in [ADR-0333](../adr/0333-development-picomatch-review.md),
with zero effective blockers. That review neither declares the dependency fixed
nor broadens runtime image exceptions.

All load and reader benchmarks used simulated models. Only bounded native
acceptance used GPT-6 Luna. Owned resources and configuration overlays were
removed through maintained cleanup. Original accounts, model/provider settings,
credentials, two machine intents and 393 sampled Workspace files were preserved.
All 37 running Pods were Ready at the final health check; the database bootstrap
Job had completed successfully. The earlier diagnostic
allocator abort and single-host/sustained-throughput limits remain separate work.
