# Runtime capacity baseline — 2026-11-15

Issue [#42](https://github.com/cookerpapa/pi-managed-agents/issues/42),
[ADR-0326](../adr/0326-runtime-capacity-benchmarks.md).
Reproduction: [runtime capacity guide](../RUNTIME_CAPACITY.md).
[Content-free machine-readable evidence](runtime-capacity-20261115.json) contains
the measured profiles, quantiles, resource samples reduced by component, source
identities and explicit selection of the 31 accepted scenarios. It retains
preliminary verdicts and names the superseded trial expectations.

## Result and scope

On this single WSL host, the short, cold text workload reaches approximately
10 completed Runs/s. Increasing concurrency from 32 to 64 adds substantial tail
latency without a corresponding throughput gain. A paced arrival rate of 8/s
completed 360/360 Runs in the 45-second offered window plus drainage, with
19 ms submission p95 and 1.53 s settlement p95.

This is a workload-specific baseline, not a limit of ten simultaneously active
Agents. A Run may contain many model requests, Tools and child Conversations.
Longer model/tool waiting changes the relationship between concurrency and
completed Runs/s. No real model provider was used, and inference speed/quality
and paid-account concurrency are not measured.

## Deployment and method

- WSL Linux, Ryzen 9 7945HX, 32 logical CPUs; 24,028 MiB visible RAM, 8 GiB swap.
- Existing single-node K3s deployment, two Workers, one Session execution owner,
  separate Session API/Tool host/resource management, PostgreSQL and three Kafka
  brokers. RF=3, transactions and fencing remain enabled; the brokers still share
  one physical host. No multi-node HA or failure-domain claim.
- Initial serving application `http-mcp-20261115-r1`, schema 225. Test bundles ran
  in a temporary Pod using an already published image. A real HTTP framing defect
  discovered by this benchmark is repaired in a provider-only release; Worker,
  Projector, API, Web and Guest artifacts remain unchanged.
- Kafka execution-log partitions: 32. Worker family capacity and Worker/Session
  model concurrency: `unlimited`. Existing Guest dispatch bound: eight active
  commands. No runtime limit was removed for the measurements.
- PG pool ceilings: Session API 12 ordinary / four history / three admission;
  Session execution 12 ordinary / 12 projection / three maintenance / four
  publication / two Inbox reads / four conversation reads. Each Worker has four
  ordinary / two lease / four cold-read connections. These are separate pools,
  not a claim that every connection is continuously busy. The load observer uses
  three connections; the isolated PG comparison below intentionally differs.
- The synthetic upstream is limited to 2 CPUs / 512 MiB. CLIProxyAPI receives one
  unique model route and the catalog one synthetic model; original defaults,
  credentials and account configuration are restored afterward.
- Cold workloads use fresh Sessions and ordinary API admission, native Pi,
  direct transactional Worker publication, projection and public SSE delivery.
  Every stage includes real terminal settlement, not merely HTTP acceptance.
- Fixed-concurrency and scheduled-arrival drivers are separate. The latter
  records work it could not issue when its explicit in-flight budget is reached.
  Throughput includes batch drainage. Fresh SSE setup/teardown is included in the
  cold task cycle; it is not hidden generator overhead.
- Heavy stages run serially. Original idle infrastructure and two existing
  development machines share the host. Samples are every two seconds, with
  Kafka Admin offsets and kubelet statistics approximately every six seconds.
  Repeated cached offset observations are not counted as new samples.

## Cold text curve

Zero synthetic first-byte delay, 4 KiB final output, one model request per Run.

| Concurrent tasks | Completed / submitted | Completed Runs/s | Submission p95 | Settlement p95 |
| --- | --- | --- | --- | --- |
| 1 | 8 / 8 | 1.03 | 22 ms | 0.96 s |
| 4 | 12 / 12 | 3.17 | 56 ms | 1.12 s |
| 8 | 24 / 24 | 6.25 | 43 ms | 1.40 s |
| 16 | 48 / 48 | 9.35 | 30 ms | 1.83 s |
| 32 | 96 / 96 | 10.29 | 36 ms | 3.56 s |
| 64 | 192 / 192 | 10.32 | 31 ms | 11.70 s |

The small sample counts at the lower points make their p99 values close to the
maximum; they are descriptive, not precise tail-SLO estimates.

## Scheduled arrivals

The model waits 300 ms before emitting a 2 KiB answer. Lower points offer traffic
for 45 seconds with an in-flight budget of 32; upper points use 30 seconds and a
budget of 64. The workload windows differ and are explicitly recorded.

| Offered rate | Scheduled | Submitted and completed | Not issued by driver | Completed Runs/s including drain | Settlement p95 |
| --- | --- | --- | --- | --- | --- |
| 1/s | 45 | 45 | 0 | 0.99 | 1.23 s |
| 2/s | 90 | 90 | 0 | 1.97 | 1.23 s |
| 4/s | 180 | 180 | 0 | 3.91 | 1.26 s |
| 8/s | 360 | 360 | 0 | 7.81 | 1.53 s |
| 16/s | 480 | 366 | 114 | 9.84 | 16.46 s |
| 32/s | 960 | 366 | 594 | 9.52 | 17.50 s |

Unissued arrivals are not API rejections. The driver deliberately bounds the
number of outstanding tasks; these upper points do **not** demonstrate sustained
16/s or 32/s service. Every submitted Run in these points completed successfully.
The generator's measured launch p99 was below one millisecond in both upper
arrival stages, while platform waits grew into seconds.

## Other real runtime paths

| Workload | Evidence |
| --- | --- |
| Paced text | 24/24 completed at concurrency eight; 1 s model first-byte delay plus paced output |
| Short MCP loops | 24/24 Runs, three actual tool calls each; 96 model requests, 72 incremental |
| Continuous tools | Eight Runs with 32 tools each; 256 real MCP calls and 264 model requests in 23.5 s |
| Linux Guest | Eight Runs, three commands each, concurrency four; ordinary Cube/Guest creation, dispatch, replies and owned cleanup |
| Native children | Six root Runs, four native child Conversations each; shared physical-Session ownership, 36 model requests |
| Multiple SSE readers | 16 Runs with three normal readers plus a delayed reader, combined with history reads |
| Retained images | Eight MCP image results; valid deterministic 512×512 PNGs, approximately 0.75 MiB each; 16 model requests |
| Larger input | Corrected 64 KiB single-message input through normal admission |
| Retained context | Twelve prior 16 KiB answers and 4 KiB inputs, then continuation, expired upstream context and native compaction |

The Guest workload shares one owned Workspace; it is not an eight-VM fleet
benchmark. The fixed image bytes are identical by design, so this is not a claim
about unique-image object-store capacity or multi-gigabyte uploads.

Warm-provider continuation settled in 844 ms, upstream-context expiry/full replay
in 838 ms and the post-compaction request in 832 ms in the retained-context probe.
These are individual checks, not percentile comparisons or proof that cold
replay is faster. Native compaction and subsequent continuation both succeeded.

## Recovery behavior

- Worker replacement: four pending remote calls, one original Run/identity per
  conversation resumed under a new writer; four physical effects, one original
  canonical reply each. No unsafe tool replay.
- Session execution/Projector restart: four active text Runs completed with
  unchanged Worker lease IDs. SSE readers reconnected. Offset lag rose during
  the interruption and was observed again after recovery.
- Stop: two of four Runs cancelled, two completed. Only after native abort
  settlement was a late reply released; it did not replace cancelled history.
- Lost MCP reply: four physical writes, four canonical UNKNOWN results, no
  repeat side effects; each Agent completed its subsequent model step.
- Synthetic 429 and upstream WebSocket loss: the pinned adapter/proxy settles
  these as terminal failures. Eight new user requests after each failure class
  succeeded. This is recovery by a new request, **not** an automatic-retry claim.
  The 429's presentation as a ChatGPT usage limit needs a separate classification
  review before treating it as representative of every provider throttle.

## Bottleneck evidence and limits

At 64 cold tasks, the simulated provider used approximately 0.035 CPU core and
67 MiB peak RSS. Its CPU capacity was not exhausted. The two Worker processes
together averaged approximately one CPU core, and Session execution averaged
0.74 core; the API averaged 0.23 core. CPU below full utilization does not imply
that a serial I/O or ordering path has spare throughput.

Mean writer preparation grew from about 413 ms at concurrency one to 1,253 ms
at 64. Environment preparation grew from 57 to 482 ms; Worker Kafka publication
from 25 to 85 ms. These timings can overlap/nest and must not be added together.
The sampled Kafka offset gap reached approximately 1,673 at the upper arrival
point. PG lock waiters were not observed in these samples. This identifies
startup/publication/projection waits as profiling candidates, but does not prove
a single root cause or justify weakening durability, fencing or PG admission.

The host retained over 10 GiB available memory in the upper-load stages; swap
use was approximately 4 MiB. Memory exhaustion was not the observed limit.
Historical WSL storage tail-latency evidence remains relevant background, not
proof that disk alone caused this curve.

## Measurement corrections

The original raw reports are retained privately, including failed fixture trials:

1. The current production Tool-host image has no `sharp` dependency. The fixture
   now generates a valid PNG using Node's built-in zlib/CRC support.
2. A 256 KiB prompt correctly hit the public 100,000-character admission limit.
   The corrected stage uses 64 KiB; larger contexts are accumulated over turns.
3. Returning HTTP 503 on WebSocket upgrade does not mean “use HTTP instead.”
   HTTP is now selected through an explicit native provider route. No production
   proxy replay policy was changed to make a benchmark pass.
   The subsequent valid HTTP probe exposed a genuine adapter bug: a stream
   beginning with `event:` or a comment was mistaken for raw WebSocket JSON.
   The repair selects the SSE framer from the native executor's HTTP media type
   before consuming chunks, with a regression test for an HTTP-only route.
4. Stop acceptance and native abort settlement are distinct. The late-reply
   probe now waits for the latter before releasing its result.
5. Provider faults validate their actual pinned terminal behavior and a new
   successful request. Earlier generic “all Runs must succeed” flags therefore
   remain false in the preliminary raw matrix/fault report; individual corrected
   cases and their separate recovery evidence determine acceptance.

## Supplementary evidence and preservation

- Four-minute MCP soak: 240/240 Runs completed, 720 actual tool calls and 960
  model requests; zero drops/failures. Settlement p50/p95/p99 was
  2.922/3.166/3.304 s, submission p95 19 ms. Peak in-flight tasks: four;
  no queued Runs or PG lock waiters were observed. Available RAM stayed above
  10.6 GiB. The two Workers' combined RSS rose from 746 to 976 MiB and Session
  execution from 272 to 385 MiB. This run does not establish a memory plateau or
  distinguish retained caches/heap arenas from a leak; longer observation is
  required before making a long-running memory claim. After drainage, queued,
  running and settling Runs, live owners and the sampled Kafka offset gap were
  all zero. All 960 usage receipts arrived; delivery p95 was 1.216 s, with no
  foreign-model invocations or customer charges.
- Explicit HTTP: the original valid-SSE probe failed before the repair and the
  same text/Tool cases passed afterward. `provider-sse-20261115-r1` was published
  through the maintained provider scope from isolated commit `b1a8e6ac`, which
  is reachable from `main`. All Go provider tests and the one-image scan passed.
  Only CLIProxyAPI changed; no worker rollout, schema migration or VM replacement.

- Paging: four 32-tool conversations crossed the history boundary and fetched
  eight pages through public cursors. Every Run/page completed; page p50 was
  346 ms, but one page took 6.87 s (also p95/p99 in this eight-page sample).
  This is a latency finding, not a smooth-paging performance claim. Profile cold
  history reconstruction and its interaction with active execution separately.

- Isolated PG: 2 CPU / 768 MiB container, 64 database connections, 256 offered
  concurrent operations, 2,000 Sessions × four complete 1 KiB entries. All
  8,000 entries/commits persisted: 116.05 batches/s, latency p50/p95/p99
  1.689/4.612/7.137 s including contention/pool waiting. WAL was 40,974,512
  bytes, 5,122 bytes/entry. Reading 500 four-entry Sessions achieved 227.3
  Sessions/s. These are synthetic native-storage batches, not whole Runs.
- Isolated Kafka: three pinned brokers, each limited to one CPU / 768 MiB,
  RF=3, 32 partitions, transactional commit acknowledgement, no batching.
  Each point commits 4,096 synthetic facts. Concurrency 1/16/64 achieved
  32.44/183.85/256.04 facts/s, with ACK p95 32.7/179.5/418.5 ms.
  This excludes PG authorization, projection, models and Tools; it is not the
  same configuration or workload as the complete-loop curve.

- Schedules: 10,000 plans in each of sparse, distributed-burst and one-Session
  burst scenarios. Burst admission achieved 148.77/s and 84.42/s respectively,
  with zero duplicate admissions/controller errors. Concurrent API reads had
  p95 10.35 ms and 3.52 ms. Only 100 and one queue heads were ready respectively;
  **zero Agents executed** in this isolated test.
- Slow-model concurrency: 64/64 root Runs completed with a deliberately imposed
  10 s model delay. The driver observed 64 active root Runs and 64 simultaneous
  simulated model requests; settlement p95 was
  15.04 s, with 16.61 s total drainage. This separately exercises held model
  requests rather than pretending short-Run throughput is an active-Agent cap.

## Acceptance and remaining work

All 31 selected workload/failure scenarios passed their explicit expectations.
Every fixture report confirms cleanup, including the earlier failed trials.
Temporary Kubernetes Pods/Services/configuration/credentials were removed;
provider/catalog restoration receipts are complete. Shared Kafka records follow
normal retention rather than being selectively deleted. Private raw reports and
configuration receipts are not committed.

The post-run preservation check verified all original account/configuration
records, both development-machine intents and 393 original Workspace files.
The host restarted after load testing had finished; services recovered, and the
preservation check ran after readiness returned. This uninstrumented restart is
not included as another capacity or HA result.

The next measurements should focus on cold writer/environment preparation and
canonical projection waits, the observed slow history page, and longer memory
observation. Larger active/historical child families, thousands of subscribers,
large unique attachments and independent compute-node fleets remain outside this
baseline. No durability, lease, ordering or replay rule was weakened to improve
a performance number.
