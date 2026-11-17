# Worker publication and model charge attribution — November 17, 2026

Issues #43–45 are deployed as `projector-admission-20261117-r1`, application
revision `692c1992635c80d86627f5f94eddd01b0b17aa78`, schema 226 and Session Topic v22.
The matched operator built six application artifacts and reused eight, including
the unchanged Cube and Guest artifacts. It drained old writers before switching
the admission protocol; native compute and Workspace storage were preserved.
[Aggregate evidence](projector-admission-delivery-20261117.json) records the
artifact/source identities, acceptance results and content-free load measurements.

## Delivered behavior

- Each Worker boot reuses one idempotent Kafka producer. A complete native batch
  is one record; replicated delivery acknowledgement precedes native adoption.
  Opening a Session writer no longer queries PG or initializes a transactional
  producer. Claims, direct lease renewal, local expiry and cold reads remain.
- Projector admits the PG-issued writer and ordered pre-seal prefix before native
  state, live output, Tool/cancellation dispatch, completion or resource activity.
  Late old records may enter Kafka but cannot become accepted effects or poison
  replacement sequence allocation. Control publication retains the existing
  partition queue and consumer-group transaction guard. No ingress or Raft service
  was added. Shared transport uncertainty retires the Worker boot without replaying
  uncertain batches; closing an ordinary Session does not close the producer.
- Human Inbox mutations register an exact notification waiter before Worker
  delivery. Success requires PG projection, with native receipt lookup after a
  lost notification/reply. Streaming commits do not wait for per-fragment PG ACKs.
  Pi remains the only Inbox owner; edits are current state, not retained copies.
- Model usage retains the actual provider receipt and price version. Each HTTP
  request carries execution identity; accepted native model settlement and writer
  closure provide independent billing evidence. CLIProxyAPI has no live fencing
  cache. Valid user Stop with complete usage is chargeable; failed/superseded or
  rejected recovery work is platform cost. Incomplete evidence stays unconfirmed.
  Statements remain informational, with no real deductions or payment collection.

The contracts are [ADR-0328](../adr/0328-projected-native-input-confirmation.md),
[ADR-0329](../adr/0329-worker-producer-and-projector-admission.md) and
[ADR-0330](../adr/0330-model-charge-eligibility.md).

## Verification

The complete workspace test run was followed by targeted successful reruns of
five corrected assertions/fixtures. Corrections covered the new admission result,
lost-ACK reconciliation, schema 226, activity admission and an isolated resource
registry test database. No production check was relaxed. TypeScript, build,
documentation, Helm/installation, image boundaries, Guest contracts and dependency
audit passed. Root discovery now excludes cached experimental source copies.

All 444 deterministic fault invariants passed across the full run and one targeted
rerun correcting a renamed test in the manifest. The RF3 transport suite passed
11 tests. The 346 release-helper and 11 Guest admission assertions passed. Provider
Go contracts ran in the image build. All 14 artifact evidence checks passed using
the owner's cached vulnerability databases and the existing exact stock-Cube
exception, without refreshing the database.

Serial deployed acceptance covered:

- HTTP and WebSocket model transport, real Pi, PG/Kafka projection, MCP and Guest
  tools, continuous tool loops, native children, retained images and SSE readers.
- Long-history continuation, expired provider-context fallback and native manual
  compaction. The concurrent history-page case had no failed pages and a maximum
  observed page time of 958 ms; the former 6.87-second outlier did not recur here.
- Worker replacement during a Tool wait: the same Run resumed, with one original
  Tool invocation/result and no replay. Projector restart preserved healthy writers.
  Stop ignored late replies; lost replies retained UNKNOWN without repeating writes.
- GPT-6 Luna/Cube input submit, retry, edit, reorder, promote, Stop and recovery.
  Native queues remained in one Run and retained no intermediate edit bodies.
  A single SSE connection observed recovered execution state.
- Actual streamed-prefix Stop and simultaneous Worker/Projector termination:
  the observed partial text survived, without storing PG streaming-body history.
  An execution-inspection request raced the terminated container and logged a
  missing-container message; replacement/exit evidence and all recovery assertions
  still passed. This was not a successful call to the removed process.

The native-input driver used GPT-6 Luna only for bounded live acceptance. All
throughput and injected provider workloads used the owned simulated provider,
with zero paid-provider requests. The injected-failure run retained 58 usage
receipts: 42 eligible and 16 platform-cost receipts, with no estimated or actual
customer charge. Both transport smoke checks matched every successful request to
accepted native financial evidence.

Eight serial load groups covered 35 stages. The final approximately four-minute
Tool soak completed all 240 offered Runs, with no driver drops, failed Runs or SSE
reconnects; settlement p95 was 2.21 seconds. Every group confirmed owned-resource
cleanup and restoration of the synthetic provider/catalog changes.

## Performance: initialization repaired, total throughput still limited

The same 24 GiB WSL host, two Workers and 32 Kafka partitions were used. Cold
short-Run cases request one 4,096-byte response with zero simulated model delay.
The historical comparison is the preceding deployed query repair, not a new
simultaneous A/B deployment. The first new repetition also sampled process
threads/FDs; the second had no such process sampler. Existing metrics remained on.

| Measurement | Preceding release | New release |
| --- | ---: | ---: |
| Mean Session writer preparation | 830 / 1,330 ms | 0.67 / 0.63 ms |
| 64 concurrent, 192 cold Runs: completed/s | 6.29 / 9.04 | 4.57 / 5.93 |
| The same case in the later capacity sequence | — | 8.50 |
| Initial two repetitions: successful Runs | 192 / 192 each | 192 / 192 each |

Worker preparation no longer pays Kafka connection/transaction setup per Session.
Observed Worker thread count stayed at 47 during the sampled 64-concurrency case;
it did not grow with Session count. This does **not** establish a fix for the
earlier injected-diagnostic native allocator abort.

Overall short-task throughput is not proven improved. In the first two cases,
mean model-response-to-settlement time was 9.81 / 7.87 seconds, while mean
provider-to-Harness response time was 131 / 117 ms. Native Kafka publication
averaged about 14 ms in the first case. Most remaining elapsed time is after the
Harness has received the model response, with projection/settlement backlog.
Read-only PG sampling did not reproduce a multi-second statement; the largest
sampled statement age in the high-load observation was about 119 ms. Sampling is
not an exhaustive query-duration trace and does not isolate the remaining cause.

Short lower-concurrency cases reached 11.53, 15.76 and 18.31 completed Runs/s at
8, 16 and 32 concurrent requests respectively. Those brief bursts are not a
sustainable-capacity claim. Continuous offered rates of 1, 2, 4 and 8 Runs/s
completed all offered work with no driver drops. At higher offered load:

| Offered load | Offered | Submitted and completed | Driver skipped before submission | Completed/s including drain |
| --- | ---: | ---: | ---: | ---: |
| 16/s for 30 s | 480 | 332 | 148 | 9.53 |
| 32/s for 30 s | 960 | 323 | 637 | 8.22 |

The driver caps in-flight work at 64; skipped arrivals were never submitted to the
server. Every accepted Run in those stages completed. The host retained over
9 GiB of available memory and did not hit the headroom guard. These results leave
the approximately 10-Run/s saturation question open. Investigate the ordered
projection, acknowledgement and settlement path before changing that architecture.

## Preservation and limits

Publication preserved the two original machine intents, accounts, model/provider
configuration and credentials. The original 393 sampled project files matched
their hashes after cutover and again after all acceptance. All serving Pods were
Ready at the final check. All owned acceptance resources and synthetic routes
are removed through the maintained physical-cleanup and receipt-delivery checks.
The final preservation and bounded-soak records accompany the aggregate evidence.

Ambiguous multi-attempt provider attribution is conservatively assigned to
platform cost; this does not identify the exact successful attempt's billable
tokens. Aggregate usage cannot be divided exactly at a seal timestamp. No live
CPA fencing cache, actual wallet or new compute-billing behavior was introduced.
Single-host tests do not establish multi-node HA or a fleet-scale capacity limit.
