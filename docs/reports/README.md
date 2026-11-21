# Delivery and research evidence

- [Projector capacity for 200-tool Runs](tool-loop-capacity-20261121.md) separates Kafka records, history messages and tool pairs under live, paced and preloaded traffic.

- [Native tool starts and grant width](display-tool-batching-20261121.md) measures actual batch membership on a complete Pi trace and separates arrival-limited throughput from backlog drain.

- [Batching admitted Tool intents in PostgreSQL](pg-tool-intent-batch-20261121.md) compares paced dispatch and backlog drain at equal Kafka grant sizes, with native commit and failure checks.

- [Partition-local Tool grants and partition count](partition-tool-grants-20261121.md) measures 32/64 partition generations and verifies a partly sent grant after a process kill.

- [Projector dispatch across 4,000 sandbox identities](projector-fanout-20261121.md) separates network receipt latency from PG projection and Kafka dispatch grants.

- [Real Bash dispatch under mixed load](guest-tool-mix-20261121.md) measures the first external execution bottleneck in the #52 candidate.

- [Warm continuation and execution-side history batching](workspace-continuation-rate-20261121.md) checks the model prompt and actual Guest cwd and marker across warm and cold continuation, and measures input, projection and terminal rates separately.

- [Execution-owned Workspace routing](workspace-operation-routing-20261121.md) verifies operation-only Stop and original Guest selection across replacement.

- [PG-only family handoff](pg-family-handoff-20261121.md) compares the existing rows with one experimental seal-time capture after removing the old Session log prefix.

- [Session API and lease-driven family handoff](serving-family-handoff-20261121.md) follows HTTP admission, ordered expiry, original-deadline recovery and user Stop through a real Guest.

- [Child handoff from PG and retained Kafka](retained-family-handoff-20261121.md) tests a physical Worker exit, original Cube reply, prior Run rotation and an incomplete recovery base.

- [Context status, gateway usage and PG contention](status-usage-contention-20261120.md) records the read paths and shared-instance controls.

- [Current usage row and bulk writes](minimal-usage-bulk-20261120.md) measures grouped upserts and distinguishes cumulative spending from current-context compaction.

- [Cumulative Conversation usage under batched projection](minimal-projection-usage-20261120.md) compares the same response traffic with and without a current-value write.

- [Where a prototype Run waits](session-tail-attribution-20261119.md) traces a single terminal record through native delivery, ordered handling and PostgreSQL, with captured SQL.

- [Successive Run throughput](session-continuation-throughput-20261119.md) compares new Sessions, returning Sessions and mixed arrivals with integrated Worker locality.

- [Worker batch size, residence and Session locality](worker-claim-sizing-20261119.md) compares 8–64 row claims, 64–256 simultaneous model waits and the cost of targeting a warm Session.

- [Directed Worker wake and batched claims](directed-worker-wake-20261119.md) measures the two-Pod Pi path and a 1,000-connection queue, including lost readiness and scale transitions.

- [Minimal Session lifecycle load](minimal-session-storage-load-20261119.md) compares two business tables and Pi Inbox, independent completion, queue depth and the PG wake-up lock.

- [Projector → PostgreSQL design and cost review](projector-pg-review-20261118.md) maps schema, SQL, staged write costs, read tradeoffs and two verified repairs.

- [Projector terminal queue and native head lookup](terminal-projector-queue-20261118.md) traces earlier same-partition work, tests pool size and measures latest-receipt lookup as a Session grows.

- [Native usage and chat history under load](native-usage-and-chat-history-20261118.md) measures current-value storage, per-message projection cost, the terminal queue and fixed-rate arrivals.

- [Kafka-backed live progress and recovery](kafka-live-progress-20261118.md) records the #50 SQL count, visible-prefix recovery and synchronized short-Run comparisons.

- [Ordered Session projection and Run tail latency](ordered-projection-tail-20261118.md).
  Source-only feasibility, fixed-connection comparisons, failure acceptance and
  limits of the single-host measurements.

- [Projector SQL cost and idle startup](projector-sql-diagnosis-20261118.md):
  client/server timings, V8 samples, twelve/thirty-two-connection comparison,
  many small SQL operations and the reproduced idle-EOF assignment regression.

- [Projector consumption and memory](projector-prefetch-memory-delivery-20261118.md):
  #46/#47, bounded prefetch, coherent identity caches, measured cleanup and serial
  recovery/load acceptance. Receive amplification is repaired; sustained capacity
  remains limited. Retains the first live admission anomaly and full rerun evidence.

- [Projector consumption diagnosis](projector-consumption-diagnosis-20261117.md):
  per-record pause purges native prefetch; isolated controls reproduce repeated
  reads and idle delivery gaps, with actual Run correlation and restored runtime
  configuration. Includes a maintained reproduction command, not a deployed fix.

- [Worker publication and model charge attribution](projector-admission-delivery-20261117.md):
  #43–45, shared producers, Projector admission, exact input confirmations,
  independent financial eligibility, deployed recovery and serial load comparison.
  Initialization is repaired; overall high-concurrency throughput remains limited.

- [OpenMA streaming, retention and capacity evidence](../research/openma-streaming-retention-capacity-20261117.md):
  actual Node/Cloudflare delta paths, retained stream buffers and the limits of
  published stress/business-latency evidence. Source inspection only.
- [Producer reuse and Raft assessment](../research/producer-reuse-and-raft-20261117.md):
  scoped Worker-query preference, Session/Worker/partition tradeoffs and the
  boundary between a replicated state machine and external Kafka publication.
- [Fencing and Kafka publication patterns](../research/kafka-fencing-patterns-20261117.md):
  official Connect/Streams source, Outbox and checkpoint comparisons; existing
  authorization checks have precedent, while producer granularity remains a
  measured design choice. Research only, with no runtime change.
- [Terminal history query repair](terminal-history-query-repair-20261117.md): direct
  root metadata, bounded child selection, published before/after measurements,
  child/history/recovery acceptance and the separate Kafka-client follow-up.
- [Cold Run latency diagnosis](run-latency-diagnosis-20261116.md): paired startup,
  provider and terminal timings, native-client contention, history-query planning
  cost, clean reproduction and the separate diagnostic allocator-abort limitation.
- [Run-claim query repair](run-claim-query-repair-20261116.md): bounded keyed
  eligibility, deployed plan measurements, normal throughput repetitions and
  recovery/preservation acceptance. Downstream throughput variability remains open.
- [Run-claim query diagnosis](run-claim-query-diagnosis-20261116.md): actual loaded
  plans isolate tenant-prefix join amplification; the subsequent repair above
  preserves current PG ownership and queue semantics.

- [Workload build isolation and measured query repairs](workload-release-throughput-20261116.md):
  derived dependency inputs, Worker source boundaries, no-op publication,
  cached scans, original-asset preservation and before/after throughput/history
  measurements, including saturation limits and recovery acceptance.
- [Capacity diagnosis](runtime-capacity-diagnosis-20261116.md) and
  [initial baseline](runtime-capacity-20261115.md): frozen-query amplification,
  cold-Run costs and bounded single-host measurements through the real runtime.

- [Shared clients, local execution and platform service minutes](client-service-delivery-20261114.md):
  #38/#39, cloud-owned Pi loops, outbound Linux tools, independent minute settlement,
  real Cube/BoxLite/Android receipts, recovery, publication repairs and preserved assets.

- [Decoupling, resource backends and scoped release delivery](decoupling-delivery-20261113.md):
  #30/#31/#33–#37, independent local storage, dynamic registry, two Skill tools,
  role-owned configuration/artifacts, actual single-component and zero-restart
  publication, serial recovery acceptance, preserved assets and limits.

- [Repository decoupling and release review](repository-decoupling-review-20261112.md):
  current authority boundaries, remaining startup/storage/build coupling, release
  scope decisions, documentation/migration cleanup and test-value assessment.
  Its authorized repairs and publication gates are delivered in the report above.

- [Integrated alternate runtimes](alternate-runtimes-20261112.md): stock BoxLite
  resource/Guest integration, Android original-operation recovery, serial live
  acceptance and explicit isolation/accounting limits.

- [Platform-owned elastic expiry](platform-idle-expiry-20261111.md): PG deadlines,
  matched rollout, actual 30-minute management-outage test, original Worker reply
  recovery and small-image/preview acceptance.
- [Stock sandbox portability](sandbox-portability-20261111.md): real BoxLite
  Guest/HTTPS checks, official-configuration limits, Android publication and live
  device/image/restart acceptance; no new production default.
- [Obsolete Cube metadata cleanup](cube-metadata-cleanup-20261111.md): native
  template deletion, bounded private-bucket cleanup and original-asset preservation.

- [Complete Cube patch inventory](../research/cube-patch-inventory-20261110.md):
  every current source/dependency patch, seven runtime responsibility groups,
  cold-connection consumers and build transformations outside patch files.

- [Guest execution and Session activity](guest-execution-20261110.md): Guest-owned
  admission/cancellation, finite retention, fenced Session activity, matched release,
  real recovery/image acceptance and the remaining resource/Volume patch boundary.
- [Guest and provider resource boundaries](../research/guest-and-resource-boundaries-20261110.md):
  pinned E2B, Daytona, OpenSandbox, BoxLite and Modal lifecycle/storage comparison.

- [Pi Managed Agents project identity](project-identity-20261110.md): repository
  rename, public branding and Web-only publication with existing resources preserved.

- [Runtime decoupling and continuation](runtime-decoupling-20261109.md):
  public API separation, Guest HTTPS completion, original-operation recovery,
  execution-side retention and external compute accounting.
- [Remaining Cube patch assessment](../research/cube-patch-assessment-20261110.md):
  native provider interfaces, portable Guest responsibilities and remaining
  execution/lifecycle protections after ADR-0306.

- [Execution-owned tools and independent publication](execution-tools-20261108.md):
  logical bindings, whole-tool provider adapter, Kafka directory broadcast,
  live Tool-host-only publication, native recovery/images/children and BoxLite assessment.

- [Independent runtime and resource accounting](runtime-accounting-20261107.md):
  Cube v0.7.3, independent receipt ingestion, kernel quotas, management-outage
  continuity, native recovery, images and original-asset preservation.

- [Model accounting](model-accounting-20261106.md): durable source receipts, exact
  versioned prices, statements, preservation and successful live recovery checks.
- [Provider upstream candidates](provider-upstream-candidates-20261106.md): separate
  contribution assessment and existing PR status; no new upstream submission.

- [CLIProxyAPI v8 provider](provider-v8-20261106.md): native Responses, continuation,
  configuration conversion, scoped publication and model/recovery acceptance.

- [Retained user attachments](retained-user-attachments-20261104.md): long paste,
  immutable originals, queue editing, Fork/recovery and source/publication status.

- [Organization Skill market](organization-skill-market-20261101.md): compact internal
  discovery, local sharing, scoped installations and live member acceptance.
- [Initial message placement](input-placement-20261101.md): transcript-first sending,
  native-only pending list, exact retries and Web-only publication acceptance.
- [November 1 review repairs](review-repairs-20261101.md): source implementation,
  focused verification and matched publication status for R01–R08.
- [Full repository review, November 1](repository-review-20261101.md): accepted
  feature intent, eight findings, retained architecture, package coverage and
  fresh isolated verification; no runtime changes or deployment.
- [Same-Worker recovery](same-worker-recovery-20261031.md): execution-scoped
  duplicate/cancellation state, delayed cleanup and actual same-process recovery.
- [Run identity diagnosis and message controls](command-identity-and-message-controls-20261031.md):
  same-Worker recovery conflict, consistent copy actions and quiet historical paging.
- [Preparation timings and manual reading](timing-and-reader-20261031.md): bounded
  startup/model diagnostics, Tool disclosure, native selection and matched acceptance.
- [Human input and Skill market](skill-market-input-20261031.md): unified admission,
  immediate feedback, immutable Skill packages and matched live acceptance.

These records describe their named source revision, workload and deployment.
They preserve successes, failures and superseded experiments; they are not
current runtime or installation instructions. Read [Architecture](../ARCHITECTURE.md)
for current behavior and [ADRs](../adr/README.md) for decisions.

## Current capability refactor

- [Sandbox capability refactor](sandbox-capabilities-20261031.md)
- [Rivet / Pi on Cloudflare comparison](../research/pi-runtime-adapter-comparison-20261031.md)

## Recorded delivery and reviews

- [Delivery evidence](provider-continuation-20261030.md)
- [Fork machine inheritance](fork-machine-binding-20261030.md)
- [Collapsible sidebar subagents](sidebar-subagents-20261030.md)
- [Implementation and verification](peer-forks-20261030.md)
- [prompt amendment](fork-context-removal-20261030.md)
- [Measurements and verification](single-path-shared-forks-20261030.md)
- [measured isolated prototype](shared-history-root-prototype-20261029.md)
- [Before/after measurements](fork-performance-20261029.md)
- [First-request comparison](first-model-request-comparison-20261028.md)
- [Delivery and measured costs](independent-history-delivery-20261029.md)
- [Delivery evidence](interrupted-text-delivery-20261028.md)
- [Tool settlement presentation](tool-settlement-presentation-20261028.md)
- [Tool group display correction](tool-group-display-correction-20261027.md)
- [Retry notices and Tool disclosure](retry-tool-display-20261027.md)
- [Native model continuation](model-continuation-delivery-20261027.md)
- [Live-tail return and provider latency](live-tail-and-model-latency-20261027.md)
- [Historical edge loading](history-edge-repair-20261027.md)
- [Conversation layout repair](conversation-layout-repair-20261027.md)
- [Subagent workspace and scheduled-input implementation plan](subagent-and-schedules-implementation-plan-20261026.md)
- [Delivery evidence](scheduled-inputs-delivery-20261026.md)
- [Delivery evidence](android-execution-delivery-20261026.md)
- [Delivery evidence](conversation-history-delivery-20261026.md)
- [Conversation commands and model waiting](conversation-commands-delivery-20261026.md)
- [Model first-response investigation](model-first-response-investigation-20261025.md)
- [coupled header-wait adjustment](model-header-budget-delivery-20261025.md)
- [Revised input-command and waiting proposal](slash-commands-plan-20261025.md)
- [Delivery](control-plane-release-delivery-20261025.md)
- [Delivery evidence](image-tool-delivery-20261025.md)
- [Preview answer-link delivery](preview-answer-delivery-20261024.md)
- [initial-message handoff repair](input-handoff-delivery-20261024.md)
- [composer and pending-queue delivery](composer-queue-delivery-20261023.md)
- [Delivery evidence](resource-intent-delivery-20261023.md)
- [delivery record](elastic-retention-delivery-20261023.md)
- [delivery record](guest-browser-delivery-20261022.md)
- [Subagent wait investigation and Ultra proposal](subagent-wait-review-20261021.md)
- [delivery](runtime-capabilities-delivery-20261021.md)
- [Run status presentation update](run-status-presentation-20261021.md)
- [Delivery evidence](component-release-20261021.md)
- [delivery evidence](gpt61-sol-20261021.md)
- [October 20 resource/interaction repair plan](resource-interaction-repair-plan-20261020.md)
- [delivery record](resource-interaction-repair-20261020.md)
- [October 20 combined resource/interaction review](resource-interaction-review-20261020.md)
- [terminal/preview slice](terminal-preview-review-20261020.md)
- [October 20 upload/attachment review](upload-lifecycle-review-20261020.md)
- [October 20 Subagent lifecycle review](subagent-lifecycle-review-20261020.md)
- [October 20 Workspace/Volume lifecycle review](workspace-volume-lifecycle-review-20261020.md)
- [October 19 model request/response review](model-request-response-review-20261019.md)
- [approved repair](model-request-response-repair-plan-20261019.md)
- [Delivery evidence](model-runtime-repair-20261020.md)
- [October 19 replacement Worker → Pi Harness review](worker-harness-recovery-review-20261019.md)
- [proposed repair plan](worker-harness-recovery-repair-plan-20261019.md)
- [Implementation and verification](worker-harness-recovery-repair-20261019.md)
- [October 19 Kafka seal → Projector → PG review](seal-projection-review-20261019.md)
- [verification and delivery](seal-projection-repair-20261019.md)
- [October 19 Outbox → Kafka seal review](seal-publication-review-20261019.md)
- [Verification and delivery](seal-publication-repair-20261019.md)
- [October 18 Control Plane → PG expiry review](control-plane-expiry-review-20261018.md)
- [Verification and release](control-plane-expiry-repair-20261018.md)
- [October 18 Worker → PG Session lease review](worker-session-lease-review-20261018.md)
- [Verification and delivery](session-lease-repair-20261018.md)
- [October 18 Projector → browser review](projector-browser-review-20261018.md)
- [delivery record](projector-browser-repair-20261018.md)
- [October 18 Session publication/completion review](session-publication-completion-review-20261018.md)
- [Delivery evidence](session-publication-completion-repair-20261018.md)
- [October 18 Worker → Pi settlement review](worker-harness-settlement-review-20261018.md)
- [Delivery evidence](worker-harness-settlement-repair-20261018.md)
- [October 17 Kafka → Worker reply review](kafka-worker-reply-review-20261017.md)
- [Delivery evidence](worker-reply-repair-20261017.md)
- [Implementation and acceptance](tool-reply-continuation-20261016.md)
- [October 16 guest → Cubelet result review](guest-cubelet-result-review-20261016.md)
- [Isolated evidence](guest-cubelet-result-review-20261016.json)
- [delivery record](tool-receipt-preview-repair-20261016.md)
- [October 15 Cubelet → guest execution review](cubelet-guest-execution-review-20261015.md)
- [isolated evidence](cubelet-guest-execution-review-20261015.json)
- [repair record](cubelet-guest-execution-repair-20261015.md)
- [October 15 Projector → Cube handoff review](projector-cube-handoff-review-20261015.md)
- [evidence](projector-cube-handoff-review-20261015.json)
- [repair](projector-cube-handoff-repair-20261015.md)
- [October 15 Projector message review](projector-envelope-review-20261015.md)
- [measurements](projector-envelope-review-20261015.json)
- [repair record](projector-repair-20261015.md)
- [initial complete Control Plane outage check](control-plane-outage-20261014.md)
- [idle Worker reconnect research](idle-worker-reconnect-plan-20261014.md)
- [Delivery evidence](idle-worker-reconnect-20261014.md)
- [Implementation and release evidence](session-work-implementation-20261013.md)
- [native Kafka fencing study](kafka-native-fencing-20261013.md)
- [verification report](native-text-presentation-20261012.md)
- [Verification and matched deployment](native-input-current-state-20261012.md)
- [Verification and release status](native-live-state-20261012.md)
- [model TLS investigation](model-egress-tls-20261012.md)
- [append-only storage research/review](append-only-storage-review-20261011.md)
- [publication-registration optimization](publication-admission-20261011.md)
- [Source and live verification](worker-claim-repair-20261010.md)
- [Source, fault and real Luna/Cube/browser evidence](working-directory-20261010.md)
- [source checks and matched rollout](input-admission-20261010.md)
- [projection and commentary release](projection-release-20261009.md)
- [Source verification](projection-sql-20261009.md)
- [native input release](native-input-release-20261009.md)
- [claim-table review](worker-claim-schema-review-20261010.md)
- [Session/Turn/Run field review](session-turn-run-review-20261008.md)
- [native input and Run consolidation report](native-input-run-consolidation-20261009.md)
- [sandbox result/portability study](sandbox-result-portability-20261008.md)
- [DeepSeek V4.1 Flash smoke check](deepseek-flash-acceptance-20261007.md)
- [readiness release](readiness-release-20261007.md)
- [implementation and verification record](private-readiness-repair-20261007.md)
- [architecture value and evidence review](architecture-evidence-review-20261007.md)
- [October 7 recovery and boundary review](repository-review-20261007-r3.md)
- [source verification and release boundaries](review-repair-20261007-r3.md)
- [matched Tool Worker protocol 4 rollout](review-release-20261007-r3.md)
- [October 7 post-release review](repository-review-20261007-r2.md)
- [approved repair plan](review-repair-plan-20261007-r2.md)
- [source implementation](review-repair-20261007-r2.md)
- [matched rollout](review-release-20261007-r2.md)
- [October 7 whole-repository review](repository-review-20261007.md)
- [approved source repairs](review-repair-20261007.md)
- [matched schema 182 rollout](review-release-20261007.md)
- [October 7 template cleanup repair](template-cleanup-20261007.md)
- [October 6 post-Durable whole-repository review](repository-review-20261006.md)
- [approved source repairs](review-repair-20261006.md)
- [matched production release](review-release-20261006.md)
- [migration and release record](pi-durable-migration-20261006.md)
- [Pi Durable 1.0 evaluation](pi-durable-evaluation-20261005.md)
- [October 5 whole-repository follow-up review](repository-review-20261005.md)
- [approved improvements](review-improvements-20261005.md)
- [October 4 whole-repository review](repository-review-20261004.md)
- [The October 5 repair and release](review-repair-20261005.md)
- [Verification and matched release](vm-allocation-20261004.md)
- [Verification and matched release](pi-tool-cancellation-20261004.md)
- [Verification and rollout](paused-inputs-20261004.md)
- [detach-only acceptance](agent-stop-correction-20261004.md)
- [October 4 repair record](review-repair-20261004.md)
- [October 3 follow-up review](repository-review-20261003-r2.md)
- [October 3 repository review](repository-review-20261003.md)
- [repair record](review-repair-20261003.md)
- [quota removal and streaming-upload acceptance](unlimited-quotas-20261003.md)
- [K3s cutover](k3s-migration-20261003.md)
- [control recovery repair and acceptance](control-recovery-repair-20261003.md)
- [October 2 whole-repository review](repository-review-20261002.md)
- [October 2 ordered control views acceptance](ordered-control-views-20261002.md)
- [October 2 execution-probe acceptance](execution-probes-20261002.md)
- [September 30 repository review](repository-review-20260930.md)
- [October 1 repair record](repository-review-repair-20261001.md)
- [authority and Cube execution acceptance](cube-execution-authority-20261001.md)
- [production cutover](cube-execution-rollout-20261001.md)
- [Cube result-delivery study](cube-result-delivery-study-20261001.md)
- [chat attachment acceptance](user-input-attachments-20260929.md)
- [runtime status acceptance](runtime-status-acceptance-20260930.md)
- [Workspace recovery acceptance](workspace-rebind-acceptance-20260930.md)
- [direct Steer recovery acceptance](direct-steer-acceptance-20260930.md)
- [September 27 repository review](repository-review-20260927.md)
- [D1/D3/D4 follow-up](repository-review-followup-20260927.md)
- [native-log layout study](native-log-layout-study-20260927.md)
- [indexed-log acceptance](indexed-native-log-acceptance-20260927.md)
- [atomic Tool invocation acceptance](tool-invocation-acceptance-20260928.md)
- [runtime and SQL polish acceptance](runtime-polish-acceptance-20260928.md)
- [Pi-alignment review](pi-alignment-review-20260926.md)
- [repair and verification record](pi-alignment-repair-20260926.md)
- [image and GPT-6 acceptance](image-attachments-20260926.md)
- [Flash catalog amendment](flash-catalog-20260926.md)
- [CloudAgentRuntime review](cloud-agent-runtime-review-20260926.md)
- [lifecycle repair](cloud-agent-runtime-repair-20260926.md)
- [pending-input acceptance](queued-inputs-20260926.md)

## Research referenced by the earlier index

- [Scheduled Agent input proposal](../research/scheduled-agent-inputs-20261026.md)
- [Subagent resource-contract research](../research/subagent-resource-contracts-20261026.md)
- [adoption note](../research/tool-preview-rendering-20261016.md)
