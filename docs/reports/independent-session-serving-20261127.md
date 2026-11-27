# Independent Session serving gate and cutover audit

The browser can submit a text Turn to the segmented queue through the authenticated Session API. A registered Worker with a current lease runs the pinned Pi Harness, calls CLIProxyAPI through its local model gateway and publishes to the Session’s Kafka partition. The Projector makes accepted deltas available over SSE and writes completed history and terminal coverage to PostgreSQL. A browser refresh and a newly registered Worker can read that history independently.

The gate uses an isolated database, a fresh four-partition RF3 topic and separate API, Projector, Worker and Web images. Production writers stay on the existing installation. The image test replaces Worker A **after the first Turn seals**. A separate fault gate is needed for a crash during generation or tool execution. [Decision](../adr/0375-independent-session-serving-entry.md) · [Acceptance runner](../../scripts/run-independent-session-serving-check.mjs).

## Baseline and failure checks

The real-provider test sends a random code through the browser, reloads the page, stops Worker A, starts Worker B with an empty local cache, and asks it to repeat the code without including it in the new input. It checks distinct released Worker leases, saved answers after refresh and the live stream. The deterministic test uses the same images and a local Responses server, queues 192 single-answer Turns through 64 HTTP clients, and also checks Stop during a deliberately delayed model response. The benchmark counts the 192 completed assistant answers. Sampled lag counts all Kafka records. The terminal p99 measures publication of the terminal fact through PG settlement. [Runner](../../scripts/run-independent-session-serving-check.mjs).

The real browser check recorded two streamed answers, two terminal events and two refreshes. PG recorded two different Worker identities. The second answer contained the previous Turn’s code. Both Turns used partition 3. An anonymous queue read returned 401, the unmapped usage route returned 501, and a second migration run preserved the existing Session.

Each diagnostic run below starts fresh with the same role images. There is one Worker with 64 slots. The upstream adds no artificial delay and each answer is 256 bytes. All four runs sealed 192 of 192 Turns and finished at zero lag. Raw lag was sampled every 250 ms. To identify actual claim transactions after the test, we group the physical Session epochs by their PG transaction ID. This works because the fixture claims each Session exactly once.

| Claim limit | Claim transactions | Largest actual batch | Answers/s | Queue to grant p95 | Longest first-to-last grant in one transaction | Terminal to projection p99 | Peak sampled raw lag |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 192 | 1 | 15.4 | 9.99 s | 0 ms | 320 ms | 12 |
| 4 | 49 | 4 | 47.7 | 2.36 s | 66 ms | 253 ms | 74 |
| 8 | 25 | 8 | 49.8 | 2.13 s | 147 ms | 223 ms | 63 |
| 32 | 10 | 32 | 19.2 | 6.55 s | 1162 ms | 663 ms | 53 |

The 32-way arm issued only ten successful transactions but held early grants until the rest of the batch could commit. In one transaction the first and last Turn start timestamps were 1.16 seconds apart. The code still obtains and checks each physical lease sequentially inside the transaction. The spread measures a lower bound on that transaction’s duration. The larger batch also releases work to the four-partition Projector in bursts. The experiment observes the longer projection tail; it does not separately attribute all of that time to PG, Kafka or CPU scheduling.

There was considerable host variability. An earlier pair on the same images gave 36.1 answers/s at limit 1 and 19.9 at limit 32. Treat these figures as local measurements. The isolated serving default is eight, with an operator limit between one and 32. Two eight-slot Workers racing for thirteen eligible Sessions returned eight and five unique grants in two selection queries. Each Session had its own lease. The directed-wake test also covered missed notifications and original lease closure. [Concurrency regression](../../scripts/experiments/minimal-session-storage/segment-wake.integration.spec.mjs).

## Review against agreed behavior

| Area | Connected in this installation | Next acceptance or missing connection |
| --- | --- | --- |
| Session admission and capacity | One authoritative PG queue, one notification listener, directed socket wakes and one independent physical lease per Session. A wake uses the free slots up to a batch limit of 32 and honors the warm-holder hint first. Candidate selection and transaction commit are batched. Lease checks and fencing epochs remain per Session. | Measure acquisition cost at larger fleet sizes. The heartbeat still updates individual active leases. Earlier boot-level renewal experiments do not change this path. [Worker](../../scripts/experiments/minimal-session-storage/independent-session-leases.mjs) · [Wake](../../scripts/experiments/minimal-session-storage/segment-wake.mjs). |
| Worker history and model | The claimed Turn supplies model configuration and logical environment. A different Worker can open settled PG history. The Worker uses the existing cache and model-gateway capability. | Exercise large image histories, SSD reuse, context status after Fork/compaction, changes to model and working directory, partial output after Stop, and cross-Worker provider incremental requests through the images. [Cache evidence](worker-ssd-continuation-20261127.md) · [Turn snapshots](../adr/0368-session-defaults-and-turn-snapshots.md). |
| Projection and recovery | The consumer handles records in partition order. The Projector opens the live endpoint after initial replay. Terminal coverage and the terminal event commit together. | Connect compatible-prefix batching to this consumer. It installs a per-record handler and the recorded-history policy. Exercise async PG crash recovery, multiple Projectors and non-reissue of external effects. Active Turns can still rewind to their **first** output offset. An expired active suffix currently fails recovery on its partition. A stuck old Turn and an actual partition gap need separate tests. [Consumer](../../scripts/experiments/minimal-session-storage/independent-session-runtime.mjs) · [Projection](../../scripts/experiments/minimal-session-storage/normal-root-projection.mjs). |
| Guest tools and resources | The model runner exposes a text-only tool list here. | First connect Bash to the real Guest through the executor, including a cold elastic allocation, cancellation and a dropped reply. Exercise replacement-Worker unknown outcomes, compute generation across sibling Forks and Workspace loss. Then cover read/edit, preview, images, Skills and alternate enabled providers. [Bash recovery](sealed-bash-unknown-20261125.md) · [Compute continuity](../adr/0347-conversation-compute-continuity.md). |
| Tool catalogue, MCP and subagents | Earlier candidate compositions verified named independent children, directed mail, HTTP MCP and retained implementations. | Connect their catalogue and execution ports to the image Worker. Check in-flight old versions, explicit disable, child creation returning after registration, explicit child-to-parent mail and executor replies across a handoff. Seven-day retention also needs deployable old implementations through an update. [MCP](../adr/0373-independent-session-mcp.md) · [Versions](../adr/0374-retained-tool-implementations.md). |
| Inputs, Web and Fork | Public root text input is registered atomically with its Turn. Pi input boundaries use the segmented mailbox. The browser reads the same PG history after reload. | Repeat steer/withdraw/reorder during generation and Stop, nested branch/fresh child navigation, subtree prune versus independent Web Fork, and pagination during streaming through the endpoints. Text admission rejects attachments. Connect retained originals, Workspace copies and explicit historical downloads. [Input boundary](../adr/0366-pg-input-boundaries.md) · [Navigation](../adr/0372-delegations-in-input-navigation.md). |
| Public commands, usage and schedules | Provider requests carry attribution. The isolated API returns an explicit unsupported response for usage, compact and legacy Run reads. | Read gateway receipts for /usage; verify /status using the current accessible context; connect active and idle /compact and scheduled messages to this queue. Check public Run consumers before switching clients. The Worker publishes native commits that can contain usage. Verify the intended Kafka policy at the correct native boundary. [Usage ownership](../adr/0355-provider-usage-outside-session-projection.md). |
| Installation and rollout | Re-runnable setup checks the installation version, topic and partition count. The current production API keeps its existing input controller. | Exercise multi-owner live forwarding, reader recovery gating, writer drain and rollback. Coordinate the supported migrations with accounts, provider configuration, credentials and retained files before a cutover. |

Two history boundaries matter for the cutover. Losing a Worker during a normal Bash leaves an unknown outcome if no canonical result had reached the sealed prefix. The successor must never run the original command again. A user Stop aborts Pi’s wait and requests physical cancellation; a lost acknowledgement leaves the process outcome uncertain. A new input after Stop can consume previously unread child mail. Prove those three behaviors separately. [Tool handoff](../adr/0359-sealed-bash-unknown.md) · [Placed inputs](../adr/0369-placed-input-navigation.md).

For a Web Fork, check the selected message boundary, retained configuration and source deletion. A child created with inherited context shares the appropriate frozen prefix but has an independent Session and lease. Removing a delegated subtree must preserve any separately forked Session and its referenced messages. Verify that appending to an active child does not touch its siblings or copy their history. [Flow ranges](../adr/0362-flat-session-fork-segments.md) · [Independent leases](../adr/0363-independent-agent-session-leases.md).

Read replicas and a scheduler that can suspend an explicit parent wait remain deferred. The current plan can validate the primary and the ordinary create-and-send child interaction without either feature.

The next capacity comparison should replay the same twenty-tool workload with 50 ms simulated acceptance delay before and after connecting batch projection. Record completed user-visible messages per second, input rate, raw partition lag over time, and last-visible-message to PG latency. The short text fixture measures the complete serving path with a different workload.

## Reproduce

Use a Linux Docker host with the repository’s dependencies, Chrome for the browser check, and the maintained control-plane, Worker and Web dependency images. The builder checks the pinned Pi, PG and Kafka client versions. Supply three existing base image references. The provider secret is mounted only in the Worker.

```sh
export PI_CLOUD_SEGMENT_CONTROL_BASE_IMAGE='<maintained control-plane image>'
export PI_CLOUD_SEGMENT_WORKER_BASE_IMAGE='<maintained Worker image>'
export PI_CLOUD_SEGMENT_WEB_BASE_IMAGE='<maintained Web image>'
export TSX_TSCONFIG_PATH="$PWD/packages/control-plane/tsconfig.json"
node scripts/build-independent-session-images.mjs text-canary
node --import tsx scripts/run-independent-session-serving-check.mjs text-canary --load
export PI_CLOUD_PROVIDER_GATEWAY_URL='<reachable CLIProxyAPI origin>'
export PI_CLOUD_PROVIDER_GATEWAY_API_KEY_FILE='<absolute path to read-only key>'
node --import tsx scripts/run-independent-session-serving-check.mjs text-canary
```
