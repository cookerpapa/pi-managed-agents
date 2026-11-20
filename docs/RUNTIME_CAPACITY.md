# Runtime capacity evaluation

The [synchronous ready-prefix study](reports/synchronous-ready-prefix-20261120.md) compares one-record transactions and bounded ready batches with the same PG checkpoint. It reports full-Run throughput, sender delay, cold reconstruction and the independent Kafka dispatch grant.

The [successive Run benchmark](reports/session-continuation-throughput-20261119.md) measures end-to-end PG settlement with bounded Worker history reuse across fresh native harnesses.

The [Worker sizing study](reports/worker-claim-sizing-20261119.md) measures batch admission, active root Sessions per Worker and the tradeoff between preferred claiming and a reusable history prefix.

The isolated [minimal Session lifecycle workload](../scripts/experiments/minimal-session-storage/README.md) exercises two tables, one Inbox per Conversation and the ordered end marker. [Measurement](reports/minimal-session-storage-load-20261119.md).

The [directed Worker wake experiment](reports/directed-worker-wake-20261119.md) compares PG claim queries, empty checks, queue delay and lost notifications. It distinguishes the two-Pod native execution measurements from the 1,000-connection queue load.

Issue #42 / [ADR-0326](adr/0326-runtime-capacity-benchmarks.md) measures the real
Pi/Worker/CLIProxyAPI/Kafka/Projector/PG/SSE path with a deterministic provider.
It measures this deployment's response to declared workloads, not model quality,
inference speed or a fleet-scale service guarantee.

The [November 17 publication delivery](reports/projector-admission-delivery-20261117.md)
repeats the workloads after shared Worker producers and Projector admission.
It separates sub-millisecond writer preparation from remaining downstream
settlement latency; rejected offered arrivals are distinguished from accepted work.

The [November 18 repair](reports/projector-prefetch-memory-delivery-20261118.md)
delivers bounded prefetch and identity caches, with actual reader counters,
memory/cleanup observations and native recovery. The 64-concurrency repetitions
improve some burst/tail measurements, but the sustained roughly 9–10-Run/s ceiling
remains. Fault reports now preserve failed operation/admission evidence before
cleanup removes their owned Runs.

The [consumer diagnosis](reports/projector-consumption-diagnosis-20261117.md)
then isolates native pause/refetch amplification and observes actual delivery gaps.
Its bounded reproduction uses an owned RF3 Topic without models or PG handlers:

```sh
PI_CLOUD_READER_DIAGNOSTIC=1 node --import tsx \
  scripts/run-projector-reader-diagnostic.mjs /private/reader-diagnostic.json
```

This is a consumer microbenchmark. Its buffered control does not implement the
production EOF, recovery or effect-dispatch contract and is not a rollout option.

With the final argument `batch`, the same diagnostic bundles the current checkout
and compares its single-record and bounded batch reads against identical preloaded
offsets. The [Session Projector measurements](reports/session-projector-batch-read-20261119.md)
also cover complete Runs, PG contention and tool dispatch confirmation.

The [direct PostgreSQL baseline](reports/projector-pg-path-baseline-20261119.md)
compares the same recorded Worker facts and bound writes through the database
and the real Projector, including per-query server and client timings.

The [CPU placement study](reports/projector-cpu-placement-20261119.md) repeats
the comparison with shared and disjoint CPU sets on disposable, matched-image
PG and Projector Pods and reports full-Run acceptance separately.

The [native PostgreSQL group-commit study](reports/pg-native-group-commit-20261119.md)
compares WAL sync count, projection backlog and full-Run tails under different
leader waits, keeping all twelve projection connections synchronous.

The [asynchronous commit study](reports/pg-async-commit-20261120.md) measures
client acknowledgements with synchronous commit on and off, the subsequent WAL
flush, and recovery following an immediate stop of an isolated test server.

For isolated cache/live-view memory checks (mock PG authority, no service changes):

```sh
node --expose-gc --import tsx scripts/run-projector-memory-check.mjs /private/projector-memory.json
```

This records forced-GC heap deltas, estimated cache budgets, hot lookup counts and
pending-text preservation across completed-holder eviction. It is not a total-RSS
or concurrent-Agent capacity guarantee.

The [November 16 repair measurements](reports/workload-release-throughput-20261116.md)
compare identical pre/post workloads, document removed history timeouts and
separate short-burst improvements from remaining sustained-rate/settlement limits.

## Run

Use the maintained deployment, a private output directory and an idle user
workload. The driver refuses unrelated active Runs and holds the existing release
operator lock. The owned provider requires a unique model and plan marker and
has no external model client. No paid provider is used by these modes.

```sh
PI_CLOUD_RUNTIME_LOAD_CHECK=1 node --import tsx scripts/run-runtime-load.mjs \
  --mode smoke --output /private/capacity-smoke
PI_CLOUD_RUNTIME_LOAD_CHECK=1 node --import tsx scripts/run-runtime-load.mjs \
  --mode http --output /private/capacity-http
PI_CLOUD_RUNTIME_LOAD_CHECK=1 node --import tsx scripts/run-runtime-load.mjs \
  --mode matrix --output /private/capacity-matrix
PI_CLOUD_RUNTIME_LOAD_CHECK=1 node --import tsx scripts/run-runtime-load.mjs \
  --mode capacity --output /private/capacity-upper-points
PI_CLOUD_RUNTIME_LOAD_CHECK=1 node --import tsx scripts/run-runtime-load.mjs \
  --mode projection-tail --output /private/projection-tail
PI_CLOUD_RUNTIME_LOAD_CHECK=1 node --import tsx scripts/run-runtime-load.mjs \
  --mode soak --count 240 --rate 1 --output /private/capacity-soak
PI_CLOUD_RUNTIME_LOAD_CHECK=1 node --import tsx scripts/run-runtime-load.mjs \
  --mode faults --output /private/capacity-faults
```

Run these serially. Workload concurrency within one stage is intentional. The
matrix covers cold text tasks, paced responses, short/continuous MCP loops, a
small real Guest workload, larger input, native children, SSE viewers/history,
retained image results, arrival-rate bursts, provider-context expiry and native
compaction. Fault mode includes synthetic provider failures, Worker handoff,
Projector interruption after visible text, Stop during text or Tools, and uncertain external results.

Set `PI_CLOUD_PROJECTION_TAIL_STAGE=arrivals-20` to run only the fixed arrival stage of the projection-tail profile. It reports the number actually issued out of 400 scheduled starts. Individual stages still use the normal fixture and cleanup.

The projection-tail profile measures serial latency, two 64-in-flight bursts,
400 arrivals scheduled at 20 per second, 128 in-flight clients with a five-second
model delay, and 32-in-flight multi-step work with three MCP calls per Run.
It counts which scheduled arrivals actually launched. After each timed stage it
joins each completed Run with its own root terminal timestamp. Reports distinguish
Worker terminal creation, the PG settlement timestamp inside the transaction, and
client receipt of the terminal SSE event. The SSE sample count is explicit.
`provider-faults` reruns only the two provider failure cases. With the pinned
Codex adapter/proxy, the synthetic 429 becomes a terminal usage-limit message,
and a lost upstream WebSocket is not automatically replayed. These checks require
terminal settlement followed by a successful new user request; they do not claim
automatic retry. Stop tests wait for native abort settlement before injecting a
late completion, since Stop admission alone does not order the final result.
The optional `capacity` extension raises concurrency to 64 and offered arrivals
to 16/32 per second. Run it only after checking the lower matrix's headroom.
Input padding stays below the public 100,000-character message limit; long
contexts are built through retained multi-turn history.

For projection-layout comparisons the diagnostic starts a private instance of the normal Session API with the candidate sources and the API's network policy. Only the owned load client uses its Pod address. After the timed stage it records counts of native system Entries, frozen frames, usage checkpoints, native journal rows and revision rows. Public usage and history are checked against the immutable assistant Entry.

The `live-burst` mode prepares 192 independent SSE subscribers before releasing all 192 short 4 KiB requests together. Compare each provider's completion with the matching Run and observe the consumer group's per-partition lag through final drainage. The standard fixture and cleanup checks apply.

Custom stages accept `--concurrency`, `--count`, `--rate`, `--kind text|mcp|guest|children`,
`--first-byte-ms`, `--chunk-ms`, `--output-bytes`, `--input-bytes`, `--rounds`,
`--views`, `--slow-reader-ms`, `--history` and `--tool echo|image`.

## Scope and resources

The fixture is a private Kubernetes service on the existing allowed test ports.
It uses the current application image plus bundled test code, rather than a new
production image or sandbox provider. CLIProxyAPI temporarily gains one uniquely
named API-key route; the projected model catalog gains its explicit synthetic
model. Original accounts, routes, defaults and credentials are retained. Calls
exercise CLIProxyAPI's real HTTP/full-context ingress and upstream WebSocket
continuation. The separate `http` mode selects a native HTTP-only provider route;
it does not assume arbitrary WebSocket failures authorize replay or fallback.

Tests create their own tenant, model profile, conversations, MCP connection and
Workspace. Guest commands are bounded and operate in that Workspace. Image
fixtures are valid generated PNG data, not user images. Synthetic metering has no
customer tariff, is checked for foreign models/nonzero charges and is removed
after source-delivery confirmation.

Private restoration receipts precede configuration mutation. SIGINT/SIGTERM stop
new arrivals and run cleanup. After an interrupted process, use the same directory:

```sh
PI_CLOUD_RUNTIME_LOAD_CHECK=1 node --import tsx scripts/run-runtime-load.mjs \
  --mode cleanup --output /private/capacity-matrix
```

The driver settles owned Runs, deletes owned conversations/Workspace through the
normal APIs, verifies physical/storage and accounting cleanup, restores catalog
and provider configuration and deletes fixture Kubernetes resources. A foreign
configuration edit or uncertain cleanup keeps the private receipt for inspection;
it does not authorize overwriting the edit or discarding ownership records.

## Interpret results

`report.json` contains workload settings, serving artifact inventory, host budget,
raw timing samples and reduced component/database measurements. Treat it as private:
it contains test resource IDs. Publish a content-free summary with the exact
revision/configuration and the scope of every result.

```sh
node scripts/summarize-runtime-load.mjs summary.json \
  /private/capacity-matrix/report.json /private/capacity-upper-points/report.json
```

- Offered, accepted and successfully completed work are separate. HTTP admission
  success does not mean an Agent finished. Cancelled fault cases are expected
  outcomes but are excluded from successful completion throughput.
- A history failure after native settlement is reported as `historyFailures`,
  preserving the completed Run's timing and state. It still fails workload
  acceptance. Successful page latencies exclude failed pages; the history error
  and total history duration remain explicit.
- Summary exports pair one-request Runs with provider timestamps to report
  `singleRequestPhases`. Multi-request, incomplete and inconsistent clock samples
  are excluded, rather than subtracting separately computed percentiles.
- The cold-task matrix includes public SSE setup/teardown and fresh Run ownership.
  Successful Runs/second includes batch drainage. Continuous tool loops separately
  measure active native work without repeated cold-start setup.
- First durable text includes the fixture's declared waiting/streaming and the
  durability/presentation path. Queue time comes from PG lifecycle timestamps.
  Never subtract separate percentiles to invent a platform-only percentile.
- Kafka observations use read-only Admin offsets and do not join a Session group.
  Lag is an offset distance, including protocol/transaction effects, not a count
  of chat messages. Report its sampling interval and observed drainage.
- Prometheus process samples and kubelet container samples have different scopes.
  Missing samples are explicit. The collector also records its own duration.
- Provider CPU, memory, active requests, bytes, cache/transport behavior and
  generator lateness reveal whether the test apparatus limits achieved load.
- The host guard stops increasing load below 3 GiB available memory or 30 GiB
  available disk. Runtime concurrency/durability limits are not silently changed.
  A highest tested point is a lower bound if no saturation was observed.

Original infrastructure, idle machines and maintenance loops share this single
host. PG/WAL and container observations include that background activity. Record
an idle baseline, and do not infer multi-node resilience or linear scaling.

## Component comparisons

After the live driver has cleaned up, run maintained component benchmarks one at
a time. They use isolated databases/brokers and measure different boundaries:

```sh
npm run eval:postgres-session-projection
PI_CLOUD_KAFKA_LOAD_CASES=concurrency-1,concurrency-16,concurrency-64 \
  PI_CLOUD_EVAL_OUTPUT=.cache/capacity-kafka.json npm run eval:kafka-accepted-facts
npm run eval:schedules -- .cache/capacity-schedules.json
```

The PG benchmark writes complete 1 KiB native entries. The Kafka benchmark uses
synthetic accepted facts rather than an Agent. The 10,000-plan scheduler check
measures admission and API responsiveness, not 10,000 active Agents. Do not merge
their throughput numbers with completed-Run throughput.
