# Runtime capacity evaluation

Issue #42 / [ADR-0326](adr/0326-runtime-capacity-benchmarks.md) measures the real
Pi/Worker/CLIProxyAPI/Kafka/Projector/PG/SSE path with a deterministic provider.
It measures this deployment's response to declared workloads, not model quality,
inference speed or a fleet-scale service guarantee.

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
  --mode soak --count 240 --rate 1 --output /private/capacity-soak
PI_CLOUD_RUNTIME_LOAD_CHECK=1 node --import tsx scripts/run-runtime-load.mjs \
  --mode faults --output /private/capacity-faults
```

Run these serially. Workload concurrency within one stage is intentional. The
matrix covers cold text tasks, paced responses, short/continuous MCP loops, a
small real Guest workload, larger input, native children, SSE viewers/history,
retained image results, arrival-rate bursts, provider-context expiry and native
compaction. Fault mode includes synthetic provider failures, Worker handoff,
Projector interruption, Stop and uncertain external results.
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
