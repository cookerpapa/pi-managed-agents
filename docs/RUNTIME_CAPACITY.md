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
  --mode matrix --output /private/capacity-matrix
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
continuation, and a controlled HTTP fallback case.

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
