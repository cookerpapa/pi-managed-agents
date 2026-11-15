import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile, writeFile, mkdir } from "node:fs/promises";
import { resolve } from "node:path";
import { cpus, totalmem } from "node:os";
import { performance } from "node:perf_hooks";
import { execFileSync } from "node:child_process";
import { setTimeout as delay } from "node:timers/promises";
import { sql } from "kysely";
import { createDatabase } from "../packages/database/src/index.ts";
import { PiCloudApi } from "../packages/web-ui/src/api.ts";
import { newIdempotencyKey } from "@pi-cloud/session-client";
import { benchmarkPrompt } from "../packages/fake-model-server/src/responses-server.ts";
import { S3AttachmentBlobStore } from "../packages/session-attachments/src/s3.ts";
import {
  createRuntimeLoadFixture,
  restoreRuntimeLoadFixture,
} from "./lib/runtime-load-fixture.mjs";
import {
  RuntimeLoadMetrics,
  runtimeInventory,
  hostResources,
} from "./lib/runtime-load-metrics.mjs";
import { runLoad, summarizeLoad, distribution } from "./lib/runtime-load-scheduler.mjs";
import {
  settleOwnedAcceptanceRuns,
  purgeOwnedAcceptanceTenant,
} from "./lib/owned-acceptance-cleanup.mjs";
import {
  runtimeDirectory,
  runtimeEnvironment,
  serviceAddress,
  kubectl,
  kubectlAsync,
} from "./lib/production-kubernetes.mjs";
import { serializeRelease } from "./lib/release-lock.mjs";
import { nativeHistory, nativeMessages } from "./lib/native-session-history.mjs";
import { waitForOwnedReceiptDelivery } from "./lib/source-delivery-barrier.mjs";

assert.equal(
  process.env.PI_CLOUD_RUNTIME_LOAD_CHECK,
  "1",
  "Authorize owned synthetic runtime load and its temporary configuration",
);
const args = process.argv.slice(2),
  argument = (name, fallback) => {
    const i = args.indexOf(name);
    return i < 0 ? fallback : args[i + 1];
  };
const allowed = new Set([
  "--output",
  "--mode",
  "--name",
  "--kind",
  "--concurrency",
  "--count",
  "--first-byte-ms",
  "--chunk-ms",
  "--output-bytes",
  "--input-bytes",
  "--rounds",
  "--tool",
  "--views",
  "--slow-reader-ms",
  "--rate",
  "--history",
]);
for (let i = 0; i < args.length; i++) {
  assert(allowed.has(args[i]), "Unknown benchmark option");
  if (args[i] !== "--history") {
    assert(args[i + 1] && !args[i + 1].startsWith("--"), "Benchmark option requires a value");
    i++;
  }
}
const directory = resolve(argument("--output", ".cache/runtime-load-" + Date.now()));
const mode = argument("--mode", "smoke");
assert(
  ["smoke", "http", "matrix", "capacity", "soak", "faults", "custom", "cleanup"].includes(mode),
  "Invalid load mode",
);
serializeRelease(runtimeDirectory);
await mkdir(directory, { recursive: true, mode: 0o700 });
const statePath = resolve(directory, "resources-private.json"),
  reportPath = resolve(directory, "report.json");
const databaseUrl = new URL(
  (await readFile(runtimeDirectory + "/secrets/database-url", "utf8")).trim(),
);
databaseUrl.hostname = serviceAddress("postgres");
const database = createDatabase({ connectionString: databaseUrl.toString(), maxConnections: 3 });
const baseUrl = `http://127.0.0.1:${runtimeEnvironment().PI_CLOUD_HTTP_PORT}`;
const terminal = (state) => ["completed", "failed", "cancelled", "timed_out"].includes(state);
async function until(read, label, ms = 120000) {
  const deadline = Date.now() + ms;
  while (Date.now() < deadline) {
    const value = await read();
    if (value) return value;
    await delay(300);
  }
  throw new Error(label + " timed out");
}
const report = {
  format: "pma.runtime-capacity.v1",
  at: new Date().toISOString(),
  mode,
  driverRevision: execFileSync("git", ["rev-parse", "HEAD"], { encoding: "utf8" }).trim(),
  dirty: execFileSync("git", ["status", "--porcelain"], { encoding: "utf8" }).trim().length > 0,
  hardware: {
    cpu: cpus()[0].model,
    logicalCpus: cpus().length,
    memoryBytes: totalmem(),
    platform: process.platform,
    architecture: process.arch,
    node: process.version,
  },
  measurements: [],
  accepted: false,
  cleanupConfirmed: false,
  realProviderRequests: false,
};
const save = () => writeFile(reportPath, JSON.stringify(report, null, 2) + "\n", { mode: 0o600 });
const phase = (name) => console.log(JSON.stringify({ phase: name, at: new Date().toISOString() }));
const stop = new AbortController();
for (const signal of ["SIGINT", "SIGTERM"])
  process.on(signal, () => stop.abort(new Error("Operator stopped arrivals")));
let fixture, tenant, api, project, mcpId, failure;
const sessions = [],
  runs = [];
const savePrivate = () =>
  writeFile(statePath, JSON.stringify({ tenant, project, mcpId, sessions, runs }) + "\n", {
    mode: 0o600,
  });
const settled = (id) =>
  until(async () => {
    const r = await api.getRun(id);
    return terminal(r.state) ? r : false;
  }, "Run settlement");

async function cleanup() {
  if (!tenant) {
    if (fixture) report.configuration = await fixture.close();
    else {
      const receipt = await readFile(resolve(directory, "fixture-private.json"), "utf8").catch(
        (e) => {
          if (e.code !== "ENOENT") throw e;
          return null;
        },
      );
      if (receipt) report.configuration = await restoreRuntimeLoadFixture(directory);
    }
    return;
  }
  const existing = await database
    .selectFrom("tenants")
    .select("id")
    .where("id", "=", tenant.tenantId)
    .executeTakeFirst();
  if (!existing) {
    report.configuration = fixture
      ? await fixture.close()
      : await restoreRuntimeLoadFixture(directory);
    return;
  }
  for (const session of sessions) {
    const rows = await database
      .selectFrom("runs")
      .select(["id", "turn_id"])
      .where("session_id", "=", session.sessionId)
      .execute();
    await settleOwnedAcceptanceRuns(api, session.sessionId, rows, settled);
    await api.deleteConversation(session.sessionId).catch((e) => {
      if (e.status !== 404) throw e;
    });
  }
  if (mcpId) {
    const { rows } =
      await sql`select revision::text,deleted_at from mcp_connections where id=${mcpId}::uuid and tenant_id=${tenant.tenantId}::uuid`.execute(
        database,
      );
    if (rows[0] && !rows[0].deleted_at) await api.removeMcpConnection(mcpId, rows[0].revision);
    await until(async () => {
      const { rows } =
        await sql`select 1 from tool_catalog_publication where scope=${"mcp:" + mcpId} and not(mcp->>'enabled')::boolean and generation=published_generation`.execute(
          database,
        );
      return rows.length > 0;
    }, "MCP withdrawal");
  }
  if (project) {
    await api.deleteWorkspace(project.workspaceId, newIdempotencyKey("load-cleanup"));
    await until(
      async () =>
        Boolean(
          (
            await database
              .selectFrom("workspaces")
              .select("storage_purged_at")
              .where("id", "=", project.workspaceId)
              .executeTakeFirst()
          )?.storage_purged_at,
        ),
      "Workspace cleanup",
    );
  }
  await until(
    async () =>
      !(
        await database
          .selectFrom("outbox")
          .select("id")
          .where("tenant_id", "=", tenant.tenantId)
          .where("published_at", "is", null)
          .execute()
      ).length,
    "Owned control settlement",
  );
  const delivered = await waitForOwnedReceiptDelivery({ tenantId: tenant.tenantId, database });
  const receiptModel =
    fixture?.model ??
    JSON.parse(await readFile(resolve(directory, "fixture-private.json"), "utf8")).model;
  const { rows: billing } =
    await sql`select count(distinct i.id)::int as calls,count(*) filter(where i.model<>${receiptModel})::int as foreign_models,
    count(*) filter(where coalesce(e.valuation->'customer'->>'amountPico','0')::numeric<>0)::int as charged,
    count(*) filter(where e.kind='usage')::int as usage_receipts
    from model_invocations i left join model_metering_events e on e.invocation_id=i.id where i.tenant_id=${tenant.tenantId}::uuid`.execute(
      database,
    );
  report.metering = billing[0];
  const { rows: delivery } = await sql`select count(*)::int as events,
    percentile_cont(0.50) within group(order by extract(epoch from(e.received_at-e.occurred_at))*1000) as p50_ms,
    percentile_cont(0.95) within group(order by extract(epoch from(e.received_at-e.occurred_at))*1000) as p95_ms,
    percentile_cont(0.99) within group(order by extract(epoch from(e.received_at-e.occurred_at))*1000) as p99_ms
    from model_metering_events e join model_invocations i on i.id=e.invocation_id
    where i.tenant_id=${tenant.tenantId}::uuid and e.kind='usage'`.execute(database);
  report.metering.deliveryLatency = delivery[0];
  assert.equal(billing[0].foreign_models, 0);
  assert.equal(billing[0].charged, 0);
  const blobs = new S3AttachmentBlobStore({
    endpoint: `http://${serviceAddress("session-images")}:8333`,
    region: "us-east-1",
    bucket: "pi-cloud-session-images",
    ...JSON.parse(
      await readFile(runtimeDirectory + "/secrets/session-image-s3-credentials.json", "utf8"),
    ),
  });
  try {
    report.cleanup = await purgeOwnedAcceptanceTenant(database, tenant, blobs, {
      awaitReceiptDelivery: async () => delivered,
    });
  } finally {
    await blobs.close();
  }
  report.configuration = fixture
    ? await fixture.close()
    : await restoreRuntimeLoadFixture(directory);
  await writeFile(statePath, JSON.stringify({ cleaned: true, tenantId: tenant.tenantId }) + "\n", {
    mode: 0o600,
  });
}

async function configureMcp() {
  const tool = (name) => ({
    name,
    description: "Owned capacity fixture " + name,
    inputSchema: { type: "object", properties: { text: { type: "string" } }, required: ["text"] },
  });
  await fixture.mcpTools(["echo", "image", "hold", "lost_write"].map(tool));
  mcpId = randomUUID();
  await savePrivate();
  await api.saveMcpConnection(mcpId, {
    expectedRevision: null,
    name: "Capacity fixture",
    endpoint: fixture.mcpUrl,
    visibility: "personal",
    enabled: true,
    allowedTools: null,
    credential: { kind: "bearer", token: fixture.key },
  });
  return until(async () => {
    const r = await api.getMcpConnection(mcpId);
    return r.connection.status === "ready"
      ? Object.fromEntries(r.tools.map((t) => [t.name, t.platformName]))
      : false;
  }, "MCP discovery");
}

async function newSession(title, thinkingLevel = "low") {
  const session = await api.createSession(
    project.projectId,
    project.workspaceId,
    title,
    "elastic",
    "starter",
    "/workspace",
    { provider: "openai-codex", modelId: fixture.model, thinkingLevel, fastMode: false },
  );
  sessions.push(session);
  await savePrivate();
  return session;
}

async function executeWork(
  session,
  plan,
  { views = 1, paddingBytes = 0, readHistory = false, slowReaderMs = 0, timeoutMs = 120000 } = {},
) {
  const streams = new AbortController(),
    ready = Promise.withResolvers(),
    events = [];
  let firstTextAt,
    visibleTerminalAt,
    textBytes = 0,
    toolResults = 0,
    sseReconnects = 0,
    submitting = 0,
    accepted;
  const onEvent = (event) => {
    if (!submitting || (accepted && event.turnId !== accepted.turnId)) return;
    if (event.type === "assistant.text.delta") {
      firstTextAt ??= Date.now();
      textBytes += Buffer.byteLength(event.payload.text);
    }
    if (event.type === "tool.completed") toolResults++;
    if (["turn.completed", "turn.failed", "turn.cancelled"].includes(event.type))
      visibleTerminalAt = Date.now();
    if (events.length < 5 && event.type === "turn.failed") events.push(event.payload);
  };
  const reader = (input, init) =>
    fetch(new URL(String(input), baseUrl), init).then((response) => {
      if (!slowReaderMs || !response.body || !response.ok) return response;
      return new Response(
        response.body.pipeThrough(
          new TransformStream({
            async transform(chunk, controller) {
              await delay(slowReaderMs);
              controller.enqueue(chunk);
            },
          }),
        ),
        { status: response.status, headers: response.headers },
      );
    });
  const subscriptions = Array.from({ length: views }, (_, i) =>
    api
      .streamSession({
        sessionId: session.sessionId,
        signal: streams.signal,
        onSnapshot: () => ready.resolve(),
        onEvent: i === 0 ? onEvent : () => {},
        onStatus: (status) => {
          if (status.phase === "reconnecting") sseReconnects++;
        },
      })
      .catch(() => {}),
  );
  // API uses its production client. A separate slow reader below exercises stream backpressure.
  let slow;
  if (slowReaderMs) {
    const { streamSessionEvents } = await import("@pi-cloud/session-client");
    slow = streamSessionEvents({
      sessionId: session.sessionId,
      signal: streams.signal,
      authorizationToken: tenant.credential.token,
      fetchImplementation: reader,
      onSnapshot: () => {},
      onEvent: () => {},
      onStatus: () => {},
    }).catch(() => {});
  }
  const setup = Date.now();
  let setupTimer;
  try {
    if (views)
      await Promise.race([
        ready.promise,
        new Promise((_, reject) => {
          setupTimer = setTimeout(() => reject(new Error("SSE setup timed out")), 15000);
          setupTimer.unref();
        }),
      ]);
    clearTimeout(setupTimer);
    submitting = Date.now();
    accepted = await api.acceptTurn(
      session.sessionId,
      benchmarkPrompt(plan, paddingBytes),
      newIdempotencyKey("capacity"),
      plan.kind === "children" ? "ultra" : "low",
    );
    const ack = Date.now();
    runs.push({ id: accepted.runId, turn_id: accepted.turnId, sessionId: session.sessionId });
    let result;
    // SSE is the normal completion signal; a coarse status fallback also covers
    // a snapshot reconnect that legitimately covers an already-settled event.
    const deadline = Date.now() + timeoutMs;
    let nextStatus = Date.now() + 1500;
    while (Date.now() < deadline) {
      if (visibleTerminalAt || Date.now() >= nextStatus) {
        result = await api.getRun(accepted.runId);
        if (terminal(result.state)) break;
        nextStatus = Date.now() + 1500;
      }
      await delay(40);
    }
    if (!result || !terminal(result.state))
      throw new Error("Accepted Run exceeded observation budget");
    const end = Date.now();
    let historyMs;
    if (readHistory) {
      const at = performance.now();
      await api.getConversation(session.sessionId);
      historyMs = performance.now() - at;
    }
    return {
      success: result.state === "completed",
      state: result.state,
      runId: accepted.runId,
      turnId: accepted.turnId,
      planId: plan.id,
      acceptedAt: ack,
      submittedAt: submitting,
      settledAt: Date.parse(result.settledAt ?? result.updatedAt),
      observedAt: end,
      acknowledgementMs: ack - submitting,
      queueMs: result.startedAt ? Date.parse(result.startedAt) - Date.parse(result.queuedAt) : null,
      settlementMs: (result.settledAt ? Date.parse(result.settledAt) : end) - submitting,
      firstTextMs: firstTextAt ? firstTextAt - submitting : null,
      firstTextAt,
      visibleTerminalAt,
      streamSetupMs: submitting - setup,
      textBytes,
      toolResults,
      sseReconnects,
      historyMs,
      ...(result.failure ? { error: result.failure.code } : {}),
      errors: events,
    };
  } catch (error) {
    return {
      success: false,
      ...(accepted ? { runId: accepted.runId, turnId: accepted.turnId } : {}),
      planId: plan.id,
      error: String(error.message).slice(0, 300),
    };
  } finally {
    clearTimeout(setupTimer);
    streams.abort();
    await Promise.allSettled([...subscriptions, ...(slow ? [slow] : [])]);
  }
}

async function stage(profile) {
  assert(
    Number.isSafeInteger(profile.count) && profile.count >= 1 && profile.count <= 2000,
    "Workload count outside the owned-host budget",
  );
  assert(
    Number.isSafeInteger(profile.concurrency) &&
      profile.concurrency >= 1 &&
      profile.concurrency <= 128,
    "Invalid workload concurrency",
  );
  assert(
    profile.arrivalRate === undefined || (profile.arrivalRate > 0 && profile.arrivalRate <= 100),
    "Invalid arrival rate",
  );
  assert(
    (profile.paddingBytes ?? 0) >= 0 && (profile.paddingBytes ?? 0) <= 96 * 1024,
    "Input padding must leave room for the plan within the public 100,000-character limit",
  );
  assert((profile.views ?? 1) >= 0 && (profile.views ?? 1) <= 8, "Invalid viewer count");
  phase(profile.name);
  const queue = [];
  for (let i = 0; i < profile.count; i++)
    queue.push(
      await newSession(profile.name + " " + i, profile.kind === "children" ? "ultra" : "low"),
    );
  const startProvider = await fixture.metrics(true),
    metrics = new RuntimeLoadMetrics({ database, tenantId: tenant.tenantId, fixture });
  await metrics.start();
  const began = Date.now();
  const signal = AbortSignal.any([stop.signal, metrics.abort.signal]);
  const load = await runLoad({
    mode: profile.arrivalRate ? "arrival" : "closed",
    concurrency: profile.concurrency,
    count: profile.count,
    rate: profile.arrivalRate,
    signal,
    work: async (index) =>
      executeWork(
        queue[index],
        {
          id: randomUUID(),
          kind: profile.kind ?? "text",
          firstByteMs: profile.firstByteMs ?? 100,
          chunkMs: profile.chunkMs ?? 0,
          chunkBytes: profile.chunkBytes ?? 256,
          outputBytes: profile.outputBytes ?? 2048,
          rounds: profile.rounds ?? 3,
          ...(profile.toolName ? { toolName: profile.toolName } : {}),
          toolDelayMs: profile.toolDelayMs ?? 0,
          children: profile.children ?? 2,
          ...(profile.fault ? { fault: profile.fault } : {}),
        },
        {
          views: profile.views ?? 1,
          paddingBytes: profile.paddingBytes ?? 0,
          readHistory: profile.readHistory ?? false,
          slowReaderMs: profile.slowReaderMs ?? 0,
        },
      ),
  });
  await metrics.stop();
  const endProvider = await fixture.metrics();
  const samples = endProvider.samples.filter((s) => s.startedAt >= began);
  const value = {
    profile,
    startedAt: began,
    summary: summarizeLoad(load),
    resources: metrics.summary(),
    provider: {
      requests: endProvider.sampled - startProvider.sampled,
      completed: endProvider.completed - startProvider.completed,
      errors: endProvider.errors - startProvider.errors,
      cpuSeconds:
        (endProvider.cpu.user +
          endProvider.cpu.system -
          (startProvider.cpu.user + startProvider.cpu.system)) /
        1e6,
      receivedBytes: endProvider.receivedBytes - startProvider.receivedBytes,
      sentBytes: endProvider.sentBytes - startProvider.sentBytes,
      incrementalRequests: samples.filter((s) => s.incremental).length,
      websocketRequests: samples.filter((s) => s.transport === "websocket").length,
      httpRequests: samples.filter((s) => s.transport === "http").length,
      maximumRss: endProvider.maximumRss,
      inflight: endProvider.inflight,
    },
    load,
    samples,
    metrics: metrics.samples,
  };
  value.historyReadsMs = distribution(load.results.map((r) => r.historyMs));
  report.measurements.push(value);
  await save();
  console.log(
    JSON.stringify({ stage: profile.name, ...value.summary, resources: value.resources }),
  );
  await savePrivate();
  if (load.results.some((r) => r.error === "Accepted Run exceeded observation budget"))
    stop.abort(new Error("Unsettled workload requires cleanup"));
  return value;
}

async function historyStage() {
  phase("retained-history-and-compaction");
  const session = await newSession("Retained synthetic history"),
    seed = [];
  for (let i = 0; i < 12; i++) {
    const r = await executeWork(
      session,
      { id: randomUUID(), kind: "text", outputBytes: 16384, chunkBytes: 2048 },
      { paddingBytes: 4096 },
    );
    assert(r.success, "History seed did not settle");
    seed.push(r);
  }
  const before = await fixture.metrics(),
    warm = await executeWork(
      session,
      { id: randomUUID(), kind: "text", outputBytes: 1024 },
      { readHistory: true },
    );
  assert(warm.success, "Warm continuation failed");
  await fixture.transport({ forgetResponses: true });
  const cold = await executeWork(
    session,
    { id: randomUUID(), kind: "text", outputBytes: 1024 },
    { readHistory: true },
  );
  assert(cold.success, "Expired provider context did not recover");
  const compact = await api.compactConversation(
    session.sessionId,
    "Summarize this synthetic capacity fixture briefly.",
    newIdempotencyKey("capacity-compact"),
  );
  const compactRun = await settled(compact.runId);
  assert.equal(compactRun.state, "completed", "Native compaction failed");
  const after = await executeWork(
    session,
    { id: randomUUID(), kind: "text", outputBytes: 1024 },
    { readHistory: true },
  );
  assert(after.success, "Post-compaction continuation failed");
  const final = await fixture.metrics();
  report.history = {
    seedRuns: seed.length,
    seedOutputBytes: 12 * 16384,
    warm,
    cold,
    after,
    requests: final.samples.filter((s) => s.startedAt >= before.samples.at(-1).finishedAt),
    outcomes: { warmContinuation: true, expiredContextFallback: true, nativeCompaction: true },
  };
  await save();
}

async function faultStage(kind, tools) {
  phase(kind);
  const count = 4,
    owned = [];
  for (let i = 0; i < count; i++) owned.push(await newSession(kind + " " + i));
  const before = await fixture.mcpState();
  const metrics = new RuntimeLoadMetrics({ database, tenantId: tenant.tenantId, fixture });
  await metrics.start();
  const began = performance.now(),
    plans = owned.map(() => ({
      id: randomUUID(),
      kind: kind === "projector-restart" ? "text" : "mcp",
      toolName: kind === "lost-tool-reply" ? tools.lost_write : tools.hold,
      rounds: 1,
      firstByteMs: kind === "projector-restart" ? 3000 : 100,
      outputBytes: kind === "projector-restart" ? 4096 : 1024,
      chunkBytes: 64,
      chunkMs: kind === "projector-restart" ? 100 : 0,
    }));
  const operations = owned.map((session, i) => executeWork(session, plans[i], { views: 2 }));
  let stoppedRuntime = false,
    evidence = {};
  const unrelated = async () =>
    assert.equal(
      (
        await database
          .selectFrom("runs")
          .select("id")
          .where("tenant_id", "!=", tenant.tenantId)
          .where("state", "not in", ["completed", "failed", "cancelled", "timed_out"])
          .execute()
      ).length,
      0,
      "Unrelated active work prevents component interruption",
    );
  try {
    if (kind !== "projector-restart")
      await until(
        async () => (await fixture.mcpState()).calls.length >= before.calls.length + count,
        "Owned Tool admission",
      );
    if (kind === "worker-handoff") {
      await unrelated();
      const target = await until(
        async () =>
          await database
            .selectFrom("runs as r")
            .innerJoin("workers as w", "w.id", "r.worker_id")
            .select(["r.id", "r.lease_id", "w.supervisor_id"])
            .where("r.session_id", "=", owned[0].sessionId)
            .where("r.state", "=", "running")
            .executeTakeFirst(),
        "Worker owner",
      );
      assert(/^pi-cloud-pi-worker-/.test(target.supervisor_id));
      kubectl([
        "delete",
        "pod",
        target.supervisor_id,
        "--grace-period=0",
        "--force",
        "--wait=false",
      ]);
      const replacement = await until(async () => {
        const r = await database
          .selectFrom("runs")
          .select(["lease_id", "state"])
          .where("id", "=", target.id)
          .executeTakeFirstOrThrow();
        return r.state === "running" && r.lease_id !== target.lease_id ? r : false;
      }, "Original Run recovery");
      evidence = { sameRun: true, writerChanged: replacement.lease_id !== target.lease_id };
      await fixture.releaseTools("PMA_ORIGINAL_REPLY");
      await metrics.refreshTargets();
    } else if (kind === "projector-restart") {
      await until(
        async () =>
          await database
            .selectFrom("runs")
            .select("id")
            .where("tenant_id", "=", tenant.tenantId)
            .where("state", "=", "running")
            .executeTakeFirst(),
        "Active text loop",
      );
      const owners = await database
        .selectFrom("runs")
        .select(["id", "lease_id"])
        .where(
          "session_id",
          "in",
          owned.map((s) => s.sessionId),
        )
        .execute();
      await unrelated();
      kubectl(["scale", "deployment/pi-cloud-session-runtime", "--replicas=0"]);
      stoppedRuntime = true;
      await delay(3500);
      await metrics.sample();
      kubectl(["scale", "deployment/pi-cloud-session-runtime", "--replicas=1"]);
      await kubectlAsync([
        "rollout",
        "status",
        "deployment/pi-cloud-session-runtime",
        "--timeout=120s",
      ]);
      stoppedRuntime = false;
      await metrics.refreshTargets();
      evidence.owners = owners;
    } else if (kind === "stop-under-load") {
      for (const session of owned.slice(0, 2)) {
        const r = await database
          .selectFrom("runs")
          .select(["id", "turn_id"])
          .where("session_id", "=", session.sessionId)
          .executeTakeFirstOrThrow();
        await api.cancelTurn(session.sessionId, r.turn_id, newIdempotencyKey("capacity-stop"));
      }
      await fixture.releaseTools("PMA_LATE_REPLY");
    }
    const results = await Promise.all(operations);
    const after = await fixture.mcpState();
    if (kind === "stop-under-load") {
      assert(results.slice(0, 2).every((r) => r.state === "cancelled"));
      assert(results.slice(2).every((r) => r.state === "completed"));
      for (const result of results.slice(0, 2))
        assert(
          !JSON.stringify(
            nativeMessages(await nativeHistory(database, { runId: result.runId })),
          ).includes("PMA_LATE_REPLY"),
        );
      evidence.cancelled = 2;
      evidence.lateReplyIgnored = true;
    } else
      assert(
        results.every((r) => r.success),
        kind + " did not settle every owned Run",
      );
    if (kind === "worker-handoff") {
      assert.equal(after.calls.length - before.calls.length, count);
      evidence.physicalCalls = count;
      for (const r of results)
        assert.equal(
          nativeMessages(await nativeHistory(database, { runId: r.runId })).filter(
            (m) => m.role === "toolResult" && JSON.stringify(m).includes("PMA_ORIGINAL_REPLY"),
          ).length,
          1,
        );
    }
    if (kind === "lost-tool-reply") {
      assert.equal(after.writes - before.writes, count);
      evidence.physicalWrites = count;
      for (const r of results)
        assert(
          nativeMessages(await nativeHistory(database, { runId: r.runId })).some(
            (m) =>
              m.role === "toolResult" &&
              m.isError &&
              JSON.stringify(m).includes("tool_result_unknown"),
          ),
        );
    }
    if (kind === "projector-restart") {
      const afterOwners = await database
        .selectFrom("runs")
        .select(["id", "lease_id"])
        .where(
          "id",
          "in",
          evidence.owners.map((r) => r.id),
        )
        .execute();
      assert(
        evidence.owners.every((r) =>
          afterOwners.some((a) => a.id === r.id && a.lease_id === r.lease_id),
        ),
        "Projector restart replaced a healthy writer",
      );
      evidence = {
        writersPreserved: true,
        sseReconnects: results.reduce((n, r) => n + r.sseReconnects, 0),
      };
    }
    await metrics.stop();
    const load = {
      mode: "fault",
      concurrency: count,
      requested: count,
      elapsedMs: performance.now() - began,
      maximumInFlight: count,
      dropped: [],
      results,
    };
    const entry = {
      profile: { name: kind, kind: "fault" },
      summary: summarizeLoad(load),
      expectedOutcomesPassed: true,
      evidence,
      resources: metrics.summary(),
      load,
      metrics: metrics.samples,
    };
    report.measurements.push(entry);
    await save();
    console.log(
      JSON.stringify({ stage: kind, passed: true, evidence, resources: entry.resources }),
    );
  } finally {
    if (stoppedRuntime) {
      kubectl(["scale", "deployment/pi-cloud-session-runtime", "--replicas=1"]);
      await kubectlAsync([
        "rollout",
        "status",
        "deployment/pi-cloud-session-runtime",
        "--timeout=120s",
      ]);
    }
    await fixture.releaseTools("PMA_CLEANUP").catch(() => {});
    await Promise.allSettled(operations);
    clearInterval(metrics.timer);
    await metrics.pending;
  }
}

try {
  if (mode === "cleanup") {
    const state = JSON.parse(await readFile(statePath, "utf8"));
    if (state.cleaned) {
      await restoreRuntimeLoadFixture(directory);
      await database.destroy();
      console.log(JSON.stringify({ cleanupConfirmed: true, alreadyCleaned: true }));
      process.exit(0);
    }
    tenant = state.tenant;
    project = state.project;
    mcpId = state.mcpId;
    sessions.push(...state.sessions);
    runs.push(...state.runs);
    api = new PiCloudApi(
      (path, init) => fetch(new URL(String(path), baseUrl), init),
      tenant.credential.token,
    );
    await cleanup();
    report.cleanupConfirmed = true;
    report.accepted = true;
  } else {
    assert.equal(
      (
        await database
          .selectFrom("runs")
          .select("id")
          .where("state", "not in", ["completed", "failed", "cancelled", "timed_out"])
          .execute()
      ).length,
      0,
      "Unrelated active Runs prevent load testing",
    );
    report.serving = JSON.parse(
      await readFile(runtimeDirectory + "/k3s/deployment.json", "utf8"),
    ).version;
    report.inventory = runtimeInventory();
    report.initialHost = await hostResources();
    assert(
      report.initialHost.memoryAvailableBytes > 5 * 1024 ** 3,
      "Insufficient host headroom for the first stage",
    );
    fixture = await createRuntimeLoadFixture(directory, { websockets: mode !== "http" });
    const { createPrivateTenant } =
      await import("../packages/control-plane/src/tenant-administration.ts");
    tenant = await createPrivateTenant(database, {
      slug: "release-acceptance-capacity-" + randomUUID(),
      ownerDisplayName: "Owned capacity evaluation",
      initialModel: { provider: "openai-codex", modelId: fixture.model },
    });
    api = new PiCloudApi(
      (path, init) => fetch(new URL(String(path), baseUrl), init),
      tenant.credential.token,
    );
    await savePrivate();
    await until(
      async () => (await api.getModelCatalog()).models.some((m) => m.modelId === fixture.model),
      "Model catalog propagation",
    );
    project = await api.createProject("Owned runtime capacity");
    await savePrivate();
    const tools = await configureMcp();
    const idle = new RuntimeLoadMetrics({ database, tenantId: tenant.tenantId, fixture });
    await idle.start();
    await delay(6000);
    await idle.stop();
    report.idleBaseline = { resources: idle.summary(), samples: idle.samples };
    const profiles =
      mode === "smoke" || mode === "http"
        ? [
            {
              name: "text-smoke",
              kind: "text",
              concurrency: 1,
              count: 2,
              firstByteMs: 100,
              chunkMs: 20,
            },
            {
              name: "mcp-smoke",
              kind: "mcp",
              toolName: tools.echo,
              concurrency: 1,
              count: 2,
              firstByteMs: 50,
              rounds: 2,
            },
          ]
        : mode === "capacity"
          ? [
              {
                name: "context-64k",
                kind: "text",
                concurrency: 4,
                count: 12,
                paddingBytes: 64 * 1024,
                firstByteMs: 100,
              },
              {
                name: "fast-text-64",
                kind: "text",
                concurrency: 64,
                count: 192,
                firstByteMs: 0,
                outputBytes: 4096,
              },
              ...[16, 32].map((rate) => ({
                name: "arrival-" + rate,
                kind: "text",
                concurrency: 64,
                arrivalRate: rate,
                count: rate * 30,
                firstByteMs: 300,
                outputBytes: 2048,
              })),
            ]
          : mode === "custom"
            ? [
                {
                  name: argument("--name", "custom"),
                  kind: argument("--kind", "text"),
                  concurrency: Number(argument("--concurrency", "4")),
                  count: Number(argument("--count", "16")),
                  firstByteMs: Number(argument("--first-byte-ms", "100")),
                  chunkMs: Number(argument("--chunk-ms", "0")),
                  outputBytes: Number(argument("--output-bytes", "2048")),
                  paddingBytes: Number(argument("--input-bytes", "0")),
                  rounds: Number(argument("--rounds", "3")),
                  toolName: argument("--tool", "echo") === "image" ? tools.image : tools.echo,
                  views: Number(argument("--views", "1")),
                  slowReaderMs: Number(argument("--slow-reader-ms", "0")),
                  readHistory: args.includes("--history"),
                  ...(argument("--rate") ? { arrivalRate: Number(argument("--rate")) } : {}),
                },
              ]
            : mode === "matrix"
              ? [
                  ...[1, 4, 8, 16, 32].map((concurrency) => ({
                    name: "fast-text-" + concurrency,
                    kind: "text",
                    concurrency,
                    count: Math.max(8, concurrency * 3),
                    firstByteMs: 0,
                    outputBytes: 4096,
                  })),
                  {
                    name: "paced-text",
                    kind: "text",
                    concurrency: 8,
                    count: 24,
                    firstByteMs: 1000,
                    chunkMs: 100,
                    chunkBytes: 64,
                    outputBytes: 1024,
                  },
                  {
                    name: "mcp-tools",
                    kind: "mcp",
                    toolName: tools.echo,
                    concurrency: 8,
                    count: 24,
                    firstByteMs: 100,
                    rounds: 3,
                  },
                  {
                    name: "continuous-tool-loops",
                    kind: "mcp",
                    toolName: tools.echo,
                    concurrency: 8,
                    count: 8,
                    firstByteMs: 100,
                    rounds: 32,
                    outputBytes: 1024,
                  },
                  {
                    name: "guest-tools",
                    kind: "guest",
                    concurrency: 4,
                    count: 8,
                    firstByteMs: 100,
                    rounds: 3,
                    toolDelayMs: 100,
                  },
                  {
                    name: "context-64k",
                    kind: "text",
                    concurrency: 4,
                    count: 12,
                    paddingBytes: 64 * 1024,
                    firstByteMs: 100,
                  },
                  {
                    name: "child-conversations",
                    kind: "children",
                    concurrency: 2,
                    count: 6,
                    children: 4,
                    firstByteMs: 300,
                    outputBytes: 512,
                  },
                  {
                    name: "sse-fanout-history",
                    kind: "text",
                    concurrency: 8,
                    count: 16,
                    views: 3,
                    slowReaderMs: 80,
                    readHistory: true,
                    firstByteMs: 100,
                    chunkMs: 30,
                  },
                  {
                    name: "mcp-images",
                    kind: "mcp",
                    toolName: tools.image,
                    concurrency: 4,
                    count: 8,
                    rounds: 1,
                    firstByteMs: 100,
                  },
                  ...[1, 2, 4, 8].map((rate) => ({
                    name: "arrival-" + rate,
                    kind: "text",
                    arrivalRate: rate,
                    concurrency: 32,
                    count: Math.max(16, rate * 45),
                    firstByteMs: 300,
                    outputBytes: 2048,
                  })),
                ]
              : mode === "soak"
                ? [
                    {
                      name: "bounded-soak",
                      kind: "mcp",
                      toolName: tools.echo,
                      concurrency: 8,
                      count: Number(argument("--count", "240")),
                      arrivalRate: Number(argument("--rate", "1")),
                      firstByteMs: 200,
                      chunkMs: 20,
                      rounds: 3,
                    },
                  ]
                : [
                    {
                      name: "once-rate-limit",
                      kind: "text",
                      concurrency: 4,
                      count: 8,
                      firstByteMs: 100,
                      fault: "rate_limit_once",
                    },
                    {
                      name: "once-disconnect",
                      kind: "text",
                      concurrency: 4,
                      count: 8,
                      firstByteMs: 100,
                      fault: "disconnect_once",
                    },
                  ];
    for (const profile of profiles) {
      if (stop.signal.aborted) break;
      await stage(profile);
    }
    if (mode === "matrix" && !stop.signal.aborted) await historyStage();
    if (mode === "http")
      assert(
        report.measurements.every(
          (m) => m.provider.httpRequests > 0 && m.provider.websocketRequests === 0,
        ),
        "Explicit HTTP route was not exercised",
      );
    if (mode === "faults" && !stop.signal.aborted)
      for (const kind of [
        "worker-handoff",
        "projector-restart",
        "stop-under-load",
        "lost-tool-reply",
      ])
        await faultStage(kind, tools);
    report.accepted =
      report.measurements.length === profiles.length + (mode === "faults" ? 4 : 0) &&
      report.measurements.every(
        (m) => (m.expectedOutcomesPassed || m.summary.failed === 0) && !m.resources.headroomStopped,
      );
    const drainage = new RuntimeLoadMetrics({ database, tenantId: tenant.tenantId, fixture });
    await drainage.start();
    await delay(6000);
    await drainage.stop();
    const last = drainage.samples.at(-1);
    report.postLoad = {
      resources: drainage.summary(),
      lastQueue: last.pg,
      lastKafkaLag: last.kafka?.unavailable ? null : last.kafka?.lag,
    };
    assert.equal(
      last.pg.running + last.pg.queued + last.pg.settling,
      0,
      "Owned Runs remained active after load drainage",
    );
  }
} catch (error) {
  failure = error;
  report.failure = String(error.message).slice(0, 600);
} finally {
  if (mode !== "cleanup")
    try {
      await cleanup();
      report.cleanupConfirmed = true;
    } catch (error) {
      report.cleanupFailure = String(error.message).slice(0, 600);
      failure ??= error;
    }
  await database.destroy();
  await save();
}
console.log(
  JSON.stringify({
    accepted: report.accepted,
    cleanupConfirmed: report.cleanupConfirmed,
    measurements: report.measurements.length,
    report: reportPath,
  }),
);
if (failure) throw failure;
if (!report.accepted) process.exitCode = 1;
