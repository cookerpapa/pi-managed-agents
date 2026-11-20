import assert from "node:assert/strict";
import { createHash, randomUUID } from "node:crypto";
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
import {
  runLoad,
  summarizeLoad,
  distribution,
  summarizeSingleRequestPhases,
} from "./lib/runtime-load-scheduler.mjs";
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
import { kafkaProducerLane } from "../packages/runtime-core/src/kafka-accepted-fact.ts";

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
  [
    "smoke",
    "http",
    "matrix",
    "capacity",
    "projection-tail",
    "live-burst",
    "soak",
    "faults",
    "provider-faults",
    "custom",
    "cleanup",
  ].includes(mode),
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
const baseUrl =
  process.env.PI_CLOUD_LOAD_API_URL ??
  `http://127.0.0.1:${runtimeEnvironment().PI_CLOUD_HTTP_PORT}`;
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
report.sourceSha256 = Object.fromEntries(
  await Promise.all(
    [
      "scripts/run-runtime-load.mjs",
      "scripts/lib/runtime-load-fixture.mjs",
      "scripts/lib/runtime-load-metrics.mjs",
      "scripts/lib/runtime-load-scheduler.mjs",
      "packages/fake-model-server/src/responses-server.ts",
      "packages/control-plane/test/mcp/http-fixture.ts",
    ].map(async (path) => [
      path,
      createHash("sha256")
        .update(await readFile(path))
        .digest("hex"),
    ]),
  ),
);
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
  const { rows: eligibility } = await sql`select
    count(*) filter(where e.customer_eligibility='billable')::int as billable,
    count(*) filter(where e.customer_eligibility='platform')::int as platform,
    count(*) filter(where e.customer_eligibility='pending')::int as pending
    from model_billing_events e join model_invocations i on i.id=e.invocation_id
    where i.tenant_id=${tenant.tenantId}::uuid and e.kind='usage'`.execute(database);
  report.metering.eligibility = eligibility[0];
  const { rows: delivery } = await sql`select count(*)::int as events,
    percentile_cont(0.50) within group(order by extract(epoch from(e.received_at-e.occurred_at))*1000) as p50_ms,
    percentile_cont(0.95) within group(order by extract(epoch from(e.received_at-e.occurred_at))*1000) as p95_ms,
    percentile_cont(0.99) within group(order by extract(epoch from(e.received_at-e.occurred_at))*1000) as p99_ms
    from model_metering_events e join model_invocations i on i.id=e.invocation_id
    where i.tenant_id=${tenant.tenantId}::uuid and e.kind='usage'`.execute(database);
  report.metering.deliveryLatency = delivery[0];
  assert.equal(billing[0].foreign_models, 0);
  assert.equal(billing[0].charged, 0);
  if (mode === "smoke" || mode === "http") {
    assert.equal(
      eligibility[0].billable,
      billing[0].usage_receipts,
      "Every successful smoke request needs exact accepted native model evidence",
    );
    assert.equal(eligibility[0].platform + eligibility[0].pending, 0);
  }
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
  {
    views = 1,
    paddingBytes = 0,
    readHistory = false,
    slowReaderMs = 0,
    timeoutMs = 120000,
    beforeSubmit,
    onText,
  } = {},
) {
  const streams = new AbortController(),
    ready = Promise.withResolvers(),
    events = [];
  let firstTextAt,
    visibleTerminalAt,
    textBytes = 0,
    toolResults = 0,
    sseReconnects = 0,
    lastStreamStatus,
    streamFailures = [],
    submitting = 0,
    accepted;
  const onEvent = (event) => {
    if (!submitting || (accepted && event.turnId !== accepted.turnId)) return;
    if (event.type === "assistant.text.delta") {
      firstTextAt ??= Date.now();
      textBytes += Buffer.byteLength(event.payload.text);
      if (event.payload.text) onText?.(event);
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
          lastStreamStatus = status;
          if (status.phase === "reconnecting") sseReconnects++;
        },
      })
      .catch((error) => {
        if (!streams.signal.aborted && streamFailures.length < 4)
          streamFailures.push({ reader: i, error: String(error.message).slice(0, 300) });
      }),
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
    if (beforeSubmit) await beforeSubmit();
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
    let historyMs, historyPages, historyError;
    const historyPageMs = [];
    if (readHistory) {
      const at = performance.now();
      let before;
      try {
        for (historyPages = 0; historyPages < 4;) {
          const pageAt = performance.now();
          const page = await api.getConversation(session.sessionId, before ? { before } : {});
          historyPageMs.push(performance.now() - pageAt);
          historyPages++;
          if (!page.history.hasBefore) break;
          assert(
            page.history.before && page.history.before !== before,
            "History cursor did not advance",
          );
          before = page.history.before;
        }
      } catch (error) {
        // A failed history read must not erase a Run's already observed outcome.
        historyError = String(error.code ?? error.message).slice(0, 300);
      }
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
      historyPages,
      historyPageMs,
      ...(historyError ? { historyError } : {}),
      ...(result.failure ? { error: result.failure.code } : {}),
      errors: events,
    };
  } catch (error) {
    return {
      success: false,
      ...(accepted ? { runId: accepted.runId, turnId: accepted.turnId } : {}),
      planId: plan.id,
      error: String(error.message).slice(0, 300),
      stage: accepted ? "run_observation" : submitting ? "input_admission" : "stream_setup",
      streamSetupMs: (submitting || Date.now()) - setup,
      sseReconnects,
      lastStreamStatus,
      streamFailures,
    };
  } finally {
    clearTimeout(setupTimer);
    streams.abort();
    await Promise.allSettled([...subscriptions, ...(slow ? [slow] : [])]);
  }
}

function projectionTailProfiles(tools) {
  const text = { kind: "text", firstByteMs: 0, outputBytes: 4096 };
  const profiles = [
    { ...text, name: "serial", concurrency: 1, count: 24 },
    { ...text, name: "warm", concurrency: 16, count: 32 },
    ...[1, 2].map((n) => ({ ...text, name: "burst64-" + n, concurrency: 64, count: 192 })),
    { ...text, name: "arrivals-20", arrivalRate: 20, concurrency: 64, count: 400 },
    {
      ...text,
      name: "waiting-128",
      concurrency: 128,
      count: 128,
      firstByteMs: 5000,
      outputBytes: 1024,
    },
    {
      name: "three-tools",
      kind: "mcp",
      toolName: tools.echo,
      rounds: 3,
      concurrency: 32,
      count: 64,
      firstByteMs: 0,
      outputBytes: 1024,
    },
  ];
  const requested = process.env.PI_CLOUD_PROJECTION_TAIL_STAGE;
  if (!requested) return profiles;
  const profile = profiles.find((p) => p.name === requested);
  assert(profile, "Unknown projection-tail stage");
  return [profile];
}

/** Inspect only the owned text fixtures, after their timed work has finished. */
async function verifyTextStorage(sessions) {
  const {
    rows: [layout],
  } = await sql`with usage_docs as materialized (
    select r.tenant_id,r.session_id,r.id from pi_durable_owned_records r
    where r.tenant_id=${tenant.tenantId}::uuid and r.kind='document'
      and r.retired_seq is null and r.fields->>'nativeKind'='"pi.usage"'
  ), system_entries as materialized (
    select g.id from pi_history_records g where g.tenant_id=${tenant.tenantId}::uuid
      and g.record_kind='entry' and g.fields->>'nativeKind'='"pi.system"'
  ) select (select count(*)::int from usage_docs) as usage_docs,
    (select count(*)::int from usage_docs u join pi_durable_current c using(tenant_id,session_id,id)) as usage_checkpoints,
    (select count(*)::int from usage_docs u join pi_durable_document_revisions d on (d.tenant_id,d.session_id,d.document_id)=(u.tenant_id,u.session_id,u.id)) as usage_revisions,
    (select count(*)::int from usage_docs u join pi_history_records g on (g.tenant_id,g.session_id,g.record_id)=(u.tenant_id,u.session_id,u.id) where g.record_kind like 'document.%') as usage_journal_rows,
    (select count(*)::int from system_entries) as native_system_entries,
    (select count(*)::int from pi_history_frames f join system_entries s on s.id=f.entry_record_id where f.tenant_id=${tenant.tenantId}::uuid) as system_frames`.execute(
    database,
  );
  assert.equal(layout.usage_docs, sessions.length);
  if (process.env.PI_CLOUD_LOAD_CHECK_INITIAL_WORLD === "1") {
    const {
      rows: [joined],
    } = await sql`select count(*)::int as n from pi_durable_owned_records w
      join pi_durable_owned_records r using(tenant_id,session_id,created_seq)
      where w.tenant_id=${tenant.tenantId}::uuid and w.kind='document' and r.kind='document'
      and w.fields->>'nativeKind'='"pi-cloud.runtime_world_state"' and r.fields->>'nativeKind'='"pi-cloud.run"'`.execute(
      database,
    );
    assert.equal(
      joined.n,
      sessions.length,
      "Each fresh root must commit world and Run configuration together",
    );
    layout.initialWorldWithRun = joined.n;
  }
  assert.equal(layout.native_system_entries, sessions.length);
  // Both sides of an A/B run verify their own public API against the raw
  // context baseline in the corresponding immutable native assistant Entry.
  const checks = [0, Math.floor(sessions.length / 2), sessions.length - 1];
  for (const index of checks) {
    const sessionId = sessions[index].sessionId;
    const {
      rows: [stored],
    } = await sql`select b.payload_text from pi_history_records g join pi_history_bodies b
      on (b.tenant_id,b.id)=(g.tenant_id,g.entry_body_id)
      where g.tenant_id=${tenant.tenantId}::uuid and g.session_id=${sessionId}
        and g.fields->>'role'='assistant'`.execute(database);
    const message = JSON.parse(stored.payload_text).model[0];
    const status = await api.getConversationStatus(sessionId);
    assert.equal(status.context.method, "latest_usage");
    assert.equal(status.context.estimatedTokens, message.usage.totalTokens);
    const history = await api.getConversation(sessionId);
    const text = history.turns
      .flatMap((t) => t.transcript?.items ?? [])
      .filter((i) => i.kind === "text")
      .map((i) => i.text)
      .join("");
    assert.equal(
      text,
      message.content
        .filter((p) => p.type === "text")
        .map((p) => p.text)
        .join(""),
    );
  }
  return { ...layout, publicStatusAndHistoryChecks: checks.length };
}

async function stage(profile) {
  assert(
    Number.isSafeInteger(profile.count) && profile.count >= 1 && profile.count <= 2000,
    "Workload count outside the owned-host budget",
  );
  assert(
    Number.isSafeInteger(profile.concurrency) &&
      profile.concurrency >= 1 &&
      (profile.concurrency <= 128 ||
        (mode === "live-burst" && profile.count === 192 && profile.concurrency === 192)),
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
  const initialCalls =
    mode === "projection-tail" && profile.kind === "mcp"
      ? (await fixture.mcpState()).calls.length
      : undefined;
  const startProvider = await fixture.metrics(true),
    metrics = new RuntimeLoadMetrics({ database, tenantId: tenant.tenantId, fixture });
  await metrics.start();
  const began = Date.now();
  const signal = AbortSignal.any([stop.signal, metrics.abort.signal]);
  // Give every client an established view before timing a single simultaneous wave.
  const wave = Promise.withResolvers();
  let readyClients = 0,
    waveTimeout;
  const beforeSubmit =
    mode === "live-burst"
      ? async () => {
          if (++readyClients === 1) {
            waveTimeout = setTimeout(() => wave.resolve(false), 25_000);
            waveTimeout.unref();
          }
          if (readyClients === profile.count) {
            clearTimeout(waveTimeout);
            wave.resolve(true);
          }
          assert(await wave.promise, "All synchronized clients must establish their SSE view");
          signal.throwIfAborted();
        }
      : undefined;
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
          beforeSubmit,
          views: profile.views ?? 1,
          paddingBytes: profile.paddingBytes ?? 0,
          readHistory: profile.readHistory ?? false,
          slowReaderMs: profile.slowReaderMs ?? 0,
        },
      ),
  });
  await metrics.stop();
  for (const result of load.results.filter((r) => !r.success && r.runId)) {
    result.nativeErrors = nativeMessages(await nativeHistory(database, { runId: result.runId }))
      .filter((m) => m.role === "assistant" && m.errorMessage)
      .map((m) => String(m.errorMessage).slice(0, 300))
      .slice(-4);
  }
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
  if (mode === "projection-tail") {
    // Read only after the timed work. Worker creates this event before Kafka
    // publication; runs.settled_at is set inside the final PG transaction.
    const results = load.results.filter((r) => r.success && r.runId);
    const { rows } = await sql`select e.run_id,e.occurred_at from session_terminal_events e
      join runs r on (r.tenant_id,r.id,r.session_id,r.turn_id)=(e.tenant_id,e.run_id,e.session_id,e.turn_id)
      where r.tenant_id=${tenant.tenantId}::uuid and r.id=any(${results.map((r) => r.runId)}::uuid[])
      and e.type='turn.completed'`.execute(database);
    const created = new Map(rows.map((r) => [r.run_id, r.occurred_at.getTime()]));
    assert.equal(created.size, results.length, "Incomplete root terminal evidence");
    value.tail = {
      ...summarizeSingleRequestPhases(results, samples),
      terminalCreationToSettlementMs: distribution(
        results.map((r) => r.settledAt - created.get(r.runId)),
      ),
      submitToVisibleTerminalMs: distribution(
        results
          .filter((r) => Number.isFinite(r.visibleTerminalAt))
          .map((r) => r.visibleTerminalAt - r.submittedAt),
      ),
    };
    if (initialCalls !== undefined) {
      const calls = (await fixture.mcpState()).calls.length - initialCalls;
      value.tail.physicalToolCalls = calls;
      assert.equal(calls, results.length * profile.rounds, "Multi-step result scope changed");
      assert.equal(
        samples.length,
        results.length * (profile.rounds + 1),
        "Multi-step model request count changed",
      );
    }
  }
  if (mode === "live-burst" && load.results.every((r) => r.success))
    value.textStorage = await verifyTextStorage(queue);
  value.historyReadsMs = distribution(load.results.map((r) => r.historyMs));
  value.historyPages = {
    count: load.results.reduce((n, r) => n + (r.historyPages ?? 0), 0),
    latencyMs: distribution(load.results.flatMap((r) => r.historyPageMs ?? [])),
  };
  if (profile.fault) {
    const expected =
      profile.fault === "rate_limit_once" ? /usage limit/i : /upstream_websocket_lost/i;
    assert(
      load.results.every(
        (r) => r.state === "failed" && r.nativeErrors?.some((message) => expected.test(message)),
      ),
      "Injected provider error did not settle with the pinned adapter's expected classification",
    );
    phase(profile.name + "-new-request-recovery");
    const recovery = await runLoad({
      concurrency: profile.concurrency,
      count: profile.count,
      signal: stop.signal,
      work: (index) =>
        executeWork(queue[index], {
          id: load.results[index].planId,
          kind: "text",
          fault: profile.fault,
          outputBytes: profile.outputBytes ?? 2048,
        }),
    });
    assert(
      recovery.results.length === profile.count && recovery.results.every((r) => r.success),
      "A new user request did not recover after the injected provider failure",
    );
    value.expectedOutcomesPassed = true;
    value.evidence = {
      terminalFailure: profile.count,
      subsequentUserRequestsCompleted: profile.count,
      automaticReplayClaimed: false,
    };
    value.recovery = { summary: summarizeLoad(recovery), load: recovery };
  }
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
  assert(warm.success && !warm.historyError, "Warm continuation or history read failed");
  await fixture.transport({ forgetResponses: true });
  const cold = await executeWork(
    session,
    { id: randomUUID(), kind: "text", outputBytes: 1024 },
    { readHistory: true },
  );
  assert(cold.success && !cold.historyError, "Expired provider context or history read failed");
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
  assert(
    after.success && !after.historyError,
    "Post-compaction continuation or history read failed",
  );
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

async function idlePartitionStreams() {
  phase("idle-partition-streams");
  const workload = report.inventory.find((w) => w.name === "pi-cloud-session-runtime");
  const partitions = Number(
    workload?.containers
      .flatMap((c) => c.configuration)
      .find((v) => v.name === "PI_CLOUD_KAFKA_PARTITIONS")?.value,
  );
  assert(
    Number.isSafeInteger(partitions) && partitions > 0 && partitions <= 64,
    "Idle stream coverage exceeds this host's declared test budget",
  );
  const selected = new Map();
  for (let attempts = 0; selected.size < partitions && attempts < partitions * 16; attempts++) {
    const session = await newSession("Idle partition snapshot");
    const native = await database
      .selectFrom("sessions")
      .select("pi_session_id")
      .where("id", "=", session.sessionId)
      .executeTakeFirstOrThrow();
    const partition = kafkaProducerLane(native.pi_session_id, partitions);
    if (!selected.has(partition)) selected.set(partition, session);
  }
  assert.equal(selected.size, partitions, "Owned Sessions did not cover every partition");
  const streams = new AbortController(),
    subscriptions = [];
  try {
    const times = await Promise.all(
      [...selected].map(async ([partition, session]) => {
        const ready = Promise.withResolvers(),
          started = Date.now();
        const timer = setTimeout(
          () => ready.reject(new Error("Idle partition snapshot timed out: " + partition)),
          15000,
        );
        subscriptions.push(
          api
            .streamSession({
              sessionId: session.sessionId,
              signal: streams.signal,
              onSnapshot: () => ready.resolve(Date.now() - started),
              onEvent() {},
              onStatus() {},
            })
            .catch((error) => ready.reject(error)),
        );
        try {
          return await ready.promise;
        } finally {
          clearTimeout(timer);
        }
      }),
    );
    return { idlePartitionsReady: selected.size, idleSnapshotMaximumMs: Math.max(...times) };
  } finally {
    streams.abort();
    await Promise.allSettled(subscriptions);
  }
}

async function faultStage(kind, tools) {
  phase(kind);
  const count = 4,
    owned = [];
  for (let i = 0; i < count; i++) owned.push(await newSession(kind + " " + i));
  const before = await fixture.mcpState();
  const metrics = new RuntimeLoadMetrics({ database, tenantId: tenant.tenantId, fixture });
  await metrics.start();
  const streaming = kind === "projector-restart" || kind === "live-stop";
  const began = performance.now(),
    plans = owned.map(() => ({
      id: randomUUID(),
      kind: streaming ? "text" : "mcp",
      toolName: kind === "lost-tool-reply" ? tools.lost_write : tools.hold,
      rounds: 1,
      firstByteMs: streaming ? 3000 : 100,
      outputBytes: streaming ? 4096 : 1024,
      chunkBytes: 64,
      chunkMs: streaming ? 100 : 0,
    }));
  const visible = owned.map(() => ({ ids: new Set(), prefix: "" }));
  const operations = owned.map((session, i) =>
    executeWork(session, plans[i], {
      views: 2,
      onText: streaming
        ? (event) => {
            const v = visible[i];
            if (v.ids.size >= 2 || v.ids.has(event.eventId)) return;
            v.ids.add(event.eventId);
            v.prefix = event.payload.replace ? event.payload.text : v.prefix + event.payload.text;
          }
        : undefined,
    }),
  );
  let stoppedRuntime = false,
    evidence = {},
    stageError;
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
    if (!streaming)
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
        async () => visible.some((v) => v.ids.size === 2 && Buffer.byteLength(v.prefix) < 4096),
        "Visible partial answer before Projector restart",
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
    } else if (kind === "live-stop") {
      await until(
        async () =>
          visible.slice(0, 2).every((v) => v.ids.size === 2 && Buffer.byteLength(v.prefix) < 4096),
        "Visible partial answers before Stop",
      );
      for (const session of owned.slice(0, 2)) {
        const r = await database
          .selectFrom("runs")
          .select("turn_id")
          .where("session_id", "=", session.sessionId)
          .executeTakeFirstOrThrow();
        await api.cancelTurn(session.sessionId, r.turn_id, newIdempotencyKey("visible-text-stop"));
      }
    } else if (kind === "stop-under-load") {
      const cancelledRuns = [];
      for (const session of owned.slice(0, 2)) {
        const r = await database
          .selectFrom("runs")
          .select(["id", "turn_id"])
          .where("session_id", "=", session.sessionId)
          .executeTakeFirstOrThrow();
        await api.cancelTurn(session.sessionId, r.turn_id, newIdempotencyKey("capacity-stop"));
        cancelledRuns.push(r.id);
      }
      // Admission of Stop is not native abort settlement. Inject a genuinely
      // late reply only after the Worker has committed its cancellation outcome.
      await Promise.all(cancelledRuns.map(settled));
      await fixture.releaseTools("PMA_LATE_REPLY");
    }
    const results = await Promise.all(operations);
    const after = await fixture.mcpState();
    report.faultAttempts ??= [];
    report.faultAttempts.push({
      kind,
      results,
      evidence,
      physicalCalls: after.calls.length - before.calls.length,
      physicalWrites: after.writes - before.writes,
    });
    await save();
    if (kind === "live-stop") {
      assert(
        results.slice(0, 2).every((r) => r.state === "cancelled") &&
          results.slice(2).every((r) => r.success),
      );
      for (const [i, r] of results.slice(0, 2).entries()) {
        const messages = nativeMessages(await nativeHistory(database, { runId: r.runId }));
        assert(
          messages.some(
            (m) =>
              m.role === "assistant" &&
              m.stopReason === "aborted" &&
              m.content
                .filter((b) => b.type === "text")
                .map((b) => b.text)
                .join("")
                .startsWith(visible[i].prefix),
          ),
          "Aborted native message lost already-visible text",
        );
      }
      evidence = {
        cancelled: 2,
        verifiedVisiblePrefixBytes: visible.slice(0, 2).map((v) => Buffer.byteLength(v.prefix)),
      };
    } else if (kind === "stop-under-load") {
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
      for (const [i, r] of results.entries()) {
        const prefix = "PMA_DONE " + plans[i].id + "\n";
        const expected = prefix + "x".repeat(plans[i].outputBytes - prefix.length);
        const answers = nativeMessages(await nativeHistory(database, { runId: r.runId }))
          .filter((m) => m.role === "assistant")
          .map((m) =>
            m.content
              .filter((b) => b.type === "text")
              .map((b) => b.text)
              .join(""),
          );
        assert.deepEqual(answers, [expected], "Replayed answer changed or was materialized twice");
      }
      evidence = {
        writersPreserved: true,
        exactAnswers: results.length,
        visibleBeforeRestart: true,
        sseReconnects: results.reduce((n, r) => n + r.sseReconnects, 0),
        ...(await idlePartitionStreams()),
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
  } catch (error) {
    stageError = error;
    report.faultFailure = {
      kind,
      error: String(error.message).slice(0, 600),
      before,
      after: await fixture.mcpState().catch(() => null),
    };
    throw error;
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
    const outcomes = await Promise.allSettled(operations);
    // Keep admission/submission evidence before cleanup removes the owned Runs.
    // Otherwise a missing fourth request looks like four missing Tool replies.
    if (stageError) {
      report.faultFailure.outcomes = outcomes.map((outcome) =>
        outcome.status === "fulfilled"
          ? outcome.value
          : { error: String(outcome.reason?.message ?? outcome.reason).slice(0, 600) },
      );
      await save();
    }
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
              {
                name: "waiting-model-64",
                kind: "text",
                concurrency: 64,
                count: 64,
                firstByteMs: 10000,
                outputBytes: 2048,
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
                : mode === "projection-tail"
                  ? projectionTailProfiles(tools)
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
    const selected =
      mode === "live-burst"
        ? [
            {
              name: "synchronized-192",
              concurrency: 192,
              count: 192,
              kind: "text",
              firstByteMs: 0,
              chunkMs: 0,
              outputBytes: 4096,
            },
          ]
        : profiles;
    for (const profile of selected) {
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
        "live-stop",
        "lost-tool-reply",
      ])
        await faultStage(kind, tools);
    report.accepted =
      report.measurements.length === selected.length + (mode === "faults" ? 5 : 0) &&
      report.measurements.every(
        (m) =>
          (m.expectedOutcomesPassed || m.summary.failed === 0) &&
          m.summary.historyFailures === 0 &&
          !m.resources.headroomStopped,
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
