import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { sql } from "kysely";
import { createDatabase } from "../packages/database/src/index.ts";
import { createPrivateTenant } from "../packages/control-plane/src/tenant-administration.ts";
import { httpMcpFixture } from "../packages/control-plane/test/mcp/http-fixture.ts";
import { mcpToolName } from "../packages/control-plane/src/mcp/http-client.ts";
import { PiCloudApi } from "../packages/web-ui/src/api.ts";
import { newIdempotencyKey } from "@pi-cloud/session-client";
import { S3AttachmentBlobStore } from "../packages/session-attachments/src/s3.ts";
import {
  runtimeDirectory,
  runtimeEnvironment,
  serviceAddress,
  kubectl,
} from "./lib/production-kubernetes.mjs";
import { nativeHistory, nativeMessages } from "./lib/native-session-history.mjs";
import {
  settleOwnedAcceptanceRuns,
  purgeOwnedAcceptanceTenant,
} from "./lib/owned-acceptance-cleanup.mjs";

assert.equal(
  process.env.PI_CLOUD_LIVE_MCP_CHECK,
  "1",
  "Authorize owned MCP/Luna and Worker replacement acceptance",
);
assert(process.argv[2], "Pass a private report directory");
const directory = resolve(process.argv[2]);
await mkdir(directory, { recursive: true, mode: 0o700 });
const report = {
  at: new Date().toISOString(),
  accepted: false,
  cleanupConfirmed: false,
  checks: [],
};
const save = () =>
  writeFile(resolve(directory, "report.json"), JSON.stringify(report, null, 2) + "\n", {
    mode: 0o600,
  });
const phase = (name) => console.log(JSON.stringify({ phase: name, at: new Date().toISOString() }));
const url = new URL(
  (await readFile(resolve(runtimeDirectory, "secrets/database-url"), "utf8")).trim(),
);
url.hostname = serviceAddress("postgres");
const database = createDatabase({ connectionString: url.toString(), maxConnections: 3 });
const base = `http://127.0.0.1:${runtimeEnvironment().PI_CLOUD_HTTP_PORT}`;
const token = randomUUID(),
  server = await httpMcpFixture({ host: "0.0.0.0", token });
const node = JSON.parse(kubectl(["get", "nodes", "-o", "json"])).items[0];
const host = node.status.addresses.find((a) => a.type === "InternalIP").address;
const endpoint = `http://${host}:${server.port}/mcp`;
const definition = (name) => ({
  name,
  description:
    name === "hold"
      ? "Wait until the operator opens the acceptance gate."
      : `Owned acceptance tool ${name}.`,
  inputSchema: { type: "object", properties: { text: { type: "string" } }, required: ["text"] },
});
server.state.tools = ["echo", "hold", "image", "lost_write"].map(definition);
const terminal = (state) => ["completed", "failed", "cancelled", "timed_out"].includes(state);
async function until(read, label, budget = 300000) {
  const end = Date.now() + budget;
  while (Date.now() < end) {
    const v = await read();
    if (v) return v;
    await delay(350);
  }
  throw new Error(label + " timed out");
}
let tenant, api, project, connectionId, failure;
const sessions = [];
const settled = (id) =>
  until(async () => {
    const run = await api.getRun(id);
    return terminal(run.state) ? run : false;
  }, "Run settlement");
const messages = async (runId) => nativeMessages(await nativeHistory(database, { runId }));
const checked = async (accepted) => {
  const run = await settled(accepted.runId),
    history = await messages(accepted.runId);
  if (run.state !== "completed") {
    report.runFailure = {
      runId: run.runId,
      state: run.state,
      failure: run.failure,
      errors: history
        .filter((m) => m.role === "assistant" && m.stopReason === "error")
        .map((m) => String(m.errorMessage ?? "").slice(0, 500)),
    };
    throw new Error("Owned MCP Run did not complete");
  }
  return history;
};
const newSession = async (title) => {
  const session = await api.createSession(
    project.projectId,
    project.workspaceId,
    title,
    "elastic",
    "starter",
    "/workspace",
    { provider: "openai-codex", modelId: "gpt-6-luna", thinkingLevel: "low", fastMode: false },
  );
  sessions.push(session);
  return session;
};
const submit = (session, text) =>
  api.acceptTurn(session.sessionId, text, newIdempotencyKey("mcp-check"), "low");
const noOtherWork = async () => {
  const { rows } =
    await sql`select count(*)::int as n from runs where state in ('queued','running','settling','cancel_requested')
    and (${tenant?.tenantId ?? null}::uuid is null or tenant_id<>${tenant?.tenantId ?? null}::uuid)`.execute(
      database,
    );
  assert.equal(rows[0].n, 0, "Refusing to interrupt unrelated active work");
};
try {
  await noOtherWork();
  tenant = await createPrivateTenant(database, {
    slug: "release-acceptance-mcp-" + randomUUID(),
    ownerDisplayName: "MCP acceptance",
    initialModel: { provider: "openai-codex", modelId: "gpt-6-luna" },
  });
  api = new PiCloudApi(
    (path, init) => fetch(new URL(String(path), base), init),
    tenant.credential.token,
  );
  project = await api.createProject("MCP acceptance");
  connectionId = randomUUID();
  report.resources = { tenantId: tenant.tenantId, workspaceId: project.workspaceId, connectionId };
  await save();
  const settings = {
    name: "Owned MCP fixture",
    endpoint,
    visibility: "personal",
    enabled: true,
    allowedTools: null,
  };
  await api.saveMcpConnection(connectionId, {
    ...settings,
    expectedRevision: null,
    credential: { kind: "bearer", token },
  });
  const ready = () =>
    until(
      async () => {
        const value = await api.getMcpConnection(connectionId);
        return value.connection.status === "ready" ? value : false;
      },
      "MCP discovery",
      60000,
    );
  await ready();
  const name = (tool) => mcpToolName(connectionId, tool);
  phase("real Luna calls with text and image results, without a VM");
  const first = await newSession("MCP text and image");
  const input = await submit(
    first,
    `Use only these two MCP tools: first ${name("echo")} with text MCP_TEXT_OK, then ${name("image")} with text MCP_IMAGE_OK. Do not use bash, files, search or delegation. Reply MCP_LIVE_OK after both results.`,
  );
  const history = await checked(input);
  assert(history.some((m) => m.role === "toolResult" && m.toolName === name("echo") && !m.isError));
  assert(
    history.some(
      (m) =>
        m.role === "toolResult" &&
        m.toolName === name("image") &&
        !m.isError &&
        m.content.some((c) => c.type === "pi_cloud_image_v1" || c.type === "image"),
    ),
  );
  assert.equal(
    (
      await database
        .selectFrom("workspace_runtimes")
        .select("workspace_runtime_id")
        .where("tenant_id", "=", tenant.tenantId)
        .execute()
    ).length,
    0,
  );
  report.checks.push("Luna text/image/native result and lazy VM");
  await save();

  phase("catalog hot update within the same native submission");
  const hot = await newSession("MCP live catalog");
  let held = server.state.calls.filter((n) => n === "hold").length;
  const hotRun = await submit(
    hot,
    `First call ${name("hold")} with text HOT_GATE. After it finishes, a new tool ${name("fresh_echo")} will be available; call that tool with text MCP_HOT_OK. Do not substitute another tool. Then reply MCP_HOT_DONE. Do not use bash, files, search or delegation.`,
  );
  await until(() => server.state.calls.filter((n) => n === "hold").length > held, "Hot gate");
  server.state.tools.push(definition("fresh_echo"));
  server.change();
  await until(
    async () =>
      (await api.getMcpConnection(connectionId)).tools.some((t) => t.name === "fresh_echo"),
    "Hot discovery",
    90000,
  );
  server.release();
  assert(
    (await checked(hotRun)).some(
      (m) => m.role === "toolResult" && m.toolName === name("fresh_echo") && !m.isError,
    ),
  );
  report.checks.push("Hot catalog within one Run");
  await save();

  phase("Worker crash restores original reply after tool withdrawal");
  const handoff = await newSession("MCP original result recovery");
  held = server.state.calls.filter((n) => n === "hold").length;
  const recovery = await submit(
    handoff,
    `Call ${name("hold")} exactly once with text RECOVERY_GATE. Wait for its original result. After that, reply MCP_RECOVERED. Do not call other tools or repeat the operation.`,
  );
  await until(() => server.state.calls.filter((n) => n === "hold").length > held, "Recovery gate");
  const before = await database
    .selectFrom("runs as r")
    .innerJoin("workers as w", "w.id", "r.worker_id")
    .select(["r.lease_id", "w.supervisor_id"])
    .where("r.id", "=", recovery.runId)
    .executeTakeFirstOrThrow();
  const old = await api.getMcpConnection(connectionId);
  await api.saveMcpConnection(connectionId, {
    ...settings,
    enabled: false,
    expectedRevision: old.connection.revision,
  });
  await until(async () => {
    const { rows } =
      await sql`select 1 from tool_catalog_publication where scope=${"mcp:" + connectionId} and not(mcp->>'enabled')::boolean and published_generation=generation`.execute(
        database,
      );
    return rows.length === 1;
  }, "Withdrawal publication");
  await noOtherWork();
  assert(/^pi-cloud-pi-worker-/.test(before.supervisor_id));
  kubectl(["delete", "pod", before.supervisor_id, "--grace-period=0", "--force", "--wait=false"]);
  const after = await until(
    async () => {
      const r = await database
        .selectFrom("runs")
        .select(["lease_id", "state"])
        .where("id", "=", recovery.runId)
        .executeTakeFirstOrThrow();
      return r.state === "running" && r.lease_id && r.lease_id !== before.lease_id ? r : false;
    },
    "Worker ownership recovery",
    180000,
  );
  server.release("ORIGINAL_MCP_AFTER_HANDOFF");
  const restored = await checked(recovery);
  assert.equal(server.state.calls.filter((n) => n === "hold").length, held + 1);
  assert.equal(
    restored.filter(
      (m) =>
        m.role === "toolResult" &&
        m.toolName === name("hold") &&
        JSON.stringify(m).includes("ORIGINAL_MCP_AFTER_HANDOFF"),
    ).length,
    1,
  );
  report.recovery = {
    runId: recovery.runId,
    oldWriter: before.lease_id,
    newWriter: after.lease_id,
    physicalCalls: 1,
  };
  report.checks.push("Worker replacement + withdrawn tool + original reply once");
  await save();

  const disabled = await api.getMcpConnection(connectionId);
  await api.saveMcpConnection(connectionId, {
    ...settings,
    expectedRevision: disabled.connection.revision,
  });
  await ready();
  phase("native Stop and late response");
  const stopped = await newSession("MCP cancellation");
  held = server.state.calls.filter((n) => n === "hold").length;
  const cancelRun = await submit(
    stopped,
    `Call ${name("hold")} exactly once with text CANCEL_GATE, wait for the result, and do not call any other tool.`,
  );
  await until(
    () => server.state.calls.filter((n) => n === "hold").length > held,
    "Cancellation gate",
  );
  const cancellations = server.state.cancellations;
  await api.cancelTurn(stopped.sessionId, cancelRun.turnId, newIdempotencyKey("mcp-stop"));
  assert.equal((await settled(cancelRun.runId)).state, "cancelled");
  await until(
    () => server.state.cancellations > cancellations,
    "MCP cancellation notification",
    10000,
  );
  server.release("LATE_MCP_MUST_NOT_REPLACE_ABORT");
  await delay(500);
  assert(
    !JSON.stringify(await messages(cancelRun.runId)).includes("LATE_MCP_MUST_NOT_REPLACE_ABORT"),
  );
  report.checks.push("Native Stop, upstream cancellation, late reply ignored");
  await save();

  phase("lost external write response is not replayed");
  const lost = await newSession("MCP uncertain result");
  const writes = server.state.writes;
  const unknown = await submit(
    lost,
    `Call ${name("lost_write")} exactly once with text WRITE_ONCE. If its result is uncertain or an error, do not retry or call another tool; report the uncertainty briefly.`,
  );
  const unknownHistory = await checked(unknown);
  assert.equal(server.state.writes, writes + 1);
  assert(
    unknownHistory.some(
      (m) =>
        m.role === "toolResult" && m.isError && JSON.stringify(m).includes("tool_result_unknown"),
    ),
  );
  report.checks.push("Lost write response UNKNOWN, one physical effect");
  report.accepted = true;
  await save();
} catch (error) {
  failure = error;
  report.failure = String(error.message).slice(0, 700);
  await save();
} finally {
  try {
    server.release();
    for (const session of sessions) {
      const runs = await database
        .selectFrom("runs")
        .select(["id", "turn_id"])
        .where("session_id", "=", session.sessionId)
        .execute();
      await settleOwnedAcceptanceRuns(api, session.sessionId, runs, settled);
      await api.deleteConversation(session.sessionId);
    }
    if (connectionId) {
      const value = await api.getMcpConnection(connectionId);
      await api.removeMcpConnection(connectionId, value.connection.revision);
      await until(async () => {
        const { rows } =
          await sql`select 1 from tool_catalog_publication where scope=${"mcp:" + connectionId} and not(mcp->>'enabled')::boolean and published_generation=generation`.execute(
            database,
          );
        return rows.length === 1;
      }, "MCP removal");
    }
    if (project) {
      await api.deleteWorkspace(project.workspaceId, newIdempotencyKey("mcp-cleanup"));
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
        "Owned Workspace cleanup",
      );
    }
    if (tenant) {
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
        "Owned controls",
      );
      const blobs = new S3AttachmentBlobStore({
        endpoint: `http://${serviceAddress("session-images")}:8333`,
        region: "us-east-1",
        bucket: "pi-cloud-session-images",
        ...JSON.parse(
          await readFile(
            resolve(runtimeDirectory, "secrets/session-image-s3-credentials.json"),
            "utf8",
          ),
        ),
      });
      try {
        report.cleanup = await purgeOwnedAcceptanceTenant(database, tenant, blobs);
      } finally {
        await blobs.close();
      }
    }
    report.cleanupConfirmed = true;
  } catch (error) {
    report.cleanupFailure = String(error.message).slice(0, 500);
    failure ??= error;
  }
  await server.close();
  await database.destroy();
  await save();
}
console.log(
  JSON.stringify({
    accepted: report.accepted,
    cleanupConfirmed: report.cleanupConfirmed,
    checks: report.checks.length,
  }),
);
if (failure) throw failure;
