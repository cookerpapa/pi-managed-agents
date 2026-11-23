import { verifyStoppedRoot } from "./stopped-root-study.mjs";
import { rootInputStudy } from "./root-input-handoff-study.mjs";
import assert from "node:assert/strict";
import { estimateStoredContext } from "../../../packages/pi-session-postgres/src/durable/conversation-usage.ts";
import { randomUUID } from "node:crypto";
import { readFile, writeFile } from "node:fs/promises";
import { browserNote, browserMessage } from "./browser-signals.mjs";
import { fork } from "node:child_process";
import { fileURLToPath } from "node:url";
import native from "@confluentinc/kafka-javascript";
import pg from "pg";
import { createDatabase, runMigrations } from "@pi-cloud/database";
import {
  createPrivateTenant,
  ProductionHttpGateway,
  PostgresTenantApiAuthenticator,
  createControlPlaneApplication,
} from "../../../packages/control-plane/src/index.ts";
import { SessionProjector } from "../../../packages/runtime-core/src/session-projector.ts";
import { PiCloudApi } from "../../../packages/web-ui/src/api.ts";
import { decodeWorkspaceFact } from "./workspace-operations.mjs";
import { installWorkspaceRoutes } from "./workspace-route-spans.mjs";
import { normalRootProjection } from "./normal-root-projection.mjs";
import { withFamilyGuest } from "./retained-family-cube.mjs";
import { until } from "./family-process.mjs";

const {
  dbUrl,
  brokers: replyBrokers,
  servingBrokers: brokers,
  cube,
  browser,
  inputMode,
  stopInputs,
  stopHandoff,
  replyBeforeReclaim,
} = JSON.parse(await readFile(process.argv[2], "utf8"));
assert(new URL(dbUrl).hostname === "127.0.0.1" && brokers.every((b) => b.startsWith("127.0.0.1:")));
const topic = "pma-root-" + randomUUID(),
  binding = randomUUID();
const url = new URL(dbUrl);
url.username = "message_service";
url.searchParams.set("options", "-c search_path=candidate,public");
const adminDb = createDatabase({ connectionString: dbUrl });
const setup = new pg.Pool({ connectionString: dbUrl, options: "-c search_path=candidate,public" });
const db = createDatabase({ connectionString: url.href, maxConnections: 10 });
const pool = new pg.Pool({ connectionString: url.href, max: 4 });
const replyAdmin = new native.KafkaJS.Kafka({
  kafkaJS: { brokers: replyBrokers, logLevel: native.KafkaJS.logLevel.NOTHING },
}).admin();
const actors = new Set(),
  replyBoots = new Set(),
  workers = [];
let composed,
  projector,
  app,
  api,
  origin,
  tenant,
  endpoint,
  evidence,
  dispatches = 0,
  cancels = 0,
  operation,
  originalReply;
let interruptedFinish,
  finishRollbackVerified = false;
const busy = inputMode ? rootInputStudy(db, pool, topic, inputMode) : undefined;
async function open() {
  composed = normalRootProjection(
    pool,
    topic,
    () => binding,
    (id) => {
      assert.equal(id, binding);
      return {
        submit: async (...args) => {
          dispatches++;
          operation = args[1].request.operationId;
          originalReply = args[2];
          await endpoint.submit(...args);
        },
        async cancel(r, request) {
          assert.equal(request.operationId, operation);
          await endpoint.cancel(r, request);
          cancels++;
        },
      };
    },
  );
  const settle = composed.nativeStore.settle;
  if (!busy)
    composed.nativeStore.settle = async (tx, fact, position) => {
      if (interruptedFinish === fact.factId && !finishRollbackVerified) {
        // This independent reader sees the last committed version while the
        // retried terminal transaction holds the row lock.
        const {
          rows: [before],
        } = await pool.query(
          `select r.state,r.history_head,
        (select count(*)::int from session_terminal_events e where e.tenant_id=r.tenant_id and e.turn_id=r.turn_id) terminals,
        (select next_offset from accepted_fact_projection_offsets where topic=$2 and partition=$3) progress
        from runs r where r.id=$1`,
          [fact.scope.runId, position.topic, position.partition],
        );
        assert.equal(before.state, "running");
        assert.equal(before.history_head, null);
        assert.equal(before.terminals, 0);
        assert(BigInt(before.progress) <= position.offset);
        finishRollbackVerified = true;
      }
      await settle(tx, fact, position);
      if (!interruptedFinish) {
        interruptedFinish = fact.factId;
        throw Error("Injected interruption before normal completion COMMIT");
      }
    };
  projector = new SessionProjector(
    {
      database: db,
      publicationDatabase: db,
      notificationConnectionString: url.href,
      brokers,
      topic,
      clientId: topic,
      partitions: 1,
      replicas: 3,
      retentionMs: 604800000,
      publicationToken: randomUUID() + randomUUID(),
      advertisedBaseUrl: "http://127.0.0.1:9",
      nativeStore: composed.nativeStore,
      ...(busy
        ? { recoveryCheckpoint: busy.guard(composed), nativeChanges: busy.nativeChanges }
        : {}),
      controlTools: { consume: (...args) => composed.dispatch(...args) },
      toolCommands: {
        consume: async (r) => assert(["work_finished", "execution_seal"].includes(r.fact.kind)),
      },
    },
    (v) => decodeWorkspaceFact(v),
  );
  await projector.start();
  await until(() => projector.ownsPartition(0), "root projector assignment");
  app = await createControlPlaneApplication({
    database: db,
    rootMessageHistory: true,
    ...(busy ? await busy.ports(projector) : {}),
    eventRuntime: { eventHub: projector.eventHub, eventStore: projector.eventStore },
    productionHttpGateway: new ProductionHttpGateway({
      authenticator: new PostgresTenantApiAuthenticator({ database: db }),
      readiness: () => true,
    }),
  });
  await app.listen(origin ? Number(new URL(origin).port) : 0, "127.0.0.1");
  origin = await app.getUrl();
  api = new PiCloudApi((path, opts) => fetch(new URL(path, origin), opts), tenant.credential.token);
}
async function close() {
  await busy?.close();
  await app?.close();
  app = undefined;
  await projector?.close();
  projector = undefined;
}
function work(runId, resumeInput = false) {
  const child = fork(fileURLToPath(new URL("./normal-root-worker.mjs", import.meta.url)), [], {
    execArgv: ["--import", "tsx"],
    stdio: ["ignore", "inherit", "inherit", "ipc"],
  });
  actors.add(child);
  busy?.attach(child);
  const done = new Promise((resolve, reject) => {
    let evidence;
    child.on("message", (m) => {
      if (m.bootId) replyBoots.add(m.bootId);
      if (m.done) evidence = m;
    });
    child.once("error", reject);
    child.once("exit", (code, signal) => {
      actors.delete(child);
      if (code !== 0 || !evidence) reject(Error("Worker failed: " + (code ?? signal)));
      else {
        workers.push(evidence);
        resolve(evidence);
      }
    });
  });
  void done.catch(() => {});
  child.send({
    dbUrl: url.href,
    topic,
    brokers,
    replyBrokers,
    runId,
    inputMode,
    resumeInput,
    stopInputs,
  });
  return done;
}
const items = (page) => page.turns.flatMap((t) => t.transcript?.items ?? []);
// The disposable ordinary-root fixture knows both identities and every projected Entry.
async function checkStatus(sessionId, runId) {
  const owner = await pool.query(
    "select r.provider,r.model_id,s.pi_session_id from runs r join sessions s on s.tenant_id=r.tenant_id and s.id=r.session_id where r.id=$1 and s.id=$2 and s.tenant_id=$3",
    [runId, sessionId, tenant.tenantId],
  );
  assert.equal(owner.rowCount, 1);
  const { provider, model_id, pi_session_id } = owner.rows[0];
  const { rows } = await pool.query(
    "select content from message_entries where tenant_id=$1 and pi_session_id=$2 and conversation_id=1 order by entry_id",
    [tenant.tenantId, pi_session_id],
  );
  const expected = estimateStoredContext(
    rows.map((row) => JSON.parse(row.content)),
    { provider, modelId: model_id },
  );
  assert(expected.estimatedTokens > 0);
  const res = await fetch(new URL("/v1/conversations/" + sessionId + "/status", origin), {
    headers: { authorization: "Bearer " + tenant.credential.token },
  });
  const actual = res.ok ? (await res.json()).context : null;
  if (!res.ok) await res.body?.cancel();
  return {
    entries: rows.length,
    expected,
    actual,
    httpStatus: res.status,
    matches:
      actual?.method === expected.method && actual?.estimatedTokens === expected.estimatedTokens,
  };
}
async function legacyCounts() {
  return Object.fromEntries(
    await Promise.all(
      [
        "pi_durable_commits",
        "pi_durable_records",
        "pi_durable_log",
        "pi_durable_current",
        "pi_history_blocks",
        "pi_history_views",
        "pi_history_frames",
      ].map(async (n) => [
        n,
        (await setup.query("select count(*)::int n from public." + n)).rows[0].n,
      ]),
    ),
  );
}
try {
  await replyAdmin.connect();
  await runMigrations(adminDb, "up");
  await setup.query(
    "create schema candidate;alter table public.runs add column history_head jsonb",
  );
  const schema = await readFile(new URL("./schema.sql", import.meta.url), "utf8");
  await setup.query(schema.slice(schema.indexOf("create table messages")));
  const initial = normalRootProjection(
    setup,
    topic,
    () => {},
    () => {},
  );
  await initial.view.install();
  await initial.view.reset(new Map([[0, 0n]]), 16, "recorded-history");
  await installWorkspaceRoutes(setup);
  tenant = await createPrivateTenant(adminDb, {
    slug: "root-study",
    ownerDisplayName: "Root fixture",
    initialModel: { provider: "openai-codex", modelId: "gpt-6.1-sol" },
  });
  await setup.query(`create role message_service login; alter role message_service set search_path=candidate,public;
    grant usage on schema candidate to message_service;
    grant select,insert,update,delete on all tables in schema public,candidate to message_service;
    grant usage,select on all sequences in schema public,candidate to message_service;
    do $$ declare obj record;begin for obj in select relname from pg_class where relnamespace='public'::regnamespace and relkind in ('r','v','m','p') and (starts_with(relname,'pi_durable_') or starts_with(relname,'pi_history_')) loop
      execute format('revoke all on public.%I from message_service',obj.relname);end loop;end $$;`);
  const privileges = (
    await pool.query(
      "select count(*)::int n from pg_class where relnamespace='public'::regnamespace and relkind in ('r','v','m','p') and (starts_with(relname,'pi_durable_') or starts_with(relname,'pi_history_')) and has_table_privilege(current_user,oid,'select')",
    )
  ).rows[0];
  assert.equal(privileges.n, 0);
  await open();
  const project = await api.createProject("normal-root"),
    session = await api.createSession(
      project.projectId,
      project.workspaceId,
      "normal root",
      "elastic",
    ),
    id = session.sessionId;
  const baseline = await legacyCounts();
  assert.equal((await api.getConversation(id)).turns.length, 0);
  evidence = await withFamilyGuest(cube, async (guest) => {
    endpoint = await guest.directory(binding);
    const prompt1 = "Check the value in the workspace.",
      prompt2 = busy
        ? "Check the saved value " + randomUUID()
        : "What did the preceding command return?",
      key = randomUUID(),
      third = "Additional instructions " + randomUUID(),
      edited = "Use the revised plan " + randomUUID();
    async function browserRun(n) {
      return until(
        async () => {
          const {
            rows: [r],
          } = await pool.query(
            "select id,turn_id from runs where session_id=$1 and mailbox_position=$2",
            [id, n],
          );
          return r && { runId: r.id, turnId: r.turn_id };
        },
        "browser submitted Run",
        70000,
      );
    }
    if (browser)
      await browserNote("/study", "ready", {
        origin,
        id,
        token: tenant.credential.token,
        inputMode,
        stopInputs,
        stopHandoff,
        third,
        edited,
        preferenceKey: "pi-cloud:composer-enter:" + tenant.tenantId + ":" + tenant.ownerUserId,
        first: prompt1,
        second: prompt2,
      });
    const first = browser ? await browserRun(1) : await api.acceptTurn(id, prompt1, key);
    if (!browser) assert.equal((await api.acceptTurn(id, prompt1, key)).runId, first.runId);
    const a = work(first.runId);
    await Promise.race([
      until(
        async () => dispatches === 1 && (await endpoint.marker()) === "once",
        "physical Bash start",
        45000,
      ),
      a.then(() => {
        throw Error("Bash completed before release");
      }),
    ]);
    const active = await api.getConversation(id);
    const tool = items(active).find((i) => i.kind === "tool");
    assert(tool?.status === "running" && tool.input.command.includes("cat result"));
    if (busy) {
      const measured = await Promise.race([
        busy.failed,
        busy.exercise({
          first: prompt1,
          second: prompt2,
          id,
          runId: first.runId,
          api,
          endpoint,
          work,
          dispatchCount: () => dispatches,
          ...(stopInputs
            ? {
                afterReceipt: (actor, receipt) =>
                  verifyStoppedRoot({
                    actor,
                    receipt,
                    stopHandoff,
                    db,
                    ...(replyBeforeReclaim
                      ? {
                          finishOriginal: async () => {
                            await endpoint.release();
                            await until(async () => {
                              const [p] = await replyAdmin.fetchTopicOffsets(originalReply.topic);
                              return BigInt(p.high ?? p.offset) > BigInt(originalReply.offset);
                            }, "original Guest reply in its exclusive fixture mailbox");
                          },
                        }
                      : {}),
                    first,
                    originalPrompt: prompt1,
                    second: prompt2,
                    third,
                    edited,
                    id,
                    api,
                    pool,
                    a,
                    work,
                    browserRun,
                    endpoint,
                    counts: () => ({ dispatches, cancels }),
                  }),
              }
            : {}),
        }),
      ]);
      assert.deepEqual(await legacyCounts(), baseline);
      await projector.checkHealth();
      return measured;
    }
    if (browser) await browserMessage("/study", "active");
    await endpoint.release();
    await a;
    await until(
      async () => (await api.getRun(first.runId)).state === "completed",
      "first run settled",
    );
    const token = await endpoint.result();
    const checkFirst = (page) => {
      const row = page.turns.find((t) => t.turnId === first.turnId);
      assert.equal(row.prompt, prompt1);
      const its = row.transcript.items;
      const tools = its.filter((i) => i.kind === "tool");
      assert.equal(tools.length, 1);
      assert.equal(tools[0].toolCallId, tool.toolCallId);
      assert.deepEqual(tools[0].input, tool.input);
      assert(tools[0].status === "completed" && JSON.stringify(tools[0].output).includes(token));
      assert(its.some((i) => i.kind === "text" && i.text === "First answer: " + token));
    };
    checkFirst(await api.getConversation(id));
    const statusChecks = [await checkStatus(id, first.runId)];
    if (browser) await browserMessage("/study", "detached");
    await close();
    await open();
    checkFirst(await api.getConversation(id));
    if (browser) await browserNote("/study", "restarted", { marker: token });
    const second = browser ? await browserRun(2) : await api.acceptTurn(id, prompt2, randomUUID());
    await work(second.runId);
    await until(
      async () => (await api.getRun(second.runId)).state === "completed",
      "second run settled",
    );
    const cold = await api.getConversation(id);
    checkFirst(cold);
    assert.deepEqual(
      cold.turns.map((t) => t.prompt),
      [prompt1, prompt2],
    );
    assert(items(cold).some((i) => i.kind === "text" && i.text === "Second answer: " + token));
    assert.equal(dispatches, 1);
    assert.equal(await endpoint.marker(), "once");
    assert(workers[1].priorEntries > 0);
    assert.deepEqual(workers[1].restoredUserPrompts, [prompt1, prompt2]);
    assert.deepEqual(await legacyCounts(), baseline);
    assert(finishRollbackVerified);
    statusChecks.push(await checkStatus(id, second.runId));
    assert(
      statusChecks.every((s) => s.matches),
      JSON.stringify(statusChecks),
    );
    await projector.checkHealth();
    return {
      ...(browser ? await browserMessage("/study", "done") : {}),
      statusChecks,
      runs: cold.turns.length,
      physicalExecutions: dispatches,
      runningToolVisible: true,
      stableToolIdentity: true,
      coldApiRestart: true,
      coldWorkerContinuation: true,
      modelCalls: workers.map((x) => x.modelCalls),
      coldPriorEntries: workers[1].priorEntries,
      legacyNativeRowsAdded: 0,
      finishRollbackVerified,
    };
  });
} catch (error) {
  if (browser) await browserNote("/study", "failure", { message: error.message });
  throw error;
} finally {
  await Promise.all(
    [...actors].map(
      (c) =>
        new Promise((resolve) => {
          c.once("exit", resolve);
          c.kill("SIGKILL");
        }),
    ),
  );
  try {
    await close();
  } finally {
    try {
      const owned = [...replyBoots].map((id) => "pi-cloud.tool-replies.v4." + id);
      const topics = (await replyAdmin.listTopics()).filter((t) => owned.includes(t));
      if (topics.length) await replyAdmin.deleteTopics({ topics });
      const groups = (await replyAdmin.listGroups()).groups
        .map((g) => g.groupId)
        .filter((g) =>
          [...replyBoots].some(
            (id) => g === "tool-reply-" + id || g.startsWith("tool-restore-" + id + "-"),
          ),
        );
      for (const group of groups)
        await until(
          async () => {
            try {
              await replyAdmin.deleteGroups([group]);
            } catch {
              /* Kafka may still be retiring the member. */
            }
            return !(await replyAdmin.listGroups()).groups.some((g) => g.groupId === group);
          },
          "owned reply group deletion",
          70000,
        );
      await until(
        async () => !(await replyAdmin.listTopics()).some((t) => owned.includes(t)),
        "owned reply topic deletion",
      );
    } finally {
      await replyAdmin.disconnect();
      await pool.end();
      await setup.end();
      await db.destroy();
      await adminDb.destroy();
    }
  }
}
const result = { ...evidence, cleanupVerified: true };
await writeFile(process.argv[3], JSON.stringify(result, null, 2) + "\n");
console.log(JSON.stringify(result));
