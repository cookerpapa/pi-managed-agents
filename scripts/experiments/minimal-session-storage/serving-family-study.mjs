// All authority and Session log traffic here must use the disposable cluster.
// The installed completion service only sees the fixture's original reply Topic.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile, writeFile } from "node:fs/promises";
import pg from "pg";
import native from "@confluentinc/kafka-javascript";
import { createDatabase, runMigrations } from "@pi-cloud/database";
import {
  createPrivateTenant,
  ProductionHttpGateway,
  PostgresTenantApiAuthenticator,
  createControlPlaneApplication,
} from "../../../packages/control-plane/src/index.ts";
import { SessionExpiryReconciler } from "../../../packages/control-plane/src/session-expiry-reconciler.ts";
import { SessionProjector } from "../../../packages/runtime-core/src/session-projector.ts";
import { createExecutionReference } from "@pi-cloud/protocol";
import { PostgresSubagentConversations } from "../../../packages/subagent-control/src/postgres-subagent-conversations.ts";
import { PiCloudApi } from "../../../packages/web-ui/src/api.ts";
import { KafkaLogConsumer } from "../../../packages/event-log/src/kafka-log-consumer.ts";
import { ReadyProjection } from "./ready-projection.mjs";
import { projectionTemplates } from "./projection-statements.mjs";
import { projectRecord } from "./project-record.mjs";
import { compareQueuedRequests, installStudyHandoff, SealStudy } from "./pg-family-handoff.mjs";
import { familyWorker, until } from "./family-process.mjs";
import { withFamilyGuest } from "./retained-family-cube.mjs";

const {
  dbUrl,
  brokers: replyBrokers,
  servingBrokers: brokers,
  cube,
  pgHandoff,
  bashFields,
} = JSON.parse(await readFile(process.argv[2], "utf8"));
assert(
  brokers.length === 3 &&
    brokers.every((b) => b.startsWith("127.0.0.1:")) &&
    new URL(dbUrl).hostname === "127.0.0.1",
);
const database = createDatabase({ connectionString: dbUrl, maxConnections: 10 });
const pool = new pg.Pool({
  connectionString: dbUrl,
  options: "-c search_path=candidate,public",
  max: 4,
});
const replies = new native.KafkaJS.Kafka({
  kafkaJS: { brokers: replyBrokers, logLevel: native.KafkaJS.logLevel.NOTHING },
});
const sessionAdmin = pgHandoff
  ? new native.KafkaJS.Kafka({
      kafkaJS: { brokers, logLevel: native.KafkaJS.logLevel.NOTHING },
    }).admin()
  : undefined;
const replyAdmin = replies.admin(),
  replyCopies = replies.producer({ kafkaJS: { acks: -1, idempotent: true } });
const replyBoots = new Set(),
  results = [];
const topic = "pma-family-" + randomUUID();
const view = new ReadyProjection(pool, topic, new Map(), projectionTemplates);
const metrics = {
  byPartition: [0],
  queries: 0,
  skipped: 0,
  peakWaiting: 0,
  inboxSnapshots: 0,
  inboxBytes: 0,
};
const env = {
  pool,
  sessions: view.sessions,
  stage: { integrated: true, sink: "single", scene: "plain" },
  metrics,
  nextOffsets: new Map(),
  partitions: 1,
  account() {},
  send() {},
};
const readerUrl = new URL(dbUrl);
readerUrl.username = "family_reader";
const cases = new Map();
let projector, app, api, tenant;
async function scenario(guest, mode) {
  const prompt = "Delegate to the child " + randomUUID(),
    project = await api.createProject(mode),
    session = await api.createSession(project.projectId, project.workspaceId, mode, "elastic"),
    input = await api.acceptTurn(session.sessionId, prompt, randomUUID());
  assert(input.runId && input.state === "queued");
  const runId = input.runId,
    binding = randomUUID(),
    dir = await guest.directory(binding),
    state = { dir, mode, binding, dispatches: 0, cancels: 0, probes: 0 };
  if (pgHandoff) state.study = new SealStudy();
  cases.set(runId, state);
  let a, b, witness;
  const scopeOf = async (actor) => {
    const { scope, bootId } = await actor.wait("claimed");
    assert.notEqual(bootId, scope.writerId);
    replyBoots.add(bootId);
    assert.equal(scope.runId, runId);
    return scope;
  };
  function spawn(seal) {
    return familyWorker({
      mode,
      dbUrl: readerUrl.toString(),
      controlUrl: dbUrl,
      runId,
      topic,
      brokers,
      replyBrokers,
      ...(pgHandoff && seal ? {} : { binding }),
      pgHandoff,
      bashFields,
      ...(pgHandoff ? { pgResume: !!seal } : { seal }),
    });
  }
  async function wait(check, label) {
    return until(async () => {
      a?.check();
      b?.check();
      if (state.error) throw state.error;
      await projector.checkHealth();
      return check();
    }, label);
  }
  try {
    a = spawn();
    const first = await scopeOf(a);
    a.proc.send({ kind: "start" });
    const request = await a.wait("delegateRequest");
    await wait(() => state.seq >= request.sequence, "accepted parent task");
    const children = new PostgresSubagentConversations({
      database,
      native: {
        start: async ({ view }) => {
          a.proc.send({ kind: "startChild", view });
          return (await a.wait("childStarted")).id;
        },
        stop: async () => {
          throw Error("Unexpected child stop RPC");
        },
      },
    });
    const child = await children.start({
      id: request.requestId,
      tenantId: first.tenantId,
      rootRunId: runId,
      parentSessionId: first.sessionId,
      parentTurnId: first.turnId,
      executionReference: createExecutionReference(first.writerId, runId, first.fencingToken),
      parentToolCallId: "task:" + request.taskId,
      source: { sequence: request.sequence, taskId: request.taskId },
      prompt: "Run the gated Bash once.",
      contextMode: "fresh",
      contextAnchor: null,
      sandboxMode: "shared",
    });
    assert(child.conversationId > 1);
    await wait(async () => state.ref && (await dir.marker()) === "once", "first dispatch");
    a.proc.send({ kind: "queue" });
    const queued = await a.wait("queued");
    if (pgHandoff)
      await wait(() => state.seq >= queued.seq, "projected edit after queued submission");
    await wait(
      async () =>
        (
          await pool.query(
            "select 1 from inboxes where tenant_id=$1 and jsonb_array_length(items)=1",
            [first.tenantId],
          )
        ).rowCount,
      "projected child follow-up",
    );
    const { bootId } = await a.wait("claimed");
    assert.equal(state.ref.topic, "pi-cloud.tool-replies.v4." + bootId);
    let reply;
    witness = new KafkaLogConsumer({
      brokers: replyBrokers,
      clientId: "observe-" + bootId,
      groupId: "observe-" + bootId,
      topic: state.ref.topic,
      commitMessages: false,
      decode: (v) => JSON.parse(v.toString()),
      handler: async ({ fact }) => {
        if (fact.kind === "tool_result" && fact.operationId === state.operation) reply = fact;
      },
    });
    await witness.start();
    await witness.waitUntilAssigned(25000);
    let expiryToSeal, cold;
    if (mode === "normal") {
      await dir.release();
    } else {
      const killed = Date.now();
      a.proc.kill("SIGKILL");
      assert.equal((await a.ended).signal, "SIGKILL");
      const reaper = new SessionExpiryReconciler(database);
      await wait(async () => (await reaper.sweep()).expiredAssignments === 1, "PG lease expiry");
      await wait(
        async () => state.seal && (await api.getRun(runId)).state === "queued",
        "ordered requeue",
      );
      const old = await database
        .selectFrom("session_leases")
        .selectAll()
        .where("lease_id", "=", first.writerId)
        .executeTakeFirstOrThrow();
      assert(
        old.writer_sealed_at &&
          old.released_at &&
          String(old.writer_seal_offset) === String(state.seal.offset),
      );
      assert.equal(state.seal.writerId, first.writerId);
      const outbox = await database
        .selectFrom("outbox")
        .select("published_at")
        .where("id", "=", state.seal.factId)
        .executeTakeFirstOrThrow();
      assert(outbox.published_at);
      expiryToSeal = Date.now() - killed;
      if (pgHandoff) {
        // Read the serving schema solely as an independent reference, from the
        // driver. Neither this receipt nor this DB role is passed to B's loader.
        const {
          rows: [legacy],
        } = await pool.query(
          `select exists(
          select 1 from public.pi_durable_records r join public.pi_durable_current c using(tenant_id,session_id,id)
          where r.kind='submission' and r.tenant_id=$1 and r.session_id=$2
          and c.body::jsonb#>>'{value,requestId}'=$3 and c.body::jsonb#>>'{value,status}'='queued'
        ) as has_request`,
          [first.tenantId, first.piSessionId, state.queuedId],
        );
        assert(legacy.has_request);
        delete state.study;
        const low = BigInt(state.seal.offset) + 1n;
        await sessionAdmin.deleteTopicRecords({
          topic,
          partitions: [{ partition: 0, offset: String(low) }],
        });
        await until(
          async () => BigInt((await sessionAdmin.fetchTopicOffsets(topic))[0].low) >= low,
          "sealed prefix physically outside Kafka read range",
        );
      }
      if (mode === "user-stop") {
        const c = await api.cancelTurn(session.sessionId, input.turnId, randomUUID());
        state.stopReceipt = c.controlRequestId;
      }
      if (mode === "already-returned") {
        await dir.release();
        await wait(() => reply, "Guest completion before B");
        await replyCopies.send({
          topic: state.ref.topic,
          messages: [{ partition: 0, key: state.operation, value: JSON.stringify(reply) }],
        });
        await witness.waitUntilInitialReplay(await witness.captureEndOffsets());
      }
      b = spawn(state.seal);
      const second = await scopeOf(b);
      assert.notEqual(first.writerId, second.writerId);
      assert.equal(second.fencingToken, first.fencingToken + 1);
      assert.equal((await b.wait("claimed")).stopOnResume, mode === "user-stop");
      view.sessions.get(runId).writer = second.writerId;
      b.proc.send({ kind: "start" });
      cold = await b.wait("cold");
      if (pgHandoff) {
        assert.equal(cold.requestHash, queued.requestHash);
        assert.equal(cold.oldWriter, first.writerId);
        assert.equal(cold.sealOffset, state.seal.offset);
        assert.deepEqual(cold.candidateChild, {
          id: child.executionId,
          sourceTask: request.taskId,
          childConversationId: child.conversationId,
        });
        assert(state.ambiguous && state.requestAbsentFromMemory && state.snapshotBytes > 0);
      }
      if (mode !== "user-stop") {
        const wait = await b.wait("reattached");
        assert.equal(wait.originalTopic, state.ref.topic);
        assert.equal(wait.operationId, state.operation);
        assert.equal(wait.deadline, state.ref.deadlineAt);
        if (mode === "serving-pending") await dir.release();
      }
    }
    const active = b ?? a;
    const outcome = await active.wait(mode === "user-stop" ? "stopped" : "finished");
    if (mode === "user-stop") {
      assert.equal((await b.wait("stopAcknowledged")).receipt, state.stopReceipt);
      assert.equal(outcome.models, 0);
      assert.equal(outcome.cancellations, 1);
    }
    if (mode === "reply-timeout") {
      const unknown = await b.wait("unknown");
      assert(unknown.at >= unknown.deadline);
      assert.equal(unknown.deadline, state.ref.deadlineAt);
    }
    const terminal = mode === "user-stop" ? "cancelled" : "completed";
    await wait(
      async () => state.end && (await api.getRun(runId)).state === terminal,
      "HTTP terminal",
    );
    assert.equal((await active.ended).code, 0);
    if (mode === "reply-timeout") {
      assert(!reply);
      await dir.release();
    }
    await wait(() => reply, "original Guest reply");
    const history = await api.getConversation(session.sessionId);
    assert(JSON.stringify(history).includes(prompt));
    const childPage = await api.getConversation(child.childSessionId);
    assert(JSON.stringify(childPage).includes("Run the gated Bash once."));
    assert(!history.turns.some((t) => t.prompt === "Run the gated Bash once."));
    assert(!JSON.stringify(childPage).includes(prompt));
    const relationship = await children.status(first.tenantId, child.executionId);
    assert.equal(relationship.conversationId, child.conversationId);
    if (["normal", "serving-pending", "already-returned"].includes(mode))
      assert(JSON.stringify(history).includes(await dir.result()));
    if (mode === "reply-timeout")
      assert(JSON.stringify(history).includes("PARENT CHILD UNKNOWN tool_reply_timeout"));
    if (mode === "user-stop") assert.equal(state.cancels, 1);
    const canonical = await pool.query(
      "select content from messages where run_id=$1 and conversation_id=$2 and role='toolResult'",
      [runId, child.conversationId],
    );
    const toolResults = canonical.rows
      .map((r) => JSON.parse(r.content).model[0])
      .filter((m) => m.toolName === "bash");
    if (mode !== "user-stop") {
      assert.equal(toolResults.length, 1);
      assert.equal(Boolean(toolResults[0].isError), mode === "reply-timeout");
    }
    if (mode === "reply-timeout") {
      assert.equal(state.probes, 1);
      assert(
        childPage.turns
          .flatMap((t) => t.transcript?.items ?? [])
          .some((i) => i.kind === "tool" && i.toolName === "bash" && i.status === "unknown"),
      );
      assert(!JSON.stringify(toolResults).includes(await dir.result()));
    }
    if (mode === "user-stop") {
      const receipt = await database
        .selectFrom("turn_control_requests")
        .select(["state", "acknowledged_at", "completed_at"])
        .where("id", "=", state.stopReceipt)
        .executeTakeFirstOrThrow();
      assert(receipt.acknowledged_at && receipt.completed_at && receipt.state === "completed");
    }
    const check = await pool.query("select projection_tools from runs where id=$1", [runId]);
    assert.deepEqual(check.rows[0].projection_tools, {});
    assert.equal((await api.getRun(runId)).state, terminal);
    assert.equal(state.dispatches, 1);
    assert.equal(await dir.marker(), "once");
    return {
      mode,
      terminal,
      publicChildView: true,
      originalRunReclaimed: !!b,
      physicalDispatches: state.dispatches,
      guestExecutions: 1,
      cancelCommands: state.cancels,
      workerKillToProjectedSealMs: expiryToSeal,
      ...(cold ? { coldMs: Math.round(cold.coldMs), cold: cold.stats } : {}),
      canonicalBashResults: toolResults.length,
      readinessProbes: state.probes,
      finalOutputToProjectionMs: outcome.finalModelAt ? state.endAt - outcome.finalModelAt : null,
      replacementBashIntents: b ? outcome.newIntents : null,
      ...(mode === "user-stop"
        ? { modelsAfterStop: outcome.models, childQueuedSubmissionAtStop: outcome.queuedStatus }
        : {}),
      replyAfterTerminal: mode === "reply-timeout",
      ...(pgHandoff
        ? {
            candidateRowsIdenticalForDifferentRequestIds: state.ambiguous,
            requestIdAbsentFromExistingProjectorView: state.requestAbsentFromMemory,
            sealStateBytes: state.snapshotBytes,
            matchedOriginalRequestId: true,
            sealedPrefixUnavailableBeforeB: true,
            servingPgHasOriginalRequest: true,
          }
        : {}),
    };
  } finally {
    await a?.close();
    await b?.close();
    await witness?.close();
  }
}
try {
  await sessionAdmin?.connect();
  await replyAdmin.connect();
  await replyCopies.connect();
  assert.equal(
    (await pool.query("select count(*)::int n from pg_tables where schemaname='public'")).rows[0].n,
    0,
  );
  await runMigrations(database, "up");
  await pool.query(
    "create schema candidate; alter table public.runs add column history_head jsonb",
  );
  const schema = await readFile(new URL("./schema.sql", import.meta.url), "utf8");
  await pool.query(schema.slice(schema.indexOf("create table messages")));
  await view.install();
  await view.reset(new Map([[0, 0n]]), 16, "recorded-history");
  await pool.query(`create role family_reader login; alter role family_reader set search_path=candidate,public;
    grant usage on schema candidate to family_reader; grant select on all tables in schema candidate to family_reader;
    grant select (id,tenant_id,session_id,state,lease_id,mailbox_position,history_head,projection_tools,projection_state) on public.runs to family_reader`);

  assert.equal(
    (
      await pool.query(
        "select has_table_privilege('family_reader','public.pi_durable_current','select') as allowed",
      )
    ).rows[0].allowed,
    false,
  );
  if (pgHandoff) await installStudyHandoff(pool);
  tenant = await createPrivateTenant(database, {
    slug: "family-study",
    ownerDisplayName: "Family study",
  });
  projector = new SessionProjector({
    database,
    publicationDatabase: database,
    notificationConnectionString: dbUrl,
    brokers,
    topic,
    clientId: topic,
    partitions: 1,
    replicas: 3,
    retentionMs: 604800000,
    publicationToken: randomUUID() + randomUUID(),
    advertisedBaseUrl: "http://127.0.0.1:9",
    executionProbes: {
      reset() {},
      consume(f, current) {
        const s = cases.get(f.scope.runId);
        assert(
          current() &&
            s &&
            s.mode === "reply-timeout" &&
            f.execution.bindings[0].bindingId === s.binding,
        );
        s.probes++;
        void s.dir.probe(f).catch((e) => {
          s.error = e;
        });
      },
    },
    toolCommands: {
      async consume(r) {
        const f = r.fact,
          s = cases.get(f.scope?.runId);
        if (!s) return;
        try {
          if (f.kind === "tool_command") {
            assert.equal(++s.dispatches, 1);
            s.ref = f.replyRecovery;
            s.operation = f.request.operationId;
            await s.dir.submit(
              r,
              f,
              s.ref,
              s.mode === "reply-timeout" ? Date.now() + 120000 : s.ref.deadlineAt,
            );
          }
          if (f.kind === "tool_cancel") {
            assert.equal(f.request.operationId, s.operation);
            assert.equal(f.request.originalWriterId, s.seal.writerId);
            await s.dir.cancel(r, f.request);
            s.cancels++;
          }
        } catch (e) {
          s.error = e;
          throw e;
        }
      },
    },
    nativeChanges: {
      async consume(r, current) {
        const f = r.fact,
          s = cases.get(f.scope.runId);
        if (!s) return;
        try {
          if (f.kind === "durable_commit") {
            if (s.study && !s.seal) {
              const q = f.commit.writes.find(
                (w) => w.type === "submission" && w.value.status === "queued",
              );
              if (q) {
                s.ambiguous = await compareQueuedRequests(pool, view, r, env, q.value);
                s.queuedId = q.value.requestId;
              }
              s.study.observe(f);
            }
            await view.apply([r], current, (item, d) => projectRecord(item, d, env));
            s.seq = f.commit.seq;
          } else if (f.kind === "execution_seal" || f.kind === "work_finished") {
            const from = view.next.get(0);
            if (s.study && f.kind === "execution_seal") {
              const original = view.sessions.get(f.scope.runId);
              assert(s.queuedId && original && original.seq === s.study.seq);
              s.requestAbsentFromMemory = !JSON.stringify(original, (_, v) =>
                typeof v === "bigint" ? String(v) : v,
              ).includes(s.queuedId);
              s.snapshotBytes = await s.study.seal(pool, r, from, bashFields);
            } else
              assert.equal(
                (
                  await pool.query(
                    "update accepted_fact_projection_offsets set next_offset=$1 where topic=$2 and partition=0 and next_offset=$3",
                    [String(r.offset + 1n), topic, String(from)],
                  )
                ).rowCount,
                1,
              );
            view.next.set(0, r.offset + 1n);
            if (f.kind === "execution_seal")
              s.seal = {
                factId: f.factId,
                offset: String(r.offset),
                writerId: f.scope.writerId,
                epoch: f.scope.fencingToken,
              };
            if (f.kind === "work_finished") {
              s.end = f.terminal.type;
              s.endAt = Date.now();
            }
          }
        } catch (e) {
          s.error = e;
          throw e;
        }
      },
    },
  });
  await projector.start();
  await until(() => projector.ownsPartition(0), "serving assignment");
  app = await createControlPlaneApplication({
    database,
    eventRuntime: { eventHub: projector.eventHub, eventStore: projector.eventStore },
    productionHttpGateway: new ProductionHttpGateway({
      authenticator: new PostgresTenantApiAuthenticator({ database }),
      readiness: () => true,
    }),
  });
  await app.listen(0, "127.0.0.1");
  const origin = await app.getUrl();
  api = new PiCloudApi((path, opts) => fetch(new URL(path, origin), opts), tenant.credential.token);
  const handoffs = ["serving-pending", "already-returned", "reply-timeout", "user-stop"];
  for (const mode of bashFields
    ? ["serving-pending", "user-stop"]
    : pgHandoff
      ? handoffs
      : ["normal", ...handoffs]) {
    console.log("Starting " + mode);
    results.push(await withFamilyGuest(cube, (guest) => scenario(guest, mode)));
    console.log(JSON.stringify(results.at(-1)));
  }
} finally {
  try {
    await app?.close();
    await projector?.close();
  } finally {
    try {
      const owned = [...replyBoots].map((id) => "pi-cloud.tool-replies.v4." + id),
        topics = await replyAdmin.listTopics();
      const existing = owned.filter((t) => topics.includes(t));
      if (existing.length) await replyAdmin.deleteTopics({ topics: existing });
      const groups = (await replyAdmin.listGroups()).groups
        .map((g) => g.groupId)
        .filter((g) => [...replyBoots].some((id) => g.includes(id)));
      for (const group of groups)
        await until(
          async () => {
            try {
              await replyAdmin.deleteGroups([group]);
              return true;
            } catch {
              return !(await replyAdmin.listGroups()).groups.some((g) => g.groupId === group);
            }
          },
          "reply group deletion",
          70000,
        );
      await until(
        async () => !(await replyAdmin.listTopics()).some((t) => owned.includes(t)),
        "reply topic deletion",
      );
    } finally {
      await replyCopies.disconnect();
      await replyAdmin.disconnect();
      await sessionAdmin?.disconnect();
      await database.destroy();
      await pool.end();
    }
  }
}
await writeFile(
  process.argv[3],
  JSON.stringify({ results, cleanupVerified: true }, null, 2) + "\n",
);
