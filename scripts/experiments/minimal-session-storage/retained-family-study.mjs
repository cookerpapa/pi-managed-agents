import assert from "node:assert/strict";
import { familyWorker, until } from "./family-process.mjs";
import { randomUUID } from "node:crypto";
import { readFile, writeFile } from "node:fs/promises";
import pg from "pg";
import native from "@confluentinc/kafka-javascript";
import { KafkaLogConsumer } from "../../../packages/event-log/src/kafka-log-consumer.ts";
import { encodeSessionLogFact } from "../../../packages/runtime-core/src/kafka-accepted-fact.ts";
import { decodeWorkspaceFact } from "./workspace-operations.mjs";
import { installWorkspaceRoutes } from "./workspace-route-spans.mjs";
import { WorkspaceOperationRouter } from "./workspace-operation-router.mjs";
import { ReadyProjection } from "./ready-projection.mjs";
import { projectRecord } from "./project-record.mjs";
import { projectionTemplates } from "./projection-statements.mjs";
import { withFamilyGuest } from "./retained-family-cube.mjs";

const { brokers, dbUrl, cube } = JSON.parse(await readFile(process.argv[2], "utf8"));
const db = new pg.Pool({ connectionString: dbUrl, max: 4 });
const kafka = new native.KafkaJS.Kafka({
  kafkaJS: { brokers, logLevel: native.KafkaJS.logLevel.NOTHING },
});
const admin = kafka.admin(),
  producer = kafka.producer({ kafkaJS: { acks: -1, idempotent: true } });
const topics = new Set(),
  prefixes = new Set(),
  replyBoots = new Set(),
  results = [];
function worker(options) {
  replyBoots.add(options.scope.writerId);
  topics.add("pi-cloud.tool-replies.v4." + options.scope.writerId);
  return familyWorker(options);
}
async function send(topic, f) {
  return Number(
    (
      await producer.send({
        topic,
        messages: [{ partition: 0, key: f.scope.piSessionId, value: encodeSessionLogFact(f) }],
      })
    )[0].baseOffset,
  );
}
async function scenario(guest, mode) {
  const topic = "pma-family-" + randomUUID();
  topics.add(topic);
  prefixes.add(topic);
  const first = {
      tenantId: randomUUID(),
      piSessionId: randomUUID(),
      sessionId: randomUUID(),
      runId: randomUUID(),
      turnId: randomUUID(),
      writerId: randomUUID(),
      fencingToken: ["prior-rotated", "pending"].includes(mode) ? 2 : 1,
    },
    second = { ...first, writerId: randomUUID(), fencingToken: first.fencingToken + 1 },
    binding = randomUUID();
  await admin.createTopics({
    topics: [
      {
        topic,
        numPartitions: 1,
        replicationFactor: 3,
        configEntries: [
          { name: "min.insync.replicas", value: "2" },
          { name: "retention.ms", value: "604800000" },
          { name: "retention.bytes", value: "-1" },
          { name: "cleanup.policy", value: "delete" },
          { name: "message.timestamp.type", value: "LogAppendTime" },
        ],
      },
    ],
  });
  const dir = await guest.directory(binding);
  const view = new ReadyProjection(db, topic, new Map(), projectionTemplates);
  await view.reset(new Map([[0, 0n]]), 16, "recorded-history");
  const prior = ["prior-rotated", "pending"].includes(mode)
    ? {
        ...first,
        runId: randomUUID(),
        turnId: randomUUID(),
        writerId: randomUUID(),
        fencingToken: 1,
      }
    : null;
  async function register(s, position) {
    await db.query(
      "insert into runs(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,model_ref,state,lease_id) values($1,$2,$3,$5,$1,'fixture','fixture','running',$4)",
      [s.runId, s.tenantId, s.piSessionId, s.writerId, position],
    );
  }
  await register(prior ?? first, 1);
  const env = {
    pool: db,
    sessions: view.sessions,
    stage: { integrated: true, sink: "single", scene: "plain" },
    metrics: {
      byPartition: [0],
      queries: 0,
      skipped: 0,
      peakWaiting: 0,
      inboxSnapshots: 0,
      inboxBytes: 0,
    },
    nextOffsets: new Map(),
    account() {},
    send() {},
    partitions: 1,
  };
  let replyWitness, childCreationOffset, creationKey;
  let acceptedScope = prior ?? first,
    seal,
    old,
    successor,
    dispatch,
    dispatchCount = 0,
    stale = 0,
    settledAt,
    failed;
  const same = (a, b) =>
    ["tenantId", "piSessionId", "sessionId", "runId", "turnId", "writerId", "fencingToken"].every(
      (k) => a[k] === b[k],
    );
  async function advance(r) {
    const p = view.next.get(0);
    assert(p <= r.offset);
    assert.equal(
      (
        await db.query(
          "update accepted_fact_projection_offsets set next_offset=$1 where topic=$2 and partition=0 and next_offset=$3",
          [String(r.offset + 1n), topic, String(p)],
        )
      ).rowCount,
      1,
    );
    view.next.set(0, r.offset + 1n);
  }
  const router = new WorkspaceOperationRouter({
    pool: db,
    view,
    env,
    select: () => binding,
    resolve: (id) => {
      assert.equal(id, binding);
      return {
        async submit(r, cmd, ref) {
          assert.equal(++dispatchCount, 1, "Duplicate physical dispatch");
          await dir.submit(r, cmd, ref);
          dispatch = { ref, op: cmd.request.operationId, offset: r.offset };
        },
      };
    },
  });
  const reader = new KafkaLogConsumer({
    brokers,
    topic,
    clientId: topic,
    groupId: topic,
    orderedEffects: true,
    groupRecovery: true,
    decode: (v) => decodeWorkspaceFact(v).fact,
    handler: async (r, current, grant) => {
      if (failed) throw failed;
      try {
        const f = r.fact;
        if (f.kind === "execution_seal") {
          assert(
            seal?.factId === f.factId &&
              same(f.scope, first) &&
              acceptedScope === first &&
              f.recover,
          );
          // Existing prototype authority row and projection position. No new
          // journal/receipt/checkpoint. The fixture supplies the control intent.
          const c = await db.connect();
          try {
            await c.query("begin");
            assert.equal(
              (
                await c.query(
                  "update runs set lease_id=null where id=$1 and tenant_id=$2 and session_id=$3 and lease_id=$4 and state='running'",
                  [first.runId, first.tenantId, first.piSessionId, first.writerId],
                )
              ).rowCount,
              1,
            );
            assert.equal(
              (
                await c.query(
                  "update accepted_fact_projection_offsets set next_offset=$1 where topic=$2 and partition=0 and next_offset=$3",
                  [String(r.offset + 1n), topic, String(view.next.get(0))],
                )
              ).rowCount,
              1,
            );
            await c.query("commit");
          } catch (e) {
            await c.query("rollback");
            throw e;
          } finally {
            c.release();
          }
          view.next.set(0, r.offset + 1n);
          acceptedScope = null;
          return;
        }
        if (!acceptedScope || !same(f.scope, acceptedScope)) {
          stale++;
          await advance(r);
          return;
        }
        if (f.kind === "durable_commit") await router.consume(r, current, grant);
        else await view.apply([r], current, (item, d) => projectRecord(item, d, env));
        if (f.kind === "durable_commit") {
          const creation = f.commit.writes.find(
            (w) =>
              w.type === "document.create" && w.record.kind === "pi-cloud.conversation-creation",
          );
          if (creation) creationKey = creation.record.key;
          if (creationKey && JSON.stringify(f).includes(creationKey))
            childCreationOffset = r.offset;
        }
        if (f.kind === "work_finished") {
          await db.query(
            "delete from workspace_route_spans where run_id=$1 and exists(select 1 from runs where id=$1 and state='completed')",
            [f.scope.runId],
          );
          router.spans.forget(f.scope);
          settledAt = Date.now();
          return;
        }
      } catch (e) {
        failed = e;
        throw e;
      }
    },
  });
  const checked = async (fn) => {
    if (failed) throw failed;
    old?.check();
    successor?.check();
    return fn();
  };
  const note = (step) => console.log(JSON.stringify({ mode, step }));
  try {
    await reader.start();
    await reader.waitUntilAssigned(30000);
    const decoy = {
      kind: "durable_commit",
      factId: randomUUID(),
      scope: { ...first, piSessionId: randomUUID() },
      occurredAt: new Date().toISOString(),
      commit: { seq: 1, nextId: 2, writes: [] },
      tools: [],
      origins: [],
      display: [],
      cancellations: [],
    };
    if (prior) {
      old = worker({ dbUrl, brokers, topic, scope: prior, mode: "seed" });
      await old.wait("seedFinished");
      assert.equal((await old.ended).code, 0);
      await reader.waitUntilInitialReplay(await reader.captureEndOffsets());
      assert(settledAt);
      acceptedScope = first;
      await register(first, 2);
      settledAt = undefined;
    }
    const decoyOffset = await send(topic, decoy);
    await reader.waitUntilInitialReplay([BigInt(decoyOffset) + 1n]);
    if (mode !== "pending")
      await admin.deleteTopicRecords({
        topic,
        partitions: [{ partition: 0, offset: String(decoyOffset + 1) }],
      });
    // A different incarnation can reach Kafka even while this Run is valid.
    const wrongWriter = { ...first, writerId: randomUUID() };
    await send(topic, { ...decoy, scope: wrongWriter, factId: randomUUID() });
    note("starting-A");
    old = worker({ dbUrl, brokers, topic, scope: first, mode });
    await until(() => checked(() => !!dispatch), "original dispatch", 55000);
    await until(() => checked(async () => (await dir.marker()) === "once"), "Guest marker");
    old.proc.send({ kind: "queue" });
    await old.wait("queued");
    await reader.waitUntilInitialReplay(await reader.captureEndOffsets());
    assert.equal(
      (
        await db.query(
          "select count(*)::int as n from inboxes where tenant_id=$1 and jsonb_array_length(items)=1",
          [first.tenantId],
        )
      ).rows[0].n,
      1,
    );
    old.proc.kill("SIGKILL");
    assert.equal((await old.ended).signal, "SIGKILL");
    note("A-killed-Guest-running");
    seal = { factId: randomUUID(), writerId: first.writerId, epoch: first.fencingToken };
    seal.offset = await send(topic, {
      kind: "execution_seal",
      factId: seal.factId,
      scope: first,
      agentId: "root",
      baseSequence: 0,
      recover: true,
      occurredAt: new Date().toISOString(),
    });
    await reader.waitUntilInitialReplay([BigInt(seal.offset) + 1n]);
    assert.equal(acceptedScope, null);
    assert.equal(
      (
        await db.query(
          "update runs set lease_id=$1 where id=$2 and state='running' and lease_id is null",
          [second.writerId, second.runId],
        )
      ).rowCount,
      1,
    );
    view.sessions.get(first.runId).writer = second.writerId;
    acceptedScope = second;
    await send(topic, { ...decoy, factId: randomUUID(), scope: first });
    await reader.waitUntilInitialReplay(await reader.captureEndOffsets());
    assert.equal(stale, 3);
    let realReply;
    replyWitness = new KafkaLogConsumer({
      brokers,
      clientId: topic + "-reply",
      groupId: topic + "-cold-reply",
      topic: dispatch.ref.topic,
      commitMessages: false,
      decode: (v) => JSON.parse(v.toString()),
      handler: async ({ fact }) => {
        if (fact.kind === "tool_result" && fact.operationId === dispatch.op) {
          assert(
            !fact.result.isError,
            "Guest reported a failed command: " + fact.result.result.content[0]?.text,
          );
          realReply = fact;
        }
      },
    });
    await replyWitness.start();
    await replyWitness.waitUntilAssigned(25000);
    async function delivered() {
      await dir.release();
      await until(() => realReply, "actual Guest tool_result in Kafka");
    }
    if (mode === "already-returned" || mode === "expired") await delivered();
    if (mode === "already-returned") {
      await producer.send({
        topic: dispatch.ref.topic,
        messages: [{ partition: 0, key: dispatch.op, value: JSON.stringify(realReply) }],
      });
      await replyWitness.waitUntilInitialReplay(await replyWitness.captureEndOffsets());
    }
    if (mode === "expired") {
      // Remove the old child-creation receipt but keep the newer Bash intent, Inbox edit and seal.
      assert(
        creationKey && childCreationOffset !== undefined && childCreationOffset < dispatch.offset,
      );
      const {
        rows: [check],
      } = await db.query(
        `select exists(select 1 from (
        select to_jsonb(r)::text as body from runs r where id=$2
        union all select to_jsonb(m)::text from messages m where run_id=$2
        union all select to_jsonb(i)::text from inboxes i where tenant_id=$3 and pi_session_id=$4
      ) v where strpos(body,$1)>0) as creation_in_pg`,
        [creationKey, first.runId, first.tenantId, first.piSessionId],
      );
      assert.equal(check.creation_in_pg, false, "Selected historical receipt is available in PG");

      await admin.deleteTopicRecords({
        topic,
        partitions: [{ partition: 0, offset: String(childCreationOffset + 1n) }],
      });
    }
    note("starting-B");
    successor = worker({ dbUrl, brokers, topic, scope: second, seal, mode });
    if (mode === "expired") {
      const gap = await successor.wait("expired");
      assert.equal(gap.writes, 0);
      assert.equal(await dir.marker(), "once");
      assert.equal((await successor.ended).code, 0);
      return {
        mode,
        workerAKilled: true,
        pgCaughtUp: true,
        retainedBashIntent: true,
        missingNativeCreationKey: true,
        originalReplyInKafka: true,
        failClosed: true,
        newNativeWrites: gap.writes,
        physicalRuns: dispatchCount,
        gap: gap.reason,
      };
    }
    const cold = await successor.wait("cold");
    const resumed = await successor.wait("reattached");
    assert.equal(resumed.deadline, dispatch.ref.deadlineAt);
    assert.equal(resumed.operationId, dispatch.op);
    assert.equal(resumed.originalTopic, dispatch.ref.topic);
    if (mode === "pending" || mode === "prior-rotated") {
      assert(!realReply);
      await delivered();
    }
    const end = await successor.wait("finished");
    await reader.waitUntilInitialReplay(await reader.captureEndOffsets());
    assert(!failed);
    assert.equal((await successor.ended).code, 0);
    const token = await dir.result();
    assert.match(token, /^GUEST-[a-f0-9-]{36}$/);
    assert(end.parentText.includes(token));
    assert.equal(await dir.marker(), "once");
    const r = await db.query("select state,projection_tools from runs where id=$1", [first.runId]);
    assert.deepEqual(r.rows, [{ state: "completed", projection_tools: {} }]);
    const records = (
      await db.query(
        "select conversation_id,content from messages where run_id=$1 and role='toolResult'",
        [first.runId],
      )
    ).rows;
    assert.equal(
      records.filter((r) => JSON.parse(r.content).model[0].toolName === "bash").length,
      1,
    );
    return {
      mode,
      duplicateReplyBeforeResume: mode === "already-returned",
      workerAKilled: true,
      freshProcess: true,
      sameRootRun: true,
      physicalRuns: 1,
      restoredToolResults: 1,
      originalDeadline: true,
      originalReplyTopic: true,
      staleAfterSealRejected: true,
      childModelCalls: end.childModelCalls,
      parentModelCalls: end.parentModelCalls,
      intentsFromB: end.newIntents,
      queuedInputSettled: true,
      parentReceivedGuestValue: true,
      coldReadMs: cold.coldMs,
      ...cold.stats,
      finalOutputToSettlementMs: settledAt - end.finalModelAt,
    };
  } catch (e) {
    console.error("Scene " + mode + ": " + e.stack);
    throw e;
  } finally {
    try {
      await old?.close();
      await successor?.close();
    } finally {
      await replyWitness?.close();
      await reader.close();
    }
  }
}
try {
  assert.equal(
    (await db.query("select count(*)::int n from pg_tables where schemaname='public'")).rows[0].n,
    0,
  );
  await db.query(await readFile(new URL("./schema.sql", import.meta.url), "utf8"));
  const setup = new ReadyProjection(db, "unused", new Map(), projectionTemplates);
  await setup.install();
  await installWorkspaceRoutes(db);
  await admin.connect();
  await producer.connect();
  for (const mode of ["already-returned", "pending", "prior-rotated", "expired"]) {
    await withFamilyGuest(cube, async (guest) => {
      results.push(await scenario(guest, mode));
    });
  }
} finally {
  try {
    await producer.disconnect();
    const names = await admin.listTopics(),
      owned = names.filter((n) => topics.has(n));
    if (owned.length) await admin.deleteTopics({ topics: owned });
    const groups = (await admin.listGroups()).groups
      .map((g) => g.groupId)
      .filter(
        (g) =>
          [...prefixes].some((p) => g === p || g.startsWith(p + "-cold-")) ||
          [...replyBoots].some((b) => g.includes(b)),
      );
    for (const g of groups)
      await until(
        async () => {
          try {
            await admin.deleteGroups([g]);
            return true;
          } catch {
            return !(await admin.listGroups()).groups.some((x) => x.groupId === g);
          }
        },
        "owned consumer group cleanup",
        70000,
      );
    await until(
      async () => !(await admin.listTopics()).some((n) => topics.has(n)),
      "owned topic deletion",
      45000,
    );
  } finally {
    await admin.disconnect();
    await db.end();
  }
}
await writeFile(
  process.argv[3],
  JSON.stringify(
    {
      replicas: 3,
      retentionDays: 7,
      newProjectionTables: 0,
      source: "Cube fixed Guest / HTTPS completion / RF3",
      results,
      cleanupVerified: true,
    },
    null,
    2,
  ) + "\n",
);
