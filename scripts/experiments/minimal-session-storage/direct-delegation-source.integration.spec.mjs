// Only the native call and the root Run determine the child across these independent fault boundaries.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { it } from "vitest";
import pg from "pg";
import { BACKGROUND_CONTEXT as ctx } from "@earendil-works/chord/context";
import { MemoryStorage } from "@earendil-works/pi-durable";
import { createDatabase } from "@pi-cloud/database";
import {
  AcceptedFamilyCheckpoint,
  readSealedFamily,
  hydrateSealedFamily,
  restoredDelegationIntent,
  saveSealedFamily,
} from "@pi-cloud/pi-session-postgres/durable";
import { durableControlIntent, durableToolState } from "@pi-cloud/protocol";
import { delegatedRunId } from "@pi-cloud/protocol/delegation-identity";
import { ControlToolDispatcher } from "../../../packages/control-plane/src/control-tool-dispatcher.ts";
import { normalRootProjection } from "./normal-root-projection.mjs";
import { delegatedControl } from "./delegated-runs.mjs";
import { encodeWorkspaceFact, decodeWorkspaceFact } from "./workspace-operations.mjs";
import { readRetainedPrefix } from "./retained-control-prefix.mjs";
import { waitingDelegation, executeChild, resumeParent } from "./direct-delegation-worker.mjs";

async function replayToOriginalWorker(pool, db, topic, records, projection, old, childId) {
  let reader;
  const deliveries = [];
  const dispatcher = new ControlToolDispatcher({
    database: db,
    partitions: 1,
    ownsPartition: () => true,
    directDelegations: {
      initialReplayComplete: (p) => reader?.initialReplayComplete(p) === true,
      source: () => assert.fail("Original Worker still owns the invocation"),
    },
    execute: (cmd, current) => executeChild(pool, cmd, old.runtime, old.provider, current),
    publish: async (t, r) => {
      assert.equal(t, old.replyTopic);
      assert.equal(r.operationId, old.ids[0]);
      deliveries.push(r.operationId);
      await old.publish(t, r);
    },
  });
  try {
    const result = await readRetainedPrefix(
      process.env.PI_CLOUD_KAFKA_INTEGRATION_BROKERS.split(","),
      topic,
      records,
      (bounds) => projection.nativeStore.replayPlan(db, topic, bounds),
      {
        async observe(r, current, group) {
          reader = group;
          const receipt = await projection.nativeStore.projection.project(r.fact, r, current);
          if (receipt.admitted) await dispatcher.consume(r, current);
        },
        async drained() {
          dispatcher.wake(records[0].scope.tenantId, records[0].scope.piSessionId);
          assert.equal((await old.submission.wait(ctx)).status, "done");
          const results = (await old.root.context(ctx)).messages.filter(
            (m) => m.role === "toolResult" && m.toolName === "subagent",
          );
          assert.equal(results.length, 1);
          assert(JSON.stringify(results).includes(childId));
        },
      },
    );
    assert.equal(deliveries.length, 1);
    return result;
  } finally {
    await dispatcher.close();
  }
}

async function replaceWaitingSuccessor(
  pool,
  db,
  topic,
  scope,
  family,
  oldTopic,
  childId,
  firstSeal,
) {
  const wait = await resumeParent(pool, db, topic, scope, family, oldTopic, childId, true);
  const capture = AcceptedFamilyCheckpoint.resume(
    scope.writerId,
    family.handoff.state,
    family.queues.map((q) => Number(q.doc_id)),
  );
  for (const record of wait.accepted) {
    assert(
      !record.commit.writes.some((w) => w.type === "entry"),
      "A waiting replacement wrote a new history entry",
    );
    capture.observe(record);
  }
  const next = { ...scope, writerId: randomUUID(), fencingToken: scope.fencingToken + 1 };
  const position = { ...firstSeal, offset: firstSeal.offset + BigInt(wait.accepted.length) + 1n },
    sealId = randomUUID();
  // No later child was registered. Use the real predecessor and new seal for C.
  await pool.query(
    "update runs set output_seal_id=$2,output_first_offset=$3,recovery_requested_at=now() where id=$1",
    [scope.runId, sealId, String(firstSeal.offset + 1n)],
  );
  await db
    .transaction()
    .execute((tx) => saveSealedFamily(tx, scope, position, sealId, capture.snapshot()));
  await pool.query(
    "update session_leases set writer_seal_offset=$2,writer_sealed_at=now(),released_at=now(),writer_failed_at=now() where lease_id=$1",
    [scope.writerId, String(position.offset)],
  );
  await pool.query("insert into session_leases values($1,$2,$3,$4,null,null,null,null)", [
    scope.tenantId,
    scope.piSessionId,
    next.writerId,
    next.fencingToken,
  ]);
  await pool.query("update runs set lease_id=$2,fencing_token=$3 where id=$1", [
    scope.runId,
    next.writerId,
    next.fencingToken,
  ]);
  const restored = await readSealedFamily(db, next, topic, { delegatedRuns: true });
  await resumeParent(pool, db, topic, next, restored, wait.replyTopic, childId);
  return next;
}

it.skipIf(!process.env.PI_CLOUD_POSTGRES_INTEGRATION_URL).each(["fresh", "branch"])(
  "reattaches one %s native delegation either side of child Run admission",
  async (mode) => {
    const tenantId = randomUUID(),
      sessionId = randomUUID(),
      runId = randomUUID(),
      writerId = randomUUID();
    const scope = {
      tenantId,
      sessionId,
      piSessionId: sessionId,
      turnId: runId,
      runId,
      writerId,
      fencingToken: 1,
    };
    const successor = { ...scope, writerId: randomUUID(), fencingToken: 2 };
    const topic = "direct-delegation-" + randomUUID(),
      sealId = randomUUID();
    const old = await waitingDelegation(scope, mode);
    const { prefix, ids } = old;
    try {
      const seal = { topic, partition: 0, offset: BigInt(prefix.length) };
      const snapshots = [];
      let replayEvidence;
      for (const [id, alreadyAdmitted, projectorOnly] of [
        [ids[0], false],
        [ids[1], false],
        [ids[0], true],
        ...(process.env.PI_CLOUD_KAFKA_INTEGRATION_BROKERS ? [[ids[0], true, true]] : []),
      ]) {
        const schema = "delegate_" + randomUUID().replaceAll("-", "");
        const url = new URL(process.env.PI_CLOUD_POSTGRES_INTEGRATION_URL);
        url.searchParams.set("options", "-c search_path=" + schema);
        const pool = new pg.Pool({ connectionString: url.href, max: 3 });
        const db = createDatabase({ connectionString: url.href, maxConnections: 2 });
        try {
          await pool.query("create schema " + schema);
          for (const f of ["schema.sql", "delegated-runs.sql"])
            await pool.query(await readFile(new URL(f, import.meta.url), "utf8"));
          // Columns normally supplied by the shared control schema.
          await pool.query(`alter table runs add turn_id uuid,add fencing_token bigint,add output_seal_id uuid,add recovery_requested_at timestamptz,add output_first_topic text,add output_first_partition int,add output_first_offset bigint,add output_display_seq bigint not null default 0,add output_display_native_seq bigint not null default 0;
        create table session_leases(tenant_id uuid,pi_session_id text,lease_id uuid,fencing_token bigint,writer_sealed_at timestamptz,writer_seal_offset bigint,released_at timestamptz,writer_failed_at timestamptz,valid_until timestamptz not null default (now()+interval '5 minutes'))`);
          const projection = normalRootProjection(
            pool,
            topic,
            () => assert.fail(),
            () => assert.fail(),
            undefined,
            { delegatedRuns: true },
          );
          await projection.view.install();
          await projection.view.reset(new Map([[0, 0n]]), 16, "recorded-history");
          await pool.query("insert into sessions values($1,$2)", [sessionId, tenantId]);
          await pool.query(
            `insert into runs(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,model_ref,state,lease_id,turn_id,fencing_token) values($1,$2,$3,1,$1,'Delegate','fixture','running',$4,$1,1)`,
            [runId, tenantId, sessionId, writerId],
          );
          const family = new AcceptedFamilyCheckpoint(),
            records = [];
          for (const [i, base] of prefix.entries()) {
            const { fact } = decodeWorkspaceFact(
              Buffer.from(
                encodeWorkspaceFact({
                  ...base,
                  tools: base.tools.map((ref) => ({ ...ref, operationId: id })),
                }),
              ),
            );
            records.push(fact);
            const pos = { topic, partition: 0, offset: BigInt(i) };
            const result = await projection.nativeStore.projection.project(fact, pos, () => true);
            assert(result.admitted);
            family.observe(fact);
          }
          const source = records.find((f) => f.tools.length),
            [ref] = source.tools;
          const intent = durableControlIntent(source.commit, ref);
          const childId = delegatedRunId(scope, ref.taskId);
          const request = {
            callerLane: 1,
            taskId: ref.taskId,
            task: intent.arguments.task,
            mode,
            at: ref.contextAnchor,
            workspace: intent.arguments.workspace,
          };
          if (alreadyAdmitted) {
            const child = await delegatedControl(pool, scope).start(request);
            assert.equal(child.id, childId);
          }
          if (projectorOnly) {
            const restart = normalRootProjection(
              pool,
              topic,
              () => assert.fail(),
              () => assert.fail(),
              undefined,
              { delegatedRuns: true },
            );
            const replay = await replayToOriginalWorker(
              pool,
              db,
              topic,
              records,
              restart,
              old,
              childId,
            );
            replayEvidence = {
              start: replay.start,
              intent: records.indexOf(source),
              seq: source.commit.seq,
            };
            assert.deepEqual(replay.found, [
              {
                offset: records.indexOf(source),
                op: id,
                reply: ref.replyTopic,
                args: intent.arguments,
              },
            ]);
            assert.equal(
              (await pool.query("select id from runs where root_run_id=$1", [runId])).rowCount,
              1,
            );
            continue;
          }
          await pool.query(
            "update runs set output_seal_id=$2,recovery_requested_at=now(),output_first_topic=$3,output_first_partition=0,output_first_offset=0 where id=$1",
            [runId, sealId, topic],
          );
          await db
            .transaction()
            .execute((tx) => saveSealedFamily(tx, scope, seal, sealId, family.snapshot()));
          await pool.query(
            `insert into session_leases values($1,$2,$3,1,now(),$5,now(),now()),($1,$2,$4,2,null,null,null,null)`,
            [tenantId, sessionId, writerId, successor.writerId, String(seal.offset)],
          );
          await pool.query("update runs set lease_id=$2,fencing_token=2 where id=$1", [
            runId,
            successor.writerId,
          ]);
          const restored = await readSealedFamily(db, successor, topic, { delegatedRuns: true });
          const task = restored.handoff.state.records.find(
            (r) => r.write.type === "task" && r.write.value.id === ref.taskId,
          );
          const native = durableToolState(task.write.value);
          assert(native.running);
          assert.deepEqual(native.intent.arguments, intent.arguments);
          assert.equal(native.intent.callId, intent.callId);
          assert.equal(restored.intents.length, 0);
          const snapshot = (
            await pool.query(
              "select native_work_handoff,projection_tools,projection_state,projection_first,projection_pin,history_head from runs where id=$1",
              [runId],
            )
          ).rows[0];
          const beforeRetry = (
            await pool.query("select id from runs where root_run_id=$1", [runId])
          ).rows;
          assert.deepEqual(beforeRetry, alreadyAdmitted ? [{ id: childId }] : []);
          if (!alreadyAdmitted) {
            snapshots.push({ restored, snapshot });
            assert(!JSON.stringify(snapshot).includes(id));
          }
          const coldStore = new MemoryStorage();
          const positions = await hydrateSealedFamily(restored, coldStore);
          coldStore.recordSequence = async (id) => positions.get(id);
          const cold = await restoredDelegationIntent(
            coldStore,
            successor,
            ref.conversationId,
            ref.taskId,
          );
          await assert.rejects(
            restoredDelegationIntent(
              coldStore,
              { ...successor, runId: randomUUID() },
              ref.conversationId,
              ref.taskId,
            ),
            /different Run/,
          );
          assert.notEqual(delegatedRunId(scope, ref.taskId + 1), childId);
          assert.equal(cold.operationId, childId);
          assert.equal(cold.sequence, source.commit.seq);
          assert.equal(cold.contextAnchor, ref.contextAnchor);
          if (mode === "branch")
            assert(cold.contextAnchor > 0 && cold.contextAnchor < task.write.value.input.assistant);
          await assert.rejects(delegatedControl(pool, scope).start(request), /no longer active/);
          const coldRequest = {
            callerLane: cold.conversationId,
            taskId: cold.taskId,
            task: cold.arguments.task,
            mode: cold.arguments.context,
            workspace: cold.arguments.workspace,
            at: cold.contextAnchor,
          };
          let currentOwner = successor;
          if (alreadyAdmitted)
            await resumeParent(pool, db, topic, successor, restored, ref.replyTopic, childId);
          else if (id === ids[0])
            currentOwner = await replaceWaitingSuccessor(
              pool,
              db,
              topic,
              successor,
              restored,
              ref.replyTopic,
              childId,
              seal,
            );
          const retries = await Promise.all(
            [1, 2].map(() => delegatedControl(pool, currentOwner).start(coldRequest)),
          );
          assert(retries.every((r) => r.id === childId));
          const children = (
            await pool.query(
              "select id from runs where root_run_id=$1 and caller_run_id=$1 and source_task_id=$2",
              [runId, ref.taskId],
            )
          ).rows;
          assert.deepEqual(children, [{ id: childId }]);
          // An independent insert with another UUID cannot claim the same task twice.
          await assert.rejects(
            pool.query(
              `insert into runs(id,tenant_id,session_id,idempotency_key,initial_input,model_ref,state,root_run_id,caller_run_id,agent_id,source_task_id,context_mode,context_at)
            select $1,tenant_id,session_id,$1,initial_input,model_ref,state,root_run_id,caller_run_id,$1,source_task_id,context_mode,context_at from runs where id=$2`,
              [randomUUID(), childId],
            ),
            (error) => error.code === "23505" && error.constraint === "delegated_root_task",
          );
          await assert.rejects(
            delegatedControl(pool, currentOwner).start({ ...coldRequest, task: "Different input" }),
            /identity changed/,
          );
        } finally {
          await db.destroy();
          try {
            await pool.query("drop schema if exists " + schema + " cascade");
          } finally {
            await pool.end();
          }
        }
      }
      assert.deepEqual(snapshots[0], snapshots[1]);
      process.stdout.write(
        "direct-delegation-source " +
          JSON.stringify({
            preparations: ids.length,
            sameProviderCall: true,
            sameSealedPgRead: true,
            childRowsBeforeAdmission: 0,
            childRowsAfterAdmittedRetry: 1,
            ...(replayEvidence ? { kafka: replayEvidence } : {}),
          }) +
          "\n",
      );
    } finally {
      await old.close();
    }
  },
  120000,
);

it("drops a stopped native delegation before the Projector finishes replay", async () => {
  const runId = randomUUID(),
    sessionId = randomUUID(),
    scope = {
      tenantId: randomUUID(),
      piSessionId: sessionId,
      sessionId,
      turnId: runId,
      runId,
      writerId: randomUUID(),
      fencingToken: 1,
    };
  const old = await waitingDelegation(scope, "fresh");
  let ready = false,
    calls = 0,
    replies = 0;
  const dispatcher = new ControlToolDispatcher({
    database: undefined,
    partitions: 1,
    ownsPartition: () => true,
    directDelegations: { initialReplayComplete: () => ready, source: () => assert.fail() },
    execute: async () => {
      calls++;
      return { waiting: true };
    },
    publish: async () => {
      replies++;
    },
  });
  try {
    await old.root.abort(ctx);
    await old.runtime.harness.waitForIdle(ctx);
    const task = old.prefix.flatMap((f) => f.tools)[0].taskId;
    assert(
      old.facts.some((f) =>
        f.commit.writes.some(
          (w) => w.type === "task" && w.value.id === task && w.value.abortRequested,
        ),
      ),
    );
    for (const [i, fact] of old.facts.entries())
      await dispatcher.consume(
        { topic: "stopped", partition: 0, offset: BigInt(i), fact },
        () => true,
      );
    ready = true;
    dispatcher.wake(scope.tenantId, sessionId);
    await new Promise((resolve) => setImmediate(resolve));
    assert.equal(calls, 0);
    assert.equal(replies, 0);
  } finally {
    await dispatcher.close();
    await old.close();
  }
});
