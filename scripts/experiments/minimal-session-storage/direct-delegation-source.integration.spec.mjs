// Compare two individually valid Worker preparations of the same native task.
// Freeze time/identities in the projected facts so the one changed field is observable.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { it } from "vitest";
import pg from "pg";
import { BACKGROUND_CONTEXT as ctx } from "@earendil-works/chord/context";
import { MemoryStorage } from "@earendil-works/pi-durable";
import { createModels } from "@earendil-works/pi-ai/models";
import {
  fauxAssistantMessage,
  fauxProvider,
  fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import { createDatabase } from "@pi-cloud/database";
import { ToolReplyMailbox, toolReplyTopic } from "@pi-cloud/event-log";
import {
  AcceptedFamilyCheckpoint,
  readSealedFamily,
  saveSealedFamily,
} from "@pi-cloud/pi-session-postgres/durable";
import {
  createExecutionReference,
  durableControlIntent,
  durableToolState,
  infrastructureToolDefinitions,
} from "@pi-cloud/protocol";
import { withToolInvocation } from "@pi-cloud/sandbox-supervisor";
import { DurableAgentSession } from "../../../packages/sandbox-supervisor/src/durable/agent-session.ts";
import { WorkerToolRuntime } from "../../../packages/supervisor-host/src/tools/worker-tool-runtime.ts";
import { ControlToolRegistry } from "../../../packages/control-plane/src/control-tool-registry.ts";
import { normalRootProjection } from "./normal-root-projection.mjs";
import { delegatedControl } from "./delegated-runs.mjs";
import { encodeWorkspaceFact, decodeWorkspaceFact } from "./workspace-operations.mjs";
import { readRetainedPrefix } from "./retained-control-prefix.mjs";

async function waitingDelegation(scope) {
  const facts = [],
    handles = [],
    mailbox = new ToolReplyMailbox();
  const arrived = Promise.withResolvers(),
    replyTopic = toolReplyTopic(randomUUID());
  const models = createModels(),
    provider = fauxProvider({ tokensPerSecond: 0 });
  models.setProvider(provider.provider);
  provider.setResponses([
    fauxAssistantMessage(
      fauxToolCall(
        "subagent",
        { task: "Check the tests", workspace: null },
        { id: "delegate-once" },
      ),
      { stopReason: "toolUse" },
    ),
  ]);
  const definition = infrastructureToolDefinitions().find((t) => t.name === "subagent");
  const bindings = await new WorkerToolRuntime({
    catalog: new ControlToolRegistry(),
    maximumDepth: null,
    implementation: () => undefined,
    nativeConversations: { childAnchor: async () => null },
    wait: (op, call, name, signal) => mailbox.wait(op, call, name, signal),
  }).create({
    command: {
      payload: {
        ...scope,
        piSession: { conversationId: 1 },
        sessionKind: "conversation",
        executionReference: createExecutionReference(scope.writerId, scope.runId, 1),
      },
    },
    environment: () => assert.fail("Unexpected sandbox"),
  });
  const tool = bindings.current().find((b) => b.tool.name === "subagent").tool;
  let runtime;
  class Store extends MemoryStorage {
    async commit(writes) {
      const p = this.prepareCommit(writes);
      const commit = { seq: p.seq, nextId: this.nextId, writes: p.writes };
      const ready = runtime?.preparePublication(commit);
      const tools = (ready?.tools ?? []).map(({ taskId, command: c }) => ({
        target: c.target,
        taskId,
        scope,
        conversationId: c.conversationId,
        toolName: c.toolName,
        contextAnchor: c.contextAnchor,
        operationId: c.operationId,
        replyTopic,
      }));
      facts.push({
        kind: "durable_commit",
        scope,
        commit,
        factId: randomUUID(),
        occurredAt: new Date().toISOString(),
        origins: [],
        tools,
        display: [],
        cancellations: [],
      });
      const seq = p.apply();
      ready?.confirmed();
      return seq;
    }
  }
  runtime = await DurableAgentSession.open(new Store(), {
    models,
    remoteTools: [definition],
    settings: { compaction: { enabled: false } },
  });
  try {
    const root = await runtime.bind({
      runId: scope.runId,
      conversationId: 1,
      model: provider.getModel(),
      agent: {},
      context: {},
      remoteTools: () => ["subagent"],
      tools: [],
      assertCurrent: async () => {},
      openModel: async (r) => ({ model: provider.getModel(), options: r.options, release() {} }),
      prepareRemote: async (v) => {
        const prepare = () =>
          withToolInvocation({ id: "task:" + v.taskId, nativeToolCallId: v.callId }, () =>
            tool.prepareExecution(v.callId, v.args, v.context.abortSignal),
          );
        const live = await prepare();
        handles.push(live, await prepare());
        return {
          ...live,
          result: () => {
            arrived.resolve();
            return live.result();
          },
        };
      },
    });
    const submission = await root.submit(
      { type: "input", requestId: scope.runId, content: "Delegate" },
      ctx,
    );
    await Promise.race([
      arrived.promise,
      submission.wait(ctx).then((result) => {
        throw Error("Delegation settled before the expected tool wait: " + result.status);
      }),
    ]);
    const prefix = facts.slice();
    assert.equal(prefix.flatMap((f) => f.tools).length, 1);
    const ids = handles.map((h) => h.command.operationId);
    assert.equal(new Set(ids).size, 2);
    return { prefix, ids };
  } finally {
    for (const h of handles) h.dispose();
    await runtime.close();
  }
}

it.skipIf(!process.env.PI_CLOUD_POSTGRES_INTEGRATION_URL)(
  "locates the original native delegation either side of child Run admission",
  async () => {
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
    const { prefix, ids } = await waitingDelegation(scope);
    const seal = { topic, partition: 0, offset: BigInt(prefix.length) };
    const snapshots = [];
    let replayEvidence;
    for (const [id, alreadyAdmitted] of [
      [ids[0], false],
      [ids[1], false],
      [ids[0], true],
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
        create table session_leases(tenant_id uuid,pi_session_id text,lease_id uuid,fencing_token bigint,writer_sealed_at timestamptz,writer_seal_offset bigint,released_at timestamptz,writer_failed_at timestamptz)`);
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
        const request = {
          operationId: id,
          callerLane: 1,
          taskId: ref.taskId,
          task: intent.arguments.task,
          mode: "fresh",
          workspace: intent.arguments.workspace,
        };
        if (alreadyAdmitted) {
          const child = await delegatedControl(pool, scope).start(request);
          assert.equal(child.id, id);
        }
        const brokers = process.env.PI_CLOUD_KAFKA_INTEGRATION_BROKERS;
        if (brokers && !snapshots.length) {
          const replay = await readRetainedPrefix(brokers.split(","), topic, records, (bounds) =>
            projection.nativeStore.replayPlan(db, topic, bounds),
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
        const beforeRetry = (await pool.query("select id from runs where root_run_id=$1", [runId]))
          .rows;
        assert.deepEqual(beforeRetry, alreadyAdmitted ? [{ id }] : []);
        if (!alreadyAdmitted) {
          snapshots.push({ restored, snapshot });
          assert(!JSON.stringify(snapshot).includes(id));
        }
        await assert.rejects(delegatedControl(pool, scope).start(request), /no longer active/);
        const retry = await delegatedControl(pool, successor).start(request);
        assert.equal(retry.id, id);
        const children = (
          await pool.query(
            "select id from runs where root_run_id=$1 and caller_run_id=$1 and source_task_id=$2",
            [runId, ref.taskId],
          )
        ).rows;
        assert.deepEqual(children, [{ id }]);
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
  },
  30000,
);
