// Capture unmodified commits from the pinned Harness and use the real fixed Bash
// definition with an inert executor. No provider request or shell escapes the fixture.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { BACKGROUND_CONTEXT as ctx } from "@earendil-works/chord/context";
import {
  Harness,
  MemoryStorage,
  createRegistry,
  defineExtension,
} from "@earendil-works/pi-durable";
import { createModels } from "@earendil-works/pi-ai/models";
import {
  fauxProvider,
  fauxToolCall,
  fauxAssistantMessage,
} from "@earendil-works/pi-ai/providers/faux";
import { durableToolState } from "@pi-cloud/protocol";
import { fixedRemoteToolDefinitions } from "../../../packages/sandbox-supervisor/src/fixed-remote-tool-definitions.ts";
import { kafkaProducerLane } from "../../../packages/runtime-core/src/kafka-accepted-fact.ts";
import { FamilyPresentation } from "./family-presentation.mjs";
import {
  decodeWorkspaceFact,
  encodeWorkspaceFact,
  operationsFor,
} from "./workspace-operations.mjs";
import { prepareToolGrants } from "./tool-grant-prefix.mjs";

async function capture(scope) {
  const frames = [],
    registry = createRegistry(),
    models = createModels(),
    fake = fauxProvider({ tokensPerSecond: 0 });
  let reply;
  class Observed extends MemoryStorage {
    async commit(writes) {
      const p = this.prepareCommit(writes);
      frames.push({ seq: p.seq, nextId: this.nextId, writes: structuredClone(p.writes) });
      return p.apply();
    }
  }
  models.setProvider(fake.provider);
  registry.install(
    defineExtension({
      name: "fixed-bash-study",
      tools: [
        {
          ...fixedRemoteToolDefinitions().find((t) => t.name === "bash"),
          execute: async (args) => {
            assert.equal(args.timeout, 10);
            return { content: [{ type: "text", text: "fixture" }] };
          },
        },
      ],
    }),
  );
  fake.setResponses([
    fauxAssistantMessage(fauxToolCall("bash", { command: "true", timeout: "10" }), {
      stopReason: "toolUse",
    }),
    fauxAssistantMessage("ok"),
  ]);
  const h = await Harness.open(
    new Observed(),
    { models, registry, settings: { retry: { maxRetries: 0 }, compaction: { enabled: false } } },
    ctx,
  );
  try {
    const m = fake.getModel(),
      root = await h.root(ctx, { agent: { model: { provider: m.provider, modelId: m.id } } });
    const sub = await root.submit(
      { type: "input", requestId: scope.runId, content: "Run the fixed test tool." },
      ctx,
    );
    await root.waitForIdle(ctx);
    assert.equal((await sub.wait(ctx)).status, "done");
  } finally {
    await h.close(ctx);
  }
  const presentation = new FamilyPresentation();
  presentation.bind(1, scope);
  const prefix = [];
  for (const commit of frames) {
    const d = presentation.prepare(commit);
    d.confirmed();
    const task = commit.writes
      .map((w) => w.type === "task" && durableToolState(w.value))
      .find((t) => t?.intent);
    const f = {
      kind: "durable_commit",
      scope,
      factId: randomUUID(),
      occurredAt: new Date().toISOString(),
      commit,
      origins: commit.writes
        .filter((w) => ["task", "entry", "submission"].includes(w.type))
        .map((w) => ({
          recordId: w.value.id,
          sessionId: scope.sessionId,
          turnId: scope.turnId,
          runId: scope.runId,
        })),
      display: [],
      tools: [],
      cancellations: [],
    };
    if (task) {
      reply = {
        topic: "pi-cloud.tool-replies.v4." + randomUUID(),
        partition: 0,
        offset: "0",
        deadlineAt: Date.now() + 60000,
        toolName: "bash",
        toolCallId: task.intent.callId,
      };
      f.workspaceOperations = [
        {
          action: "execute",
          operationId: randomUUID(),
          taskId: task.id,
          reply,
          maximumOutputBytes: 51200,
          timeoutMs: 10000,
        },
      ];
      assert.deepEqual(commit.writes.map((w) => w.type).sort(), ["document.change", "task"]);
      const withDisplay = decodeWorkspaceFact(
        encodeWorkspaceFact({ ...f, display: d.display }),
      ).fact;
      assert(
        withDisplay.display.some((s) => s.events.some((e) => e.event.type === "tool.started")),
      );
      return { prefix, intent: decodeWorkspaceFact(encodeWorkspaceFact(f)).fact, withDisplay };
    }
    prefix.push(decodeWorkspaceFact(encodeWorkspaceFact(f)).fact);
  }
  throw Error("Missing native execute checkpoint");
}

export async function checkCapturedIntents(db, state, sender, seed, topic) {
  const { view, router } = state,
    records = [],
    bindings = new Map();
  let off = view.next.get(0);
  for (let i = 0; i < 2; i++) {
    let sessionId;
    do {
      sessionId = randomUUID();
    } while (kafkaProducerLane(sessionId, state.env.partitions) !== 0);
    const runId = randomUUID(),
      scope = {
        tenantId: randomUUID(),
        runId,
        sessionId,
        piSessionId: sessionId,
        turnId: runId,
        writerId: randomUUID(),
        fencingToken: 1,
      };
    const { prefix, intent, withDisplay } = await capture(scope);
    await db.query(
      "insert into runs(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,model_ref,state,lease_id) values($1,$2,$3,1,$1,'test','fixture','running',$4)",
      [runId, scope.tenantId, sessionId, scope.writerId],
    );
    const binding = randomUUID();
    bindings.set(runId, binding);
    await router.spans.activate(
      scope,
      1,
      { topic, partition: 0, offset: off },
      binding,
      () => true,
    );
    for (const f of prefix) await seed({ topic, partition: 0, offset: off++, fact: f });
    const record = { topic, partition: 0, fact: intent };
    assert(router.canGroupOperation(record));
    assert(
      !router.canGroupOperation({ ...record, fact: withDisplay }),
      "Display must keep its delivery boundary",
    );
    records.push(record);
  }
  for (const r of records) r.offset = off++;
  const calls = [],
    before = view.stats.commits;
  const n = await prepareToolGrants(
    records,
    () => true,
    async (count) => {
      assert.equal(count, 2);
      assert.equal(
        await db
          .query("select 1 from runs where id=any($1::uuid[]) and projection_tools<>'{}'::jsonb", [
            records.map((r) => r.fact.scope.runId),
          ])
          .then((r) => r.rowCount),
        2,
      );
      return 0;
    },
    {
      batchProjection: true,
      limit: 8,
      sender,
      router,
      targetFor: (r) => r.fact.scope.sessionId,
      prepareSend: (r, { command, op, route }) => {
        assert.equal(route.bindingId, bindings.get(r.fact.scope.runId));
        assert.equal(command.request.args.timeout, 10);
        return () => calls.push(op.operationId);
      },
    },
  );
  assert.equal(n, 2);
  assert.equal(view.stats.commits - before, 1);
  assert.deepEqual(
    calls,
    records.map((r) => operationsFor(r.fact)[0].operationId),
  );
  for (const r of records) {
    const change = r.fact.commit.writes.find((w) => w.type === "document.change");
    assert(
      view.sessions
        .get(r.fact.scope.runId)
        .liveDocs[change.id].value.tools.some(
          (s) => s.status === "running" && s.callId === operationsFor(r.fact)[0].reply.toolCallId,
        ),
    );
  }
}
