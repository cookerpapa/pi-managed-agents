import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import pg from "pg";
import { it } from "vitest";
import { BACKGROUND_CONTEXT as ctx } from "@earendil-works/chord/context";
import {
  Harness,
  MemoryStorage,
  createRegistry,
  defineDocFamily,
  defineExtension,
} from "@earendil-works/pi-durable";
import { createModels } from "@earendil-works/pi-ai/models";
import {
  fauxProvider,
  fauxAssistantMessage,
  fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import { fixedRemoteToolDefinitions } from "../../../packages/sandbox-supervisor/src/fixed-remote-tool-definitions.ts";
import { installPrefixSql } from "./ready-projection-sql.mjs";
import { projectionTemplates } from "./projection-statements.mjs";
import { projectChildOutcomes } from "./child-outcomes.mjs";

it("commits Pi child transitions, preserves the original answer and rejects stale or repeated delivery", async () => {
  const url = process.env.PI_CLOUD_POSTGRES_INTEGRATION_URL;
  assert(url);
  const schema = "child_outcome_" + randomUUID().replaceAll("-", "");
  const pool = new pg.Pool({ connectionString: url, options: "-c search_path=" + schema, max: 4 });
  const tenant = randomUUID(),
    session = randomUUID(),
    run = randomUUID(),
    writer = randomUUID();
  const scope = { tenantId: tenant, piSessionId: session, runId: run, writerId: writer };
  const read = (file) => readFile(new URL(file, import.meta.url), "utf8");
  let offset = 0,
    view = {},
    lastFrame,
    harness;
  const applied = [];
  const save = async (frame, from, next) =>
    (
      await pool.query("select project_ready('child',0,$1,$2,$3) as result", [
        from,
        next,
        JSON.stringify([frame]),
      ])
    ).rows[0].result;
  class Projected extends MemoryStorage {
    async commit(writes) {
      const p = this.prepareCommit(writes),
        frame = { ops: [], offset: String(offset) },
        draft = { ...view };
      projectChildOutcomes(
        draft,
        { scope, commit: { seq: p.seq, writes: p.writes }, occurredAt: new Date().toISOString() },
        frame,
      );
      if (frame.ops.length) {
        assert.equal(await save(frame, offset, offset + 1), "applied");
        offset++;
        lastFrame = frame;
        applied.push(p.seq);
      }
      view = draft;
      return p.apply();
    }
  }
  const state = async (id) =>
    (
      await pool.query(
        "select state,failure_code,native_answer_id,native_outcome_seq from subagent_executions where tenant_id=$1 and id=$2",
        [tenant, id],
      )
    ).rows[0];
  try {
    await pool.query("create schema " + schema);
    await pool.query(await read("./schema.sql"));
    await pool.query(
      "create table subagent_conversations(id uuid primary key,tenant_id uuid,root_run_id uuid,updated_at timestamptz)",
    );
    await pool.query(await read("./child-outcome-schema.sql"));
    await installPrefixSql(pool, projectionTemplates);
    await pool.query("insert into accepted_fact_projection_offsets values('child',0,0)");
    await pool.query(
      "insert into runs(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,model_ref,state,lease_id) values($1,$2,$3,1,$1,'child','fake','running',$4)",
      [run, tenant, session, writer],
    );
    const store = new Projected(),
      models = createModels(),
      fake = fauxProvider({ tokensPerSecond: 0 });
    models.setProvider(fake.provider);
    const registry = createRegistry();
    let executions = 0;
    registry.install(
      defineExtension({
        name: "counted-child-tool",
        tools: [
          {
            ...fixedRemoteToolDefinitions().find((t) => t.name === "bash"),
            execute: async () => {
              executions++;
              return { content: [{ type: "text", text: "checked" }] };
            },
          },
        ],
      }),
    );
    harness = await Harness.open(
      store,
      {
        models,
        registry,
        settings: { retry: { maxRetries: 0 }, compaction: { enabled: false } },
      },
      ctx,
    );
    await harness.root(ctx);
    const receipt = defineDocFamily({
      kind: "pi-cloud.conversation-creation",
      version: 3,
      scope: "session",
      family: true,
      initial: (x) => x,
    });
    async function child() {
      const id = randomUUID(),
        ref = fake.getModel();
      await pool.query(
        "insert into subagent_conversations(id,tenant_id,root_run_id) values($1,$2,$3)",
        [id, tenant, run],
      );
      const c = await harness.createConversation(
        {
          ownership: { kind: "ownerless" },
          agent: { model: { provider: ref.provider, modelId: ref.id } },
        },
        ctx,
      );
      // The control relationship and receipt are supplied by the test. All task
      // and submission transitions below are written by the actual Pi Harness.
      await harness.commit(async (tx) => {
        await tx.doc(receipt, id, { conversationId: c.id, ownerTaskId: 900 + c.id });
      }, ctx);
      return [id, c];
    }
    async function runInput(c, key, text) {
      return c.submit({ type: "input", requestId: key, content: text }, ctx);
    }
    const [id, c] = await child();
    assert.equal((await state(id)).state, "preparing");
    fake.setResponses([fauxAssistantMessage("answer one")]);
    const first = await runInput(c, run, "first");
    await c.waitForIdle(ctx);
    assert.equal((await first.wait(ctx)).status, "done");
    const original = (await store.submissionByRequest(c.id, run, ctx)).answer;
    let got = await state(id);
    assert.equal(got.state, "completed");
    assert.equal(Number(got.native_answer_id), original);
    const entered = Promise.withResolvers(),
      release = Promise.withResolvers();
    fake.setResponses([
      async () => {
        entered.resolve();
        await release.promise;
        return fauxAssistantMessage("answer two");
      },
    ]);
    const second = await runInput(c, randomUUID(), "again");
    try {
      await entered.promise;
      assert.equal((await state(id)).state, "running");
      await pool.query("update runs set state='completed' where id=$1", [run]);
      assert.equal((await state(id)).state, "unknown");
      await pool.query("update runs set state='running' where id=$1", [run]);
    } finally {
      release.resolve();
    }
    await c.waitForIdle(ctx);
    assert.equal((await second.wait(ctx)).status, "done");
    got = await state(id);
    assert.equal(got.state, "completed");
    assert.equal(Number(got.native_answer_id), original);
    assert.equal(await save(lastFrame, offset - 1, offset), "covered");
    const stale = structuredClone(lastFrame);
    stale.ops[0].args[3] = randomUUID();
    stale.offset = String(offset);
    assert.equal(await save(stale, offset, offset + 1), "obsolete");
    offset++;
    assert.deepEqual(await state(id), got);
    const [failed, fc] = await child();
    fake.setResponses([
      fauxAssistantMessage("", { stopReason: "error", errorMessage: "fixture failure" }),
    ]);
    const bad = await runInput(fc, run, "fail");
    await fc.waitForIdle(ctx);
    assert.equal((await bad.wait(ctx)).status, "unanswered");
    assert.equal((await state(failed)).state, "failed");
    assert.equal((await state(failed)).failure_code, "native_task_failed");
    const [cancelled, cc] = await child(),
      streaming = Promise.withResolvers();
    fake.setResponses([
      async (_, options) => {
        streaming.resolve();
        if (!options.signal.aborted)
          await new Promise((done) =>
            options.signal.addEventListener("abort", done, { once: true }),
          );
        return fauxAssistantMessage("", { stopReason: "aborted" });
      },
    ]);
    const aborted = await runInput(cc, run, "cancel");
    await streaming.promise;
    await cc.abort(ctx);
    await cc.waitForIdle(ctx);
    assert.equal((await aborted.wait(ctx)).status, "unanswered");
    assert.equal((await state(cancelled)).state, "cancelled");
    const [many, work] = await child();
    fake.setResponses([
      ...Array.from({ length: 20 }, (_, i) =>
        fauxAssistantMessage(fauxToolCall("bash", { command: "test", timeout: 10 }, "call-" + i), {
          stopReason: "toolUse",
        }),
      ),
      fauxAssistantMessage("complete"),
    ]);
    const fromSeq = store.nextSeq,
      fromChanges = applied.length;
    const job = await runInput(work, run, "20 checks");
    await work.waitForIdle(ctx);
    assert.equal((await job.wait(ctx)).status, "done");
    assert.equal(executions, 20);
    assert.equal((await state(many)).state, "completed");
    const changes = applied.length - fromChanges;
    assert(changes >= 2 && changes <= 3, "Ordinary tools repeatedly wrote the child status");
    process.stdout.write(
      "child-outcome-workload " +
        JSON.stringify({
          toolCalls: executions,
          nativeCommits: store.nextSeq - fromSeq,
          outcomeCommits: changes,
        }) +
        "\n",
    );
    const precreate = randomUUID(),
      parentTask = 90001;
    await pool.query(
      "insert into subagent_conversations(id,tenant_id,root_run_id) values($1,$2,$3)",
      [precreate, tenant, run],
    );
    const parentView = {};
    projectChildOutcomes(parentView, {
      scope,
      commit: {
        seq: store.nextSeq,
        writes: [
          {
            type: "document.create",
            record: {
              id: 90002,
              kind: "pi-cloud.tool-hostcall",
              scope: { kind: "task", taskId: parentTask },
              key: precreate,
            },
            content: {
              kind: "base",
              value: { toolName: "subagent", arguments: { task: "child" } },
            },
          },
        ],
      },
    });
    for (const [i, aborted] of [true, false].entries()) {
      const frame = { ops: [], offset: String(offset) };
      projectChildOutcomes(
        parentView,
        {
          scope,
          occurredAt: new Date().toISOString(),
          commit: {
            seq: store.nextSeq + 1 + i,
            writes: [
              {
                type: "task",
                value: {
                  id: parentTask,
                  conversationId: 1,
                  abortRequested: aborted,
                  state: { status: aborted ? "running" : "terminal" },
                },
              },
            ],
          },
        },
        frame,
      );
      assert.equal(await save(frame, offset, offset + 1), "applied");
      offset++;
      assert.equal((await state(precreate)).state, "cancelled");
    }
    assert(applied.length < store.nextSeq - 1, "Every native commit wrote a child outcome");
  } finally {
    try {
      await harness?.close(ctx);
    } finally {
      await pool.query("drop schema if exists " + schema + " cascade");
      await pool.end();
    }
  }
});
