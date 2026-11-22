import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import pg from "pg";
import { it } from "vitest";
import { Type } from "@earendil-works/pi-ai";
import { createModels } from "@earendil-works/pi-ai/models";
import {
  fauxProvider,
  fauxAssistantMessage,
  fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import {
  Harness,
  MemoryStorage,
  createRegistry,
  defineTool,
  defineExtension,
} from "@earendil-works/pi-durable";
import { BACKGROUND_CONTEXT as ctx } from "@earendil-works/chord/context";
import { ReadyProjection } from "./ready-projection.mjs";
import { projectionTemplates } from "./projection-statements.mjs";
import { projectRecord } from "./project-record.mjs";
import { claimBatchSql } from "./fixture.mjs";
import { delegatedControl, admitDelegatedRun } from "./delegated-runs.mjs";
import {
  runDelegation,
  sendDelegatedMessage,
  sendToActiveConversation,
} from "./delegated-native.mjs";
import {
  encodeSessionLogFact,
  decodeSessionLogFact,
} from "../../../packages/runtime-core/src/kafka-accepted-fact.ts";
import { openDelegatedTransport } from "./delegated-test-transport.mjs";
import { loadSettledDelegations } from "./delegated-cold.mjs";

const url = process.env.PI_CLOUD_POSTGRES_INTEGRATION_URL;
it.skipIf(!url)(
  "projects delegated Runs, delivers to a busy sibling and explicitly continues the original agent on a cold owner",
  async () => {
    const schema = "delegated_" + randomUUID().replaceAll("-", "");
    const pool = new pg.Pool({
      connectionString: url,
      options: "-c search_path=" + schema,
      max: 5,
    });
    const tenantId = randomUUID(),
      piSessionId = randomUUID(),
      topic = "delegated-" + randomUUID(),
      ready = Promise.withResolvers(),
      release = Promise.withResolvers();
    const one = async (sql, params) => (await pool.query(sql, params)).rows[0];
    const text = (s) => ({ content: [{ type: "text", text: s }] });
    const say = (s) => fauxAssistantMessage(s);
    const tool = (name, args, id) =>
      fauxAssistantMessage(fauxToolCall(name, args, id), { stopReason: "toolUse" });
    let transport,
      scope,
      h,
      store,
      control,
      bId,
      aId,
      bNative,
      r2,
      claims = 0,
      offset = 0n;
    const effects = [],
      requests = [],
      errors = [];
    const messagesAccepted = Promise.withResolvers();
    const projector = new ReadyProjection(pool, topic, new Map(), projectionTemplates);
    projector.delegatedRuns = true;
    const env = {
      pool,
      sessions: projector.sessions,
      partitions: 1,
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
    };
    const project = async (fact) => {
      if (transport) return transport.publish(fact);
      const wire = decodeSessionLogFact(encodeSessionLogFact(fact)).fact;
      await projector.apply(
        [{ fact: wire, topic, partition: 0, offset: offset++ }],
        () => true,
        (r, d) => projectRecord(r, d, env),
      );
    };
    class Store extends MemoryStorage {
      async commit(writes) {
        const p = this.prepareCommit(writes);
        await project({
          kind: "durable_commit",
          scope,
          commit: { seq: p.seq, writes: p.writes, nextId: this.nextId },
          factId: randomUUID(),
          occurredAt: new Date().toISOString(),
          tools: [],
          display: [],
          cancellations: [],
          origins: [],
        });
        return p.apply();
      }
    }
    async function startRoot(position, prompt) {
      const id = randomUUID();
      await pool.query(
        "insert into runs(id,tenant_id,session_id,idempotency_key,mailbox_position,initial_input,model_ref,state) values($1,$2,$3,$1,$4,$5,'fake','queued')",
        [id, tenantId, piSessionId, position, prompt],
      );
      const rows = (await pool.query(claimBatchSql, [8])).rows;
      assert.deepEqual(
        rows.map((r) => r.id),
        [id],
      );
      claims++;
      scope = {
        tenantId,
        piSessionId,
        sessionId: piSessionId,
        turnId: id,
        runId: id,
        fencingToken: position,
        writerId: rows[0].lease_id,
      };
      const owner = delegatedControl(pool, scope);
      control = {
        ...owner,
        async start(request) {
          const value = await owner.start(request);
          const registered = (await owner.list()).agents.find((c) => c.id === value.id);
          assert.equal(registered?.state, "preparing");
          assert.equal(registered.task, request.task);
          return value;
        },
      };
    }
    async function settleRoot(base) {
      await project({
        kind: "work_finished",
        agentId: "root",
        factId: randomUUID(),
        scope,
        baseSequence: base,
        nativeSequence: store.nextSeq - 1,
        terminal: { type: "turn.completed", payload: { stopReason: "stop" } },
        occurredAt: new Date().toISOString(),
      });
      assert.equal(
        (await one("select state from runs where id=$1", [scope.runId])).state,
        "completed",
      );
    }
    async function openWorker() {
      store = new Store();
      const restored = await loadSettledDelegations(pool, scope, store);
      const models = createModels(),
        fake = fauxProvider({ tokensPerSecond: 0 });
      models.setProvider(fake.provider);
      const model = fake.getModel();
      const agent = { model: { provider: model.provider, modelId: model.id } };
      const registry = createRegistry();
      const register = (name, parameters, exec) =>
        defineTool({
          name,
          description: name,
          parameters,
          replay: "safe",
          execute: async (...args) => {
            try {
              return await exec(...args);
            } catch (e) {
              errors.push(e);
              throw e;
            }
          },
        });
      registry.install(
        defineExtension({
          name: "delegated-run-study",
          tools: [
            register("workflow", Type.Object({}), async (_args, api, context) => {
              const anchor = await api.commit(
                async (tx) =>
                  (await tx.scanEntries({ conversationId: 1 }, 8)).items.find(
                    (e) => e.model?.[0]?.role === "user",
                  ).id,
                context,
              );
              const result = await Promise.all([
                runDelegation(
                  api,
                  store,
                  control,
                  { task: "ALPHA_JOB", mode: "branch", at: anchor },
                  context,
                  "alpha",
                ),
                runDelegation(
                  api,
                  store,
                  control,
                  { task: "BETA_JOB", mode: "fresh" },
                  context,
                  "beta",
                ),
              ]);
              [aId, bId] = result.map((r) => r.agentId);
              assert(result.every((r) => r.status === "done"));
              assert.equal(result[0].output, "ALPHA_PRIVATE");
              return text("Both checks finished");
            }),
            register(
              "subagent_control",
              Type.Object({
                action: Type.String(),
                target: Type.String(),
                message: Type.String(),
                delivery: Type.Optional(Type.String()),
              }),
              async (args, api, context) => {
                if (args.action === "continue") {
                  const result = await runDelegation(
                    api,
                    store,
                    control,
                    { task: args.message, mode: "continue", agentId: args.target },
                    context,
                  );
                  assert.equal(result.agentId, args.target);
                  assert.equal(result.output, "ALPHA_CONTINUED");
                  return text("Continued " + result.status);
                }
                assert.equal(args.action, "send");
                const result = await sendDelegatedMessage(
                  api,
                  control,
                  args.target,
                  args.message,
                  args.delivery,
                  context,
                );
                const repeat = await sendDelegatedMessage(
                  api,
                  control,
                  args.target,
                  args.message,
                  args.delivery,
                  context,
                );
                assert.equal(result, repeat);
                effects.push(result);
                if (effects.length === 2) messagesAccepted.resolve();
                return text(result);
              },
            ),
          ],
        }),
      );
      let sent = 0;
      async function response(transcript) {
        const messages = JSON.stringify(transcript.messages);
        requests.push(messages);
        const last = transcript.messages.at(-1);
        if (messages.includes("RESUME_ALPHA") && messages.includes("ALPHA_JOB")) {
          assert(messages.includes("ALPHA_PRIVATE"));
          assert(!messages.includes("BETA_PRIVATE"));
          return say("ALPHA_CONTINUED");
        }
        if (messages.includes("ROOT_THREE")) {
          assert(!messages.includes("ALPHA_PRIVATE") && !messages.includes("BETA_PRIVATE"));
          if (last.role === "toolResult") return say("root three finished");
          return tool(
            "subagent_control",
            { action: "continue", target: aId, message: "RESUME_ALPHA" },
            "again",
          );
        }
        if (messages.includes("ALPHA_JOB")) {
          assert(messages.includes("ROOT_TWO"));
          await ready.promise;
          if (sent < 2) {
            const mode = sent++ === 0 ? "steer" : "follow_up";
            return tool(
              "subagent_control",
              { action: "send", target: bId, message: mode + "_FROM_ALPHA", delivery: mode },
              mode,
            );
          }
          return say("ALPHA_PRIVATE");
        }
        if (messages.includes("BETA_JOB")) {
          assert(!messages.includes("ROOT_TWO"));
          const b = (await control.list()).agents.find((r) => r.task === "BETA_JOB");
          bId = b.agent_id;
          bNative = (await control.address(bId)).lane;
          ready.resolve();
          await release.promise;
          return say("BETA_PRIVATE");
        }
        if (messages.includes("ROOT_TWO") && last.role !== "toolResult")
          return tool("workflow", {}, "family");
        return say("root done");
      }
      fake.setResponses(Array.from({ length: 32 }, () => response));
      h = await Harness.open(
        store,
        {
          models,
          registry,
          settings: { compaction: { enabled: false }, retry: { maxRetries: 0 } },
        },
        ctx,
      );
      return { root: await h.root(ctx, { agent }), restored };
    }
    try {
      await pool.query("create schema " + schema);
      for (const file of ["schema.sql", "delegated-runs.sql"])
        await pool.query(await readFile(new URL(file, import.meta.url), "utf8"));
      await projector.install();
      await projector.reset(new Map([[0, 0n]]), 16, "recorded-history");
      if (process.env.PI_CLOUD_KAFKA_INTEGRATION_BROKERS)
        transport = await openDelegatedTransport(
          process.env.PI_CLOUD_KAFKA_INTEGRATION_BROKERS.split(","),
          topic,
          (r, current) =>
            projector.apply([r], current, (record, d) => projectRecord(record, d, env)),
        );
      await pool.query("insert into sessions values($1,$2)", [piSessionId, tenantId]);
      await startRoot(1, "ROOT_TWO");
      r2 = scope;
      const { root } = await openWorker();
      const pending = await root.submit(
        { type: "input", requestId: scope.runId, content: "ROOT_TWO" },
        ctx,
      );
      await ready.promise;
      const before = (await control.list()).agents;
      const beta = before.find((r) => r.task === "BETA_JOB");
      const source = await one("select source_task_id from runs where id=$1", [beta.id]);
      const duplicate = {
        id: beta.id,
        callerRunId: r2.runId,
        taskId: Number(source.source_task_id),
        task: "BETA_JOB",
        mode: "fresh",
      };
      assert.equal((await admitDelegatedRun(pool, r2, duplicate)).id, beta.id);
      await assert.rejects(
        admitDelegatedRun(pool, r2, { ...duplicate, task: "changed" }),
        /identity/,
      );
      await assert.rejects(
        admitDelegatedRun(pool, r2, {
          ...duplicate,
          id: randomUUID(),
          agentId: beta.id,
          mode: "continue",
        }),
        /settled/,
      );
      assert.equal((await pool.query(claimBatchSql, [8])).rowCount, 0);
      await assert.rejects(
        pool.query("update runs set state='completed' where id=$1", [scope.runId]),
        /open child/,
      );
      await messagesAccepted.promise;
      release.resolve();
      await root.waitForIdle(ctx);
      if (errors.length) throw errors[0];
      assert.equal((await pending.wait(ctx)).status, "done");
      assert.deepEqual(effects, ["accepted", "accepted"]);
      assert(
        requests.some(
          (m) =>
            m.includes("steer_FROM_ALPHA") &&
            m.includes("follow_up_FROM_ALPHA") &&
            m.includes("BETA_JOB"),
        ),
      );
      const list = (await control.list()).agents;
      assert.equal(list.length, 2);
      assert(list.every((r) => r.state === "completed"));
      assert.equal(await control.address(bId), null);
      assert.equal(
        await sendToActiveConversation(h, { lane: bNative }, "late", "steer", randomUUID(), ctx),
        "missed",
      );
      await settleRoot(0);
      const prevSeq = store.nextSeq - 1;
      await h.close(ctx);
      h = undefined;
      await startRoot(2, "ROOT_THREE");
      assert.notEqual(scope.writerId, r2.writerId);
      await assert.rejects(
        admitDelegatedRun(pool, r2, {
          id: randomUUID(),
          callerRunId: r2.runId,
          taskId: 1,
          task: "stale",
          mode: "fresh",
        }),
        /writer/,
      );
      const next = await openWorker();
      assert(next.restored.entries > 0);
      const r = await next.root.submit(
        { type: "input", requestId: scope.runId, content: "ROOT_THREE" },
        ctx,
      );
      await next.root.waitForIdle(ctx);
      if (errors.length) throw errors[0];
      assert.equal((await r.wait(ctx)).status, "done");
      const [a2] = (await control.list()).agents;
      assert(
        a2 &&
          a2.agent_id === aId &&
          a2.id !== aId &&
          a2.previous_run_id === aId &&
          a2.caller_run_id === scope.runId &&
          a2.state === "completed",
      );
      assert(requests.some((m) => m.includes("ALPHA_PRIVATE") && m.includes("RESUME_ALPHA")));
      const history = await control.history(aId);
      assert.deepEqual(new Set(history.entries.map((e) => e.runId)), new Set([aId, a2.id]));
      const own = JSON.stringify(history.entries);
      assert(
        own.includes("ALPHA_PRIVATE") &&
          own.includes("ALPHA_CONTINUED") &&
          !own.includes("BETA_PRIVATE") &&
          !own.includes("ROOT_THREE"),
      );
      await settleRoot(prevSeq);
      assert.equal(claims, 2);
      assert.equal((await one("select count(*)::int n from sessions")).n, 1);
      assert.equal((await one("select count(*)::int n from runs where lease_id is not null")).n, 2);
      assert.equal((await one("select count(*)::int n from runs")).n, 5);
      assert.equal((await one("select to_regclass('subagent_conversations') as t")).t, null);
    } finally {
      release.resolve();
      ready.resolve();
      try {
        await h?.close(ctx);
      } finally {
        await transport?.close();
        await pool.query("drop schema if exists " + schema + " cascade");
        await pool.end();
      }
    }
  },
  60000,
);
