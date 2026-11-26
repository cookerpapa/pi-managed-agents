import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import Fastify from "fastify";
import { it } from "vitest";
import pg from "pg";
import { createDatabase, runMigrations } from "@pi-cloud/database";
import { PostgresAttachmentStore } from "@pi-cloud/session-attachments";
import { Type } from "@earendil-works/pi-ai";
import {
  Harness,
  createRegistry,
  defineExtension,
  defineTool,
  hook,
  GenerationTask,
  InboxDoc,
} from "@earendil-works/pi-durable";
import { createModels } from "@earendil-works/pi-ai/models";
import {
  fauxProvider,
  fauxAssistantMessage,
  fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import { BACKGROUND_CONTEXT as ctx } from "@earendil-works/chord/context";
import { createPrivateTenant } from "../../../packages/control-plane/src/tenant-administration.ts";
import { registerTestWorker } from "../../../packages/control-plane/test/fixtures/registered-worker.ts";
import { PiCloudApi } from "../../../packages/web-ui/src/api.ts";
import { IndependentSessionRuntime, openSegmentNative } from "./independent-session-runtime.mjs";
import { installIndependentSessionLeases } from "./independent-session-leases.mjs";
import { installPagingProjection } from "./flow-paging-fixture.mjs";
import { installForkTables, setForkLayout } from "./fork-storage.mjs";
import { installWorkspaceRoutes } from "./workspace-route-spans.mjs";
import { connectSessionInputs } from "./segment-input-worker.mjs";

const endpoint = process.env.PI_CLOUD_POSTGRES_INTEGRATION_URL;
it.skipIf(!endpoint)(
  "delivers, reorders and withdraws by Session through the PG queue and native tool/final boundaries",
  async () => {
    const admin = createDatabase({ connectionString: endpoint }),
      schema = "mail_" + randomUUID().replaceAll("-", "");
    const url = new URL(endpoint);
    url.searchParams.set("options", "-c search_path=" + schema + ",public");
    const pool = new pg.Pool({ connectionString: url.href }),
      rt = new IndependentSessionRuntime({ pool, topic: "mail", partitions: 1 });
    const app = Fastify(),
      token = randomUUID() + randomUUID(),
      sender = randomUUID(),
      session = randomUUID();
    const abort = new AbortController(),
      finish = Promise.withResolvers(),
      modelGate = Promise.withResolvers(),
      entered = Promise.withResolvers();
    let scheduler,
      worker,
      loop,
      pump,
      native,
      error,
      offset = 0n,
      job,
      history,
      source;
    const commits = [],
      contexts = [];
    const fail = (e) => {
      process.stderr.write("Session input worker failed: " + e.message + "\n");
      error = e;
    };
    const wait = async (fn) => {
      for (let i = 0; i < 1200; i++) {
        if (error) throw error;
        const v = await fn();
        if (v) return v;
        await delay(10);
      }
      throw Error("Session mail observation timed out");
    };
    try {
      await runMigrations(admin, "up");
      const { tenantId: tenant } = await createPrivateTenant(admin, {
        slug: schema.replace("_", "-"),
        ownerDisplayName: "test",
      });
      await pool.query("create schema " + schema);
      await installPagingProjection(pool);
      await installForkTables(pool);
      await setForkLayout(pool, true, false);
      await installIndependentSessionLeases(pool);
      await installWorkspaceRoutes(pool);
      await rt.store.register(tenant, [session]);
      await rt.replayOffsets([{ partition: 0, low: 0n, high: 0n }]);
      const registeredUploads = new Map();
      rt.installInputApi(app, async (req, s) =>
        s === session && req.headers.authorization === "Bearer " + token
          ? {
              tenant,
              sender,
              model: "fixture",
              resolveUploads: async (ids) => ids.map((id) => registeredUploads.get(id)),
            }
          : null,
      );
      const origin = await app.listen({ host: "127.0.0.1", port: 0 }),
        api = new PiCloudApi((path, opts) => fetch(new URL(path, origin), opts), token);
      const puts = await Promise.allSettled(
        Array.from({ length: 5 }, () => api.acceptTurn(session, "first", "turn:" + randomUUID())),
      );
      assert.equal(puts.filter((r) => r.status === "fulfilled").length, 1);
      assert(puts.filter((r) => r.status === "rejected").every((r) => r.reason.status === 409));
      const put = (text, mode = "follow_up", key = "turn:" + randomUUID()) =>
        api.acceptTurn(session, text, key, undefined, undefined, mode);
      const withdraw = async (id) =>
        api.mutateQueuedInput(
          session,
          id,
          { action: "cancel", revision: (await api.getInputQueue(session)).revision },
          "turn:" + randomUUID(),
        );
      const blobs = new Map();
      const objects = new PostgresAttachmentStore(
        admin,
        {
          async put(key, bytes) {
            blobs.set(key, Buffer.from(bytes));
          },
          async *readStream(key) {
            yield blobs.get(key);
          },
          async delete(key) {
            blobs.delete(key);
          },
        },
        admin,
        schema,
      );
      const original = await objects.put(
        { tenantId: tenant },
        Buffer.from("draft attachment"),
        "text/plain",
      );
      const attachment = {
        id: randomUUID(),
        sessionId: session,
        workspaceId: randomUUID(),
        name: "notes.txt",
        path: "/workspace/notes.txt",
        mimeType: "text/plain",
        byteLength: original.byteLength,
        sha256: original.sha256,
        original: { type: "pi_cloud_attachment_v1", attachment: original },
      };
      registeredUploads.set(attachment.id, attachment);
      const early = await api.acceptTurn(
        session,
        "early",
        "turn:" + randomUUID(),
        undefined,
        [attachment.id],
        "follow_up",
      );
      assert.equal(
        (
          await pool.query(
            "select 1 from flow_input_assets where tenant_id=$1 and attachment_id=$2",
            [tenant, original.id],
          )
        ).rowCount,
        1,
      );
      await withdraw(early.inputId);
      const returned = await api.getReturnedInputs(session);
      assert.equal(returned.items[0].text, "early");
      assert.deepEqual(returned.items[0].uploads, [attachment]);
      await api.acknowledgeReturnedInputs(session, returned.requestId);
      assert.equal((await api.getReturnedInputs(session)).items.length, 0);
      const ackReplay = await api.getReturnedInputs(session, undefined, {
        requestId: returned.requestId,
        afterPosition: returned.items[0].mailboxPosition,
      });
      assert.equal(ackReplay.requestId, returned.requestId);
      assert.deepEqual(ackReplay.items, []);
      const registration = {
        workerId: randomUUID(),
        bootId: randomUUID(),
        capacity: 1,
        leaseDurationMs: 30000,
      };
      await admin
        .insertInto("workers")
        .values({
          id: registration.workerId,
          supervisor_id: registration.workerId,
          boot_id: registration.bootId,
          state: "ready",
          max_concurrent_sessions: 1,
        })
        .execute();
      await registerTestWorker(admin, registration.workerId);
      scheduler = await rt.serveScheduler({ connectionString: url.href, token, tickMs: 150 });
      worker = rt.worker(registration);
      loop = worker.serve({
        scheduler: { url: scheduler.url, token },
        signal: abort.signal,
        onError: fail,
        execute: async (claimed) => {
          job = claimed;
          const opened = await openSegmentNative(rt.store, job, "mail", async (commit) => {
            commits.push(commit);
            const fact = {
              kind: "durable_commit",
              scope: job.scope,
              commit,
              factId: randomUUID(),
              occurredAt: new Date().toISOString(),
              origins: [],
              tools: [],
              display: [],
              cancellations: [],
            };
            await rt.consume(
              { fact, topic: "mail", partition: 0, offset: offset++ },
              () => true,
              async () => true,
            );
          });
          const models = createModels(),
            fake = fauxProvider({ tokensPerSecond: 0 });
          models.setProvider(fake.provider);
          fake.setResponses([
            fauxAssistantMessage(fauxToolCall("probe", {}, { id: "tool-a" }), {
              stopReason: "toolUse",
            }),
            ...Array.from({ length: 8 }, () => async (c) => {
              contexts.push(
                c.messages.map((m) =>
                  typeof m.content === "string"
                    ? m.content
                    : m.content?.map((x) => x.text ?? "").join(""),
                ),
              );
              return fauxAssistantMessage("done");
            }),
          ]);
          const registry = createRegistry();
          registry.install(
            defineExtension({
              name: "mail-boundary",
              tools: [
                defineTool({
                  name: "probe",
                  description: "Controlled round",
                  parameters: Type.Object({}),
                  execute: async () => {
                    entered.resolve();
                    await modelGate.promise;
                    return { content: [{ type: "text", text: "tool completed" }] };
                  },
                }),
              ],
              hooks: [
                hook(GenerationTask, {
                  afterTools: (_a, _r, api, context) => pump.postTools(api.taskId, context),
                }),
              ],
            }),
          );
          native = await Harness.open(opened.storage, { models, registry }, ctx);
          const m = fake.getModel(),
            root = await native.root(ctx, {
              agent: { model: { provider: m.provider, modelId: m.id } },
            });
          pump = connectSessionInputs({
            job,
            inputs: rt.inputs,
            conversation: root,
            harness: native,
            storage: opened.storage,
          });
          await pump.start();
          source = root;
          await finish.promise;
        },
      });
      await wait(() => source);
      await wait(() =>
        pool
          .query(
            "select 1 from user_or_agent_messages where source='user' and mode='input' and state='placed'",
          )
          .then((r) => r.rowCount),
      );
      await entered.promise;
      if (process.env.PMA_SEGMENT_BROWSER === "1") {
        const b = await put("browser input");
        await wait(() =>
          pool
            .query("select 1 from user_or_agent_messages where id=$1 and state='pending'", [
              b.inputId,
            ])
            .then((r) => r.rowCount),
        );
        const uiIds = await (
          await import("./segment-input-browser.mjs")
        ).verifyBrowserTakeback(origin, session, token);
        for (const id of uiIds) {
          if (error) throw error;
          await withdraw(id);
        }
      }
      const key = "turn:" + randomUUID(),
        one = await put("one", "follow_up", key);
      assert.equal((await put("one", "follow_up", key)).inputId, one.inputId);
      const two = await put("two");
      const queued = await wait(async () => {
        const q = await api.getInputQueue(session);
        return q.items.filter((i) => i.status === "queued").length === 2 && q;
      });
      await api.mutateQueuedInput(
        session,
        two.inputId,
        { action: "move", direction: "up", revision: queued.revision },
        "turn:" + randomUUID(),
      );
      assert.deepEqual(
        (await api.getInputQueue(session)).items.map((i) => i.text),
        ["two", "one"],
      );
      await api.mutateQueuedInput(
        session,
        two.inputId,
        { action: "steer", revision: (await api.getInputQueue(session)).revision },
        "turn:" + randomUUID(),
      );
      await withdraw(one.inputId);
      assert.equal((await api.getInputQueue(session)).items[0].status, "steering");
      const before = commits.length;
      const keep = await put("follow-after-answer");
      for (let i = 0; i < 32; i++) {
        const m = await put("withdraw-before-boundary-" + i);
        await withdraw(m.inputId);
      }
      assert.equal(commits.length, before, "PG queue edits triggered native publication");
      assert.equal((await native.snapshot(InboxDoc, source.id, ctx)).items.length, 0);
      assert.equal(
        (await pool.query("select count(*)::int n from native_inbox_payloads")).rows[0].n,
        0,
      );
      assert.equal(scheduler.snapshot().waiting, 0);
      const passive = await source.submit(
        {
          type: "write",
          requestId: "passive",
          entry: {
            kind: "test.maintenance",
            model: [{ role: "user", content: "maintenance-before-steer", timestamp: 1 }],
          },
        },
        ctx,
      );
      assert.equal(
        (await pool.query("select jsonb_array_length(items) as n from native_inbox_payloads"))
          .rows[0].n,
        1,
      );
      modelGate.resolve();
      await pump.finish();
      assert.equal(contexts.length, 2);
      assert(contexts[0].includes("two") && !contexts[0].includes("follow-after-answer"));
      assert(contexts[1].includes("follow-after-answer"));
      assert.equal((await passive.wait(ctx)).status, "done");
      assert(
        contexts[0].indexOf("maintenance-before-steer") >= 0 &&
          contexts[0].indexOf("maintenance-before-steer") < contexts[0].indexOf("two"),
      );
      for (const id of [two.inputId, keep.inputId]) {
        const matching = commits.flatMap((c) =>
          c.writes
            .filter((w) => w.type === "submission" && w.value.requestId === id)
            .map((w) => [c, w.value]),
        );
        assert.deepEqual(
          matching.map(([, v]) => v.status),
          ["placed", "done"],
        );
        const [adopted, receipt] = matching[0];
        assert(
          adopted.writes.some(
            (w) => w.type === "entry" && w.value.id === receipt.entry && w.value.kind === "pi.user",
          ),
        );
      }
      assert(!JSON.stringify(commits).includes("withdraw-before-boundary-"));
      assert.equal(
        (await pool.query("select count(*)::int n from native_inbox_payloads")).rows[0].n,
        0,
      );
      await wait(() =>
        pool
          .query("select 1 from user_or_agent_messages where id=$1 and state='placed'", [
            two.inputId,
          ])
          .then((r) => r.rowCount),
      );
      await assert.rejects(withdraw(two.inputId), (e) => e.status === 409);
      // The last initial input can be withdrawn after a claim but before
      // startup. Later mail must open a successor, not join the cancelled Turn.
      const spareSession = randomUUID(),
        spareUser = randomUUID(),
        id = randomUUID();
      await rt.store.register(tenant, [spareSession]);
      const pending = await rt.inputs.admit({
        tenant,
        session: spareSession,
        sender: spareUser,
        id,
        source: "user",
        mode: "input",
        model: "fixture",
        text: "take this back",
      });
      const spareId = randomUUID(),
        bootId = randomUUID();
      await admin
        .insertInto("workers")
        .values({
          id: spareId,
          supervisor_id: spareId,
          boot_id: bootId,
          state: "ready",
          max_concurrent_sessions: 1,
        })
        .execute();
      await registerTestWorker(admin, spareId);
      const spare = rt.worker({ workerId: spareId, bootId, capacity: 1, leaseDurationMs: 30000 });
      try {
        const claimed = await spare.claimNext();
        assert.equal(claimed.id, pending.turn_id);
        assert.equal(
          (await rt.inputs.withdraw(tenant, spareSession, spareUser, id)).state,
          "withdrawn",
        );
        assert.equal(
          (await pool.query("select state from runs where id=$1", [pending.turn_id])).rows[0].state,
          "cancel_requested",
        );
        const successor = await rt.inputs.admit({
          tenant,
          session: spareSession,
          sender: spareUser,
          id: randomUUID(),
          source: "user",
          mode: "follow_up",
          model: "fixture",
          text: "replacement",
        });
        assert.notEqual(successor.turn_id, claimed.id);
        assert.equal(await rt.inputs.selectAt(claimed.scope, "start"), undefined);
        await claimed.release();
      } finally {
        await spare.close();
      }
      history = JSON.stringify((await source.context(ctx)).messages);
      assert(
        history.includes("two") &&
          !history.includes('"one"') &&
          !history.includes("early") &&
          !history.includes("queued 0"),
      );
    } finally {
      modelGate.resolve();
      await pump?.close();
      await native?.close(ctx);
      abort.abort();
      finish.resolve();
      await loop;
      await worker?.close();
      await scheduler?.close();
      await app.close();
      await pool.query("drop schema if exists " + schema + " cascade");
      await rt.store.db.destroy();
      await admin.destroy();
    }
  },
  90000,
);
