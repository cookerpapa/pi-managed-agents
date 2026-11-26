import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import Fastify from "fastify";
import { it } from "vitest";
import pg from "pg";
import { createDatabase, runMigrations } from "@pi-cloud/database";
import { PostgresAttachmentStore } from "@pi-cloud/session-attachments";
import { Harness, createRegistry } from "@earendil-works/pi-durable";
import { createModels } from "@earendil-works/pi-ai/models";
import { fauxProvider, fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
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
  "delivers, reorders and withdraws by Session through the active full Worker and projected Pi receipts",
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
            async () => {
              entered.resolve();
              await modelGate.promise;
              return fauxAssistantMessage("first answer");
            },
            ...Array.from({ length: 45 }, () => fauxAssistantMessage("done")),
          ]);
          native = await Harness.open(opened.storage, { models, registry: createRegistry() }, ctx);
          const m = fake.getModel(),
            root = await native.root(ctx, {
              agent: { model: { provider: m.provider, modelId: m.id } },
            });
          pump = connectSessionInputs({
            job,
            storage: opened.storage,
            inputs: rt.inputs,
            harness: native,
            conversation: root,
            fail,
          });
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
            .query("select 1 from user_or_agent_messages where id=$1 and state='queued'", [
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
      await pool.query(
        `create table test_mail_latency(ms double precision);create function test_mail_sample() returns trigger language plpgsql set search_path from current as $$begin insert into test_mail_latency values(extract(epoch from clock_timestamp()-new.accepted_at)*1000);return null;end$$;create trigger sample after update of state on user_or_agent_messages for each row when(old.state<>new.state and new.state='queued' and new.body like 'queued %') execute function test_mail_sample()`,
      );
      const samples = [],
        admission = [];
      for (let i = 0; i < 32; i++) {
        const at = performance.now();
        const msg = await put("queued " + i);
        admission.push(performance.now() - at);
        await wait(() =>
          pool
            .query("select state='queued' ok from user_or_agent_messages where id=$1", [
              msg.inputId,
            ])
            .then((r) => r.rows[0]?.ok),
        );
        samples.push(performance.now() - at);
        await withdraw(msg.inputId);
      }
      samples.sort((a, b) => a - b);
      admission.sort((a, b) => a - b);
      const measured = (await pool.query("select ms from test_mail_latency order by ms")).rows;
      assert.equal(measured.length, 32);
      process.stdout.write(
        "segment-input-latency " +
          JSON.stringify({
            samples: samples.length,
            pgToInboxP95: measured[30].ms,
            pgToInboxP99: measured[31].ms,
            admissionP95: admission[30],
            admissionP99: admission[31],
            inboxP95: samples[30],
            inboxP99: samples[31],
          }) +
          "\n",
      );
      assert.equal(scheduler.snapshot().waiting, 0);
      modelGate.resolve();
      await source.waitForIdle(ctx);
      await wait(() =>
        pool
          .query("select 1 from user_or_agent_messages where id=$1 and state='placed'", [
            two.inputId,
          ])
          .then((r) => r.rowCount),
      );
      await assert.rejects(withdraw(two.inputId), (e) => e.status === 409);
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
