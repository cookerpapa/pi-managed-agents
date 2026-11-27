import { FamilyPresentation } from "./family-presentation.mjs";
import { openPageApi } from "./flow-page-api.mjs";
import { ControlPlaneStore } from "../../../packages/control-plane/src/control-plane-store.ts";
import { placedInputNavigation } from "../../../packages/control-plane/src/placed-input-navigation.ts";
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
      sender = randomUUID();
    const abort = new AbortController(),
      finish = Promise.withResolvers(),
      modelGate = Promise.withResolvers(),
      entered = Promise.withResolvers();
    let fullServer,
      scheduler,
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
      const identity = await createPrivateTenant(admin, {
        slug: schema.replace("_", "-"),
        ownerDisplayName: "test",
      });
      const tenant = identity.tenantId,
        servingStore = new ControlPlaneStore({ database: admin, ...identity });
      const project = await servingStore.createProject({
        name: "Navigation",
        source: { kind: "empty" },
      });
      const { sessionId: session } = await servingStore.createSession(
        project.projectId,
        project.workspaceId,
        "Inputs",
        "elastic",
      );
      const web = schema + "_web";
      await pool.query(
        "create schema " +
          schema +
          "; create schema " +
          web +
          "; create view " +
          web +
          ".sessions as select * from public.sessions",
      );
      await installPagingProjection(pool);
      await installForkTables(pool);
      await setForkLayout(pool, true, false);
      await installWorkspaceRoutes(pool);
      await installIndependentSessionLeases(pool);
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
      const inputsPage = (limit = 80, before) =>
        placedInputNavigation(rt.store.db, schema, tenant, session, limit, before);
      assert.deepEqual((await inputsPage()).entries, [], "queued input has no navigation entry");
      assert.equal((await api.getInputQueue(session)).latestRead, null);
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
          const presenter = new FamilyPresentation();
          presenter.bind(1, job.scope);
          const opened = await openSegmentNative(rt.store, job, "mail", async (commit) => {
            commits.push(commit);
            const presented = presenter.prepare(commit);
            const fact = {
              kind: "durable_commit",
              scope: job.scope,
              commit,
              factId: randomUUID(),
              occurredAt: new Date().toISOString(),
              origins: [],
              tools: [],
              display: presented.display,
              cancellations: [],
            };
            await rt.consume(
              { fact, topic: "mail", partition: 0, offset: offset++ },
              () => true,
              async () => true,
            );
            presented.confirmed();
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
              if (contexts.length === 1)
                return fauxAssistantMessage(fauxToolCall("probe", {}, { id: "tool-b" }), {
                  stopReason: "toolUse",
                });
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
      assert.deepEqual(
        (await inputsPage()).entries.map((e) => e.text),
        ["first"],
      );
      await entered.promise;
      const originalSelect = rt.inputs.selectAt.bind(rt.inputs);
      let toolReads = 0;
      rt.inputs.selectAt = (scope, at, ...rest) => {
        if (at === "postTools") toolReads++;
        return originalSelect(scope, at, ...rest);
      };
      assert.equal(job.steerHint.ticket, null, "claim did not bootstrap the inbox");
      for (let i = 0; i < 100; i++) await pump.postTools(i + 1, ctx);
      assert.equal(toolReads, 0, "clean tool boundaries read PG");
      // Drop the first receipt in PG after the lamp has lit. Use the existing
      // renew/reconcile path to retry without re-enqueuing the user's message.
      await pool.query(`create table missed_ack(id uuid);
        create function skip_test_ack() returns trigger language plpgsql set search_path from current as $$
        begin insert into missed_ack values(new.id);return null;end $$;
        create trigger skip_test_ack before update of steer_seen_epoch on user_or_agent_messages for each row
        when(new.body='browser input' and new.steer_seen_epoch is not null) execute function skip_test_ack()`);
      const browserInput = await put("browser input", "steer");
      await wait(() =>
        pool
          .query("select 1 from missed_ack where id=$1", [browserInput.inputId])
          .then((r) => r.rowCount),
      );
      assert.notEqual(job.steerHint.ticket, null);
      assert.equal(
        (await api.getInputQueue(session)).items.find((m) => m.inputId === browserInput.inputId)
          .status,
        "sending",
      );
      const repair = async () => {
        await pool.query("drop trigger skip_test_ack on user_or_agent_messages");
        await worker.renew();
        await wait(
          async () =>
            (await api.getInputQueue(session)).items.find((m) => m.inputId === browserInput.inputId)
              ?.status === "steering",
        );
      };
      if (process.env.PMA_SEGMENT_BROWSER === "1") {
        const uiIds = await (
          await import("./segment-input-browser.mjs")
        ).verifyBrowserTakeback(origin, session, token, repair);
        for (const id of uiIds) {
          if (error) throw error;
          await withdraw(id);
        }
      } else {
        await repair();
        await withdraw(browserInput.inputId);
      }
      await pump.postTools(101, ctx);
      assert.equal(toolReads, 1, "a withdrawn steer costs at most one empty check");
      assert.equal(job.steerHint.ticket, null);
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
      // The owner keeps running with its only slot occupied. A missed notice
      // must remain visibly unconfirmed until the same lease can acknowledge.
      assert.equal(
        job.steerHint.ticket,
        null,
        "follow-ups and reordering should not arm the steer flag",
      );
      const port = Number(new URL(scheduler.url).port);
      await scheduler.close();
      await api.mutateQueuedInput(
        session,
        two.inputId,
        { action: "steer", revision: (await api.getInputQueue(session)).revision },
        "turn:" + randomUUID(),
      );
      assert.equal(
        (await api.getInputQueue(session)).items.find((m) => m.inputId === two.inputId).status,
        "sending",
      );
      // A deferred boundary makes this same message pending again. A delayed
      // acknowledgement for its first notice must not certify the new notice.
      const deferred = await originalSelect(job.scope, "postTools", 999);
      assert.equal(deferred.id, two.inputId);
      await rt.inputs.releaseBeforePlacement(job.scope, deferred.id);
      await pool.query("select acknowledge_segment_steer($1,$2,$3,$4)", [
        registration.workerId,
        registration.bootId,
        job.scope.writerId,
        deferred.steer_notice,
      ]);
      assert.equal(
        (await api.getInputQueue(session)).items.find((m) => m.inputId === two.inputId).status,
        "sending",
      );
      // The lease is still live; an ACK from a different Worker boot is insufficient.
      await pool.query(
        "select acknowledge_segment_steer($1,$2,$3,steer_notice) from user_or_agent_messages where id=$4",
        [registration.workerId, randomUUID(), job.scope.writerId, two.inputId],
      );
      assert.equal(
        (await api.getInputQueue(session)).items.find((m) => m.inputId === two.inputId).status,
        "sending",
      );
      scheduler = await rt.serveScheduler({ connectionString: url.href, token, port, tickMs: 150 });
      await wait(
        async () =>
          (await api.getInputQueue(session)).items.find((m) => m.inputId === two.inputId)
            ?.status === "steering",
      );
      assert.notEqual(job.steerHint.ticket, null);
      const reserved = await originalSelect(job.scope, "postTools", 1000);
      assert.equal(reserved.id, two.inputId);
      assert.equal(
        (await api.getInputQueue(session)).items.find((m) => m.inputId === two.inputId).status,
        "steering",
        "an acknowledged steer must not flash back to sending during placement",
      );
      await rt.inputs.releaseBeforePlacement(job.scope, reserved.id);
      await wait(
        async () =>
          (await api.getInputQueue(session)).items.find((m) => m.inputId === two.inputId)
            ?.status === "steering",
      );
      await withdraw(one.inputId);
      assert.equal((await api.getInputQueue(session)).items[0].status, "steering");
      const agentSteer = randomUUID();
      await rt.inputs.admit({
        tenant,
        session,
        id: agentSteer,
        sender: randomUUID(),
        source: "agent",
        mode: "steer",
        text: "agent-urgent-test",
        model: "fixture",
      });
      await wait(() =>
        pool
          .query("select 1 from user_or_agent_messages where id=$1 and steer_seen_epoch=$2", [
            agentSteer,
            job.scope.fencingToken,
          ])
          .then((r) => r.rowCount),
      );
      assert(!(await api.getInputQueue(session)).items.some((m) => m.inputId === agentSteer));
      await pool.query(`create table test_steer_latency(ms double precision); create function sample_seen_steer() returns trigger language plpgsql set search_path from current as $$
        begin insert into test_steer_latency values(extract(epoch from clock_timestamp()-new.accepted_at)*1000);return null;end $$;
        create trigger sample_seen after update of steer_seen_epoch on user_or_agent_messages for each row
        when(new.steer_seen_epoch is not null and old.steer_seen_epoch is null and new.body like 'withdraw-before-boundary-%') execute function sample_seen_steer()`);
      const before = commits.length;
      const keep = await put("follow-after-answer");
      for (let i = 0; i < 32; i++) {
        const m = await put("withdraw-before-boundary-" + i, "steer");
        await wait(() =>
          pool
            .query("select 1 from user_or_agent_messages where id=$1 and steer_seen_epoch=$2", [
              m.inputId,
              job.scope.fencingToken,
            ])
            .then((r) => r.rowCount),
        );
        await withdraw(m.inputId);
      }
      const ackMs = (await pool.query("select ms from test_steer_latency order by ms")).rows.map(
        (r) => r.ms,
      );
      assert.equal(ackMs.length, 32);
      process.stdout.write(
        "steer-receipt-ms " +
          JSON.stringify({ samples: 32, p95: ackMs[30], p99: ackMs[31] }) +
          "\n",
      );
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
      assert.equal(contexts.length, 3);
      assert(contexts[0].includes("two") && !contexts[0].includes("follow-after-answer"));
      assert(!contexts[0].some((s) => s?.includes("agent-urgent-test")));
      assert(
        contexts[1].some((s) => s?.includes("agent-urgent-test")) &&
          !contexts[1].includes("follow-after-answer"),
      );
      assert(contexts[2].includes("follow-after-answer"));
      assert.equal(toolReads, 3, "one withdrawn hint and both acknowledged steers queried PG");
      assert.equal(job.steerHint.ticket, null, "last steer should clear the hint");
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
      await wait(
        async () => (await api.getInputQueue(session)).latestRead?.inputId === keep.inputId,
      );
      const readUrl = new URL(endpoint);
      readUrl.searchParams.set("options", "-c search_path=" + web + "," + schema + ",public");
      fullServer = await openPageApi(
        endpoint,
        readUrl.href,
        true,
        undefined,
        undefined,
        false,
        schema,
      );
      const client = new PiCloudApi(
        (path, opts) => fetch(new URL(path, fullServer.origin), opts),
        identity.credential.token,
      );
      const tree = await client.getConversationTree(session, "focus");
      const nav = await inputsPage();
      assert.deepEqual(tree.branches[0].entries, nav.entries);
      const target = nav.entries.find((e) => e.text === "two");
      const detail = await client.getConversation(session, {
        turnId: target.turnId,
        entryId: target.entryId,
      });
      assert(
        detail.turns.some((t) =>
          t.transcript?.items.some(
            (i) =>
              i.kind === "user" && i.text === "two" && i.messageId === "entry:" + target.entryId,
          ),
        ),
      );
      assert.deepEqual(
        nav.entries.map((e) => e.text),
        ["first", "two", "agent-urgent-test", "follow-after-answer"],
      );
      assert.deepEqual(
        nav.entries.map((e) => e.source),
        ["user", "user", "agent", "user"],
      );
      const newest = await inputsPage(2);
      assert.deepEqual(
        newest.entries,
        nav.entries.slice(-2).map((entry, index, page) => ({
          ...entry,
          parentEntryId: index ? page[index - 1].entryId : null,
        })),
      );
      assert.deepEqual(
        (await inputsPage(2, newest.earlierEntryId)).entries,
        nav.entries.slice(0, 2),
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
      const inherited = await rt.store.delegate(
        tenant,
        session,
        { run: job.id, lane: 1, entry: Number(target.entryId) },
        randomUUID(),
        "inherit",
        "Child task",
        "fixture",
        { run: job.id, writer: job.scope.writerId },
      );
      assert.deepEqual(
        (await placedInputNavigation(rt.store.db, schema, tenant, inherited, 80)).entries,
        nav.entries.slice(0, 2),
        "a child sees only the already placed prefix frozen at its anchor",
      );
      // Stop the active Turn with both kinds of unadopted input. The API
      // returns the human's draft and keeps placed entries navigable.
      const pendingAtStop = await put("draft at stop");
      const agentAtStop = await rt.inputs.admit({
        tenant,
        session,
        sender: randomUUID(),
        id: randomUUID(),
        source: "agent",
        mode: "follow_up",
        text: "agent report at stop",
      });
      const reservedAtStop = await rt.inputs.selectAt(job.scope, "idle");
      // The previous human input may precede the agent report in the queue.
      if (reservedAtStop.id !== agentAtStop.id)
        assert.equal((await rt.inputs.selectAt(job.scope, "idle")).id, agentAtStop.id);
      const stopKey = "stop:" + randomUUID();
      assert(!(await api.cancelTurn(session, job.id, stopKey)).replayed);
      assert((await api.cancelTurn(session, job.id, stopKey)).replayed);
      const agentMail = (text) =>
        rt.inputs.admit({
          tenant,
          session,
          sender: agentAtStop.sender_id,
          id: randomUUID(),
          source: "agent",
          mode: "steer",
          text,
        });
      const during = await agentMail("during stop");
      assert.equal(during.state, "held");
      const sealStop = (scope, seq) =>
        rt.consume(
          {
            topic: "mail",
            partition: 0,
            offset: offset++,
            fact: {
              kind: "work_finished",
              scope,
              factId: randomUUID(),
              agentId: "root",
              baseSequence: 0,
              nativeSequence: seq,
              terminal: {
                type: "turn.cancelled",
                payload: { reason: "user_request", forced: false },
              },
              occurredAt: new Date().toISOString(),
            },
          },
          () => true,
          async () => true,
        );
      assert(await sealStop(job.scope, commits.at(-1).seq));
      const stopped = (
        await pool.query(
          "select id,state,return_requested from user_or_agent_messages where id=any($1::uuid[])",
          [[pendingAtStop.inputId, agentAtStop.id]],
        )
      ).rows;
      assert.deepEqual(
        stopped.find((r) => r.id === pendingAtStop.inputId),
        { id: pendingAtStop.inputId, state: "withdrawn", return_requested: true },
      );
      assert.deepEqual(
        stopped.find((r) => r.id === agentAtStop.id),
        { id: agentAtStop.id, state: "held", return_requested: false },
      );
      assert.deepEqual((await inputsPage()).entries, nav.entries);
      assert.equal(
        (await api.getReturnedInputs(session, undefined, { requestId: pendingAtStop.inputId }))
          .items[0]?.text,
        "draft at stop",
      );
      const after = await agentMail("after stop");
      assert.equal(after.turn_id, null);
      assert.equal(
        (await pool.query("select count(*)::int n from turns where session_id=$1", [session]))
          .rows[0].n,
        1,
      );
      const resumed = (
        await api.acceptTurn(session, "continue payment tests", "turn:" + randomUUID())
      ).turnId;
      const carried = (
        await pool.query(
          "select id,state,turn_id,starts_turn,boundary_task from user_or_agent_messages where id=any($1::uuid[]) order by position",
          [[agentAtStop.id, during.id, after.id]],
        )
      ).rows;
      assert.deepEqual(
        carried,
        [agentAtStop, during, after].map((m) => ({
          id: m.id,
          state: "pending",
          turn_id: resumed,
          starts_turn: false,
          boundary_task: null,
        })),
      );
      assert.deepEqual(
        (await inputsPage()).entries,
        nav.entries,
        "held and rebound mail is invisible until placed",
      );
      // A human can reopen before the old writer's cancellation reaches PG.
      // A fresh queued Turn has no writer and can stop immediately. First
      // claim this successor to exercise the running-to-closing race.

      // The independently created child has a separate lease and queue.
      await rt.store.retire(tenant, inherited);
      const finalWorker = randomUUID(),
        finalBoot = randomUUID();
      await admin
        .insertInto("workers")
        .values({
          id: finalWorker,
          supervisor_id: finalWorker,
          boot_id: finalBoot,
          max_concurrent_sessions: 1,
          state: "ready",
        })
        .execute();
      await registerTestWorker(admin, finalWorker);
      const racer = rt.worker({
        workerId: finalWorker,
        bootId: finalBoot,
        capacity: 1,
        leaseDurationMs: 30000,
      });
      try {
        const nextJob = await racer.claimNext();
        assert.equal(nextJob.id, resumed);
        await api.cancelTurn(session, resumed, "stop:" + randomUUID());
        const late = await agentMail("while closing again");
        const newer = (await api.acceptTurn(session, "next instruction", "turn:" + randomUUID()))
          .turnId;
        assert.equal(
          (await pool.query("select ready_at from turns where id=$1", [newer])).rows[0].ready_at,
          null,
        );
        // This lease-only claimant has no native harness or model.
        assert(
          await rt.leases.projectBoundary(
            {
              topic: "mail",
              partition: 0,
              offset: offset++,
              fact: {
                kind: "work_finished",
                scope: nextJob.scope,
                factId: randomUUID(),
                agentId: "root",
                baseSequence: 0,
                nativeSequence: 0,
                terminal: {
                  type: "turn.cancelled",
                  payload: { reason: "user_request", forced: false },
                },
                occurredAt: new Date().toISOString(),
              },
            },
            async () => {},
          ),
        );
        assert.deepEqual(
          (
            await pool.query(
              "select distinct turn_id,state from user_or_agent_messages where id=any($1::uuid[])",
              [[agentAtStop.id, during.id, after.id, late.id]],
            )
          ).rows,
          [{ turn_id: newer, state: "pending" }],
        );
        assert(
          (await pool.query("select ready_at from turns where id=$1", [newer])).rows[0].ready_at,
        );
        await nextJob.release();
        // A message arrives after the Worker's last empty read, before its
        // ordered normal seal. All unadopted inputs share one successor.
        const sid = randomUUID();
        await rt.store.register(tenant, [sid]);
        const t17 = randomUUID();
        await pool.query(
          "insert into turns(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,state) values($1,$2,$3,1,$1,'previous','queued')",
          [t17, tenant, sid],
        );
        const active = await racer.claimNext([tenant, sid, t17, randomUUID(), sid, sid]);
        assert.equal(active.id, t17);
        assert.equal(await rt.inputs.selectAt(active.scope, "idle"), undefined);
        const admit = (id, text, mode) =>
          rt.inputs.admit({ id, session: sid, tenant, sender, source: "user", text, mode });
        const m = await admit(randomUUID(), "late message", "follow_up"),
          n = await admit(randomUUID(), "urgent", "steer");
        assert.equal(m.turn_id, t17);
        assert.equal(n.turn_id, t17);
        assert.equal(
          (await pool.query("select count(*)::int n from turns where session_id=$1", [sid])).rows[0]
            .n,
          1,
        );
        assert(
          await rt.leases.projectBoundary(
            {
              topic: "mail",
              partition: 0,
              offset: offset++,
              fact: {
                kind: "work_finished",
                scope: active.scope,
                factId: randomUUID(),
                agentId: "root",
                baseSequence: 0,
                nativeSequence: 0,
                terminal: { type: "turn.completed", payload: { stopReason: "stop" } },
                occurredAt: new Date().toISOString(),
              },
            },
            async () => {},
          ),
        );
        const { rows: mail } = await pool.query(
          "select id,turn_id,starts_turn,state,mode from user_or_agent_messages where session_id=$1 order by position",
          [sid],
        );
        assert.equal(mail.length, 2);
        assert.notEqual(mail[0].turn_id, t17);
        assert.deepEqual(
          mail.map((i) => [i.id, i.turn_id === mail[0].turn_id, i.starts_turn, i.mode, i.state]),
          [
            [m.id, true, true, "follow_up", "pending"],
            [n.id, true, false, "steer", "pending"],
          ],
        );
        assert(
          (await pool.query("select ready_at from turns where id=$1", [mail[0].turn_id])).rows[0]
            .ready_at,
        );
        await rt.inputs.withdraw(tenant, sid, sender, m.id);
        const {
          rows: [promoted],
        } = await pool.query(
          "select m.starts_turn,t.initial_input from user_or_agent_messages m join turns t on t.id=m.turn_id where m.id=$1",
          [n.id],
        );
        assert.deepEqual(promoted, { starts_turn: true, initial_input: "urgent" });
        await active.release();
      } finally {
        await racer.close();
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
      await fullServer?.close();
      await pump?.close();
      await native?.close(ctx);
      abort.abort();
      finish.resolve();
      await loop;
      await worker?.close();
      await scheduler?.close();
      await app.close();
      await pool.query("drop schema if exists " + schema + "_web cascade");
      await pool.query("drop schema if exists " + schema + " cascade");
      await rt.store.db.destroy();
      await admin.destroy();
    }
  },
  90000,
);
