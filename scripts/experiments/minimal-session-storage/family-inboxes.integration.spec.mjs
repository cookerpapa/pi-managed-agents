import { Type } from "@earendil-works/pi-ai";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import pg from "pg";
import { it, expect } from "vitest";
import {
  Harness,
  MemoryStorage,
  createRegistry,
  defineExtension,
  defineTool,
  InboxDoc,
} from "@earendil-works/pi-durable";
import { BACKGROUND_CONTEXT as ctx } from "@earendil-works/chord/context";
import { createModels } from "@earendil-works/pi-ai/models";
import {
  fauxProvider,
  fauxAssistantMessage,
  fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import { ReadyProjection } from "./ready-projection.mjs";
import { projectRecord } from "./project-record.mjs";
import { projectionTemplates } from "./projection-statements.mjs";
import { openingHead, priorState } from "./session-prefix.mjs";

const url = process.env.PI_CLOUD_POSTGRES_INTEGRATION_URL;
it.skipIf(!url)(
  "projects and restores separate parent and child Inboxes under one writer",
  async () => {
    const schema = "family_" + randomUUID().replaceAll("-", "");
    const admin = new pg.Client(url),
      db = new pg.Pool({ connectionString: url, max: 3, options: "-c search_path=" + schema });
    await admin.connect();
    let h;
    const release = Promise.withResolvers(),
      childDone = Promise.withResolvers();
    try {
      await admin.query("create schema " + schema);
      await db.query(await readFile(new URL("./schema.sql", import.meta.url), "utf8"));
      const scope = {
        tenantId: randomUUID(),
        piSessionId: randomUUID(),
        runId: randomUUID(),
        writerId: randomUUID(),
      };
      const topic = "family-test",
        sessions = new Map();
      let view = new ReadyProjection(db, topic, sessions, projectionTemplates),
        offset = 0n;
      await view.install();
      await view.reset(new Map([[0, 0n]]), 16, "recorded-history");
      await db.query(
        "insert into runs(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,model_ref,state,lease_id) values ($1,$2,$3,1,$1,'test','local','running',$4)",
        [scope.runId, scope.tenantId, scope.piSessionId, scope.writerId],
      );
      const confirmations = [];
      const env = {
        pool: db,
        sessions,
        stage: { integrated: true, scene: "steer", sink: "single" },
        metrics: {
          byPartition: [0],
          queries: 0,
          skipped: 0,
          peakWaiting: 0,
          inboxSnapshots: 0,
          inboxBytes: 0,
        },
        account() {},
        send(kind, event) {
          if (kind === "confirmed") confirmations.push(event);
        },
        nextOffsets: new Map(),
        partitions: 1,
      };
      const records = [];
      const fold = (r, d) => projectRecord(r, d, env);
      class Store extends MemoryStorage {
        async commit(writes) {
          const p = this.prepareCommit(writes);
          const fact = {
            kind: "durable_commit",
            scope,
            factId: randomUUID(),
            occurredAt: new Date().toISOString(),
            commit: { seq: p.seq, nextId: this.nextId, writes: p.writes },
            tools: [],
            cancellations: [],
            display: [],
          };
          const r = { partition: 0, offset: offset++, fact };
          await view.apply([r], () => true, fold);
          records.push(r);
          return p.apply();
        }
      }
      const store = new Store(),
        models = createModels(),
        fake = fauxProvider({ tokensPerSecond: 0 });
      models.setProvider(fake.provider);
      const model = fake.getModel(),
        agent = { model: { provider: model.provider, modelId: model.id } };
      const entered = Promise.withResolvers(),
        delegated = Promise.withResolvers();
      const registry = createRegistry();
      registry.install(
        defineExtension({
          name: "family-fixture",
          tools: [
            defineTool({
              name: "delegate",
              description: "Wait for the child",
              parameters: Type.Object({}),
              execute: async (_args, api) => {
                delegated.resolve(api.taskId);
                await childDone.promise;
                return { content: [{ type: "text", text: "child finished" }] };
              },
            }),
            defineTool({
              name: "inspect",
              description: "Hold a child tool",
              parameters: Type.Object({}),
              execute: async (_args, api) => {
                entered.resolve(api.taskId);
                await release.promise;
                return { content: [{ type: "text", text: "child-only result" }] };
              },
            }),
          ],
        }),
      );
      fake.setResponses([
        fauxAssistantMessage("parent answer"),
        fauxAssistantMessage(fauxToolCall("delegate", {}), { stopReason: "toolUse" }),
        fauxAssistantMessage(fauxToolCall("inspect", {}), { stopReason: "toolUse" }),
        (context) => {
          expect(
            context.messages.some((m) => m.role === "toolResult" && m.toolName === "inspect"),
          ).toBe(true);
          return fauxAssistantMessage("child answer");
        },
        fauxAssistantMessage("follow-up answer"),
        (context) => {
          expect(
            context.messages.some((m) => m.role === "toolResult" && m.toolName === "delegate"),
          ).toBe(true);
          expect(
            context.messages.some((m) => m.role === "toolResult" && m.toolName === "inspect"),
          ).toBe(false);
          return fauxAssistantMessage("parent done");
        },
        fauxAssistantMessage("parent follow-up done"),
      ]);
      h = await Harness.open(
        store,
        {
          models,
          registry,
          settings: { compaction: { enabled: false }, retry: { maxRetries: 0 } },
        },
        ctx,
      );
      const root = await h.root(ctx, { agent });
      const input = await root.submit(
        { type: "input", requestId: scope.runId, content: "parent input" },
        ctx,
      );
      await root.waitForIdle(ctx);
      expect((await input.wait(ctx)).status).toBe("done");
      const at = (await root.context(ctx)).entries.at(-1).id;
      const parentWork = await root.submit(
        { type: "input", requestId: "parent-work", content: "ask the child" },
        ctx,
      );
      const parentToolId = await delegated.promise;
      const child = await root.fork(
        at,
        { ownership: { kind: "task", taskId: parentToolId }, agent },
        ctx,
      );
      const childInput = await child.submit(
        { type: "input", requestId: scope.runId, content: "child input" },
        ctx,
      );
      const childToolId = await entered.promise;
      expect((await store.conversation(child.id, ctx)).owner).toMatchObject({
        conversationId: root.id,
        taskId: parentToolId,
      });
      const nativeTask = await store.task(childToolId, ctx);
      expect(nativeTask).toMatchObject({
        conversationId: child.id,
        kind: "pi.tool",
        state: { status: "running" },
      });
      expect((await store.task(parentToolId, ctx)).state.status).toBe("running");
      const queued = await child.submit(
        {
          type: "input",
          requestId: "child-follow-up",
          whenBusy: "followUp",
          content: "child follow-up",
        },
        ctx,
      );
      const queuedRoot = await root.submit(
        {
          type: "input",
          requestId: "parent-follow-up",
          whenBusy: "followUp",
          content: "parent queued",
        },
        ctx,
      );
      await root.commit(async (tx) => {
        const r = await tx.doc(InboxDoc, root.id),
          c = await tx.doc(InboxDoc, child.id);
        r.items[0].content = "revised parent";
        c.items[0].content = "revised child";
      }, ctx);
      expect(confirmations.at(-1).queues).toEqual([
        { conversation: root.id, depth: 1, placed: false },
        { conversation: child.id, depth: 1, placed: false },
      ]);
      const rootQueue = await h.snapshot(InboxDoc, root.id, ctx);
      const queue = await h.snapshot(InboxDoc, child.id, ctx);
      expect(queue.items.length).toBe(1);
      const read = async () =>
        (await db.query("select conversation_id::int as lane,items from inboxes order by lane"))
          .rows;
      expect(await read()).toEqual([
        { lane: root.id, items: rootQueue.items },
        { lane: child.id, items: queue.items },
      ]);
      const before = await db.query(
        "select projection_first::text,projection_pin::text from runs where id=$1",
        [scope.runId],
      );
      expect(before.rows[0].projection_pin).not.toBeNull();
      // Rebuild an independent Projector from its persisted checkpoint plus the
      // protected Kafka suffix, while the native Worker is still waiting.
      view.revoke();
      view = new ReadyProjection(db, topic, new Map(), projectionTemplates);
      view.configure(16, "recorded-history");
      const starts = await view.recover([{ partition: 0, low: 0n, high: offset }]);
      const start = starts.get(0);
      assert(typeof start === "bigint" && start < offset);
      const rootConfigCreation = records.find((r) =>
        r.fact.commit.writes.some(
          (w) =>
            w.type === "document.create" &&
            w.record.kind === "pi.agent" &&
            w.record.scope.conversationId === root.id,
        ),
      );
      // Projector replay is sufficient for queues. A new Pi Harness also
      // needs this older configuration and the ownership edges of active tasks.
      expect(rootConfigCreation.offset).toBeLessThan(start);
      console.log(
        JSON.stringify({
          rootConfigBeforeReplay: true,
          rootInboxDepth: rootQueue.items.length,
          childInboxDepth: queue.items.length,
          liveNativeTasks: (await h.inspect(ctx)).tasks.length,
          protectedSuffixRecords: Number(offset - start),
        }),
      );
      for (const r of records.filter((r) => r.offset >= start))
        await view.apply([r], () => true, fold);
      const docs = Object.values(view.sessions.get(scope.runId).inboxes);
      expect(docs.find((d) => d.conversation === child.id).value).toEqual(queue);
      expect(docs.find((d) => d.conversation === root.id).value).toEqual(rootQueue);
      release.resolve();
      await child.waitForIdle(ctx);
      expect((await childInput.wait(ctx)).status).toBe("done");
      expect((await queued.wait(ctx)).status).toBe("done");
      childDone.resolve();
      await root.waitForIdle(ctx);
      expect((await parentWork.wait(ctx)).status).toBe("done");
      expect((await queuedRoot.wait(ctx)).status).toBe("done");
      expect(await read()).toEqual([
        { lane: root.id, items: [] },
        { lane: child.id, items: [] },
      ]);
      expect(
        (
          await db.query(
            "select conversation_id::int as lane,source_input_id from messages where source_input_id is not null order by commit_seq,ordinal",
          )
        ).rows,
      ).toEqual([
        { lane: root.id, source_input_id: scope.runId },
        { lane: root.id, source_input_id: "parent-work" },
        { lane: child.id, source_input_id: scope.runId },
        { lane: child.id, source_input_id: "child-follow-up" },
        { lane: root.id, source_input_id: "parent-follow-up" },
      ]);
      expect(view.sessions.get(scope.runId).count).toBe(
        (await db.query("select count(*)::int as n from messages where conversation_id=1")).rows[0]
          .n,
      );
      const rootFloor = view.sessions.get(scope.runId).floor;
      const childBoundary = (await child.context(ctx)).entries.at(-1).id;
      const summary = (first) => ({
        kind: "pi.compaction",
        head: first,
        data: { reason: "manual" },
        model: [{ role: "user", content: [{ type: "text", text: "test summary" }], timestamp: 1 }],
      });
      await child.commit((tx) => tx.appendEntry(child.id, summary(childBoundary)), ctx);
      expect(view.sessions.get(scope.runId).floor).toBe(rootFloor);
      await root.commit((tx) => tx.appendEntry(root.id, summary(at)), ctx);
      const final = {
        kind: "work_finished",
        scope,
        factId: randomUUID(),
        occurredAt: new Date().toISOString(),
        baseSequence: 0,
        nativeSequence: store.nextSeq - 1,
        terminal: { type: "turn.completed", payload: { stopReason: "stop" } },
      };
      await view.apply([{ partition: 0, offset, fact: final }], () => true, fold);
      const {
        rows: [settled],
      } = await db.query("select history_head as head,state from runs where id=$1", [scope.runId]);
      expect(settled.state).toBe("completed");
      const {
        rows: [range],
      } = await db.query(
        "select count(*)::int as count,sum(octet_length(content))::int as bytes from messages where conversation_id=$1 and entry_id>=$2",
        [root.id, at],
      );
      expect(settled.head).toMatchObject({ ...range, floor: at });
      expect(settled.head).not.toHaveProperty("recount");
      // A successor needs previously created routes even when their PG rows
      // have advanced. An Inbox created after the predecessor is invisible.
      const successor = randomUUID();
      await db.query(
        "insert into runs(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,model_ref,state,lease_id) values($1,$2,$3,2,$1,'next','local','running',$4)",
        [successor, scope.tenantId, scope.piSessionId, randomUUID()],
      );
      await db.query("insert into inboxes values($1,$2,999,1000,$3,0,'[]',$3)", [
        scope.tenantId,
        scope.piSessionId,
        settled.head.seq + 1,
      ]);
      const {
        rows: [row],
      } = await db.query(openingHead, [successor, scope.tenantId, scope.piSessionId]);
      expect(priorState(row).version).toBe(settled.head.version);
      expect(Object.values(row.inbox_routes).sort((a, b) => a - b)).toEqual([root.id, child.id]);
      expect(settled.head).not.toHaveProperty("inboxRoutes");
    } finally {
      release.resolve();
      childDone.resolve();
      try {
        await h?.close(ctx);
      } finally {
        await db.end();
        try {
          await admin.query("drop schema if exists " + schema + " cascade");
        } finally {
          await admin.end();
        }
      }
    }
  },
  30000,
);
