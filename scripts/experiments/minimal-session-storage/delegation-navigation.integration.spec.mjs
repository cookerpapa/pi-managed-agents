import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import pg from "pg";
import { it } from "vitest";
import { sql } from "kysely";
import { createDatabase, runMigrations } from "@pi-cloud/database";
import {
  createPrivateTenant,
  ControlPlaneStore,
} from "../../../packages/control-plane/src/index.ts";
import { registerTestWorker } from "../../../packages/control-plane/test/fixtures/registered-worker.ts";
import { PiCloudApi } from "../../../packages/web-ui/src/api.ts";
import { IndependentSessionRuntime } from "./independent-session-runtime.mjs";
import { installIndependentSessionLeases } from "./independent-session-leases.mjs";
import { installPagingProjection } from "./flow-paging-fixture.mjs";
import { installForkTables, setForkLayout } from "./fork-storage.mjs";
import { openPageApi } from "./flow-page-api.mjs";

const endpoint = process.env.PI_CLOUD_POSTGRES_INTEGRATION_URL;
it.skipIf(!endpoint)(
  "navigates the request's input and prunes only its newly founded child subtrees",
  async () => {
    const schema = "nav_" + randomUUID().replaceAll("-", ""),
      url = new URL(endpoint);
    url.searchParams.set("options", "-c search_path=" + schema + ",public");
    const db = createDatabase({ connectionString: endpoint }),
      pool = new pg.Pool({ connectionString: url.href });
    const rt = new IndependentSessionRuntime({ pool, topic: "nav", partitions: 1 });
    let apiServer, worker;
    try {
      await runMigrations(db, "up");
      const identity = await createPrivateTenant(db, {
          slug: schema.replace("_", "-"),
          ownerDisplayName: "Test",
          initialModel: { provider: "openai-codex", modelId: "gpt-6-luna" },
        }),
        tenant = identity.tenantId,
        sender = identity.ownerUserId,
        publicStore = new ControlPlaneStore({ database: db, ...identity });
      const project = await publicStore.createProject({
        name: "Navigation",
        source: { kind: "empty" },
      });
      const { sessionId: root } = await publicStore.createSession(
        project.projectId,
        project.workspaceId,
        "Root",
        "elastic",
        { ownerUserId: sender },
      );
      await pool.query(
        "create schema " +
          schema +
          ";create schema " +
          schema +
          "_web;create view " +
          schema +
          "_web.sessions as select * from public.sessions",
      );
      await installPagingProjection(pool);
      await installForkTables(pool);
      await setForkLayout(pool, true, false);
      await installIndependentSessionLeases(pool);
      await rt.store.register(tenant, [root]);
      const wid = randomUUID();
      await db
        .insertInto("workers")
        .values({
          id: wid,
          boot_id: wid,
          supervisor_id: wid,
          max_concurrent_sessions: 10,
          state: "ready",
        })
        .execute();
      await registerTestWorker(db, wid);
      worker = rt.worker({
        workerId: wid,
        bootId: wid,
        capacity: 10,
        leaseDurationMs: 120000,
        historyCache: { diskDirectory: false },
      });
      const reads = new URL(endpoint);
      reads.searchParams.set("options", "-c search_path=" + schema + "_web," + schema + ",public");
      apiServer = await openPageApi(endpoint, reads.href, true, undefined, undefined, true, schema);
      const api = new PiCloudApi(
        (p, o) => fetch(new URL(p, apiServer.origin), o),
        identity.credential.token,
      );
      let pos = 1;
      const claim = async () => {
        const job = await worker.claimNext();
        assert(job);
        return job;
      };
      const admit = (text, mode = "input") =>
        rt.inputs.admit({
          tenant,
          session: root,
          sender,
          id: randomUUID(),
          source: "user",
          mode,
          text,
        });
      async function entry(job, role, blocks, { input, final = false } = {}) {
        const id = ++pos,
          content = {
            id,
            conversationId: 1,
            kind: "pi." + role,
            model: [
              {
                role,
                content: blocks,
                timestamp: 10,
                ...(role === "assistant" ? { stopReason: final ? "stop" : "toolUse" } : {}),
              },
            ],
          };
        await pool.query(
          "insert into messages(tenant_id,pi_session_id,run_id,entry_id,conversation_id,role,commit_seq,ordinal,content,navigation_caption,source_input_id) values($1,$2,$3,$4,1,$5,$4,0,$6,$7,$8)",
          [
            tenant,
            job.flow_id,
            job.id,
            id,
            role,
            JSON.stringify(content),
            role === "user" ? blocks : final ? "done" : null,
            input ?? null,
          ],
        );
        await pool.query("update turns set output_display_seq=$2 where id=$1", [job.id, id]);
        return id;
      }
      async function place(job, boundary = "start") {
        const mail = await rt.inputs.selectAt(
          job.scope,
          boundary,
          boundary === "postTools" ? pos : null,
        );
        assert(mail);
        const id = await entry(job, "user", mail.body, { input: mail.id });
        await pool.query("select segment_input_receipts($1,$2,$3)", [
          tenant,
          job.id,
          JSON.stringify([{ id: mail.id, state: "placed", native_id: id }]),
        ]);
        return id;
      }
      function proposal(name, mode) {
        return {
          type: "toolCall",
          id: randomUUID(),
          name: "subagent",
          arguments: { name, context: mode, task: name, workspace: null },
        };
      }
      async function delegate(job, name, mode, request) {
        const block = proposal(name, mode);
        return registerCall(job, request, await entry(job, "assistant", [block]), block);
      }
      async function registerCall(job, request, assistant, block) {
        const { id: call, arguments: args } = block,
          mode = args.context;
        return rt.store.delegateFromIntent(
          tenant,
          job.logical_id,
          { run: job.id, writer: job.scope.writerId },
          {
            sequence: assistant + 1,
            operationId: randomUUID(),
            conversationId: 1,
            toolName: "subagent",
            nativeCallId: call,
            arguments: args,
            contextAnchor: mode === "branch" ? request : null,
            contextThrough: request,
            requestEntry: request,
            assistantEntry: assistant,
          },
          "fixture",
        );
      }
      async function finish(job) {
        const id = await entry(job, "assistant", [{ type: "text", text: "done" }], { final: true }),
          key = randomUUID();
        const fact = {
          kind: "work_finished",
          factId: key,
          scope: job.scope,
          agentId: "root",
          nativeSequence: id,
          baseSequence: 0,
          terminal: { type: "turn.completed", payload: { stopReason: "stop" } },
          occurredAt: new Date().toISOString(),
        };
        assert(
          await rt.leases.projectBoundary(
            { fact, topic: "nav", partition: 0, offset: BigInt(id) },
            async (tx) => {
              await sql`update turns set history_head=jsonb_build_object('version',${key}::text,'seq',${id}::int,'nextId',${id + 1}::int) where id=${job.id}::uuid`.execute(
                tx,
              );
            },
          ),
        );
        await job.release();
        return String(id);
      }
      const path = async (session) => (await api.getConversationTree(session, "focus")).branches[0];
      const labels = async (session) => (await path(session)).entries.map((e) => e.text);
      await admit("A");
      const first = await claim();
      await place(first);
      await admit("B", "steer");
      const b = await place(first, "postTools");
      const x = await delegate(first, "X", "branch", b);
      const batch = [proposal("Y", "fresh"), proposal("Z", "branch")];
      const assistant = await entry(first, "assistant", batch);
      // Dispatch can finish out of order. Navigation follows the model's call order.
      const z = await registerCall(first, b, assistant, batch[1]),
        y = await registerCall(first, b, assistant, batch[0]);
      const other = await delegate(first, "fourth", "fresh", b);
      assert.equal((await path(x)).contextMode, "branch");
      assert.deepEqual(await labels(x), ["A", "B"]);
      assert.equal((await path(y)).contextMode, "fresh");
      assert.deepEqual(await labels(y), []);
      const cutoff = await finish(first);
      const jobs = new Map();
      for (let i = 0; i < 4; i++) {
        const j = await claim();
        jobs.set(j.logical_id, j);
        await place(j);
      }
      const head = await path(root),
        link = head.entries.find((e) => e.text === "B").children;
      assert.deepEqual(
        link.items.map((c) => c.name),
        ["X", "Y", "Z"],
      );
      assert(link.next);
      const next = await api.getNavigationChildren(root, String(b), link.next);
      assert.deepEqual(
        new Set([...link.items, ...next.items].map((c) => c.sessionId)),
        new Set([x, y, z, other]),
      );
      assert.deepEqual(await labels(x), ["A", "B", "X"]);
      assert.deepEqual(await labels(y), ["Y"]);
      assert(!(await path(x)).entries.some((e) => e.children));
      assert.equal((await api.getSessionModel(x)).sessionId, x);
      const shown = await api.getConversation(x, { turnId: first.id, entryId: String(b) });
      assert(
        shown.turns.some((t) =>
          t.transcript.items.some((i) => i.kind === "user" && i.text === "B"),
        ),
      );
      for (const [id, j] of jobs) if (id !== x) await finish(j);
      await admit("P");
      const second = await claim();
      const p = await place(second);
      assert.equal(await delegate(second, "X", "branch", p), x);
      const m = await delegate(second, "M", "branch", p);
      await finish(second);
      const mJob = await claim(),
        mInput = await place(mJob);
      const n = await delegate(mJob, "N", "branch", mInput);
      await finish(mJob);
      const nJob = await claim();
      await place(nJob);
      assert.deepEqual(await labels(n), ["A", "B", "P", "M", "N"]);
      assert.deepEqual(
        (await path(m)).entries.at(-1).children.items.map((c) => c.name),
        ["N"],
      );
      await api.getConversation(root);
      await api.getConversation(m);
      await api.getConversation(n);
      if (process.env.PI_CLOUD_NAVIGATION_BROWSER === "1") {
        const { verifyDelegationNavigationBrowser } =
          await import("./delegation-navigation-browser.mjs");
        await verifyDelegationNavigationBrowser(apiServer.origin, identity.credential.token, root);
      }
      const key = randomUUID();
      await assert.rejects(
        api.pruneConversation(root, first.id, cutoff, key),
        (e) => e.status === 409,
      );
      assert.deepEqual(await labels(n), ["A", "B", "P", "M", "N"]);
      const nEnd = await finish(nJob);
      const g = (await api.forkConversation(n, nJob.id, nEnd, "Independent", randomUUID())).session
        .sessionId;
      const result = await api.pruneConversation(root, first.id, cutoff, key);
      assert.equal(result.prunedSessionCount, 2);
      assert.equal(
        (await api.pruneConversation(root, first.id, cutoff, key)).prunedSessionCount,
        2,
      );
      await assert.rejects(path(n), (e) => e.status === 404);
      await assert.rejects(api.getConversation(m), (e) => e.status === 404);
      assert.deepEqual(await labels(x), ["A", "B", "X"]);
      assert.deepEqual(await labels(root), ["A", "B"]);
      assert.deepEqual(await labels(g), ["A", "B", "P", "M", "N"]);
      await api.deleteConversation(z);
      assert.deepEqual(
        (await api.getNavigationChildren(root, String(b), link.next)).items.map((c) => c.name),
        ["fourth"],
      );
      await assert.rejects(
        api.getNavigationChildren(root, String(b), "invalid"),
        (e) => e.status === 400,
      );
      const xOrigin = Number((await path(x)).entries.at(-1).entryId);
      const leaf = await delegate(jobs.get(x), "leaf", "fresh", xOrigin);
      await finish(jobs.get(x));
      const leafJob = await claim();
      await place(leafJob);
      await finish(leafJob);
      await api.deleteConversation(x);
      await assert.rejects(path(leaf), (e) => e.status === 404);
      assert.deepEqual(await labels(y), ["Y"]);
      assert.deepEqual(await labels(root), ["A", "B"]);
      await api.deleteConversation(root);
      await rt.store.collect(tenant);
      assert.deepEqual(await labels(g), ["A", "B", "P", "M", "N"]);
      await assert.rejects(path(x), (e) => e.status === 404);
    } finally {
      await apiServer?.close();
      await worker?.close();
      await rt.store.db.destroy();
      await db.destroy();
    }
  },
  120000,
);
