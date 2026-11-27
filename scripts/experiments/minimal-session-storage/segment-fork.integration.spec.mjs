// Exercise the same Fork transaction and history reader that the browser calls.
// Only the candidate history relations live in a separate schema.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { it } from "vitest";
import pg from "pg";
import { createDatabase, runMigrations } from "@pi-cloud/database";
import {
  ControlPlaneStore,
  createPrivateTenant,
} from "../../../packages/control-plane/src/index.ts";
import {
  PostgresAttachmentStore,
  retainedAttachmentReferences,
} from "@pi-cloud/session-attachments";
import { SessionAttachmentReader } from "../../../packages/control-plane/src/session-attachment-reader.ts";
import { PiCloudApi } from "../../../packages/web-ui/src/api.ts";
import { installPagingProjection } from "./flow-paging-fixture.mjs";
import { createPiCloudEventFactory } from "@pi-cloud/protocol";
import { checkStudySse } from "./replica-study.mjs";
import { openPageApi } from "./flow-page-api.mjs";
import {
  ForkStorage,
  installForkAttachmentChecks,
  installForkTables,
  setForkLayout,
} from "./fork-storage.mjs";

const url = process.env.PI_CLOUD_POSTGRES_INTEGRATION_URL;
it.skipIf(!url)(
  "shares frozen message ranges through public Fork, nested delegation, cold paging and source deletion",
  async () => {
    const schema = "segments_" + randomUUID().replaceAll("-", "");
    const web = schema + "_web";
    const publicDb = createDatabase({ connectionString: url, maxConnections: 3 });
    const pool = new pg.Pool({ connectionString: url, options: "-c search_path=" + schema });
    const store = new ForkStorage(pool);
    let server;
    try {
      await runMigrations(publicDb, "up");
      const tenant = await createPrivateTenant(publicDb, {
        slug: schema.replaceAll("_", "-"),
        ownerDisplayName: "Fork test",
        initialModel: { provider: "openai-codex", modelId: "gpt-6-luna" },
      });
      const t = tenant.tenantId;
      const apiStore = new ControlPlaneStore({ database: publicDb, ...tenant });
      const project = await apiStore.createProject({
        name: "fork ranges",
        source: { kind: "empty" },
      });
      const p = (
        await apiStore.createSession(project.projectId, project.workspaceId, "source", "elastic")
      ).sessionId;
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
      await pool.query(
        "alter table runs add provider text,add model_id text,add model_runtime_snapshot jsonb,add recovery_requested_at timestamptz",
      );
      await installForkTables(pool);
      await setForkLayout(pool, true, false);
      await installForkAttachmentChecks(pool);
      await store.register(t, [p]);
      const blobs = new Map();
      const objects = new PostgresAttachmentStore(
        publicDb,
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
        publicDb,
        schema,
      );
      const image = await objects.put({ tenantId: t }, Buffer.from("retained bytes"), "image/png");
      const headerFile = await objects.put(
        { tenantId: t },
        Buffer.from("original input"),
        "text/plain",
      );
      const followFile = await objects.put(
        { tenantId: t },
        Buffer.from("follow-up input"),
        "text/plain",
      );
      function upload(ref) {
        return {
          id: randomUUID(),
          workspaceId: project.workspaceId,
          sessionId: p,
          name: "log.txt",
          path: "/workspace/log.txt",
          mimeType: ref.mimeType,
          byteLength: ref.byteLength,
          sha256: ref.sha256,
          original: { type: "pi_cloud_attachment_v1", attachment: ref },
        };
      }
      const hiddenImage = await objects.put(
        { tenantId: t },
        Buffer.from("later bytes"),
        "image/png",
      );
      await pool.query(
        "update public.session_attachment_objects set created_at=now()-interval '6 years' where tenant_id=$1 and id=any($2::uuid[])",
        [t, [image.id, hiddenImage.id, headerFile.id, followFile.id]],
      );
      const reader = new SessionAttachmentReader(publicDb, objects, schema);
      async function openImage(session, ref) {
        return reader.withImage(
          t,
          session,
          ref.id,
          async (_, chunks) => {
            const data = [];
            for await (const part of chunks) data.push(part);
            return Buffer.concat(data).toString();
          },
          new AbortController().signal,
        );
      }
      const age = 5 * 365 * 24 * 60 * 60 * 1000;
      const read = new URL(url);
      read.searchParams.set("options", "-c search_path=" + web + "," + schema + ",public");
      server = await openPageApi(url, read.href, true, undefined, undefined, true, schema);
      const api = new PiCloudApi(
        (path, o) => fetch(new URL(path, server.origin), o),
        tenant.credential.token,
      );
      async function turn(flow, pos, prompt) {
        const id = randomUUID();
        await pool.query(
          "insert into runs(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,model_ref,state,history_head,output_display_seq) values($1,$2,$3,$4,$1,$5,'fake','completed','{}',1)",
          [id, t, flow, pos, prompt],
        );
        return id;
      }
      async function entry(flow, run, id, seq, text, extra = {}) {
        const value = {
          id,
          conversationId: 1,
          kind: "pi.assistant",
          model: [
            {
              role: "assistant",
              api: "faux",
              provider: "faux",
              model: "faux",
              timestamp: id,
              stopReason: "stop",
              usage: { input: 1, output: 1, totalTokens: 2 },
              content: [{ type: "text", text }],
            },
          ],
          ...extra,
        };
        const refs = retainedAttachmentReferences(value);
        await pool.query(
          "insert into messages(tenant_id,pi_session_id,entry_id,conversation_id,run_id,role,content,commit_seq,ordinal,navigation_caption,attachment_refs) values($1,$2,$3,1,$4,'assistant',$5,$6,0,$7,$8)",
          [
            t,
            flow,
            id,
            run,
            JSON.stringify(value),
            seq,
            text,
            refs.length
              ? JSON.stringify(Object.fromEntries(refs.map((ref) => [ref.id, ref])))
              : null,
          ],
        );
      }
      const a = await turn(p, 1, "A");
      await entry(p, a, 10, 1, "public-A", {
        art: { type: "pi_cloud_image_v1", attachment: image },
      });
      await pool.query("update runs set input_uploads=$2 where id=$1", [
        a,
        JSON.stringify([upload(headerFile)]),
      ]);
      const b = await turn(p, 2, "B");
      await entry(p, b, 15, 2, "within-B");
      await entry(p, b, 20, 3, "public-B");
      const c = await turn(p, 3, "C");
      await entry(p, c, 30, 4, "private-C", {
        art: { type: "pi_cloud_image_v1", attachment: hiddenImage },
      });
      // A lower ID written by a later Turn before the user opens Fork is
      // outside the selected sealed Turn even though its ID sorts before B.
      await entry(p, c, 17, 7, "later-turn-reserved-id");
      const key = randomUUID();
      const result = await api.forkConversation(p, b, "20", "peer", key);
      const q = result.session.sessionId;
      assert.equal((await api.forkConversation(p, b, "20", "peer", key)).session.sessionId, q);
      await assert.rejects(api.forkConversation(p, b, "15", "invalid", randomUUID()));
      await entry(p, b, 18, 9, "late-published-low-id");
      assert.equal(await openImage(q, image), "retained bytes");
      await assert.rejects(openImage(q, hiddenImage), /not retained/);
      const navigation = JSON.stringify((await api.getConversationTree(q, "focus")).branches);
      assert(
        navigation.includes("public-B") &&
          !navigation.includes("private-C") &&
          !navigation.includes("late-published-low-id"),
        navigation,
      );
      const status = await api.getConversationStatus(q);
      assert(status.context.estimatedTokens > 0);
      const readFile = async (session, ref) =>
        reader.withAttachment(
          t,
          session,
          ref.id,
          async (_, chunks) => {
            const parts = [];
            for await (const part of chunks) parts.push(part);
            return Buffer.concat(parts).toString();
          },
          new AbortController().signal,
        );
      assert.equal(await readFile(q, headerFile), "original input");
      await pool.query("update runs set root_human_inputs=$2 where id=$1", [
        b,
        JSON.stringify({ inputs: { "input-late": { uploads: [upload(followFile)] } } }),
      ]);
      await pool.query(
        "update messages set source_input_id='input-late' where run_id=$1 and entry_id=18",
        [b],
      );
      assert.equal(await readFile(p, followFile), "follow-up input");
      await assert.rejects(readFile(q, followFile), /not retained/);
      assert(!(await store.page(t, q)).some((row) => Number(row.entry_id) === 17));
      const page = await api.getConversation(q, {});
      const shown = JSON.stringify(page.turns);
      assert(
        shown.includes("public-B") &&
          shown.includes("public-A") &&
          !shown.includes("private-C") &&
          !shown.includes("late-published-low-id"),
      );
      await assert.rejects(api.getConversation(q, { turnId: b, entryId: "18" }));
      const f = await turn(q, 1, "F");
      await entry(q, f, 40, 10, "visible-F");
      await entry(q, f, 50, 11, "private-G");
      const childKey = randomUUID(),
        caller = { run: f, writer: randomUUID() };
      await pool.query("update runs set state='running',lease_id=$2 where id=$1", [
        f,
        caller.writer,
      ]);
      const liveEvents = createPiCloudEventFactory(
        { sessionId: q, turnId: f, agentId: "root" },
        { initialSequence: 1 },
      );
      const delta = liveEvents.next({
        type: "assistant.text.delta",
        payload: { messageId: "local", text: "stream-on-branch", phase: "final_answer" },
      });
      let sent;
      await checkStudySse(
        { stream: (opts) => api.streamSession(opts) },
        q,
        (snapshot) => {
          const text = JSON.stringify(snapshot.conversation);
          assert(text.includes("public-B") && !text.includes("private-C"));
          sent = server.liveEvent(t, delta);
        },
        (event, stop) => {
          assert.equal(event.seq, delta.seq);
          stop.abort();
        },
      );
      await sent;
      const r = await store.delegate(
        t,
        q,
        { entry: 40, lane: 1, run: f },
        childKey,
        "tests",
        "inspect",
        "fake",
        caller,
      );
      assert.equal(
        await store.delegate(
          t,
          q,
          { entry: 40, lane: 1, run: f },
          childKey,
          "tests",
          "inspect",
          "fake",
          caller,
        ),
        r,
      );
      const rangeRows = await pool.query(
        "select source_flow,last_entry::int as hi from flow_segments where tenant_id=$1 and destination_flow=$2 order by segment_no",
        [t, r],
      );
      assert.deepEqual(rangeRows.rows, [
        { source_flow: p, hi: 20 },
        { source_flow: q, hi: 40 },
      ]);
      const dg = (await pool.query("select * from runs where session_id=$1", [r])).rows[0];
      assert.equal(dg.initial_input, "inspect");
      assert.equal(dg.state, "queued");
      const anchor = { entry: 40, lane: 1, run: f };
      assert.equal(
        await store.delegate(t, q, anchor, randomUUID(), "tests", "inspect", "fake", caller),
        r,
      );
      await assert.rejects(
        store.delegate(t, q, anchor, randomUUID(), "tests", "other task", "fake", caller),
        /different creation/,
      );
      await assert.rejects(
        store.delegate(t, q, anchor, randomUUID(), "unauthorized", "task", "fake", {
          run: f,
          writer: randomUUID(),
        }),
        /no longer active/,
      );
      const isolated = await store.delegate(
        t,
        q,
        null,
        randomUUID(),
        "fresh",
        "independent",
        "fake",
        caller,
      );
      assert.deepEqual(await store.page(t, isolated), []);
      await entry(q, f, 39, 12, "arrived-before-request");
      await entry(q, f, 38, 14, "arrived-after-request");
      const intent = {
        operationId: randomUUID(),
        nativeCallId: "call-test",
        toolName: "subagent",
        conversationId: 1,
        sequence: 17,
        contextAnchor: 40,
        contextThrough: 12,
        arguments: { name: "request-scoped", task: "inspect", context: "branch", workspace: null },
      };
      const scoped = await store.delegateFromIntent(t, q, caller, intent, "fake");
      const ids = (await store.page(t, scoped)).map((row) => Number(row.entry_id));
      assert(ids.includes(39) && ids.includes(40) && !ids.includes(38) && !ids.includes(50));
      const rootFork = await store.fork(t, r, { entry: 10, lane: 1, run: a });
      assert.deepEqual(
        (await store.page(t, rootFork)).map((x) => Number(x.entry_id)),
        [10],
      );
      const cursor = (await store.pageWithCursor(t, r, 1))[0].cursor;
      assert.deepEqual(
        (await store.pageWithCursor(t, r, 2, cursor)).map((x) => Number(x.entry_id)),
        [20, 15],
      );
      assert.deepEqual(
        (await store.contextPrefix(t, r)).map((x) => x.entry.id),
        [10, 15, 20, 40],
      );
      // A public fork at an older inherited position stays independent of its management tree.
      const peer = (await api.forkConversation(q, a, "10", "nested peer", randomUUID())).session
        .sessionId;
      const peerPage = await api.getConversation(peer, {});
      assert.equal(peerPage.turns.length, 1);
      const replayKey = randomUUID();
      await assert.rejects(api.pruneConversation(q, a, "10", replayKey));
      await api.deleteConversation(p);
      await store.collect(t);
      assert.equal(await objects.reap(age), 2);
      assert.equal(await openImage(peer, image), "retained bytes");
      assert(JSON.stringify((await api.getConversation(q, {})).turns).includes("public-B"));
      await pool.query("update runs set state='completed',lease_id=null where id=$1", [f]);
      const pruned = await api.pruneConversation(q, a, "10", replayKey);
      assert(pruned.prunedTurnCount >= 2);
      await assert.rejects(
        api.getConversation(q, { at: page.turns[0].historyCursor }),
        /storage changed/i,
      );
      assert.equal((await api.pruneConversation(q, a, "10", replayKey)).replayed, true);
      assert(!JSON.stringify((await api.getConversation(q, {})).turns).includes("visible-F"));
      assert.equal(pruned.prunedSessionCount, 3);
      assert.equal(
        (
          await pool.query("select 1 from sessions where tenant_id=$1 and id=any($2::uuid[])", [
            t,
            [r, isolated, scoped],
          ])
        ).rowCount,
        0,
      );
      assert.deepEqual(
        (await store.page(t, rootFork)).map((x) => Number(x.entry_id)),
        [10],
      );
      await api.deleteConversation(q);
      await store.collect(t);
      assert.deepEqual(
        (await store.page(t, rootFork)).map((x) => Number(x.entry_id)),
        [10],
      );
      assert.equal((await pool.query("select count(*)::int n from messages")).rows[0].n, 1);
      assert.equal((await pool.query("select count(*)::int n from sessions")).rows[0].n, 2);
      assert(JSON.stringify((await api.getConversation(peer, {})).turns).includes("public-A"));
      await api.deleteConversation(peer);
      const raced = await Promise.allSettled([
        ...Array.from({ length: 8 }, () => store.fork(t, rootFork, { run: a, lane: 1, entry: 10 })),
        store.retire(t, rootFork),
        store.collect(t),
      ]);
      assert.equal(raced[8].status, "fulfilled");
      assert.equal(raced[9].status, "fulfilled");
      await store.collect(t);
      for (const result of raced.slice(0, 8)) {
        if (result.status === "rejected") {
          assert.match(result.reason.message, /Unknown Fork source/);
          continue;
        }
        assert.deepEqual(
          (await store.page(t, result.value)).map((m) => Number(m.entry_id)),
          [10],
        );
        await store.retire(t, result.value);
      }
      await store.collect(t);
      assert.equal(await objects.reap(age), 2);
      assert.equal(blobs.size, 0);
      for (const table of ["flow_segments", "flows", "messages", "runs", "sessions"])
        assert.equal(
          (await pool.query("select count(*)::int n from " + table)).rows[0].n,
          0,
          table,
        );
    } finally {
      await server?.close();
      try {
        await pool.query(
          "drop schema if exists " + web + " cascade;drop schema if exists " + schema + " cascade",
        );
      } finally {
        await store.db.destroy();
        await publicDb.destroy();
      }
    }
  },
  120000,
);
