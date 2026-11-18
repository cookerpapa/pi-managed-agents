import { ConversationDeletionService } from "../src/conversation-deletion-service.ts";
import { randomUUID } from "node:crypto";
import { Pool } from "pg";
import { beforeAll, afterAll, it, expect } from "vitest";
import { sql, type Kysely, type CompiledQuery } from "kysely";
import {
  assertProjectionHistoryCompatible,
  createDatabase,
  runMigrations,
  type Database,
} from "@pi-cloud/database";
import { DurablePostgresProjection } from "@pi-cloud/pi-session-postgres/durable";
import {
  parseConversationDetailResource,
  HISTORY_PAGE_BYTES,
  type DurableRecordOrigin,
} from "@pi-cloud/protocol";
import { createPrivateTenant } from "../src/tenant-administration.ts";
import { ControlPlaneStore } from "../src/control-plane-store.ts";
import { ConversationTreeService } from "../src/conversation-tree-service.ts";
import { conversationMarkdown } from "@pi-cloud/runtime-core/conversation-markdown";
import {
  captureNativePresentation,
  mayCaptureNativePresentation,
} from "../../runtime-core/src/frozen-history.ts";
import { settleHistoryTurn } from "../../runtime-core/src/presentation-settlement.ts";
import { readCanonicalHistoryWindow } from "../../runtime-core/src/canonical-history-window.ts";
import { readFrozenHistoryWindow } from "../../runtime-core/src/frozen-history-window.ts";
import { decodeHistoryPosition } from "../../runtime-core/src/history-position.ts";

let db: Kysely<Database>, admin: Pool, name: string;
beforeAll(async () => {
  if (!process.env.PI_CLOUD_POSTGRES_INTEGRATION_URL) throw new Error("Use isolated PG runner");
  admin = new Pool({ connectionString: process.env.PI_CLOUD_POSTGRES_INTEGRATION_URL, max: 1 });
  name = "history_" + randomUUID().replaceAll("-", "");
  await admin.query(`create database "${name}"`);
  const url = new URL(process.env.PI_CLOUD_POSTGRES_INTEGRATION_URL);
  url.pathname = "/" + name;
  db = createDatabase({ connectionString: url.toString(), maxConnections: 4 });
  await runMigrations(db, "up");
}, 60000);
afterAll(async () => {
  await db?.destroy();
  if (admin) {
    await admin.query(`drop database "${name}"`);
    await admin.end();
  }
});
async function fixture() {
  const tenant = await createPrivateTenant(db, {
    slug: "history-" + randomUUID(),
    ownerDisplayName: "History window",
    initialModel: { provider: "openai-codex", modelId: "gpt-6-luna" },
  });
  const store = new ControlPlaneStore({ database: db, ...tenant });
  const project = await store.createProject({ name: "history", source: { kind: "empty" } });
  const session = await store.createSession(
    project.projectId,
    project.workspaceId,
    "history",
    "elastic",
  );
  const accepted = await store.acceptTurn(session.sessionId, randomUUID(), {
    prompt: "original input",
  });
  const native = await db
    .selectFrom("sessions")
    .select("pi_session_id")
    .where("id", "=", session.sessionId)
    .executeTakeFirstOrThrow();
  const projection = new DurablePostgresProjection(db, {
    tenantId: tenant.tenantId,
    sessionId: native.pi_session_id,
  });
  let sequence = 0,
    nextId = 2;
  const apply = async (writes: unknown[]) => {
    const origins: DurableRecordOrigin[] = writes.map((w: any) => ({
      recordId: w.value?.id ?? w.record?.id ?? w.id,
      runId: accepted.runId,
      turnId: accepted.turnId,
      sessionId: session.sessionId,
    }));
    await projection.apply({ seq: ++sequence, nextId, writes } as never, origins, {
      topic: "history-fixture",
      partition: 0,
      offset: BigInt(sequence),
    });
  };
  await apply([{ type: "conversation", value: { id: 1 } }]);
  const append = async (text: string) => {
    const id = nextId++;
    await apply([
      {
        type: "entry",
        value: {
          id,
          conversationId: 1,
          kind: "pi.assistant",
          model: [
            {
              role: "assistant",
              content: [{ type: "text", text }],
              api: "openai-codex-responses",
              provider: "openai-codex",
              model: "gpt-6-luna",
              timestamp: Date.now(),
              stopReason: "stop",
              usage: {
                input: 1,
                output: 1,
                cacheRead: 0,
                cacheWrite: 0,
                totalTokens: 2,
                cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
              },
            },
          ],
        },
      },
    ]);
    return id;
  };
  const finish = () =>
    db
      .updateTable("runs")
      .set({
        state: "completed",
        started_at: new Date(),
        settled_at: new Date(),
        output_display_seq: String(sequence + 1),
        output_display_native_seq: String(sequence),
      })
      .where("id", "=", accepted.runId)
      .execute();
  return {
    tenant,
    store,
    session,
    accepted,
    append,
    apply,
    finish,
    freeze: async () => {
      await finish();
      for (let seq = 1; seq <= sequence; seq++)
        await db
          .transaction()
          .execute((tx) =>
            captureNativePresentation(
              tx,
              { tenantId: tenant.tenantId, sessionId: native.pi_session_id },
              seq,
            ),
          );
      await db
        .transaction()
        .execute((tx) =>
          settleHistoryTurn(tx, tenant.tenantId, session.sessionId, accepted.turnId),
        );
    },
    projection,
    native,
    nextId: () => nextId,
    sequence: () => sequence,
    setId: (id: number) => {
      nextId = id;
    },
  };
}

it("fetches a tiny frozen page by immutable references even with underestimated tenant statistics", async () => {
  // Analyse before adding a new tenant, reproducing the small-row estimate that
  // made joins scan an entire tenant repeatedly in the live capacity workload.
  for (const table of ["pi_history_records", "pi_history_bodies", "pi_history_frames"])
    await sql.raw(`analyze ${table}`).execute(db);
  const f = await fixture();
  const entries = Array.from({ length: 512 }, (_, i) => ({
    type: "entry",
    value: {
      id: i + 2,
      conversationId: 1,
      kind: "pi.assistant",
      model: [
        { role: "assistant", stopReason: "stop", content: [{ type: "text", text: `item-${i}` }] },
      ],
    },
  }));
  f.setId(514);
  await f.apply(entries);
  await f.freeze();
  const queries: CompiledQuery[] = [];
  const observed = db.withPlugin({
    transformQuery(args) {
      queries.push(db.getExecutor().compileQuery(args.node, args.queryId));
      return args.node;
    },
    async transformResult(args) {
      return args.result;
    },
  });
  const page = await readFrozenHistoryWindow(observed, {
    tenantId: f.tenant.tenantId,
    sessionId: f.session.sessionId,
    direction: "before",
    maxRecords: 3,
    turns: [
      {
        turnId: f.accepted.turnId,
        originSessionId: f.session.sessionId,
        piSessionId: f.native.pi_session_id,
        operationKind: "chat",
        frozen: true,
      },
    ],
  });
  expect(page.positions).toHaveLength(3);
  expect(page.more).toBe(true);
  expect(
    page.transcripts
      .get(f.accepted.turnId)
      ?.items.map((i) => (i.kind === "text" ? i.text : i.kind)),
  ).toEqual(["item-509", "item-510", "item-511"]);
  // Bound measured work, not a specific plan string or machine-dependent time.
  // The regression performed millions of buffer accesses for three results.
  for (const q of queries) {
    const explained = await db.executeQuery<{
      "QUERY PLAN": Array<{ Plan: Record<string, number> }>;
    }>({
      ...q,
      sql: `explain (analyze,buffers,format json) ${q.sql}`,
    });
    const plan = explained.rows[0]!["QUERY PLAN"][0]!.Plan;
    expect((plan["Shared Hit Blocks"] ?? 0) + (plan["Shared Read Blocks"] ?? 0)).toBeLessThan(
      20_000,
    );
  }
}, 30000);

it("pages inside one long Run in both directions and seeks an Entry without earlier bodies", async () => {
  const f = await fixture();
  const ids = [];
  for (let i = 0; i < 150; i++) ids.push(await f.append(`entry-${i}`));
  await f.finish();
  const latest = parseConversationDetailResource(
    await f.store.getConversation(f.session.sessionId),
  );
  expect(latest.turns).toHaveLength(1);
  expect(latest.turns[0]?.continuesBefore).toBe(true);
  expect(latest.history.hasBefore).toBe(true);
  expect(latest.history.hasAfter).toBe(false);
  expect(latest.turns[0]?.transcript?.items).toHaveLength(64);
  const previous = await f.store.getConversation(f.session.sessionId, {
    before: latest.history.before!,
  });
  expect(previous.turns[0]?.transcript?.items[0]).toMatchObject({ kind: "text", text: "entry-22" });
  const next = await f.store.getConversation(f.session.sessionId, {
    after: previous.history.after!,
  });
  expect(next.turns[0]?.transcript?.items).toEqual(latest.turns[0]?.transcript?.items);
  const first = await f.store.getConversation(f.session.sessionId, {
    before: previous.history.before!,
  });
  expect(first.history.hasBefore).toBe(false);
  expect(first.turns[0]?.prompt).toBe("original input");
  const around = await f.store.getConversation(f.session.sessionId, {
    turnId: f.accepted.turnId,
    entryId: String(ids[80]),
  });
  expect(around.turns[0]?.transcript?.items.at(-1)).toMatchObject({ text: "entry-80" });
}, 30000);

it("keeps system context out of live and frozen pages, including mixed commits and Forks", async () => {
  const f = await fixture();
  const first = await f.append("before");
  const system = (id: number) => ({
    type: "entry",
    value: {
      id,
      conversationId: 1,
      kind: "pi.system",
      model: [
        {
          role: "system",
          content: "",
          sections: { instructions: "internal context" },
          timestamp: id,
        },
      ],
    },
  });
  const systems = Array.from({ length: 65 }, (_, i) => system(f.nextId() + i));
  const mixedSystem = system(f.nextId() + systems.length);
  const last = mixedSystem.value.id + 1;
  const usageId = last + 1;
  f.setId(usageId + 1);
  const mixed = [
    mixedSystem,
    {
      type: "entry",
      value: {
        id: last,
        conversationId: 1,
        kind: "pi.assistant",
        model: [
          {
            role: "assistant",
            content: [{ type: "text", text: "after" }],
            stopReason: "stop",
            timestamp: last,
          },
        ],
      },
    },
    {
      type: "document.create",
      record: {
        id: usageId,
        kind: "pi.usage",
        scope: { kind: "conversation", conversationId: 1 },
        history: "latest",
        fork: "initial",
      },
      content: {
        kind: "base",
        version: 1,
        value: {
          models: { "test/model": { input: 11, output: 8, cacheRead: 0, cacheWrite: 0 } },
          tools: {},
        },
      },
    },
  ];
  expect(mayCaptureNativePresentation(systems as never)).toBe(false);
  expect(mayCaptureNativePresentation(mixed as never)).toBe(true);
  await f.apply(systems);
  await f.apply(mixed);
  // An active Run exposes native writes only through its existing coverage.
  await db
    .updateTable("runs")
    .set({
      state: "running",
      output_display_seq: "2",
      output_display_native_seq: String(f.sequence()),
    })
    .where("id", "=", f.accepted.runId)
    .execute();
  async function checkPages(frozen: boolean) {
    const turns = [
      {
        turnId: f.accepted.turnId,
        originSessionId: f.session.sessionId,
        piSessionId: f.native.pi_session_id,
        operationKind: "chat" as const,
        frozen,
      },
    ];
    for (const direction of ["before", "after"] as const) {
      const page = await readCanonicalHistoryWindow(db, {
        tenantId: f.tenant.tenantId,
        sessionId: f.session.sessionId,
        turns,
        direction,
      });
      expect(page.more).toBe(false);
      expect(page.positions.map((p) => [p.sequence, p.ordinal])).toEqual([
        [0, 0],
        [2, 0],
        [4, 1],
      ]);
      expect(
        page.transcripts
          .get(f.accepted.turnId)
          ?.items.map((i) => (i.kind === "text" ? i.text : i.kind)),
      ).toEqual(["before", "after"]);
    }
  }
  await checkPages(false);
  await f.freeze();
  await checkPages(true);
  const {
    rows: [frames],
  } = await sql<{ n: number }>`select count(*)::int as n from pi_history_frames f
    join pi_history_records g on g.tenant_id=f.tenant_id and g.id=f.entry_record_id
    where g.tenant_id=${f.tenant.tenantId}::uuid and g.session_id=${f.native.pi_session_id} and g.fields->>'nativeKind'='"pi.system"'`.execute(
    db,
  );
  expect(frames?.n).toBe(0);
  await expect(
    db.transaction().execute(async (tx) => {
      await sql`insert into pi_history_frames(tenant_id,block_id,seq,ordinal,record_id,entry_id,entry_record_id,body_id,body_bytes,fields)
      select f.tenant_id,f.block_id,g.seq,g.ordinal,g.record_id,g.record_id,g.id,f.body_id,f.body_bytes,f.fields
      from pi_history_frames f join pi_history_records g on g.tenant_id=f.tenant_id
      where f.tenant_id=${f.tenant.tenantId}::uuid and f.entry_id=${last} and g.session_id=${f.native.pi_session_id}
        and g.record_kind='entry' and g.record_id=${mixedSystem.value.id}`.execute(tx);
      await assertProjectionHistoryCompatible(tx);
    }),
  ).rejects.toThrow("pi.system chat frames");
  const fork = await new ConversationTreeService({ database: db }).fork(
    f.tenant.tenantId,
    f.session.sessionId,
    randomUUID(),
    { turnId: f.accepted.turnId, entryId: String(last) },
  );
  expect((await f.store.getConversationUsage(f.session.sessionId)).tokens.total).toBe(19);
  expect((await f.store.getConversationUsage(fork.session.sessionId)).tokens.total).toBe(0);
  const branch = new DurablePostgresProjection(db, {
    tenantId: f.tenant.tenantId,
    sessionId: fork.session.sessionId,
  });
  expect((await branch.get(first))?.kind).toBe("entry");
  await new ConversationDeletionService({ database: db }).delete(
    f.tenant.tenantId,
    f.session.sessionId,
  );
  for (const id of [systems[0]!.value.id, mixedSystem.value.id]) {
    expect((await branch.get(id))?.value).toMatchObject({
      kind: "pi.system",
      model: [{ sections: { instructions: "internal context" } }],
    });
  }
  expect(
    (await f.store.getConversation(fork.session.sessionId)).turns
      .flatMap((t) => t.transcript?.items ?? [])
      .filter((i) => i.kind === "text")
      .map((i) => i.text),
  ).toEqual(["before", "after"]);
});

it("applies the byte budget before body fetching and retains every page", async () => {
  const f = await fixture();
  for (let i = 0; i < 40; i++) await f.append(`${i}:` + "x".repeat(180000));
  await f.finish();
  await checkPages();
  await f.freeze();
  await checkPages();
  async function checkPages() {
    let page = await f.store.getConversation(f.session.sessionId),
      seen = 0;
    for (;;) {
      expect(Buffer.byteLength(JSON.stringify(page))).toBeLessThan(HISTORY_PAGE_BYTES + 65536);
      seen += page.turns
        .flatMap((t) => t.transcript?.items ?? [])
        .filter((i) => i.kind === "text").length;
      if (!page.history.hasBefore) break;
      page = await f.store.getConversation(f.session.sessionId, { before: page.history.before! });
    }
    expect(seen).toBe(40);
  }
}, 30000);

it("rejects a cursor or Entry outside the visible Session and preserves empty history", async () => {
  const a = await fixture(),
    b = await fixture();
  const id = await a.append("private");
  await a.finish();
  await b.finish();
  const view = await a.store.getConversation(a.session.sessionId);
  await expect(
    b.store.getConversation(b.session.sessionId, { before: view.history.before! }),
  ).rejects.toMatchObject({ code: "invalid_request" });
  await expect(
    b.store.getConversation(b.session.sessionId, {
      turnId: a.accepted.turnId,
      entryId: String(id),
    }),
  ).rejects.toMatchObject({ code: "not_found" });
  expect((await b.store.getConversation(b.session.sessionId)).turns[0]?.transcript?.items).toEqual(
    [],
  );
});

it("reads a new empty conversation without expanding history, then sees its accepted Run", async () => {
  const f = await fixture();
  const session = await f.store.createSession(
    f.session.projectId,
    f.session.workspaceId,
    "empty",
    "elastic",
  );
  const queries: string[] = [];
  const observed = db.withPlugin({
    transformQuery(args) {
      queries.push(db.getExecutor().compileQuery(args.node, args.queryId).sql);
      return args.node;
    },
    async transformResult(args) {
      return args.result;
    },
  });
  const store = new ControlPlaneStore({ database: observed, ...f.tenant });
  const empty = parseConversationDetailResource(await store.getConversation(session.sessionId));
  expect(empty.turns).toEqual([]);
  expect(empty.history).toMatchObject({ hasBefore: false, hasAfter: false });
  expect(queries.some((q) => q.includes("from pi_history_turns"))).toBe(false);
  await expect(
    store.getConversation(session.sessionId, { turnId: f.accepted.turnId }),
  ).rejects.toMatchObject({ code: "not_found" });
  await expect(
    store.getConversation(session.sessionId, { before: "invalid" }),
  ).rejects.toMatchObject({ code: "invalid_request" });
  const accepted = await store.acceptTurn(session.sessionId, randomUUID(), { prompt: "new input" });
  queries.length = 0;
  await store.getConversation(session.sessionId);
  expect(queries.some((q) => q.includes("from pi_history_turns"))).toBe(true);
  await db
    .updateTable("runs")
    .set({ state: "completed", started_at: new Date(), settled_at: new Date() })
    .where("id", "=", accepted.runId)
    .execute();
  const active = await store.getConversation(session.sessionId);
  expect(active.turns).toHaveLength(1);
  expect(active.turns[0]).toMatchObject({ runId: accepted.runId, prompt: "new input" });
});

it("pairs off-page Tool arguments by Task even when provider call IDs repeat", async () => {
  const f = await fixture();
  const tasks: number[] = [];
  for (const path of ["first.ts", "second.ts"]) {
    const assistant = f.nextId(),
      task = assistant + 1;
    f.setId(task + 1);
    tasks.push(task);
    await f.apply([
      {
        type: "entry",
        value: {
          id: assistant,
          kind: "pi.assistant",
          conversationId: 1,
          model: [
            {
              role: "assistant",
              stopReason: "toolUse",
              content: [{ type: "toolCall", id: "same-call", name: "read", arguments: { path } }],
            },
          ],
        },
      },
      {
        type: "task",
        value: {
          id: task,
          kind: "pi.tool",
          conversationId: 1,
          version: 1,
          input: { assistant, callId: "same-call" },
          background: false,
          abortRequested: false,
          state: {
            status: "running",
            checkpoint: { phase: "execute", arguments: { path }, replay: "safe" },
          },
        },
      },
    ]);
  }
  for (let i = 0; i < 90; i++) await f.append("intervening " + i);
  // Results finish in the reverse order; neither the previous page nor the latest
  // assistant can substitute for this Task's original intent.
  for (const [index, task] of [...tasks].reverse().entries()) {
    const result = f.nextId();
    f.setId(result + 1);
    await f.apply([
      {
        type: "entry",
        value: {
          id: result,
          conversationId: 1,
          byTaskId: task,
          kind: "pi.tool-result",
          model: [
            {
              role: "toolResult",
              toolCallId: "same-call",
              toolName: "read",
              content: [{ type: "text", text: "result " + index }],
              isError: false,
            },
          ],
        },
      },
      {
        type: "task",
        value: {
          id: task,
          kind: "pi.tool",
          conversationId: 1,
          version: 1,
          input: { assistant: task - 1, callId: "same-call" },
          background: false,
          abortRequested: false,
          state: {
            status: "terminal",
            outcome: { status: "completed", result: { entryId: result } },
          },
        },
      },
    ]);
  }
  await f.finish();
  const view = await f.store.getConversation(f.session.sessionId);
  const tools = view.turns
    .flatMap((t) => t.transcript?.items ?? [])
    .filter((i) => i.kind === "tool");
  expect(tools.map((i) => [i.toolCallId, i.input])).toEqual([
    [`task:${tasks[1]}`, { path: "second.ts" }],
    [`task:${tasks[0]}`, { path: "first.ts" }],
  ]);
  await f.freeze();
  const frozen = await f.store.getConversation(f.session.sessionId);
  const canonical = (items: typeof tools) =>
    items.map((i) => ({
      ...i,
      historyCursor: decodeHistoryPosition(i.historyCursor!, f.session.sessionId),
    }));
  expect(
    canonical(
      frozen.turns.flatMap((t) => t.transcript?.items ?? []).filter((i) => i.kind === "tool"),
    ),
  ).toEqual(canonical(tools));
});

it("reads only a consumed input's metadata slice, preserving JSON escapes and Unicode", async () => {
  const f = await fixture();
  const doc = f.nextId(),
    entry = doc + 1,
    submission = doc + 2;
  f.setId(submission + 1);
  const inputId = "input-" + "a".repeat(64),
    text = 'message with } braces, "quotes", 😀' + "汉".repeat(95000);
  const input = {
    idempotencyKey: "one",
    fingerprint: "a".repeat(64),
    text,
    uploads: [],
    acceptedAt: new Date().toISOString(),
    revision: 1,
    cancelled: false,
    nativeId: submission,
  };
  await f.apply([
    {
      type: "document.create",
      record: {
        id: doc,
        kind: "pi-cloud.human-inputs",
        version: 1,
        scope: { kind: "conversation", conversationId: 1 },
        key: f.accepted.runId,
        history: "latest",
        fork: "initial",
        createdAt: f.sequence() + 1,
      },
      content: {
        kind: "base",
        version: 1,
        value: {
          runId: f.accepted.runId,
          inputs: {
            [inputId]: input,
            "input-unrelated": { ...input, text: "\u0000\ud800" + "unrelated".repeat(100000) },
          },
          order: [inputId],
          receipts: {},
        },
      },
    },
    {
      type: "entry",
      value: {
        id: entry,
        kind: "pi.user",
        conversationId: 1,
        model: [{ role: "user", content: text, timestamp: Date.now() }],
      },
    },
    {
      type: "submission",
      value: {
        id: submission,
        conversationId: 1,
        type: "input",
        requestId: inputId,
        status: "done",
        entry,
      },
    },
  ]);
  await f.finish();
  const view = await f.store.getConversation(f.session.sessionId);
  expect(view.turns[0]?.transcript?.items.find((i) => i.kind === "user")).toMatchObject({
    inputId,
    text,
  });
  expect(JSON.stringify(view)).not.toContain("unrelated");
});

it("pages navigation captions and streams every completed message through one export", async () => {
  const f = await fixture();
  for (let i = 0; i < 170; i++) await f.append("export-" + i);
  await f.finish();
  const service = new ConversationTreeService({ database: db });
  const nav = await service.tree(f.tenant.tenantId, f.session.sessionId, "focus");
  expect(nav.branches[0]?.entries).toHaveLength(80);
  expect(nav.branches[0]?.earlierEntryId).toBeDefined();
  const old = await service.tree(f.tenant.tenantId, f.session.sessionId, "focus", {
    beforeEntryId: nav.branches[0]!.earlierEntryId!,
  });
  expect(old.branches[0]?.entries.at(-1)?.text).toBe("export-89");
  const parts = [];
  for await (const part of conversationMarkdown(f.store.completedHistoryPages(f.session.sessionId)))
    parts.push(part);
  const exported = parts.join("");
  expect(exported.match(/## Turn 1/g)).toHaveLength(1);
  for (let i = 0; i < 170; i++) expect(exported).toContain("export-" + i + "\n");
  expect(parts.length).toBeGreaterThan(170);
  let cursor: string | undefined,
    found = false;
  for (let i = 0; i < 5; i++) {
    const result = await f.store.searchConversation(f.session.sessionId, "export-7", cursor);
    if (result.results.some((r) => r.snippet === "export-7")) {
      found = true;
      break;
    }
    if (!result.nextCursor) break;
    cursor = result.nextCursor;
  }
  expect(found).toBe(true);
}, 30000);

it("backfills disposable indexes without changing native history bodies or fingerprints", async () => {
  const before =
    await sql`select tenant_id,session_id,seq,ordinal,md5(body) as fingerprint from pi_durable_log order by tenant_id,session_id,seq,ordinal`.execute(
      db,
    );
  const fields =
    await sql`select tenant_id,session_id,id,fields from pi_durable_records where kind in ('entry','task') order by tenant_id,session_id,id`.execute(
      db,
    );
  const scopes = (
    await sql<{
      tenant_id: string;
      session_id: string;
    }>`select distinct tenant_id,session_id from pi_durable_commits`.execute(db)
  ).rows;
  for (const scope of scopes)
    await new DurablePostgresProjection(db, {
      tenantId: scope.tenant_id,
      sessionId: scope.session_id,
    }).rebuild();
  expect(
    (
      await sql`select tenant_id,session_id,seq,ordinal,md5(body) as fingerprint from pi_durable_log order by tenant_id,session_id,seq,ordinal`.execute(
        db,
      )
    ).rows,
  ).toEqual(before.rows);
  expect(
    (
      await sql`select tenant_id,session_id,id,fields from pi_durable_records where kind in ('entry','task') order by tenant_id,session_id,id`.execute(
        db,
      )
    ).rows,
  ).toEqual(fields.rows);
});
