import { sql } from "kysely";
import { readReturnedInputs } from "../../../packages/control-plane/src/stopped-inputs.ts";
import { reopenRootProjection } from "./root-projection-handoff.mjs";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import pg from "pg";
import { normalRootProjection } from "./normal-root-projection.mjs";
import { expect, it } from "vitest";
import { createDatabase } from "@pi-cloud/database";
import { nativeNavigation } from "@pi-cloud/pi-session-postgres/durable";
import {
  rootMessageHeaders,
  readRootMessageWindow,
  rootMessageNavigation,
  rootMessagePosition,
} from "../../../packages/runtime-core/src/root-message-history.ts";
import {
  rootCompaction,
  rootSubmission,
} from "../../../packages/control-plane/src/root-input-reads.ts";

it("pages a Tool across proposal and result, and restores original input and command metadata within the root scope", async () => {
  const url = new URL(process.env.PI_CLOUD_POSTGRES_INTEGRATION_URL),
    schema = "root_" + randomUUID().replaceAll("-", "");
  const admin = new pg.Client({ connectionString: url.href });
  await admin.connect();
  url.searchParams.set("options", "-c search_path=" + schema);
  const db = createDatabase({ connectionString: url.href }),
    pgClient = new pg.Client({ connectionString: url.href });
  const tenant = randomUUID(),
    session = randomUUID(),
    run = randomUUID(),
    turn = randomUUID(),
    key = "input-" + randomUUID(),
    input = {
      tenantId: tenant,
      sessionId: session,
      direction: "before",
      maxRecords: 1,
      turns: [
        { turnId: turn, originSessionId: session, piSessionId: session, operationKind: "chat" },
      ],
    },
    scope = { tenantId: tenant, sessionId: session, conversationId: 1 };
  try {
    await admin.query("create schema " + schema);
    await pgClient.connect();
    await pgClient.query(await readFile(new URL("./schema.sql", import.meta.url), "utf8"));
    await pgClient.query(`create table sessions(tenant_id uuid,id uuid,pi_session_id text,pi_conversation_id bigint,pruned_at timestamptz);
      create table session_terminal_events(tenant_id uuid,session_id uuid,turn_id uuid,seq bigint,type text,payload jsonb,occurred_at timestamptz);
      alter table runs add turn_id uuid,add pruned_at timestamptz,add input_text text,add input_uploads jsonb default '[]',
        add projection_tools jsonb default '{}', add output_display_seq bigint default 12,
        add root_input_receipts jsonb,add root_human_inputs text,add root_compacts jsonb`);
    await pgClient.query("insert into sessions values($1,$2,$2::uuid::text,1,null)", [
      tenant,
      session,
    ]);
    await pgClient.query(
      "insert into runs(tenant_id,session_id,id,turn_id,mailbox_position,idempotency_key,initial_input,input_text,model_ref,state) values($1,$2,$3,$4,1,$5,'start','start','fake','running')",
      [tenant, session, run, turn, randomUUID()],
    );
    async function entry(id, role, model, extra = {}) {
      const body = {
        id,
        conversationId: 1,
        kind: "pi." + role,
        model: [{ role, ...model }],
        ...(extra.task ? { byTaskId: extra.task } : {}),
      };
      await pgClient.query(
        `insert into messages(tenant_id,pi_session_id,entry_id,conversation_id,run_id,role,content,source_input_id,commit_seq,ordinal,assistant_entry_id,tool_task_id,navigation_caption)
        values($1,$2,$3,1,$4,$5,$6,$7,$3,0,$8,$9,$10)`,
        [
          tenant,
          session,
          id,
          run,
          role,
          JSON.stringify(body),
          extra.source,
          extra.assistant,
          extra.task,
          nativeNavigation(body).preview,
        ],
      );
    }
    const args = { command: "npm test" },
      proposal = { type: "toolCall", id: "call", name: "bash", arguments: args };
    await entry(3, "assistant", { content: [proposal], stopReason: "toolUse" });
    const wait = {
      7: {
        conversationId: 1,
        seq: 4,
        ordinal: 2,
        arguments: args,
        startedAt: new Date().toISOString(),
        assistantEntryId: 3,
        reply: { toolCallId: "call", topic: "original-tool-replies", offset: "81" },
      },
    };
    await pgClient.query("update runs set projection_tools=$1", [wait]);
    const { brokerTimedRetention } = normalRootProjection(
      pgClient,
      "test",
      () => {},
      () => {},
    ).nativeStore;
    const replyFloors = async () =>
      (await brokerTimedRetention.replyFloors(db, ["original-tool-replies"])).rows;
    expect(await replyFloors()).toEqual([{ topic: "original-tool-replies", floor: "81" }]);
    const latest = async () => (await readRootMessageWindow(db, input)).transcripts.get(turn).items;
    expect(await latest()).toMatchObject([
      { kind: "tool", toolCallId: "task:7", status: "running", input: args },
    ]);
    const runRow = { run_id: run, pi_session_id: session };
    expect(await rootMessagePosition(db, tenant, runRow, "task", "7")).toEqual({
      seq: "4",
      ordinal: 2,
    });
    await entry(
      8,
      "toolResult",
      { toolCallId: "call", toolName: "bash", content: [{ type: "text", text: "passed" }] },
      { task: 7, assistant: 3 },
    );
    await pgClient.query("update runs set projection_tools='{}'");
    expect(await replyFloors()).toEqual([]);
    expect(await latest()).toMatchObject([
      { kind: "tool", toolCallId: "task:7", status: "completed", input: args },
    ]);
    expect(await rootMessagePosition(db, tenant, runRow, "task", "7")).toEqual({
      seq: "8",
      ordinal: 0,
    });
    const cursor = (await readRootMessageWindow(db, input)).positions[0];
    expect(
      (await readRootMessageWindow(db, { ...input, position: cursor })).positions[0].sequence,
    ).toBe(3);
    await expect(readRootMessageWindow(db, { ...input, maxBytes: 10 })).rejects.toThrow(
      "page boundary",
    );
    expect(
      (await readRootMessageWindow(db, { ...input, tenantId: randomUUID() })).positions,
    ).toEqual([]);
    const upload = {
      id: randomUUID(),
      workspaceId: randomUUID(),
      sessionId: session,
      name: "a.log",
      path: "/workspace/a.log",
      mimeType: "text/plain",
      byteLength: 1,
      sha256: "a".repeat(64),
    };
    const receipt = { id: 9, status: "done", entry: 10 },
      command = { runId: run, requestId: "manual", taskId: 12 };
    const compact = {
      latest: "manual",
      seq: 13,
      requests: { manual: command },
      tasks: {
        12: {
          kind: "pi.compaction",
          state: {
            status: "terminal",
            outcome: { status: "completed", result: { submissionId: 14 } },
          },
        },
      },
      submissions: { 14: { status: "queued" } },
    };
    await pgClient.query(
      "update runs set root_human_inputs=$1,root_input_receipts=$2,root_compacts=$3",
      [
        JSON.stringify({
          inputs: { [key]: { text: "edited text", nativeId: 9, uploads: [upload] } },
        }),
        { [key]: receipt },
        compact,
      ],
    );
    await entry(10, "user", { content: "internal prompt\u0000suffix" }, { source: key });
    expect(await latest()).toMatchObject([
      { kind: "user", inputId: key, text: "edited text", uploads: [upload] },
    ]);
    expect(await rootSubmission(db, scope, key)).toEqual({ ...receipt, runId: run });
    expect(await rootSubmission(db, { ...scope, tenantId: randomUUID() }, key)).toBeUndefined();
    expect(await rootCompaction(db, scope)).toMatchObject({
      requestId: "manual",
      state: "awaiting_placement",
      nativeSequence: 13,
    });
    expect(await rootCompaction(db, scope, "other")).toBeUndefined();
    expect((await rootMessageNavigation(db, { ...scope, limit: 1 })).entries).toMatchObject([
      { id: 10, role: "user", text: "internal prompt�suffix" },
    ]);
    const child = randomUUID(),
      other = randomUUID(),
      childTurn = randomUUID(),
      otherTurn = randomUUID();
    await pgClient.query(
      "create table subagent_conversations(tenant_id uuid,root_run_id uuid,root_session_id uuid,child_session_id uuid unique,child_turn_id uuid unique,created_at timestamptz default now(),output_display_seq bigint)",
    );
    await pgClient.query(
      "insert into sessions values($1,$2,$4::uuid::text,2,null),($1,$3,$4::uuid::text,3,null)",
      [tenant, child, other, session],
    );
    await pgClient.query(
      "insert into subagent_conversations values($1,$2,$3,$4,$5,now(),8),($1,$2,$3,$6,$7,now(),19)",
      [tenant, run, session, child, childTurn, other, otherTurn],
    );
    async function childEntry(id, lane, role, model, source = null) {
      await pgClient.query(
        "insert into messages(tenant_id,pi_session_id,entry_id,conversation_id,run_id,role,content,source_input_id,commit_seq,ordinal) values($1,$2,$3,$4,$5,$6,$7,$8,$3,0)",
        [
          tenant,
          session,
          id,
          lane,
          run,
          role,
          JSON.stringify({
            id,
            conversationId: lane,
            kind: "pi." + role,
            model: [{ role, ...model }],
          }),
          source,
        ],
      );
    }
    const prompt = "检查子任务\u0000并保留换行\n下一步";
    await childEntry(70, 2, "user", { content: [{ type: "text", text: prompt }] }, run);
    await childEntry(71, 3, "user", { content: "Sibling private prompt" }, run);
    await childEntry(73, 2, "assistant", { content: [proposal] });
    await childEntry(78, 3, "assistant", { content: [{ type: "text", text: "sibling answer" }] });
    await pgClient.query("update runs set projection_tools=$1", [
      { 77: { ...wait[7], seq: 74, conversationId: 2, assistantEntryId: 73 } },
    ]);
    const q = {
      ...input,
      sessionId: child,
      turns: [{ ...input.turns[0], turnId: childTurn, originSessionId: child }],
    };
    const target = { ...runRow, turn_id: childTurn, origin_session_id: child };
    expect(
      (await rootMessageHeaders(db, tenant, child, [childTurn], true)).get(childTurn).input_text,
    ).toBe(prompt);
    expect((await rootMessageHeaders(db, tenant, other, [childTurn], true)).size).toBe(0);
    expect(
      (await readRootMessageWindow(db, q, true)).transcripts.get(childTurn).items,
    ).toMatchObject([{ status: "running", input: args, toolCallId: "task:77" }]);
    expect(await rootMessagePosition(db, tenant, target, "task", "77", true)).toMatchObject({
      seq: "74",
    });
    expect(await rootMessagePosition(db, tenant, target, "entry", "78", true)).toBeUndefined();
    expect(
      await rootMessagePosition(db, randomUUID(), target, "entry", "73", true),
    ).toBeUndefined();
    expect((await readRootMessageWindow(db, { ...q, sessionId: other }, true)).positions).toEqual(
      [],
    );
    expect(
      (await readRootMessageWindow(db, { ...q, tenantId: randomUUID() }, true)).positions,
    ).toEqual([]);
  } finally {
    await db.destroy();
    await pgClient.end();
    try {
      await admin.query("drop schema if exists " + schema + " cascade");
    } finally {
      await admin.end();
    }
  }
});

it("binds the cold root projection to the sealed writer, the exact next native commit and the retained Inbox", async () => {
  const c = new pg.Client({ connectionString: process.env.PI_CLOUD_POSTGRES_INTEGRATION_URL });
  await c.connect();
  try {
    await c.query(`create temporary table runs(id uuid,session_id uuid,tenant_id uuid,lease_id uuid,fencing_token int,mailbox_position bigint,native_work_handoff jsonb,projection_tools jsonb,projection_pin bigint,history_head jsonb,state text);
      create temporary table session_leases(tenant_id uuid,lease_id uuid,pi_session_id text,fencing_token int,writer_seal_offset bigint,released_at timestamptz);
      create temporary table inboxes(tenant_id uuid,pi_session_id uuid,doc_id bigint,conversation_id int,commit_seq bigint,items jsonb)`);
    const session = randomUUID(),
      scope = {
        runId: randomUUID(),
        tenantId: randomUUID(),
        sessionId: session,
        piSessionId: session,
        turnId: randomUUID(),
        writerId: randomUUID(),
        fencingToken: 2,
      };
    const old = randomUUID(),
      point = { topic: "root", partition: 0, offset: 15n },
      h = {
        scope: { ...scope, writerId: old, fencingToken: 1 },
        topic: "root",
        partition: 0,
        offset: "14",
        nativeText: JSON.stringify({ seq: 9, nextId: 20, records: [], heads: [] }),
      },
      items = [{ id: 8, mode: "steer", content: "follow the edit" }];
    await c.query("insert into session_leases values($1,$2,$3,1,14,now()),($1,$4,$3,2,null,null)", [
      scope.tenantId,
      old,
      session,
      scope.writerId,
    ]);
    await c.query("insert into runs values($1,$2,$3,$4,2,1,$5,'{}',12)", [
      scope.runId,
      session,
      scope.tenantId,
      scope.writerId,
      h,
    ]);
    await c.query("insert into inboxes values($1,$2,4,1,8,$3)", [
      scope.tenantId,
      session,
      JSON.stringify(items),
    ]);
    expect(await reopenRootProjection(c, scope, point, 10)).toMatchObject({
      seq: 9,
      nextId: 20,
      writer: scope.writerId,
      inboxes: { 4: { conversation: 1, value: { items } } },
    });
    for (const [s, p, seq] of [
      [{ ...scope, writerId: old }, point, 10],
      [scope, { ...point, partition: 1 }, 10],
      [scope, { ...point, offset: 14n }, 10],
      [scope, point, 11],
      [{ ...scope, turnId: randomUUID() }, point, 10],
    ]) {
      await expect(reopenRootProjection(c, s, p, seq)).rejects.toThrow(/sealed root|sealed input/);
    }
    await c.query("update runs set mailbox_position=2");
    await expect(reopenRootProjection(c, scope, point, 10)).rejects.toThrow("Missing predecessor");
    await c.query(
      "insert into runs(id,tenant_id,session_id,mailbox_position,history_head,state) values($1,$2,$3,1,$4,'completed')",
      [randomUUID(), scope.tenantId, session, { seq: 5, nextId: 12, floor: 7 }],
    );
    await c.query("insert into inboxes values($1,$2,16,15,9,'[]')", [scope.tenantId, session]);
    expect(await reopenRootProjection(c, scope, point, 10)).toMatchObject({
      floor: 7,
      baseSeq: 5,
      inboxes: { 4: { conversation: 1 }, 16: { conversation: 15 } },
    });
    await c.query("update inboxes set commit_seq=10");
    await expect(reopenRootProjection(c, scope, point, 10)).rejects.toThrow("sealed input");
  } finally {
    await c.end();
  }
});

it("returns only unplaced inputs of the sealed root Run, including retained attachment metadata", async () => {
  const db = createDatabase({
    connectionString: process.env.PI_CLOUD_POSTGRES_INTEGRATION_URL,
    maxConnections: 1,
  });
  const tenant = randomUUID(),
    session = randomUUID(),
    run = randomUUID(),
    stop = randomUUID(),
    workspaceId = randomUUID();
  const ids = Array.from({ length: 4 }, (_, i) => "input-" + String(i + 1).repeat(64));
  const file = {
    id: randomUUID(),
    workspaceId,
    sessionId: session,
    name: "notes.txt",
    path: "/workspace/notes.txt",
    mimeType: "text/plain",
    byteLength: 6,
    sha256: "a".repeat(64),
  };
  const state = {
    runId: run,
    order: [ids[3], ids[0], ids[2], ids[1]],
    receipts: {},
    inputs: Object.fromEntries(
      ids.map((id, i) => [
        id,
        {
          text: "item " + i,
          idempotencyKey: randomUUID(),
          uploads: i === 3 ? [file] : [],
          cancelled: i === 2,
        },
      ]),
    ),
  };
  const outcomes = Object.fromEntries(
    ids.map((id, i) => [
      id,
      i === 0 ? { status: "done", entry: 12 } : { status: "unanswered", reason: "aborted" },
    ]),
  );
  try {
    await db.transaction().execute(async (tx) => {
      await sql
        .raw(
          "create temporary table sessions(id uuid,tenant_id uuid,pi_session_id text,pi_conversation_id bigint,pruned_at timestamptz) on commit drop",
        )
        .execute(tx);
      await sql
        .raw(
          "create temporary table runs(id uuid,tenant_id uuid,session_id uuid,state text,lease_id uuid,output_sealed_at timestamptz,pruned_at timestamptz,root_human_inputs text,root_input_receipts jsonb) on commit drop",
        )
        .execute(tx);
      await sql
        .raw(
          "create temporary table turn_control_requests(id uuid,tenant_id uuid,session_id uuid,target_run_id uuid,kind text,payload jsonb,created_at timestamptz default now()) on commit drop",
        )
        .execute(tx);
      await sql`insert into sessions values(${session}::uuid,${tenant}::uuid,${session},1,null)`.execute(
        tx,
      );
      await sql`insert into runs values(${run}::uuid,${tenant}::uuid,${session}::uuid,'cancelled',${randomUUID()}::uuid,null,null,${JSON.stringify(state)},${JSON.stringify(outcomes)}::jsonb),
      (${randomUUID()}::uuid,${tenant}::uuid,${session}::uuid,'running',${randomUUID()}::uuid,null,null,null,null)`.execute(
        tx,
      );
      await sql`insert into turn_control_requests(id,tenant_id,session_id,target_run_id,kind,payload) values(${stop},${tenant},${session},${run},'cancel','{"inputReturnPending":true}')`.execute(
        tx,
      );
      const read = (page = {}) => readReturnedInputs(tx, tenant, session, page, true);
      expect(await read()).toMatchObject({ pending: true, items: [] });
      await sql`update runs set output_sealed_at=now() where id=${run}`.execute(tx);
      expect((await read()).items.map((i) => [i.inputId, i.uploads, i.mailboxPosition])).toEqual([
        [ids[3], [file], 1],
        [ids[1], [], 4],
      ]);
      await sql`update turn_control_requests set payload='{"inputReturnPending":false}'`.execute(
        tx,
      );
      expect((await read()).requestId).toBeNull();
      expect(
        (await read({ requestId: stop, afterPosition: 1 })).items.map((i) => i.inputId),
      ).toEqual([ids[1]]);
      await expect(readReturnedInputs(tx, randomUUID(), session, {}, true)).rejects.toThrow(
        "not found",
      );
    });
  } finally {
    await db.destroy();
  }
});
