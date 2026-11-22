// Query the real candidate reader and EXPLAIN the parameterized SQL it issued.
// Disposable PostgreSQL only. Synthetic input, no broker or provider traffic.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { performance } from "node:perf_hooks";
import { Kysely, PostgresDialect } from "kysely";
import pg from "pg";
import {
  readRootMessageWindow,
  rootMessageNavigation,
  rootMessagePosition,
} from "../../../packages/runtime-core/src/root-message-history.ts";

const url = process.env.PI_CLOUD_POSTGRES_INTEGRATION_URL;
assert(url, "Use run-with-test-postgres.mjs");
const name = "read_study_" + randomUUID().replaceAll("-", "");
const admin = new pg.Client({ connectionString: url });
await admin.connect();
const pool = new pg.Pool({ connectionString: url, options: "-c search_path=" + name, max: 3 });
let capture;
const db = new Kysely({
  dialect: new PostgresDialect({ pool }),
  log: (event) => {
    if (capture && event.level === "query") capture.push(event.query);
  },
});
const tenant = randomUUID(),
  root = randomUUID(),
  child = randomUUID(),
  rootTurn = randomUUID(),
  childTurn = randomUUID(),
  run = randomUUID();
const rounds = 31;
const stats = (n) => {
  const a = n.toSorted((x, y) => x - y);
  const q = (f) => +a[Math.ceil(f * a.length) - 1].toFixed(2);
  return { p50: q(0.5), p95: q(0.95) };
};
function scans(plan, out = []) {
  if (plan["Relation Name"] === "messages")
    out.push({
      type: plan["Node Type"],
      index: plan["Index Name"],
      rows: plan["Actual Rows"],
      loops: plan["Actual Loops"],
      removed: plan["Rows Removed by Filter"] ?? 0,
      blocks: (plan["Shared Hit Blocks"] ?? 0) + (plan["Shared Read Blocks"] ?? 0),
      indexCondition: plan["Index Cond"]
        ?.replaceAll(/[a-f0-9]{8}-[a-f0-9-]{27}/gi, "uuid")
        .slice(0, 500),
    });
  for (const n of plan.Plans ?? []) scans(n, out);
  return out;
}
async function measure(action, verify) {
  capture = [];
  const result = await action();
  const sqls = capture;
  capture = undefined;
  verify(result);
  const plans = [];
  for (const q of sqls) {
    const { rows } = await pool.query("explain (analyze,buffers,format json) " + q.sql, [
      ...q.parameters,
    ]);
    const p = rows[0]["QUERY PLAN"][0];
    plans.push({
      ms: p["Execution Time"],
      blocks: p.Plan["Shared Hit Blocks"] + p.Plan["Shared Read Blocks"],
      scans: scans(p.Plan),
    });
  }
  const times = [];
  for (let i = 0; i < rounds + 3; i++) {
    const t = performance.now();
    const v = await action();
    const dt = performance.now() - t;
    verify(v);
    if (i >= 3) times.push(dt);
  }
  return { ms: stats(times), sqlCount: sqls.length, plans };
}
try {
  await admin.query("create schema " + name);
  await pool.query(await readFile(new URL("./schema.sql", import.meta.url), "utf8"));
  await pool.query(`create table sessions(tenant_id uuid,id uuid primary key,pi_session_id text,pi_conversation_id bigint,pruned_at timestamptz);
    create table session_terminal_events(tenant_id uuid,session_id uuid,turn_id uuid,seq bigint,type text,payload jsonb,occurred_at timestamptz,primary key(tenant_id,session_id,turn_id,seq));
    create table subagent_conversations(tenant_id uuid,root_run_id uuid,root_session_id uuid,child_session_id uuid unique,child_turn_id uuid unique,created_at timestamptz default now(),output_display_seq bigint);
    alter table runs add turn_id uuid,add pruned_at timestamptz,add input_text text,add input_uploads jsonb default '[]',
      add projection_tools jsonb default '{}',add output_display_seq bigint default 12,add root_input_receipts jsonb,add root_human_inputs text;
    create unique index study_turn on runs(tenant_id,session_id,turn_id)`);
  await pool.query(
    "insert into sessions values($1,$2,$2::uuid::text,1,null),($1,$3,$2::uuid::text,2,null)",
    [tenant, root, child],
  );
  await pool.query(
    "insert into runs(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,model_ref,state,turn_id,input_text) values($1,$2,$3,1,$1,'initial','fixed','completed',$4,'initial')",
    [run, tenant, root, rootTurn],
  );
  await pool.query(
    "insert into subagent_conversations(tenant_id,root_run_id,root_session_id,child_session_id,child_turn_id,output_display_seq) values($1,$2,$3,$4,$5,12)",
    [tenant, run, root, child, childTurn],
  );
  const reports = [];
  const lane = (id) => (id % 128 === 0 ? 2 : id % 2 === 1 ? 1 : 3 + (Math.floor(id / 2) % 30));
  let old = 0;
  for (const size of [8192, 131072]) {
    await pool.query(
      `insert into messages(tenant_id,pi_session_id,entry_id,conversation_id,run_id,role,content,navigation_caption,commit_seq,ordinal)
      select $1,$2,n,case when n%128=0 then 2 when n%2=1 then 1 else 3+(n/2)%30 end,$3,'assistant',
      json_build_object('id',n,'kind','pi.assistant','model',json_build_array(json_build_object('role','assistant','content',json_build_array(json_build_object('type','text','text',n::text||' '||repeat(md5(n::text),24))))))::text,n::text,n,0
      from generate_series($4::int,$5::int) n`,
      [tenant, root, run, old + 1, size],
    );
    old = size;
    await pool.query("vacuum analyze messages");
    const arms = {};
    for (const childView of [false, true]) {
      const sessionId = childView ? child : root,
        turnId = childView ? childTurn : rootTurn,
        l = childView ? 2 : 1;
      const turn = { turnId, originSessionId: sessionId, piSessionId: root, operationKind: "chat" };
      const ids = Array.from({ length: size }, (_, i) => i + 1).filter((n) => lane(n) === l);
      for (const [scenario, direction, cut] of [
        ["latest", "before", null],
        ["older", "before", size / 2],
        ["newer", "after", size / 2],
      ]) {
        const input = {
          tenantId: tenant,
          sessionId,
          turns: [turn],
          direction,
          maxRecords: 32,
          ...(cut === null
            ? {}
            : {
                position: {
                  version: 2,
                  sessionId,
                  storageId: root,
                  originSessionId: sessionId,
                  turnId,
                  sequence: cut,
                  ordinal: 0,
                },
              }),
        };
        const eligible = ids.filter(
          (n) => cut === null || (direction === "before" ? n < cut : n > cut),
        );
        const expected = direction === "before" ? eligible.slice(-32) : eligible.slice(0, 32);
        arms[(childView ? "child" : "root") + "-" + scenario] = await measure(
          () => readRootMessageWindow(db, input, childView),
          (page) => {
            assert.deepEqual(
              page.transcripts.get(turnId).items.map((i) => Number(i.text.split(" ")[0])),
              expected,
            );
          },
        );
      }
      if (size === 131072) {
        const mid = size / 2;
        const input = {
          tenantId: tenant,
          sessionId,
          turns: [turn],
          direction: "before",
          position: { turnId, sequence: mid, ordinal: 0 },
        };
        const expectedIds = ids.filter((i) => i < mid).slice(-64);
        arms[(childView ? "child" : "root") + "-default64-older"] = await measure(
          () => readRootMessageWindow(db, input, childView),
          (v) =>
            assert.deepEqual(
              v.transcripts.get(turnId).items.map((i) => Number(i.text.split(" ")[0])),
              expectedIds,
            ),
        );
      }
      const last = ids.at(-1);
      arms[(childView ? "child" : "root") + "-entry-anchor"] = await measure(
        () =>
          rootMessagePosition(
            db,
            tenant,
            { run_id: run, turn_id: turnId, origin_session_id: sessionId, pi_session_id: root },
            "entry",
            String(last),
            childView,
          ),
        (r) => assert.equal(Number(r.seq), last),
      );
    }
    arms.navigation = await measure(
      () =>
        rootMessageNavigation(db, {
          tenantId: tenant,
          sessionId: root,
          conversationId: 2,
          limit: 32,
          through: size / 2,
          includeIds: [size],
        }),
      (v) => assert.equal(v.entries.at(-1).id, size),
    );
    // Locate a completed task by its public task ID, independent of the Entry cursor.
    for (const [childView, entryId] of [
      [false, size / 2 + 1],
      [true, size / 2],
    ]) {
      const dec = childView ? 128 : 2,
        proposed = entryId - dec,
        taskId = size * 2 + (childView ? 2 : 1),
        call = "call-" + taskId;
      const proposal = {
        id: proposed,
        conversationId: lane(entryId),
        kind: "pi.assistant",
        model: [
          {
            role: "assistant",
            stopReason: "toolUse",
            content: [{ type: "toolCall", id: call, name: "bash", arguments: { command: "pwd" } }],
          },
        ],
      };
      const result = {
        id: entryId,
        conversationId: lane(entryId),
        kind: "pi.tool-result",
        byTaskId: taskId,
        model: [
          {
            role: "toolResult",
            toolName: "bash",
            toolCallId: call,
            content: [{ type: "text", text: "/workspace" }],
          },
        ],
      };
      await pool.query("update messages set content=$2 where entry_id=$1", [
        proposed,
        JSON.stringify(proposal),
      ]);
      await pool.query(
        "update messages set content=$2,role='toolResult',tool_task_id=$3,assistant_entry_id=$4 where entry_id=$1",
        [entryId, JSON.stringify(result), taskId, proposed],
      );
      const row = {
        run_id: run,
        pi_session_id: root,
        turn_id: childView ? childTurn : rootTurn,
        origin_session_id: childView ? child : root,
      };
      arms[(childView ? "child" : "root") + "-tool-anchor"] = await measure(
        () => rootMessagePosition(db, tenant, row, "task", String(taskId), childView),
        (v) => assert.equal(Number(v.seq), entryId),
      );
    }
    if (size === 131072) {
      // Exercise the outer Turn ordering and the unconstrained non-anchor turns.
      const turns = [];
      for (let i = 1; i <= 40; i++) {
        const id = randomUUID(),
          turn = randomUUID();
        await pool.query(
          "insert into runs(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,model_ref,state,turn_id,input_text) values($1,$2,$3,$4,$1,'new','fixed','completed',$5,'new')",
          [id, tenant, root, i + 1, turn],
        );
        for (let j = 0; j < 3; j++) {
          const entry = size + 4 * i + j;
          const content = {
            id: entry,
            kind: "pi.assistant",
            model: [{ role: "assistant", content: [{ type: "text", text: String(entry) }] }],
          };
          await pool.query(
            "insert into messages(tenant_id,pi_session_id,conversation_id,run_id,entry_id,commit_seq,ordinal,role,content) values($1,$2,1,$3,$4,$5,$6,'assistant',$7)",
            [tenant, root, id, entry, size + i, j, JSON.stringify(content)],
          );
        }
        turns.push({
          turnId: turn,
          originSessionId: root,
          piSessionId: root,
          operationKind: "chat",
          number: i,
        });
      }
      for (const direction of ["before", "after"])
        for (const inclusive of [false, true]) {
          const ts = direction === "before" ? turns.toReversed() : turns,
            anchor = ts[0];
          const input = {
            tenantId: tenant,
            sessionId: root,
            turns: ts,
            direction,
            inclusive,
            maxRecords: 32,
            position: { turnId: anchor.turnId, sequence: size + anchor.number, ordinal: 1 },
          };
          const expected = ts
            .flatMap((t) => {
              const coordinates = [
                [-1, 0, 0],
                ...[0, 1, 2].map((j) => [size + 4 * t.number + j, size + t.number, j]),
              ];
              return (direction === "before" ? coordinates.toReversed() : coordinates)
                .filter(
                  ([, seq, ord]) =>
                    t.turnId !== anchor.turnId ||
                    (direction === "before"
                      ? seq < input.position.sequence ||
                        (seq === input.position.sequence && (inclusive ? ord <= 1 : ord < 1))
                      : seq > input.position.sequence ||
                        (seq === input.position.sequence && (inclusive ? ord >= 1 : ord > 1))),
                )
                .map(([, seq, ord]) => [t.turnId, seq, ord]);
            })
            .slice(0, 32);
          if (direction === "before") expected.reverse();
          arms["40-turns-" + direction + "-" + (inclusive ? "inclusive" : "exclusive")] =
            await measure(
              () => readRootMessageWindow(db, input),
              (v) =>
                assert.deepEqual(
                  v.positions.map((p) => [p.turnId, p.sequence, p.ordinal]),
                  expected,
                ),
            );
        }
    }
    reports.push({ entries: size, arms });
  }
  const ver = await pool.query("show server_version");
  console.log(
    JSON.stringify(
      {
        pg: ver.rows[0].server_version,
        rounds,
        mode: "warm sequential, one physical Session and Run, 32 records per page, child every 128th entry",
        reports,
      },
      null,
      2,
    ),
  );
} finally {
  try {
    await db.destroy();
    await admin.query("drop schema if exists " + name + " cascade");
  } finally {
    await admin.end();
  }
}
