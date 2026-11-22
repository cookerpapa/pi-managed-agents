// Measure the part obscured by Kafka and Tool acknowledgement: append the same
// staged native-shaped history to a populated PG table on one backend.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import pg from "pg";
import { testPostgres } from "../../lib/test-postgres.mjs";
import { nativeBashTrace } from "./native-bash-trace.mjs";
import { studyEntryIndexSql } from "./entry-index.mjs";

const fixture = await testPostgres({}); // CHECKPOINT and WAL readings need an exclusive PG instance.
const db = new pg.Client({ connectionString: fixture.url });
try {
  await db.connect();
  const one = async (sql, args) => (await db.query(sql, args)).rows[0];
  await db.query(await readFile(new URL("./schema.sql", import.meta.url), "utf8"));
  const tenant = randomUUID(),
    scope = { runId: randomUUID() };
  const trace = await nativeBashTrace(scope, undefined, {
    callsPerRun: 20,
    callIdAt: (i) => "call-" + i,
  });
  const entries = trace.flatMap((c) =>
    c.writes.filter((w) => w.type === "entry").map((w) => w.value),
  );
  assert.equal(entries.length, 43);
  assert.equal(entries.filter((e) => e.kind === "pi.tool-result").length, 20);

  // Eight physical Sessions, 128 native conversations each. The sequence is
  // interleaved across all lanes. Input preparation is outside the timer.
  await db.query(
    `insert into runs(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,model_ref,state)
    select gen_random_uuid(),$1,gen_random_uuid(),1,gen_random_uuid(),'fixed','fixed','completed' from generate_series(1,8)`,
    [tenant],
  );
  await db.query(
    `create temp table input as with positions as (
    select epoch,lane,r.tenant_id,r.session_id,r.id as run_id, v.body, v.ord,
      (epoch*43+v.ord-1)*128+lane as pos
      from generate_series(0,3) epoch cross join runs r cross join generate_series(1,128) lane
      cross join jsonb_array_elements($1::jsonb) with ordinality v(body,ord))
    select epoch,tenant_id,session_id as pi_session_id,pos as entry_id,lane as conversation_id,run_id,
      coalesce(body#>>'{model,0,role}',body->>'kind') as role,
      (body||jsonb_build_object('id',pos,'conversationId',lane))::text as content,
      case when body->>'kind'='pi.user' then epoch::text end as source_input_id,
      pos as commit_seq,0::int as ordinal from positions`,
    [JSON.stringify(entries)],
  );
  await db.query("create index input_order on input(epoch,entry_id,run_id); analyze input");
  const columns =
    "tenant_id,pi_session_id,entry_id,conversation_id,run_id,role,content,source_input_id,commit_seq,ordinal";
  const append = (which) =>
    `insert into messages(${columns}) select ${columns} from input where epoch ${which} order by epoch,entry_id,run_id`;
  const rows = 43 * 128 * 8,
    trials = [];
  const ordered = [false, true, false, true, true, false, true, false, false, true];
  for (const [i, index] of ordered.entries()) {
    await db.query("truncate messages");
    await db.query("drop index if exists messages_conversation_entry");
    if (index) await db.query(studyEntryIndexSql);
    assert.equal((await db.query(append("<3"))).rowCount, 3 * rows);
    await db.query("analyze messages");
    await db.query("checkpoint");
    const { lsn } = await one("select pg_current_wal_insert_lsn() as lsn");
    const plan = (await one("explain(analyze,buffers,wal,timing off,format json) " + append("=3")))[
      "QUERY PLAN"
    ][0];
    const facts = await one(
      `select pg_wal_lsn_diff(pg_current_wal_insert_lsn(),$1::pg_lsn)::float8 as "walBytes",
      coalesce(pg_relation_size(to_regclass('messages_conversation_entry')),0)::float8 as "entryIndexBytes",
      pg_indexes_size('messages')::float8 as "allMessageIndexBytes", pg_table_size('messages')::float8 as "messageTableBytes"`,
      [lsn],
    );
    assert.equal((await one("select count(*)::int n from messages")).n, 4 * rows);
    assert.equal(plan.Plan.Plans[0]["Actual Rows"], rows);
    if (i >= 2) trials.push({ index, ms: plan["Execution Time"], ...facts });
  }
  console.log(
    JSON.stringify({
      seedRows: 3 * rows,
      appendedRows: rows,
      physicalSessions: 8,
      conversationsPerSession: 128,
      pg: (await one("show server_version")).server_version,
      trials,
    }),
  );
} finally {
  try {
    await db.end();
  } finally {
    await fixture.close();
  }
}
