// Fixed prefix control for the usage study. Own the disposable database, warm
// the plans, and compare the two real projection statements on the same bytes.
import assert from "node:assert/strict";
import { randomBytes, randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import pg from "pg";
import { testPostgres } from "../../lib/test-postgres.mjs";
import { installPrefixSql, applyMovingSql, applyBulkUsageSql } from "./ready-projection-sql.mjs";
import { projectionTemplates } from "./projection-statements.mjs";
import { usageTableSql, usageValue } from "./usage-study.mjs";

const sessions = Array.from({ length: 16 }, () => ({
    s: randomUUID(),
    r: randomUUID(),
    w: randomUUID(),
  })),
  tenant = randomUUID();
const prefixes = [];
for (let seq = 1; seq <= 64; seq++) {
  const frames = sessions.map(({ s, r, w }, i) => {
    const response = (seq + i) % 2 === 0,
      offset = (seq - 1) * sessions.length + i;
    const entry = {
      id: seq,
      conversation: 1,
      role: response ? "assistant" : "toolResult",
      ordinal: 0,
      content: randomBytes(3072).toString("base64"),
    };
    const args = [tenant, s, r, seq, JSON.stringify([entry]), w];
    const frame = {
      offset: String(offset),
      history: true,
      ops: [{ kind: "message", count: 1, args }],
    };
    if (response)
      frame.usage = [
        {
          tenant,
          session: s,
          conversation: 1,
          document: 5,
          seq,
          ordinal: 1,
          value: usageValue(Math.ceil(seq / 2)),
        },
      ];
    return frame;
  });
  prefixes.push(["bench", 0, (seq - 1) * 16, seq * 16, JSON.stringify(frames), true, true]);
}
const fixture = await testPostgres({}),
  db = new pg.Client(fixture.url);
async function measure(bulk) {
  await db.query("truncate runs,messages,usage_latest");
  await db.query("update accepted_fact_projection_offsets set next_offset=0");
  for (const { s, r, w } of sessions)
    await db.query(
      "insert into runs(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,model_ref,state,lease_id) values($1,$2,$3,1,$1,'fixture','local','running',$4)",
      [r, tenant, s, w],
    );
  const start = performance.now();
  for (const args of prefixes) {
    const {
      rows: [result],
    } = await db.query(bulk ? applyBulkUsageSql : applyMovingSql, args);
    assert.deepEqual(result, { outcome: "applied", mode: "off" });
  }
  const elapsed = performance.now() - start;
  const { rows } = await db.query("select value,commit_seq::int as seq from usage_latest");
  assert.equal(rows.length, 16);
  for (const r of rows) {
    assert([63, 64].includes(r.seq));
    assert.deepEqual(r.value, usageValue(32));
  }
  assert.equal((await db.query("select count(*)::int as n from messages")).rows[0].n, 1024);
  return elapsed;
}
try {
  await db.connect();
  await db.query(await readFile(new URL("./schema.sql", import.meta.url), "utf8"));
  await installPrefixSql(db, projectionTemplates);
  await db.query(usageTableSql);
  await db.query("insert into accepted_fact_projection_offsets values ('bench',0,0)");
  await measure(false);
  await measure(true);
  const pairs = [];
  for (let i = 0; i < 12; i++) {
    const pair = {};
    for (const bulk of i % 2 ? [true, false] : [false, true])
      pair[bulk ? "bulkMs" : "rowMs"] = await measure(bulk);
    pairs.push(pair);
  }
  console.log(
    JSON.stringify(
      {
        postgres: (await db.query("show server_version")).rows[0].server_version,
        clients: 1,
        framesPerTransaction: 16,
        usageRowsPerTransaction: 8,
        transactionsPerSample: 64,
        entriesPerSample: 1024,
        pairs,
      },
      null,
      2,
    ),
  );
} finally {
  try {
    await db.end();
  } finally {
    await fixture.close();
  }
}
