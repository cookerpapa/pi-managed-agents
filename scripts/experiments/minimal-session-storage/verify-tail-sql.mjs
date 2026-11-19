import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import pg from "pg";
import { openingHead } from "./session-prefix.mjs";
import { runtimeDirectory, serviceAddress } from "../../lib/production-kubernetes.mjs";

assert.equal(process.env.PI_CLOUD_RUNTIME_LOAD_CHECK, "1");
const [traceFile, sqlFile] = process.argv.slice(2);
const traced = JSON.parse(await readFile(traceFile, "utf8")).tests.find(
  (t) => t.tail && t.sink === "single",
);
const actual = { ...traced.tail.templates, open: openingHead };
const source = (await readFile(sqlFile, "utf8")).replace(/^--.*$/gm, "");
const approved = new Map();
for (const m of source.matchAll(/PREPARE pma_(\w+)\s*\([^;]*?\) AS\s+([^;]+);/gi))
  approved.set(m[1], m[2]);
assert.equal(approved.size, 4);
const normalize = (q) => q.toLowerCase().replace(/\s/g, "");
for (const [name, text] of approved)
  assert.equal(normalize(text), normalize(actual[name]), name + " differs from trace/source");
const url = new URL((await readFile(runtimeDirectory + "/secrets/database-url", "utf8")).trim());
url.hostname = serviceAddress("postgres");
const db = new pg.Client({
  connectionString: url.toString(),
  application_name: "prototype-sql-plan-check",
});
const [tenant, session, run, lease, request] = Array.from({ length: 5 }, () => randomUUID());
const message = [
  tenant,
  session,
  run,
  3,
  JSON.stringify([
    {
      id: 11,
      conversation: 1,
      role: "assistant",
      content: "x".repeat(4096),
      input: null,
      ordinal: 0,
    },
  ]),
  lease,
];
const inbox = [tenant, session, 3, 1, 0, "[]", run, lease],
  finish = [
    tenant,
    session,
    run,
    lease,
    JSON.stringify({ version: randomUUID(), seq: 6, nextId: 12, floor: 1, count: 1, bytes: 4096 }),
  ];
await db.connect();
try {
  await db.query("begin");
  await db.query("set local search_path=pg_temp,pg_catalog");
  const schema = await readFile(new URL("./schema.sql", import.meta.url), "utf8");
  await db.query(schema.replace(/^create table /gm, "create temporary table "));
  const temp = await db.query(
    "select count(*)::int n from pg_class where relnamespace=pg_my_temp_schema() and relname=any($1::text[])",
    [["runs", "messages", "inboxes"]],
  );
  assert.equal(temp.rows[0].n, 3);
  await db.query(
    `insert into runs(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,model_ref,state)
   select gen_random_uuid(),$1,gen_random_uuid(),1,gen_random_uuid(),'fixture','none',case when i<=256 then 'running' else 'completed' end from generate_series(1,8192)i`,
    [tenant],
  );
  await db.query("insert into inboxes select tenant_id,session_id,1,3,1,0,'[]'::jsonb from runs");
  await db.query(
    "insert into messages select tenant_id,session_id,i,1,id,'user','fixture',null,i,0 from runs cross join generate_series(4,5)i",
  );
  await db.query(
    "insert into runs(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,model_ref,state,lease_id) values($1,$2,$3,1,$4,'fixture','none','running',$5)",
    [run, tenant, session, request, lease],
  );
  await db.query("analyze runs; analyze messages; analyze inboxes");
  await db.query(source);
  const call = (name, args) => db.query(approved.get(name), args);
  async function plan(name, args) {
    await db.query("savepoint before_plan");
    try {
      const [p] = (
        await db.query("explain (analyze, buffers, wal, format json) " + approved.get(name), args)
      ).rows[0]["QUERY PLAN"];
      const nodes = [];
      function visit(n) {
        nodes.push(
          [
            n["Node Type"],
            n["Relation Name"],
            n["Index Name"],
            n["Conflict Arbiter Indexes"],
          ].filter(Boolean),
        );
        for (const c of n.Plans ?? []) visit(c);
      }
      visit(p.Plan);
      return { name, nodes, planningMs: p["Planning Time"], executionMs: p["Execution Time"] };
    } finally {
      await db.query("rollback to savepoint before_plan");
    }
  }
  const plans = [];
  for (const [name, args] of [
    ["inbox", inbox],
    ["message", message],
    ["finish", finish],
  ])
    plans.push(await plan(name, args));
  assert.deepEqual((await call("inbox", inbox)).rows, [{ accepted: true, writes: 1 }]);
  assert.deepEqual((await call("inbox", inbox)).rows, [{ accepted: true, writes: 0 }]);
  assert.deepEqual((await call("message", message)).rows, [{ accepted: true, writes: 1 }]);
  assert.deepEqual((await call("message", message)).rows, [{ accepted: true, writes: 1 }]);
  const changed = [...message];
  changed[4] = changed[4].replace("xxxx", "yyyy");
  assert.deepEqual((await call("message", changed)).rows, [{ accepted: true, writes: 0 }]);
  assert.equal((await call("finish", finish)).rowCount, 1);
  assert.equal((await call("finish", finish)).rowCount, 0);
  assert.deepEqual((await call("message", message)).rows, [{ accepted: false, writes: 0 }]);
  const next = randomUUID();
  await db.query(
    "insert into runs(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,model_ref,state,lease_id) values ($1,$2,$3,2,$1,'next','none','running',$4)",
    [next, tenant, session, randomUUID()],
  );
  plans.push(await plan("open", [next, tenant, session]));
  const {
    rows: [head],
  } = await call("open", [next, tenant, session]);
  assert.deepEqual(head.prior_head, JSON.parse(finish[4]));
  assert.deepEqual(head.inbox_items, []);
  console.log(JSON.stringify({ plans, exactTemplatesVerified: approved.size }, null, 2));
} finally {
  try {
    await db.query("rollback");
  } finally {
    await db.end();
  }
}
