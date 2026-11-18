/** Measure two physical read selectors with every other owned-record index present. */
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile, writeFile } from "node:fs/promises";
import pg from "pg";
import { runtimeDirectory, serviceAddress } from "../../lib/production-kubernetes.mjs";
assert.equal(process.env.PI_CLOUD_PROJECTOR_SQL_DIAGNOSTIC, "1");
assert(process.argv[2]);
const url = new URL((await readFile(runtimeDirectory + "/secrets/database-url", "utf8")).trim());
url.hostname = serviceAddress("postgres");
const client = new pg.Client({
  connectionString: url.toString(),
  application_name: "pma-projection-read-cost",
});
const tenant = randomUUID(),
  session = randomUUID(),
  count = 100000;
const names = ["pi_durable_submission_request", "pi_durable_pending_tool_reply"];
function accesses(node) {
  return [node, ...(node.Plans ?? []).flatMap(accesses)];
}
const samples = [];
await client.connect();
try {
  await client.query("begin");
  await client.query("set local search_path=pg_temp,public");
  await client.query(
    "create temp table projection_read_fixture (like public.pi_durable_owned_records including defaults including constraints) on commit drop",
  );
  const definitions = (
    await client.query(
      `select c.relname as name,pg_get_indexdef(i.indexrelid) as definition from pg_index i join pg_class c on c.oid=i.indexrelid where i.indrelid='public.pi_durable_owned_records'::regclass order by c.relname`,
    )
  ).rows;
  async function install(index) {
    const from = " ON public.pi_durable_owned_records ";
    assert(index.definition.includes(from));
    await client.query(index.definition.replace(from, " ON pg_temp.projection_read_fixture "));
  }
  for (const index of definitions) await install(index);
  await client.query(
    `insert into pg_temp.projection_read_fixture(tenant_id,session_id,id,kind,seq,ordinal,fields,created_seq)
   select $1,$2,n,case n%10 when 0 then 'task' when 1 then 'submission' else 'entry' end,n,0,
   case n%10 when 0 then jsonb_build_object('conversation','1','status',case when n%100=0 then 'running' else 'terminal' end,'nativeKind','"pi.tool"')||
      case when n%100=0 then jsonb_build_object('replyTopic','topic-'||((n/100)%100),'replyOffset',n::text) else '{}'::jsonb end
   when 1 then jsonb_build_object('conversation','1','status','done','request',to_jsonb('request-'||n)::text)
   else jsonb_build_object('conversation','1','navigation','assistant','role','assistant') end,1 from generate_series(1,$3::int) n`,
    [tenant, session, count],
  );
  await client.query("analyze pg_temp.projection_read_fixture");
  const queries = [
    {
      text: "select id from pg_temp.projection_read_fixture where tenant_id=$1::uuid and session_id=$2 and kind='submission' and fields->>'conversation'=$3 and fields->>'request'=$4 order by id limit 1",
      values: [tenant, session, "1", JSON.stringify("request-99991")],
    },
    {
      text: `select fields->>'replyTopic' as topic,min((fields->>'replyOffset')::bigint)::text as floor from pg_temp.projection_read_fixture where kind='task' and fields ? 'replyTopic' and fields->>'status'<>'terminal' and fields->>'replyTopic'=any($1::text[]) group by fields->>'replyTopic'`,
      values: [["topic-99"]],
    },
  ];
  for (const [i, query] of queries.entries()) {
    const name = names[i],
      definition = definitions.find((x) => x.name === name);
    const expected = (await client.query(query)).rows;
    assert.deepEqual(
      expected,
      i === 0 ? [{ id: "99991" }] : [{ topic: "topic-99", floor: "9900" }],
    );
    for (const phase of ["present", "removed", "restored"]) {
      if (phase === "removed") await client.query("drop index pg_temp." + name);
      if (phase === "restored") await install(definition);
      assert.deepEqual((await client.query(query)).rows, expected);
      let plan;
      const elapsed = [];
      for (let repeat = 0; repeat < 18; repeat++) {
        const r = await client.query({
          ...query,
          text: "explain (analyze,buffers,timing off,format json) " + query.text,
        });
        plan = r.rows[0]["QUERY PLAN"][0];
        if (repeat >= 3) elapsed.push(plan["Execution Time"]);
      }
      elapsed.sort((a, b) => a - b);
      samples.push({
        index: name,
        phase,
        medianMs: elapsed[7],
        indexNames: accesses(plan.Plan)
          .map((x) => x["Index Name"])
          .filter(Boolean),
        rowsRemoved: accesses(plan.Plan).reduce(
          (n, x) => n + (x["Rows Removed by Filter"] ?? 0) * x["Actual Loops"],
          0,
        ),
        localBlocks: plan.Plan["Local Hit Blocks"] + plan.Plan["Local Read Blocks"],
      });
    }
  }
} finally {
  try {
    await client.query("rollback");
  } finally {
    await client.end();
  }
}
await writeFile(process.argv[2], JSON.stringify({ rows: count, samples }, null, 2) + "\n");
