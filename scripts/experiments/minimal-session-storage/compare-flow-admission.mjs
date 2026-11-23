// Paired, single-host admission experiment. A block runs on exactly one schema at a time.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import pg from "pg";
import { flowPage, installFlowHistory } from "./flow-history.mjs";

const url = process.env.PI_CLOUD_POSTGRES_INTEGRATION_URL;
assert(url && new URL(url).hostname === "127.0.0.1", "Use the isolated PostgreSQL runner");
const pageDiagnostic = process.argv.includes("--page-plans");
const layouts = [],
  tenant = randomUUID(),
  workspace = randomUUID();
const window = () => ({ id: randomUUID(), flow: randomUUID() });
const long = window(),
  hot = window();
const separate = Array.from({ length: 8 }, window);
const first = Array.from({ length: 1024 }, window);
const parent = randomUUID();
const windows = [long, hot, ...separate, ...first];
const queries = {
  admit: "select flow_admit($1,$2,$3,$4,'shared admission benchmark') id",
  latest: "select s.tip_run_id id from flow_session_tips s where tenant_id=$1 and id=$2",
};
const percentile = (xs, p) => xs.toSorted((a, b) => a - b)[Math.ceil(xs.length * p) - 1];
const round = (n) => Math.round(n * 1000) / 1000;
const distribution = (xs) => ({
  p50: round(percentile(xs, 0.5)),
  p95: round(percentile(xs, 0.95)),
  p99: round(percentile(xs, 0.99)),
});
const schedule = [0, 1, 1, 0, 1, 0, 0, 1];
const blockSize = 32,
  cycles = 8,
  clients = 8;
const requests = Array.from({ length: (blockSize * cycles * schedule.length) / 2 }, () =>
  randomUUID(),
);
assert.equal(requests.length, first.length);

async function setup(arm) {
  const { pool, schema, anchor } = arm;
  await pool.query("create schema " + schema);
  await pool.query(await readFile(new URL("./schema.sql", import.meta.url), "utf8"));
  await installFlowHistory(pool, false, false, anchor);
  // Disable table autovacuum in the disposable schemas to avoid interleaved maintenance WAL.
  for (const table of ["sessions", "runs", "messages"])
    await pool.query("alter table " + table + " set(autovacuum_enabled=false)");
  await pool.query(
    `insert into runs(id,tenant_id,flow_id,prev_run_id,mailbox_position,initial_input,model_ref,idempotency_key,state,history_head)
 select case when n=100000 then $3::uuid else md5($1::text||n)::uuid end,$2::uuid,$1::uuid,
 case when n>1 then md5($1::text||(n-1))::uuid end,n,'seed','fixture',md5($1::text||n)::uuid,'completed','{"nextId":100001,"floor":1}'
 from generate_series(1,100000)n`,
    [long.flow, tenant, parent],
  );
  const column = anchor === "base" ? "base_run_id" : "head_run_id";
  await pool.query(
    `insert into sessions(id,tenant_id,flow_id,${column},workspace_id)
 select id,$1::uuid,flow,case when $5::boolean and id=$3::uuid then null else $4::uuid end,$2::uuid
 from jsonb_to_recordset($6::jsonb) as v(id uuid,flow uuid)`,
    [tenant, workspace, long.id, parent, anchor === "base", JSON.stringify(windows)],
  );
  // The latest 24 previews are real payloads in both schemas.
  await pool.query(
    `insert into messages(tenant_id,pi_session_id,entry_id,conversation_id,run_id,role,content,commit_seq,ordinal)
 select $1,$2,r.mailbox_position,1,r.id,'assistant',repeat('abcd',256),r.mailbox_position,0 from runs r
 where r.flow_id=$2 and r.mailbox_position>99976`,
    [tenant, long.flow],
  );
  await pool.query("analyze messages");
  await pool.query("analyze sessions");
  await pool.query("analyze runs");
}

async function stats(arm) {
  const connections = await Promise.all(Array.from({ length: clients }, () => arm.pool.connect()));
  try {
    await Promise.all(connections.map((c) => c.query("select pg_stat_force_next_flush()")));
  } finally {
    for (const c of connections) c.release();
  }
  const {
    rows: [s],
  } = await arm.pool.query(
    `select sum(n_tup_ins) filter(where relname='runs')::int inserts,
 sum(n_tup_upd) filter(where relname='sessions')::int updates from pg_stat_user_tables where schemaname=$1`,
    [arm.schema],
  );
  return s;
}
async function verifyTipPlan(arm, target, expectedRows) {
  const {
    rows: [result],
  } = await arm.pool.query("explain (analyze,buffers,format json) " + queries.latest, [
    tenant,
    target.id,
  ]);
  function nodes(plan) {
    return [plan, ...(plan.Plans ?? []).flatMap(nodes)];
  }
  const plan = nodes(result["QUERY PLAN"][0].Plan);
  const lookup = plan.find((n) => n["Relation Name"] === "runs");
  assert(lookup);
  assert.equal(lookup["Scan Direction"], "Backward");
  assert.equal(lookup["Actual Rows"], expectedRows);
  assert.equal(lookup["Actual Loops"], 1);
  assert(!plan.some((n) => n["Node Type"] === "Seq Scan" || n["Node Type"] === "Sort"));
  console.log(
    JSON.stringify({
      tipPlan: expectedRows ? "long" : "empty",
      index: lookup["Index Name"],
      direction: lookup["Scan Direction"],
      rows: expectedRows,
    }),
  );
}
// The disposable Linux PG fixture exposes its two-CPU cgroup counters.
async function cpuCounters(pool) {
  const {
    rows: [{ body }],
  } = await pool.query("select pg_read_file('/sys/fs/cgroup/cpu.stat') body");
  const counters = Object.fromEntries(
    body
      .trim()
      .split("\n")
      .map((line) => line.split(" ")),
  );
  return { cpuUs: Number(counters.usage_usec), throttledPeriods: Number(counters.nr_throttled) };
}
async function study(name, targets, parallel, operation = "admit") {
  const readOnly = operation !== "admit";
  const page = operation === "page" || operation === "preparedPage";
  function pageQuery(target) {
    const statement = { text: flowPage, values: [tenant, target.id, 24] };
    if (operation === "preparedPage") statement.name = "latest_24_previews";
    return statement;
  }
  const results = layouts.map(() => ({
    latencies: [],
    ms: 0,
    wal: 0,
    blocks: 0,
    offset: 0,
    cpuUs: 0,
    throttledPeriods: 0,
  }));
  const before = await Promise.all(layouts.map(stats));
  for (let cycle = 0; cycle < cycles; cycle++)
    for (const index of schedule) {
      const arm = layouts[index],
        result = results[index],
        pool = arm.pool;
      const {
        rows: [{ lsn }],
      } = await pool.query("select pg_current_wal_insert_lsn()::text lsn");
      const cpuBefore = pageDiagnostic ? await cpuCounters(pool) : undefined;
      const begin = performance.now();
      let cursor = result.offset;
      const end = cursor + blockSize;
      const latencies = [];
      await Promise.all(
        Array.from({ length: parallel }, async () => {
          while (cursor < end) {
            const i = cursor++,
              target = targets[i % targets.length],
              start = performance.now();
            const { rows } = page
              ? await pool.query(pageQuery(target))
              : readOnly
                ? await pool.query(queries.latest, [tenant, target.id])
                : await pool.query(queries.admit, [tenant, target.id, target.flow, requests[i]]);
            latencies.push(performance.now() - start);
            assert.equal(rows[0].id, readOnly ? parent : requests[i]);
            if (page) {
              assert.equal(rows.length, 24);
              assert.equal(rows[23].content.length, 1024);
            }
          }
        }),
      );
      result.ms += performance.now() - begin;
      if (cpuBefore) {
        const after = await cpuCounters(pool);
        result.cpuUs += after.cpuUs - cpuBefore.cpuUs;
        result.throttledPeriods += after.throttledPeriods - cpuBefore.throttledPeriods;
      }
      result.latencies.push(...latencies);
      result.offset = end;
      result.blocks++;
      result.wal += Number(
        (
          await pool.query("select pg_wal_lsn_diff(pg_current_wal_insert_lsn(),$1::pg_lsn) bytes", [
            lsn,
          ])
        ).rows[0].bytes,
      );
    }
  for (const [i, arm] of layouts.entries()) {
    const now = await stats(arm),
      res = results[i],
      count = res.latencies.length;
    const sessionUpdates = now.updates - before[i].updates,
      runInserts = now.inserts - before[i].inserts;
    assert.equal(count, requests.length);
    assert.equal(runInserts, readOnly ? 0 : count);
    assert.equal(sessionUpdates, readOnly || arm.anchor === "base" ? 0 : count);
    if (!readOnly) {
      // Check every admitted link in the actual serialized order, including the first in each branch.
      const {
        rows: [check],
      } = await arm.pool.query(
        `with ordered as (
       select id,flow_id,mailbox_position,prev_run_id,
       lag(id) over (partition by flow_id order by mailbox_position) as prior
       from runs where tenant_id=$1 and flow_id=any($2::uuid[]) and mailbox_position is not null
     ) select count(*)::int n,count(*) filter(where prev_run_id is distinct from coalesce(prior,$3::uuid))::int bad
       from ordered where id=any($4::uuid[])`,
        [tenant, targets.map((s) => s.flow), parent, requests],
      );
      assert.equal(check.n, count);
      assert.equal(check.bad, 0);
    }
    console.log(
      JSON.stringify({
        name,
        anchor: arm.anchor,
        parallel,
        count,
        blocks: res.blocks,
        latencyMs: distribution(res.latencies),
        operationsPerSecond: round((count * 1000) / res.ms),
        ...(pageDiagnostic
          ? { cpuUsPerRead: round(res.cpuUs / count), throttledPeriods: res.throttledPeriods }
          : {}),
        walBytes: res.wal,
        walBytesPerOperation: round(res.wal / count),
        sessionUpdates,
        runInserts,
      }),
    );
  }
  // Each scenario needs a fresh set of admission IDs. Both arms receive the same ones.
  for (let i = 0; i < requests.length; i++) requests[i] = randomUUID();
}

try {
  const suffix = randomUUID().replaceAll("-", "");
  for (const anchor of ["head", "base"]) {
    const schema = "tip_" + anchor + "_" + suffix;
    const arm = {
      anchor,
      schema,
      pool: new pg.Pool({
        connectionString: url,
        options: "-c search_path=" + schema,
        max: clients,
      }),
    };
    layouts.push(arm);
    await setup(arm);
  }
  const config = (
    await layouts[0].pool.query(
      "select current_setting('server_version') pg,current_setting('fsync') fsync,current_setting('synchronous_commit') sync",
    )
  ).rows[0];
  assert.equal(config.fsync, "on");
  assert.equal(config.sync, "on");
  console.log(JSON.stringify({ config, historyRuns: 100000, blockSize, cycles, schedule }));
  await layouts[0].pool.query("checkpoint");
  await verifyTipPlan(layouts[1], first[0], 0);
  await verifyTipPlan(layouts[1], long, 1);
  if (pageDiagnostic) {
    for (const arm of layouts) {
      const plans = [];
      for (let i = 0; i < 30; i++) {
        const result = (
          await arm.pool.query("explain(analyze,buffers,format json,timing off) " + flowPage, [
            tenant,
            long.id,
            24,
          ])
        ).rows[0]["QUERY PLAN"][0];
        if (i >= 5) plans.push(result);
      }
      console.log(
        JSON.stringify({
          anchor: arm.anchor,
          planningMs: distribution(plans.map((p) => p["Planning Time"])),
          executionMs: distribution(plans.map((p) => p["Execution Time"])),
        }),
      );
    }
    await study("concurrent_page_unnamed", [long], clients, "page");
    await study("concurrent_page_prepared", [long], clients, "preparedPage");
  } else {
    await study("read_empty_branch", first, 1, "latest");
    await study("read_populated_flow", [long], 1, "latest");
    await study("page_24_previews", [long], 1, "page");
    await study("concurrent_page_24_previews", [long], clients, "page");
    await study("first_run_on_empty_branch", first, 1);
    await study("append_to_long_flow", [long], 1);
    await study("concurrent_one_window", [hot], clients);
    await study("concurrent_eight_windows", separate, clients);
  }
} finally {
  for (const { pool, schema } of layouts)
    try {
      await pool.query("drop schema if exists " + schema + " cascade");
    } finally {
      await pool.end();
    }
}
