// Feed the same authenticated pages to the reference and candidate Session APIs.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { withMatchedPageFixture } from "./matched-page-cost.mjs";
import { pageCpuSnapshot, pageCpuDelta } from "./page-backend-cpu.mjs";

const url = process.env.PI_CLOUD_POSTGRES_INTEGRATION_URL,
  baseline = process.argv[2];
assert(
  url && new URL(url).hostname === "127.0.0.1" && baseline,
  "Pass the reference checkout and use the disposable PG runner",
);
const tenant = randomUUID(),
  round = (n) => +n.toFixed(3);
const order = [
  "reference",
  "candidate",
  "candidate",
  "reference",
  "candidate",
  "reference",
  "reference",
  "candidate",
];
const directions = ["latest", "older", "newer"];

async function deepWindow(db, id, n = 128) {
  const seed = randomUUID();
  await db.query(
    `insert into runs(id,tenant_id,session_id,prev_run_id,mailbox_position,initial_input,model_ref,idempotency_key,state,history_head,output_display_seq,created_at)
 select fixture_run(f.physical,j),$1,f.physical,case when j>1 then fixture_run(f.physical,j-1) when i>1 then fixture_run(fixture_run($2,i-1),46) end,j,
 'deep:'||i||':'||j,'fixture',fixture_run(f.physical,j),'completed','{"floor":1,"nextId":3}',j,'2026-01-01'
 from generate_series(1,$3::int)i cross join lateral(select fixture_run($2,i) physical)f cross join generate_series(1,48)j`,
    [tenant, seed, n],
  );
  await db.query(
    `insert into messages(tenant_id,pi_session_id,entry_id,conversation_id,run_id,role,content,commit_seq,ordinal)
 select tenant_id,session_id,mailbox_position,1,id,'assistant',json_build_object('id',mailbox_position,'kind','pi.assistant',
 'model',json_build_array(json_build_object('role','assistant','stopReason','stop','timestamp',1,'content',json_build_array(json_build_object('type','text','text',initial_input||repeat('abcd',256))))))::text,mailbox_position,0 from runs where tenant_id=$1 and initial_input like 'deep:%'`,
    [tenant],
  );
  await db.query(
    "insert into sessions(id,tenant_id,flow_id,base_run_id) values($1,$2,fixture_run($3,$4),fixture_run(fixture_run($3,$4::int-1),46))",
    [id, tenant, seed, n],
  );
  await db.query("analyze runs;analyze sessions;analyze messages");
  return seed;
}
async function selectStatement(api, id, query, mode) {
  const { statements } = await api.capture(query, id);
  const matches = statements.filter((r) =>
    mode === "reference"
      ? r.text.includes("flow_read_ranges(") && r.text.includes("order by mailbox_position")
      : r.text.includes("flow_page_turns_"),
  );
  assert.equal(matches.length, 1, "Check that the two APIs loaded different implementations");
  return matches[0];
}
async function blockSql(client, cfg, expected) {
  for (let i = 0; i < 6; i++) assert.deepEqual((await client.query(cfg)).rows, expected);
  const stamp = async () =>
    Number(
      (await client.query("select split_part(pg_read_file('/proc/self/schedstat'),' ',1) ns"))
        .rows[0].ns,
    );
  const n = 48,
    begin = await stamp();
  for (let i = 0; i < n; i++) await client.query(cfg);
  return round(((await stamp()) - begin) / 1e6 / n);
}
async function plan(client, cfg) {
  const stmt = (
    await client.query(
      `select format('explain(analyze,buffers,format json,timing off) execute %I (%s)',$1::text,string_agg(quote_nullable(v),',' order by i)) stmt from unnest($2::text[]) with ordinality a(v,i)`,
      [cfg.name, cfg.values],
    )
  ).rows[0].stmt;
  const p = (await client.query(stmt)).rows[0]["QUERY PLAN"][0];
  const cache = (
    await client.query(
      "select generic_plans::int generic,custom_plans::int custom from pg_prepared_statements where name=$1",
      [cfg.name],
    )
  ).rows[0];
  return {
    planMs: p["Planning Time"],
    execMs: p["Execution Time"],
    hits: p.Plan["Shared Hit Blocks"],
    cache,
  };
}
async function compareWindow(db, client, arm, id, name) {
  const latest = await arm.page({}, id),
    older = await arm.page({ before: latest.history.before }, id);
  const queries = [{}, { before: latest.history.before }, { after: older.history.after }];
  const apis = { reference: arm.reference, candidate: arm };
  for (const [i, query] of queries.entries()) {
    const expectedPage = await arm.reference.page(query, id);
    assert.deepEqual(await arm.page(query, id), expectedPage);
    const selectors = {},
      stats = {};
    for (const mode of ["reference", "candidate"]) {
      selectors[mode] = {
        name: name + "_" + i + "_" + mode,
        ...(await selectStatement(apis[mode], id, query, mode)),
      };
      stats[mode] = { selectorCpuMs: [], pageCpuMs: [], poolReadMs: [], p95HttpMs: [] };
    }
    const rows = (await client.query(selectors.reference)).rows;
    assert(rows.length > 0 && rows.length <= 41);
    for (const mode of order) {
      const api = apis[mode],
        bucket = stats[mode],
        cfg = selectors[mode];
      bucket.selectorCpuMs.push(await blockSql(client, cfg, rows));
      await Promise.all(Array.from({ length: 8 }, () => api.page(query, id)));
      const n = 64,
        latencies = [],
        before = await pageCpuSnapshot(db);
      await Promise.all(
        Array.from({ length: 8 }, async () => {
          for (let j = 0; j < n / 8; j++) {
            const t = performance.now(),
              p = await api.page(query, id);
            latencies.push(performance.now() - t);
            assert.equal(p.history.before, expectedPage.history.before);
          }
        }),
      );
      const delta = pageCpuDelta(before, await pageCpuSnapshot(db));
      bucket.pageCpuMs.push(round((delta.history + delta.control) / n));
      bucket.poolReadMs.push(round(delta.history / n));
      bucket.p95HttpMs.push(round(latencies.sort((a, b) => a - b)[Math.ceil(0.95 * n) - 1]));
    }
    for (const mode of Object.keys(stats)) stats[mode].plan = await plan(client, selectors[mode]);
    console.log(
      JSON.stringify({
        kind: "comparison",
        window: name,
        direction: directions[i],
        rows: rows.length,
        pagesPerArm: (64 * order.length) / 2,
        ...stats,
      }),
    );
  }
}
async function visibility(db, arm, deep, seed, createWindow) {
  // Introduce skipped rows on both sides of an inherited boundary. Include an
  // entire pending segment, a compact command, and an unsettled sealed Run.
  await db.query(
    "alter table runs drop constraint runs_state_check, add check(state in('queued','running','cancel_requested','completed','cancelled','failed','timed_out'))",
  );
  await db.query(
    `update runs set state='queued' where session_id=fixture_run($1,127) or (session_id=fixture_run($1,128) and mailbox_position<=7)`,
    [seed],
  );
  await db.query(
    `update runs set operation_kind='compact' where session_id=fixture_run($1,128) and mailbox_position=2`,
    [seed],
  );
  await db.query(
    `update runs set state='cancelled',started_at=case when mailbox_position=9 then now() end where session_id=fixture_run($1,128) and mailbox_position in(8,9)`,
    [seed],
  );
  await db.query(
    `update runs set output_seal_id=gen_random_uuid() where session_id=fixture_run($1,128) and mailbox_position=10`,
    [seed],
  );
  await db.query(
    `update runs set pruned_at=now() where session_id=fixture_run($1,128) and mailbox_position=11`,
    [seed],
  );
  await db.query(
    `update runs set state=case mailbox_position when 12 then 'failed' when 13 then 'timed_out' when 14 then 'running' else 'completed' end,
    output_seal_id=case when mailbox_position=15 then gen_random_uuid() end, output_sealed_at=case when mailbox_position=15 then now() end
    where session_id=fixture_run($1,128) and mailbox_position between 12 and 15`,
    [seed],
  );
  const empty = await createWindow("Empty");
  await db.query("insert into sessions(id,tenant_id,flow_id) values($1,$2,gen_random_uuid())", [
    empty,
    tenant,
  ]);
  const oracle = [];
  for (let s = 1; s <= 128; s++)
    for (let n = 1; n <= (s === 128 ? 48 : 46); n++) {
      if (s === 127 || (s === 128 && ((n <= 8 && n !== 2) || n === 11))) continue;
      oracle.push({
        pos: (s - 1) * 46 + n,
        complete: s !== 128 || (n !== 2 && n !== 10 && n !== 14),
      });
    }
  let checked = 0;
  const tail = (128 - 1) * 46;
  for (const ord of ["asc", "desc"])
    for (const complete of [false, true])
      for (const [lo, hi] of [
        [1, 10000],
        [tail - 56, tail + 16],
        [tail + 2, tail + 2],
        [tail + 10, tail + 9],
        [45, 47],
        [tail + 48, 10000],
      ]) {
        const candidates = oracle.filter(
          (r) => r.pos >= lo && r.pos <= hi && (!complete || r.complete),
        );
        const expected = (ord === "asc" ? candidates : candidates.reverse())
          .slice(0, 41)
          .map((r) => r.pos);
        const q = `select mailbox_position::int pos from flow_page_turns_${ord}($1,$2,$3,$4,$5,41) order by mailbox_position ${ord}`;
        assert.deepEqual(
          (await db.query(q, [tenant, deep, lo, hi, complete])).rows.map((r) => r.pos),
          expected,
        );
        checked++;
        for (const [t, s] of [
          [randomUUID(), deep],
          [tenant, empty],
        ])
          assert.equal((await db.query(q, [t, s, lo, hi, complete])).rowCount, 0);
      }
  for (const id of [empty, deep]) {
    let query = {};
    for (let i = 0; i < 3; i++) {
      const p = await arm.page(query, id);
      assert.deepEqual(p, await arm.reference.page(query, id));
      if (!p.history.hasBefore) break;
      query = { before: p.history.before };
    }
  }
  // An old depth-126 anchor cannot reveal its discarded suffix from depth 128.
  const invisible = (await db.query("select fixture_run(fixture_run($1,126),48) id", [seed]))
    .rows[0].id;
  for (const api of [arm, arm.reference])
    await assert.rejects(api.page({ turnId: invisible }, deep), { status: 404 });
  console.log(
    JSON.stringify({
      kind: "boundaries",
      oracleComparisons: checked,
      tenantAndEmptyChecks: 2 * checked,
      filteredPages: true,
      discardedSuffixRejected: true,
    }),
  );
}
// Share a physical flow identifier across tenants to exercise both plan skew
// and the tenant predicate; then put a long, invisible queue at the hot tip.
async function skewChecks(db, client, arm) {
  const emptyTenant = randomUUID(),
    tiny = randomUUID(),
    onlyRun = randomUUID();
  const captured = {};
  for (const [mode, api] of [
    ["reference", arm.reference],
    ["candidate", arm],
  ])
    captured[mode] = await selectStatement(api, arm.id, {}, mode);
  await db.query("insert into sessions(id,tenant_id,flow_id) values($1,$2,$3)", [
    tiny,
    emptyTenant,
    arm.id,
  ]);
  await db.query(
    `insert into runs(id,tenant_id,session_id,mailbox_position,initial_input,model_ref,idempotency_key,state,history_head)
    values($1,$2,$3,1,'tiny','fixture',$1,'completed','{}')`,
    [onlyRun, emptyTenant, arm.id],
  );
  for (const q of Object.values(captured)) {
    const rows = (
      await client.query(
        q.text,
        q.values.map((v) => (v === arm.id ? tiny : v === tenant ? emptyTenant : v)),
      )
    ).rows;
    assert.deepEqual(
      rows.map((r) => r.run_id),
      [onlyRun],
    );
  }
  await client.query("begin isolation level repeatable read read only");
  try {
    const q = captured.candidate;
    const snapshot = (await client.query(q.text, q.values)).rows;
    assert.equal(Number(snapshot[0].mailbox_position), 100000);
    await db.query(
      "update runs set state='queued' where tenant_id=$1 and session_id=$2 and mailbox_position>95904",
      [tenant, arm.id],
    );
    assert.deepEqual((await client.query(q.text, q.values)).rows, snapshot);
  } finally {
    await client.query("rollback");
  }
  const expected = (await client.query(captured.reference.text, captured.reference.values)).rows;
  assert.equal(Number(expected[0].mailbox_position), 95904);
  assert.deepEqual(await arm.page({}), await arm.reference.page({}));
  for (const mode of ["reference", "candidate"]) {
    const cfg = { name: "sparse_" + mode, ...captured[mode] };
    const cpuMs = await blockSql(client, cfg, expected);
    console.log(
      JSON.stringify({
        kind: "sparse-tip",
        mode,
        hiddenRuns: 4096,
        cpuMs,
        plan: await plan(client, cfg),
        smallTenantIsolated: true,
        snapshotHeld: true,
      }),
    );
  }
}
await withMatchedPageFixture(
  url,
  tenant,
  async ({ arms, db, createWindow }) => {
    const client = await db.connect();
    try {
      if (!process.argv.includes("--edges-only"))
        for (const name of ["root", "branch"])
          await compareWindow(db, client, arms[name], arms[name].id, name);
      const deep = await createWindow("Deep"),
        seed = await deepWindow(db, deep);
      if (!process.argv.includes("--edges-only"))
        await compareWindow(db, client, arms.branch, deep, "deep128");
      await client.query("set plan_cache_mode=force_custom_plan");
      await client.query("select * from flow_page_turns_desc($1,$2,1,10000,false,41)", [
        tenant,
        deep,
      ]);
      assert.equal(
        (await client.query("show plan_cache_mode")).rows[0].plan_cache_mode,
        "force_custom_plan",
      );
      await client.query("reset plan_cache_mode");
      await visibility(db, arms.branch, deep, seed, createWindow);
      await skewChecks(db, client, arms.root);
    } finally {
      client.release();
    }
  },
  baseline,
);
