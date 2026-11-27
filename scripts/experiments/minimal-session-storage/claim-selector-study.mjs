// Explain the exact parameterized selector emitted by the Worker. Only the
// disposable fixture is populated. Every locking measurement rolls back.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdtemp, rm, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import pg from "pg";
import { Kysely, PostgresDialect } from "kysely";
import { createDatabase, runMigrations } from "@pi-cloud/database";
import { createPrivateTenant } from "../../../packages/control-plane/src/tenant-administration.ts";
import { testPostgres } from "../../lib/test-postgres.mjs";
import { IndependentSessionRuntime } from "./independent-session-runtime.mjs";
import { installIndependentSessionLeases, SegmentWorker } from "./independent-session-leases.mjs";
import { installForkTables, setForkLayout } from "./fork-storage.mjs";
import { installPagingProjection } from "./flow-paging-fixture.mjs";
const indexSql = await readFile(new URL("./claim-selector-index.sql", import.meta.url), "utf8");
const fixture = await testPostgres({}),
  url = new URL(fixture.url),
  cache = await mkdtemp(join(tmpdir(), "claim-select-"));
url.searchParams.set("options", "-c search_path=selector,public");
const admin = createDatabase({ connectionString: fixture.url }),
  pool = new pg.Pool({ connectionString: url.href });
const rt = new IndependentSessionRuntime({ pool, topic: "study" }),
  captured = [];
const db = new Kysely({
  dialect: new PostgresDialect({ pool: new pg.Pool({ connectionString: url.href }) }),
  log: (e) => {
    if (
      e.level === "query" &&
      e.query.sql.includes("ready_agent_sessions") &&
      e.query.sql.includes("skip locked")
    )
      captured.push(e.query);
  },
});
const worker = new SegmentWorker({
  database: db,
  workerId: randomUUID(),
  bootId: randomUUID(),
  capacity: 8,
  leaseDurationMs: 30000,
  historyCache: { diskDirectory: cache },
});
const flatten = (n) => [n, ...(n.Plans ?? []).flatMap(flatten)];
try {
  await runMigrations(admin, "up");
  const { tenantId: t } = await createPrivateTenant(admin, {
    slug: "claim-select",
    ownerDisplayName: "Study",
  });
  await pool.query("create schema selector");
  await installPagingProjection(pool);
  await installForkTables(pool);
  await setForkLayout(pool, true, false);
  await installIndependentSessionLeases(pool);
  const head = randomUUID(),
    hot = Array.from({ length: 8 }, () => randomUUID()),
    first = randomUUID(),
    hint = [t, head, first, "version", head];
  await worker.claimBatch(8);
  await worker.claimBatch(1, hint);
  const fifo = captured[0],
    target = captured[1];
  assert(fifo && target);
  // Change just the projected boolean in this arm. An empty/withdrawn-only
  // mailbox still has two different meanings for first_message and has_inputs.
  function pointMail(query) {
    const scalar = query.sql.replace(
      "exists(select from user_or_agent_messages m where m.turn_id=r.id) as has_inputs",
      "coalesce((select true from user_or_agent_messages m where m.turn_id=r.id limit 1),false) as has_inputs",
    );
    assert.notEqual(scalar, query.sql, "Recheck the study after changing the Worker SELECT");
    return { ...query, sql: scalar };
  }
  const point = pointMail(fifo);
  async function sameMail(query) {
    const c = await pool.connect();
    try {
      await c.query("begin");
      const a = await c.query(query.sql, query.parameters),
        b = await c.query(pointMail(query).sql, query.parameters);
      assert.deepEqual(b.rows, a.rows);
      return a.rows[0];
    } finally {
      await c.query("rollback");
      c.release();
    }
  }
  function median(values) {
    const sorted = values.toSorted((a, b) => a - b),
      mid = Math.floor(sorted.length / 2);
    return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
  }
  async function measure(label, query, expected) {
    const c = await pool.connect();
    const plans = [];
    let ids;
    try {
      for (let i = 0; i < 6; i++) {
        await c.query("begin");
        if (i === 0) {
          ids = (await c.query(query.sql, query.parameters)).rows.map((j) => j.logical_id);
          assert.equal(ids.length, expected);
          if (expected === 1) assert.equal(ids[0], head);
        }
        const { rows } = await c.query(
          "explain (analyze,buffers,timing off,format json) " + query.sql,
          query.parameters,
        );
        await c.query("rollback");
        if (i) plans.push(rows[0]["QUERY PLAN"][0]);
      }
    } finally {
      await c.query("rollback");
      c.release();
    }
    const plan = plans[2].Plan;
    const nodes = flatten(plan);
    console.log(
      JSON.stringify({
        label,
        returned: ids.length,
        planningMs: median(plans.map((p) => p["Planning Time"])),
        execMs: median(plans.map((p) => p["Execution Time"])),
        hits: plan["Shared Hit Blocks"],
        reads: plan["Shared Read Blocks"],
        nodes: nodes.map((n) => ({
          op: n["Node Type"],
          rel: n["Relation Name"],
          index: n["Index Name"],
          alias: n.Alias,
          rows: n["Actual Rows"],
          loops: n["Actual Loops"],
          filtered: n["Rows Removed by Filter"],
          discarded: n["Rows Removed by Join Filter"],
          sort: n["Sort Method"],
        })),
      }),
    );
    return ids;
  }
  async function comparePrepared(label, query) {
    const c = await pool.connect(),
      name = "pma_claim_" + label,
      samples = { named: [], unnamed: [] };
    try {
      for (let i = 0; i < 18; i++) {
        for (const arm of i % 2 ? ["named", "unnamed"] : ["unnamed", "named"]) {
          await c.query("begin");
          const start = performance.now();
          const { rows } = await c.query({
            text: query.sql,
            values: query.parameters,
            ...(arm === "named" ? { name } : {}),
          });
          const elapsed = performance.now() - start;
          assert.equal(rows.length, 8);
          await c.query("rollback");
          if (i >= 6) samples[arm].push(elapsed);
        }
      }
      const {
        rows: [usage],
      } = await c.query(
        "select generic_plans::int,custom_plans::int from pg_prepared_statements where name=$1",
        [name],
      );
      console.log(
        JSON.stringify({
          label,
          unnamedMedianMs: median(samples.unnamed),
          namedMedianMs: median(samples.named),
          ...usage,
        }),
      );
    } finally {
      await c.query("rollback");
      c.release();
    }
  }
  async function enqueue(ids, pos = 1) {
    const { rows } = await pool.query(
      "insert into turns(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,state) select gen_random_uuid(),$1,id,$3,gen_random_uuid(),'test','queued' from unnest($2::uuid[]) id returning id,session_id",
      [t, ids, pos],
    );
    await pool.query(
      `insert into user_or_agent_messages(tenant_id,session_id,id,sender_id,starts_turn,source,mode,request_key,body,fingerprint,turn_id)
   select tenant_id,session_id,gen_random_uuid(),tenant_id,true,'user','input',id::text,'test','study',id from turns where id=any($1::uuid[])`,
      [rows.map((r) => r.id)],
    );
  }
  async function analyze() {
    for (const table of [
      "sessions",
      "turns",
      "ready_agent_sessions",
      "user_or_agent_messages",
      "public.session_leases",
    ])
      await pool.query("analyze " + table);
  }
  const fresh = Array.from({ length: 192 }, () => randomUUID());
  await rt.store.register(t, fresh);
  await enqueue(fresh);
  await measure("fresh192-default-stats", fifo, 8);
  await analyze();
  await measure("fresh192", fifo, 8);
  await comparePrepared("prepared_fresh", fifo);
  const blocker = await pool.connect();
  try {
    await blocker.query("begin");
    const { rows: locked } = await blocker.query(fifo.sql, fifo.parameters);
    const other = await measure("fresh192-eight-locked", fifo, 8);
    assert(!other.some((id) => locked.some((j) => j.logical_id === id)));
  } finally {
    await blocker.query("rollback");
    blocker.release();
  }
  // Settle the fixture's old work and add real immutable Turn settings and input rows.
  await pool.query(
    "update user_or_agent_messages set state='placed',native_id=2 where state='pending'",
  );
  await pool.query("update turns set state='completed',output_display_seq=4");
  assert.equal((await pool.query("select count(*)::int n from ready_agent_sessions")).rows[0].n, 0);
  await rt.store.register(t, [head, ...hot]);
  await pool.query(
    `insert into turns(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,state,output_display_seq,history_head)
    select gen_random_uuid(),$1,s,n,gen_random_uuid(),'history','completed',n,jsonb_build_object('version','version')
    from unnest($2::uuid[]) s cross join generate_series(1,4096) n`,
    [t, [head, ...hot]],
  );
  await pool.query(
    `insert into user_or_agent_messages(tenant_id,session_id,id,sender_id,starts_turn,source,mode,request_key,body,fingerprint,turn_id,state,native_id)
  select tenant_id,session_id,gen_random_uuid(),tenant_id,true,'user','input',id::text,'history','study',id,'placed',2*mailbox_position
  from turns where session_id=any($1::uuid[])`,
    [[head, ...hot]],
  );
  await enqueue(hot, 4097);
  await pool.query(
    "insert into turns(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,state) values($1,$2,$3,4097,$1,'target','queued')",
    [first, t, head],
  );
  await analyze();
  const longIds = await measure("nine-ready-after-4096", fifo, 8);
  await measure("warm-hint-after-4096", target, 1);
  await pool.query(indexSql);
  assert.deepEqual(await measure("nine-ready-indexed", fifo, 8), longIds);
  await measure("warm-hint-indexed", target, 1);
  const seal = randomUUID();
  await pool.query(
    "update turns set output_seal_id=$3 where tenant_id=$1 and session_id=$2 and mailbox_position=4096",
    [t, head, seal],
  );
  await measure("hint-before-seal-indexed", target, 0);
  await pool.query("drop index study_unfinished");
  await measure("hint-before-seal", target, 0);
  await pool.query("update turns set output_sealed_at=clock_timestamp() where output_seal_id=$1", [
    seal,
  ]);
  await measure("hint-after-seal", target, 1);
  const backlog = Array.from({ length: 4096 }, () => randomUUID());
  await rt.store.register(t, backlog);
  await enqueue(backlog);
  await analyze();
  const busyIds = await measure("4105-ready-with-history", fifo, 8);
  assert.deepEqual(await measure("4105-ready-point-mail", point, 8), busyIds);
  await comparePrepared("prepared_long", fifo);
  // An experimental partial index exists only inside this disposable fixture.
  await pool.query(indexSql);
  assert.deepEqual(await measure("4105-ready-indexed", fifo, 8), busyIds);
  assert.deepEqual(await measure("4105-ready-both", point, 8), busyIds);
  await sameMail(fifo);
  const empty = await sameMail(target);
  assert.equal(empty.first_message, null);
  assert.equal(empty.has_inputs, false);
  await pool.query(
    `insert into user_or_agent_messages(tenant_id,session_id,id,sender_id,starts_turn,source,mode,request_key,body,fingerprint,turn_id,state)
  values($1,$2,gen_random_uuid(),$1,true,'user','input','withdrawn','test','test',$3,'withdrawn')`,
    [t, head, first],
  );
  const withdrawn = await sameMail(target);
  assert.equal(withdrawn.first_message, null);
  assert.equal(withdrawn.has_inputs, true);
  await comparePrepared("prepared_indexed", fifo);
} finally {
  await worker.close();
  await db.destroy();
  await rt.store.db.destroy();
  await admin.destroy();
  await fixture.close();
  await rm(cache, { recursive: true, force: true });
}
