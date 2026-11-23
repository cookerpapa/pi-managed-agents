// Disposable PostgreSQL only. Run after the Kafka workload releases its fixtures.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import { readFile } from "node:fs/promises";
import pg from "pg";
import { MemoryStorage } from "@earendil-works/pi-durable";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { claimBatchSql } from "./fixture.mjs";
import { projectionTemplates } from "./projection-statements.mjs";
import { installFlowHistory, flowCold, flowPage } from "./flow-history.mjs";
const url = process.env.PI_CLOUD_POSTGRES_INTEGRATION_URL;
assert(url && new URL(url).hostname === "127.0.0.1", "Use the isolated PostgreSQL runner");
const schema = "flow_" + randomUUID().replaceAll("-", ""),
  pool = new pg.Pool({ connectionString: url, options: "-c search_path=" + schema, max: 10 });
const tenant = randomUUID(),
  root = randomUUID(),
  otherTenant = randomUUID();
const query = (sql, args = []) => pool.query(sql, args);
const one = async (sql, args) => (await query(sql, args)).rows[0];
const scalar = async (sql, args) => Object.values(await one(sql, args))[0];
const elapsed = async (f) => {
  const start = performance.now();
  await f();
  return performance.now() - start;
};
const round = (n) => Math.round(n * 1000) / 1000;
const spread = (xs) => {
  const sorted = xs.toSorted((a, b) => a - b);
  const at = (n) => round(sorted[Math.ceil(sorted.length * n) - 1]);
  return { samples: xs.length, p50Ms: at(0.5), p95Ms: at(0.95), p99Ms: at(0.99) };
};
const session = (s) =>
  one("select s.* from flow_session_tips s where tenant_id=$1 and id=$2", [tenant, s]);
const create = (s, f) =>
  query("insert into sessions(id,tenant_id,flow_id) values($1,$2,$3)", [s, tenant, f]);
const admit = (s, f, r, body) =>
  scalar("select flow_admit($1,$2,$3,$4,$5)", [tenant, s, f, r, body]);
const fork = (s, at, target = randomUUID()) =>
  scalar("select flow_fork($1,$2,$3,$4,$5)", [tenant, s, at, target, randomUUID()]).then(
    () => target,
  );
const detach = (s, f, to = null) => scalar("select flow_detach($1,$2,$3,$4)", [tenant, s, f, to]);
async function collect(job) {
  let total = 0,
    batches = 0;
  const times = [];
  if (job)
    for (;;) {
      let n;
      times.push(
        await elapsed(async () => {
          n = await scalar("select flow_collect($1,256)", [job]);
        }),
      );
      total += n;
      batches++;
      if (n < 256) break;
    }
  return { total, batches, batchMs: times.length ? spread(times) : null };
}
const claim = () =>
  one(
    claimBatchSql
      .replaceAll("session_id", "flow_id")
      .replace(
        "p.state<>'completed'",
        "p.state not in ('completed','cancelled','failed','timed_out')",
      ),
    [1],
  );
const guarded = projectionTemplates.message.replace(/\bsession_id\b/g, "flow_id");
async function append(row, entry) {
  const msg = {
    id: entry,
    conversation: 1,
    role: "assistant",
    content: JSON.stringify({
      id: entry,
      conversationId: 1,
      kind: "pi.assistant",
      model: [{ role: "assistant", content: [{ type: "text", text: row.initial_input }] }],
    }),
    ordinal: 0,
  };
  return (
    await one(guarded, [tenant, row.flow_id, row.id, entry, JSON.stringify([msg]), row.lease_id])
  ).accepted;
}
async function complete(s, f, id, position) {
  await admit(s, f, id, "Run " + position);
  const r = await claim();
  assert.equal(r.id, id);
  assert.equal(r.flow_id, f);
  assert(await append(r, position));
  await query("update runs set state='completed',history_head=$2 where id=$1 and lease_id=$3", [
    id,
    { floor: 1, nextId: position + 1 },
    r.lease_id,
  ]);
  return r;
}
async function correctness(withDelegates) {
  const flow = randomUUID(),
    ids = Array.from({ length: 5 }, () => randomUUID());
  await create(root, flow);
  let stale;
  for (const [i, id] of ids.entries()) stale = await complete(root, flow, id, i + 1);
  if (withDelegates) {
    const child = randomUUID();
    await query(
      `insert into runs(id,tenant_id,flow_id,root_run_id,caller_run_id,agent_id,source_task_id,context_mode,idempotency_key,initial_input,model_ref,state)
    values($1,$2,$3,$4,$4,$1,12,'fresh',$1,'child','fixture','completed')`,
      [child, tenant, flow, ids[1]],
    );
  }
  const other = await fork(root, ids[1]);
  const selected = await session(other);
  assert.equal(selected.workspace_id, (await session(root)).workspace_id);
  await query("update sessions set working_directory='/home/user/other' where id=$1", [other]);
  assert.equal((await session(root)).working_directory, "/workspace");
  assert.equal((await session(other)).tip_run_id, ids[1]);
  assert.deepEqual(
    (await query(flowPage, [tenant, other, 10])).rows.map((r) => r.id),
    ids.slice(0, 2).reverse(),
  );
  const job = await detach(root, flow, ids[1]),
    fresh = (await session(root)).flow_id;
  assert.notEqual(fresh, flow);
  assert.equal((await collect(job)).total, 3);
  await assert.rejects(admit(root, flow, randomUUID(), "stale"), /Flow changed/);
  assert.equal(await append(stale, 9), false);
  const e = randomUUID();
  await complete(root, fresh, e, 3);
  assert.deepEqual(
    (await query(flowPage, [tenant, root, 10])).rows.map((r) => r.id),
    [e, ids[1], ids[0]],
  );
  await assert.rejects(fork(other, e), /Invisible/);
  await assert.rejects(
    query("select flow_fork($1,$2,$3,$4,$5)", [
      otherTenant,
      other,
      ids[1],
      randomUUID(),
      randomUUID(),
    ]),
  );
  assert.equal((await collect(await detach(root, fresh))).total, 1);
  const g = randomUUID(),
    branch = (await session(other)).flow_id;
  await admit(other, branch, g, "new work");
  await assert.rejects(detach(other, branch), /still has work/);
  const leased = await claim();
  assert.equal(leased.id, g);
  assert.equal(leased.environment_snapshot.cwd, "/home/user/other");
  await assert.rejects(detach(other, branch), /still has work/);
  assert(await append(leased, 3));
  await query("update runs set state='completed',history_head=$2 where id=$1", [
    g,
    { nextId: 4, floor: 1 },
  ]);
  const final = await detach(other, branch);
  assert.equal(await scalar("select flow_collect($1,1)", [final]), 1);
  // A fresh collector can discover the remaining work after the initiator exits.
  const resumed =
    await query(`with job as materialized(select id from flow_gc_outbox order by id for update skip locked limit 1)
    select flow_collect(id,256) n from job`);
  assert.equal(resumed.rows[0].n, 2);
  assert.equal((await query("select id from flow_gc_outbox")).rowCount, 0);
  assert.equal((await collect(final)).total, 0);
  assert.equal(await scalar("select count(*)::int from messages"), 0);
  assert.equal(await scalar("select count(*)::int from runs"), 0);
  const qs = randomUUID(),
    qf = randomUUID(),
    q0 = randomUUID(),
    q1 = randomUUID(),
    q2 = randomUUID();
  await create(qs, qf);
  await complete(qs, qf, q0, 1);
  await admit(qs, qf, q1, "one");
  await admit(qs, qf, q2, "two");
  await query("select flow_swap_pending($1,$2,$3,$4)", [tenant, qs, q1, q2]);
  assert.equal((await session(qs)).tip_run_id, q1);
  assert.deepEqual(
    (await query(flowPage, [tenant, qs, 5])).rows.map((r) => r.id),
    [q1, q2, q0],
  );
  assert.equal((await query(flowCold, [tenant, qs])).rowCount, 1);
  await query("update runs set state='cancelled' where id=$1 and state='queued'", [q2]);
  const next = await claim();
  assert.equal(next.id, q1);
  await assert.rejects(
    query("select flow_swap_pending($1,$2,$3,$4)", [tenant, qs, q2, q1]),
    /Queue changed/,
  );
  assert(await append(next, 2));
  await query("update runs set state='completed',history_head=$2 where id=$1", [
    q1,
    { floor: 1, nextId: 3 },
  ]);
  assert.equal((await query(flowCold, [tenant, qs])).rowCount, 2);
  assert.equal((await collect(await detach(qs, qf))).total, 3);
  // A source row lock serializes Fork with its deletion; a committed empty branch pins its base.
  const s = randomUUID(),
    f = randomUUID(),
    a = randomUUID(),
    b = randomUUID();
  await create(s, f);
  await complete(s, f, a, 1);
  const c = await pool.connect();
  try {
    await c.query("begin");
    await c.query("select flow_fork($1,$2,$3,$4,$5)", [tenant, s, a, b, randomUUID()]);
    const unlinked = detach(s, f);
    let blocked = false;
    for (let retry = 0; retry < 200 && !blocked; retry++) {
      const observed = await query(
        "select exists(select from pg_stat_activity where $1::int=any(pg_blocking_pids(pid))) as blocked",
        [c.processID],
      );
      blocked = observed.rows[0].blocked;
      if (!blocked) await delay(5);
    }
    await c.query("commit");
    assert(blocked, "Fork and detach did not actually contend");
    assert.equal((await collect(await unlinked)).total, 0);
  } finally {
    await c.query("rollback");
    c.release();
  }
  const bState = await session(b);
  assert.equal(bState.tip_run_id, a);
  assert.equal((await collect(await detach(b, bState.flow_id))).total, 1);
}
// A window can own a tip even after the last successor and base references go away.
async function branchDeletionOrders(anchor) {
  for (const rootFirst of [false, true])
    for (const populated of [false, true]) {
      const s = randomUUID(),
        flow = randomUUID(),
        a = randomUUID(),
        b = randomUUID();
      await create(s, flow);
      await complete(s, flow, a, 1);
      await complete(s, flow, b, 2);
      const branch = await fork(s, b),
        writable = (await session(branch)).flow_id;
      const g = randomUUID();
      if (populated) await complete(branch, writable, g, 3);
      if (anchor === "base") assert.equal((await session(branch)).base_run_id, b);
      const firstJob = rootFirst ? await detach(s, flow) : await detach(branch, writable);
      assert.equal((await collect(firstJob)).total, !rootFirst && populated ? 1 : 0);
      const survivor = rootFirst ? branch : s;
      const page = (await query(flowPage, [tenant, survivor, 8])).rows.map((r) => r.id);
      assert.deepEqual(page, rootFirst && populated ? [g, b, a] : [b, a]);
      const lastJob = await detach(survivor, (await session(survivor)).flow_id);
      assert.equal((await collect(lastJob)).total, page.length);
      assert.equal(await scalar("select count(*)::int from messages"), 0);
    }
}

async function concurrentAdmission(anchor) {
  const s = randomUUID(),
    f = randomUUID(),
    first = randomUUID(),
    second = randomUUID();
  await create(s, f);
  const c = await pool.connect();
  try {
    await c.query("begin");
    await c.query("select flow_admit($1,$2,$3,$4,'first')", [tenant, s, f, first]);
    const waiter = admit(s, f, second, "second");
    let blocked = false;
    for (let i = 0; i < 200 && !blocked; i++) {
      blocked = await scalar(
        "select exists(select from pg_stat_activity where $1::int=any(pg_blocking_pids(pid)))",
        [c.processID],
      );
      if (!blocked) await delay(5);
    }
    await c.query("commit");
    await waiter;
    assert(blocked);
  } finally {
    await c.query("rollback");
    c.release();
  }
  assert.equal(await admit(s, f, second, "second"), second);
  await assert.rejects(admit(s, f, second, "different"), /identity changed/);
  assert.deepEqual(
    (await query(flowPage, [tenant, s, 8])).rows.map((r) => r.id),
    [second, first],
  );
  if (anchor === "base") assert.equal((await session(s)).base_run_id, null);
  await query("update runs set state='cancelled' where flow_id=$1", [f]);
  assert.equal((await collect(await detach(s, f))).total, 2);
}
async function seed(n, anchor) {
  const f = randomUUID();
  await create(root, f);
  await query(
    `insert into runs(id,tenant_id,flow_id,prev_run_id,mailbox_position,initial_input,model_ref,idempotency_key,state,history_head)
    select md5($1::text||g)::uuid,$2,$1::uuid,case when g>1 then md5($1::text||(g-1))::uuid end,g,'study','fixture',md5($1::text||g)::uuid,'completed',
    jsonb_build_object('floor',greatest(1,(g-1)/128*128-15),'nextId',g+1) from generate_series(1,$3::int)g`,
    [f, tenant, n],
  );
  await query(
    `insert into messages(tenant_id,pi_session_id,entry_id,conversation_id,run_id,role,content,commit_seq,ordinal)
 select $1::uuid,$2::uuid,g,1,md5($2::text||g)::uuid,'user',jsonb_strip_nulls(jsonb_build_object('id',g,'conversationId',1,'kind',case when g>1 and g%128=1 then 'pi.compaction' else 'pi.user' end,
 'head',case when g>1 and g%128=1 then greatest(1,g-16) end,'model',jsonb_build_array(jsonb_build_object('role','user','content',repeat('abcd',256),'timestamp',g))))::text,g,0 from generate_series(1,$3::int)g`,
    [tenant, f, n],
  );
  const mid = await scalar("select md5($1::text||$2::text)::uuid", [f, Math.floor(n / 2)]),
    head = await scalar("select md5($1::text||$2::text)::uuid", [f, n]);
  if (anchor === "head")
    await query("update sessions set head_run_id=$2 where id=$1", [root, head]);
  await query("analyze runs");
  await query("analyze messages");
  await query("analyze sessions");
  return { mid, head, flow: f };
}
async function series(f, iterations = 100, concurrency = 1) {
  const xs = [];
  let next = 0;
  await Promise.all(
    Array.from({ length: concurrency }, async () => {
      while (next++ < iterations) xs.push(await elapsed(f));
    }),
  );
  return spread(xs);
}
async function measure(n, anchor) {
  const { mid, head, flow } = await seed(n, anchor);
  const latest = async () =>
    assert.equal((await query(flowPage, [tenant, root, 1])).rows[0].id, head);
  const page = async () => assert.equal((await query(flowPage, [tenant, root, 24])).rowCount, 24);
  const cold = async () => {
    const rows = (await query(flowCold, [tenant, root])).rows;
    const floor = Math.max(1, Math.floor((n - 1) / 128) * 128 - 15);
    assert.equal(rows.length, n - floor + 1);
    // Rebuild the returned suffix in a fresh native storage instance for each sample.
    const storage = new MemoryStorage();
    storage.prepareCommit([{ type: "conversation", value: { id: 1 } }], 1).apply();
    for (const r of rows) {
      const entry = JSON.parse(r.content);
      storage.prepareCommit([{ type: "entry", value: entry }], entry.id).apply();
    }
    assert.equal(
      (await storage.scanEntries({ conversationId: 1 }, 1, undefined, BACKGROUND_CONTEXT)).items[0]
        ?.id,
      n,
    );
  };
  const {
    rows: [explained],
  } = await query("explain(analyze,buffers,format json) " + flowPage, [tenant, root, 24]);
  const scanned = {};
  function recordPlan(p) {
    if (p["Relation Name"])
      scanned[p["Relation Name"]] =
        (scanned[p["Relation Name"]] ?? 0) +
        p["Actual Loops"] * (p["Actual Rows"] + (p["Rows Removed by Filter"] ?? 0));
    for (const ch of p.Plans ?? []) recordPlan(ch);
  }
  recordPlan(explained["QUERY PLAN"][0].Plan);
  const latestMs = await series(latest),
    pageMs = await series(page),
    coldMs = await series(cold, 30);
  const branches = [];
  const forkMs = await series(async () => branches.push(await fork(root, mid)));
  const concurrentPageMs = await series(page, 256, 8);
  const deleteSamples = [],
    emptyGcSamples = [];
  while (branches.length) {
    const s = branches.pop(),
      state = await session(s);
    let job;
    deleteSamples.push(
      await elapsed(async () => {
        job = await detach(s, state.flow_id);
      }),
    );
    emptyGcSamples.push(await elapsed(async () => assert.equal((await collect(job)).total, 0)));
  }
  const emptyDeleteMs = spread(deleteSamples),
    emptyGcMs = spread(emptyGcSamples);
  const submitSession = await fork(root, mid);
  const sf = (await session(submitSession)).flow_id;
  const admissionMs = await series(() =>
    admit(submitSession, sf, randomUUID(), "measure admission"),
  );
  const count = await one(
    "select count(*)::int n,max(mailbox_position)::int last from runs where tenant_id=$1 and flow_id=$2",
    [tenant, sf],
  );
  assert.equal(count.n, 100);
  assert.equal(count.last, 100);
  await query("update runs set state='cancelled' where tenant_id=$1 and flow_id=$2", [tenant, sf]);
  assert.equal((await collect(await detach(submitSession, sf))).total, 100);
  const empty = await fork(root, mid);
  let job;
  const deleteMs = await elapsed(async () => {
    job = await detach(root, flow);
  });
  let first, second;
  const exclusiveGcMs = await elapsed(async () => {
    first = await collect(job);
  });
  assert.equal(first.total, n - Math.floor(n / 2));
  assert.equal((await session(empty)).tip_run_id, mid);
  const restJob = await detach(empty, (await session(empty)).flow_id);
  const sharedGcMs = await elapsed(async () => {
    second = await collect(restJob);
  });
  assert.equal(first.total + second.total, n);
  assert.equal(await scalar("select count(*)::int from messages"), 0);
  return {
    n,
    admissionMs,
    scannedPageRows: scanned,
    coldEntries: n - Math.max(1, Math.floor((n - 1) / 128) * 128 - 15) + 1,
    latestMs,
    pageMs,
    concurrentPageMs,
    coldMs,
    forkMs,
    emptyDeleteMs,
    emptyGcMs,
    deleteMs: round(deleteMs),
    exclusiveGcMs: round(exclusiveGcMs),
    first,
    sharedGcMs: round(sharedGcMs),
    second,
  };
}
async function nested(anchor) {
  const n = 10000,
    { head, flow } = await seed(n, anchor);
  // Each local segment contains ten Runs; reaching the original anchor crosses 1,000 flows.
  await query(
    "update runs set flow_id=md5($1::text||'part'||((mailbox_position-1)/10)::text)::uuid,mailbox_position=1+(mailbox_position-1)%10 where tenant_id=$2",
    [flow, tenant],
  );
  await query("update sessions set flow_id=(select flow_id from runs where id=$1) where id=$2", [
    head,
    root,
  ]);
  await query("analyze runs");
  const at = await scalar("select md5($1::text||'1')::uuid", [flow]);
  const created = [];
  const forkMs = await series(async () => created.push(await fork(root, at)), 100);
  for (const s of created) await collect(await detach(s, (await session(s)).flow_id));
  const pageMs = await series(async () =>
    assert.equal((await query(flowPage, [tenant, root, 24])).rowCount, 24),
  );
  const deleted = await collect(await detach(root, (await session(root)).flow_id));
  assert.equal(deleted.total, n);
  return { nestedFlows: n / 10, runs: n, forkAtFirstRunMs: forkMs, pageMs };
}
export async function runFlowStudy(acceptanceOnly = false, anchor = "head") {
  try {
    await query("create schema " + schema);
    await query(await readFile(new URL("./schema.sql", import.meta.url), "utf8"));
    await installFlowHistory(pool, false, acceptanceOnly, anchor);
    await correctness(acceptanceOnly);
    await branchDeletionOrders(anchor);
    await concurrentAdmission(anchor);
    console.log(
      JSON.stringify({
        anchor,
        branchDeletionOrdersVerified: true,
        concurrentAdmissionVerified: true,
        claimedAfterPrune: true,
        emptyForkRetained: true,
        workspaceIndependent: true,
        staleWriterRejected: true,
        delegatedLayout: acceptanceOnly,
        cleanupResumed: true,
      }),
    );
    if (acceptanceOnly) return;
    for (const n of [1000, 10000, 100000]) console.log(JSON.stringify(await measure(n, anchor)));
    console.log(JSON.stringify(await nested(anchor)));
  } finally {
    try {
      await query("drop schema if exists " + schema + " cascade");
    } finally {
      await pool.end();
    }
  }
}
if (import.meta.main) await runFlowStudy(false, process.argv[2] ?? "head");
