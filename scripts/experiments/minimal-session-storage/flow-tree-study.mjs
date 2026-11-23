// Independently model each open window as a copied array of Run IDs.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import { readFile } from "node:fs/promises";
import pg from "pg";
import { installFlowHistory, flowPage } from "./flow-history.mjs";

const url = process.env.PI_CLOUD_POSTGRES_INTEGRATION_URL;
assert(url && new URL(url).hostname === "127.0.0.1");
const schema = "tree_" + randomUUID().replaceAll("-", "");
const pool = new pg.Pool({ connectionString: url, options: "-c search_path=" + schema, max: 5 });
const q = (text, args) => pool.query(text, args);
async function trial(seed) {
  let state = seed;
  function pick(xs) {
    state = (Math.imul(1664525, state) + 1013904223) >>> 0;
    return xs[Math.floor((state / 2 ** 32) * xs.length)];
  }
  const tenant = randomUUID(),
    windows = new Map(),
    bodies = new Map(),
    jobOrder = new Map();
  const counts = {
    append: 0,
    fork: 0,
    prune: 0,
    drop: 0,
    viewChecks: 0,
    collections: 0,
    maxWindows: 0,
  };
  let runs = 0;
  async function create() {
    const s = randomUUID(),
      f = randomUUID();
    await q("insert into sessions(id,tenant_id,flow_id) values($1,$2,$3)", [s, tenant, f]);
    windows.set(s, { flow: f, path: [] });
    return s;
  }
  async function add(s, label) {
    const w = windows.get(s),
      id = randomUUID(),
      text = label ?? "run-" + ++runs;
    await q("select flow_admit($1,$2,$3,$4,$5)", [tenant, s, w.flow, id, text]);
    await q(
      "update runs set state='completed',history_head='{\"floor\":1,\"nextId\":2}' where id=$1",
      [id],
    );
    await q(
      "insert into messages(tenant_id,pi_session_id,entry_id,conversation_id,run_id,role,content,commit_seq,ordinal) select tenant_id,flow_id,mailbox_position,1,id,'assistant',initial_input,mailbox_position,0 from runs where id=$1",
      [id],
    );
    w.path.push(id);
    bodies.set(id, text);
    counts.append++;
    return id;
  }
  async function fork(s, at) {
    const dst = randomUUID(),
      flow = randomUUID(),
      path = windows.get(s).path;
    await q("select flow_fork($1,$2,$3,$4,$5)", [tenant, s, at, dst, flow]);
    windows.set(dst, { flow, path: path.slice(0, path.indexOf(at) + 1) });
    counts.fork++;
    return dst;
  }
  async function detach(s, at = null) {
    const w = windows.get(s);
    const {
      rows: [{ job }],
    } = await q("select flow_detach($1,$2,$3,$4) job", [tenant, s, w.flow, at]);
    if (job) jobOrder.set(job, jobOrder.size);
    if (at === null) {
      windows.delete(s);
      counts.drop++;
      return job;
    }
    const {
      rows: [r],
    } = await q("select flow_id,base_run_id from sessions where id=$1", [s]);
    assert.notEqual(r.flow_id, w.flow);
    assert.equal(r.base_run_id, at);
    w.flow = r.flow_id;
    w.path = w.path.slice(0, w.path.indexOf(at) + 1);
    counts.prune++;
  }
  async function verify(drained = false) {
    counts.maxWindows = Math.max(counts.maxWindows, windows.size);
    const reachable = new Set();
    for (const [s, w] of windows) {
      const rows = (await q(flowPage, [tenant, s, w.path.length + 1])).rows;
      assert.deepEqual(rows.map((r) => r.id).reverse(), w.path);
      for (const r of rows) {
        assert.equal(r.content, bodies.get(r.id));
        reachable.add(r.id);
      }
      const ranges = await q(
        `select r.id from flow_read_ranges($1,$2) f join runs r on
       r.tenant_id=$1 and r.flow_id=f.physical and r.mailbox_position<=f.through_position
       order by f.prior_positions+r.mailbox_position`,
        [tenant, s],
      );
      assert.deepEqual(
        ranges.rows.map((r) => r.id),
        w.path,
      );
      counts.viewChecks++;
    }
    if (drained) {
      for (const [table, col] of [
        ["runs", "id"],
        ["messages", "run_id"],
      ]) {
        const { rows } = await q("select " + col + " id from " + table + " where tenant_id=$1", [
          tenant,
        ]);
        assert.deepEqual(rows.map((r) => r.id).sort(), [...reachable].sort());
      }
    }
  }
  async function gc(drain) {
    do {
      const jobs = (
        await q("select id from flow_gc_outbox where tenant_id=$1 order by id", [tenant])
      ).rows;
      if (!jobs.length) break;
      const j = pick(jobs.sort((a, b) => jobOrder.get(a.id) - jobOrder.get(b.id)));
      await q("select flow_collect($1,$2)", [j.id, pick([1, 2, 3])]);
      counts.collections++;
    } while (drain);
  }
  const root = await create(),
    ids = [];
  for (const c of "ABCDE") ids.push(await add(root, c));
  const left = await fork(root, ids[1]),
    right = await fork(root, ids[2]),
    empty = await fork(root, ids[1]);
  const f = await add(left, "F");
  await add(left, "G");
  const nested = await fork(left, f);
  await add(nested, "H");
  await add(right, "I");
  const emptyTwin = await fork(empty, ids[1]);
  await verify();
  await detach(root);
  await gc(true);
  await verify(true);
  await detach(left, ids[0]);
  await gc(true);
  await verify(true);
  await add(left, "J");
  await verify();
  await assert.rejects(fork(left, f), /Invisible/);
  await assert.rejects(detach(left, ids[2]), /Invisible/);
  await verify();
  await detach(empty);
  await detach(emptyTwin);
  await gc(false);
  await verify();
  for (let i = 0; i < 180; i++) {
    const s = pick([...windows.keys()]),
      w = windows.get(s),
      at = pick(w.path);
    const action = pick(["add", "add", "fork", "fork", "prune", "drop"]);
    if (action === "fork" && windows.size < 15) await fork(s, at);
    else if (action === "prune") await detach(s, at);
    else if (action === "drop" && windows.size > 1) await detach(s);
    else await add(s);
    if (i % 13 === 0) {
      await gc(true);
      await verify(true);
    } else {
      if (i % 2 === 0) await gc(false);
      await verify();
    }
  }
  while (windows.size) {
    await detach(pick([...windows.keys()]));
    await gc(false);
    await verify();
  }
  await gc(true);
  await verify(true);
  // Two collectors converge on the same ancestor. Roll back the first
  // transaction while the second is actually waiting, then retry its job.
  const common = await create();
  await add(common);
  const joint = await add(common);
  const one = await fork(common, joint),
    two = await fork(common, joint);
  const pin = await fork(common, joint);
  await add(one);
  await add(two);
  await detach(common);
  await gc(true);
  const firstJob = await detach(one),
    secondJob = await detach(two);
  const held = await pool.connect();
  let competing;
  try {
    await held.query("begin");
    assert.equal((await held.query("select flow_collect($1,8) n", [firstJob])).rows[0].n, 1);
    competing = q("select flow_collect($1,8) n", [secondJob]);
    competing.catch(() => {});
    let blocked = false;
    for (let i = 0; i < 200 && !blocked; i++) {
      blocked = (
        await q(
          "select exists(select from pg_stat_activity where $1::int=any(pg_blocking_pids(pid))) blocked",
          [held.processID],
        )
      ).rows[0].blocked;
      if (!blocked) await delay(5);
    }
    assert(blocked, "Collectors did not contend on the shared ancestor");
  } finally {
    await held.query("rollback");
    held.release();
    if (competing) assert.equal((await competing).rows[0].n, 1);
  }
  await gc(true);
  await verify(true);
  await detach(pin);
  await gc(true);
  await verify(true);
  return { seed, ...counts, rolledBackContendingCollector: true };
}
try {
  await q("create schema " + schema);
  await q(await readFile(new URL("./schema.sql", import.meta.url), "utf8"));
  await installFlowHistory(pool, false, false, "base");
  for (const seed of [17, 47, 81, 20261123]) console.log(JSON.stringify(await trial(seed)));
} finally {
  try {
    await q("drop schema if exists " + schema + " cascade");
  } finally {
    await pool.end();
  }
}
