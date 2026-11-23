// Exercise the existing delegated Run and Fork layouts together in a disposable PG schema.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import pg from "pg";
import { readProjectedContextEstimate } from "../../../packages/control-plane/src/projected-context.ts";
import { claimBatchSql } from "./fixture.mjs";
import { ForkStorage, setForkLayout } from "./fork-storage.mjs";
import { ReadyProjection } from "./ready-projection.mjs";
import { projectionTemplates } from "./projection-statements.mjs";

const uri = process.env.PI_CLOUD_POSTGRES_INTEGRATION_URL;
assert(uri && new URL(uri).hostname === "127.0.0.1", "Use the isolated PostgreSQL runner");
const schema = "matched_identity_" + randomUUID().replaceAll("-", "");
const pool = new pg.Pool({ connectionString: uri, options: "-c search_path=" + schema, max: 3 });
const tenant = randomUUID(),
  root = randomUUID(),
  turn = randomUUID(),
  key = randomUUID();
const layout = (name) => readFile(new URL(name, import.meta.url), "utf8");
try {
  await pool.query("create schema " + schema);
  await pool.query(await layout("schema.sql"));
  await new ReadyProjection(pool, "test", new Map(), projectionTemplates).install();
  await pool.query(await layout("fork-layout.sql"));
  await setForkLayout(pool, true);
  await pool.query(await layout("delegated-runs.sql"));
  const storage = new ForkStorage(pool);
  await storage.register(tenant, [root]);
  await pool.query("insert into sessions values($1,$2)", [root, tenant]);
  const admit = (id, s, position, state, client = pool) =>
    client.query(
      "insert into runs(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,model_ref,state) values($1,$2,$3,$4,$1,'input','fixture',$5)",
      [id, tenant, s, position, state],
    );
  await admit(turn, root, 1, "queued");
  const {
    rows: [claimed],
  } = await pool.query(claimBatchSql, [1]);
  assert.equal(claimed.id, turn);
  for (const id of [2, 3, 4]) {
    const entry = {
      id,
      conversationId: 1,
      kind: "pi.assistant",
      model: [
        {
          role: "assistant",
          provider: "example",
          model: "test",
          usage: {
            input: id * 100,
            cacheRead: 0,
            cacheWrite: 0,
            output: 1,
            totalTokens: id * 100 + 1,
          },
          content: [{ type: "text", text: "answer " + id }],
          stopReason: "stop",
        },
      ],
    };
    await pool.query(
      "insert into messages(tenant_id,pi_session_id,entry_id,conversation_id,run_id,role,content,commit_seq,ordinal) values($1,$2,$3,1,$4,'assistant',$5,$3,0)",
      [tenant, root, id, turn, JSON.stringify(entry)],
    );
  }
  const finish = await pool.query("select * from prefix_finish($1,$2,$3,$4,$5)", [
    tenant,
    root,
    turn,
    claimed.lease_id,
    JSON.stringify({ seq: 4, nextId: 5, floor: 1, recount: true }),
  ]);
  assert.equal(finish.rowCount, 1);
  const point = { entry: 2, run: turn, lane: 1 };
  const fork = await storage.fork(tenant, root, { ...point, entry: 3 }, key);
  await pool.query("insert into sessions values($1,$2)", [fork, tenant]);
  const fresh = await storage.retire(tenant, root, point);
  assert.notEqual(fresh, root);
  const inspect = async (id) => (await storage.page(tenant, id)).map((r) => Number(r.entry_id));
  assert.deepEqual(await inspect(root), [2]);
  assert.deepEqual(await inspect(fork), [3, 2]);
  const status = (id, physical) =>
    storage.db
      .transaction()
      .setIsolationLevel("repeatable read")
      .execute((tx) =>
        readProjectedContextEstimate(
          tx,
          { tenant, publicRoot: id, physical, fork: id === fork },
          { provider: "example", modelId: "test" },
        ),
      );
  assert.deepEqual(await status(root, fresh), { estimatedTokens: 201, method: "latest_usage" });
  assert.deepEqual(await status(fork, fork), { estimatedTokens: 301, method: "latest_usage" });
  const other = randomUUID();
  await admit(other, fork, 1, "queued");
  assert.equal((await pool.query(claimBatchSql, [1])).rows[0].id, other);
  const failures = [];
  for (const [key, id] of [
    ["public_session", root],
    ["new_stream", fresh],
  ]) {
    const client = await pool.connect();
    try {
      await client.query("begin");
      await admit(randomUUID(), id, 1, "queued", client);
    } catch (e) {
      if (e.code !== "23503") throw e;
      failures.push({ key, constraint: e.constraint });
    } finally {
      try {
        await client.query("rollback");
      } finally {
        client.release();
      }
    }
  }
  if (failures.some((f) => f.key === "public_session")) process.exitCode = 2;
  console.log(
    JSON.stringify({
      beforePrune: "claimed",
      independentFork: "claimed",
      afterPruneHistory: [2],
      independentForkHistory: [3, 2],
      statusTokens: { trimmed: 201, independentFork: 301 },
      admission: failures,
    }),
  );
} finally {
  try {
    await pool.query("drop schema if exists " + schema + " cascade");
  } finally {
    await pool.end();
  }
}
