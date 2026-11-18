/** Compare the actual native pre-write SELECT in one isolated PG backend. */
import assert from "node:assert/strict";
import { createHash, randomUUID } from "node:crypto";
import { readFile, writeFile } from "node:fs/promises";
import { performance } from "node:perf_hooks";
import pg from "pg";
import { Kysely, PostgresDialect } from "kysely";
import { DurablePostgresProjection } from "../../../packages/pi-session-postgres/src/durable/postgres-projection.ts";
import { indexNative } from "../../../packages/pi-session-postgres/src/durable/types.ts";
import { runtimeDirectory, serviceAddress } from "../../lib/production-kubernetes.mjs";

assert.equal(process.env.PI_CLOUD_PROJECTOR_SQL_DIAGNOSTIC, "1");
assert(process.argv[2], "Supply a path for the content-free measurements");
const u = new URL((await readFile(runtimeDirectory + "/secrets/database-url", "utf8")).trim());
u.hostname = serviceAddress("postgres");
const pool = new pg.Pool({
  connectionString: u.toString(),
  max: 1,
  application_name: "pma-native-head-plan",
});
const db = new Kysely({ dialect: new PostgresDialect({ pool }) });
const scope = { tenantId: randomUUID(), sessionId: randomUUID() };

async function captureHead(seq, cold = false) {
  // Ask the maintained projection to compile its read and stop before dispatch.
  const stop = new Error("compiled");
  let query;
  const executor = db.getExecutor().withConnectionProvider({
    provideConnection: (use) =>
      use({
        executeQuery: async (compiled) => {
          query = compiled;
          throw stop;
        },
      }),
  });
  const captured = { getExecutor: () => executor };
  const projection = new DurablePostgresProjection(captured, scope);
  const request = cold
    ? projection.head()
    : projection.applyLockedIn(
        captured,
        {
          seq,
          nextId: 4,
          writes: [
            { type: "document.change", id: 2, content: { kind: "base", version: 1, value: {} } },
          ],
        },
        [],
      );
  await assert.rejects(request, (error) => error === stop);
  assert(query?.sql.includes(cold ? "select h.seq,h.next_id" : "select coalesce(h.seq,0)::text"));
  return { text: query.sql, values: [...query.parameters] };
}
// Reproduce the old plan by removing only the local bound. The candidate SQL
// and its parameters are compiled by the two production callers.
function unboundedControl(query) {
  const pattern =
    /\((select seq,next_id from pi_durable_commits c where [\s\S]+?)\s+order by c\.seq desc limit 1\)(\s+union all select through_seq,next_id)/g;
  assert.equal([...query.text.matchAll(pattern)].length, 1, "Native head query shape changed");
  return { ...query, text: query.text.replace(pattern, "$1$2") };
}
async function compare(client, query, seq, nextId) {
  const original = await client.query(unboundedControl(query));
  const bounded = await client.query(query);
  assert.deepEqual(original.rows, bounded.rows);
  assert.equal(Number(original.rows[0].seq), seq);
  assert.equal(Number(original.rows[0].next_id ?? 2), nextId);
}
function stats(values) {
  values.sort((a, b) => a - b);
  return {
    median: Number(values[Math.ceil(values.length * 0.5) - 1].toFixed(3)),
    p95: Number(values[Math.ceil(values.length * 0.95) - 1].toFixed(3)),
    samples: values.length,
  };
}
function nodes(node) {
  return [node, ...(node.Plans ?? []).flatMap(nodes)];
}
const measurements = [];
const tables = [
  "pi_durable_commits",
  "pi_durable_bases",
  "pi_durable_current",
  "pi_durable_owned_records",
];
const client = await pool.connect();
try {
  await client.query("begin");
  await client.query("set local search_path=pg_temp,public");
  for (const table of tables) {
    await client.query(
      `create temporary table ${table} (like public.${table} including all) on commit drop`,
    );
    const {
      rows: [resolution],
    } = await client.query("select to_regclass($1)=to_regclass('pg_temp.'||$1) as temporary", [
      table,
    ]);
    assert.equal(resolution.temporary, true);
  }
  const ids = [scope.tenantId, scope.sessionId];
  const setBase = (seq, nextId) =>
    client.query(
      "insert into pg_temp.pi_durable_bases(tenant_id,session_id,through_seq,next_id,history_count,history_fingerprint) values($1,$2,$3,$4,0,'fixture') on conflict(tenant_id,session_id) do update set through_seq=excluded.through_seq,next_id=excluded.next_id",
      [...ids, seq, nextId],
    );
  const cold = await captureHead(1, true);
  await compare(client, cold, 0, 2);
  await compare(client, await captureHead(1), 0, 2);
  await setBase(7, 22);
  await compare(client, await captureHead(8), 7, 22);
  await compare(client, cold, 7, 22);
  await client.query("delete from pg_temp.pi_durable_bases");
  const peers = 64,
    perPeer = 128;
  await client.query(
    `insert into pg_temp.pi_durable_commits(tenant_id,session_id,seq,next_id,fingerprint,write_count,history_write_count,history_fingerprint,current_hashes)
    select $1, 'peer-'||s, n,4,'fixture',1,1,'fixture','{}' from generate_series(1,$2::int) s cross join generate_series(1,$3::int) n`,
    [scope.tenantId, peers, perPeer],
  );
  const document = {
    id: 2,
    kind: "pi.live",
    scope: { kind: "conversation", conversationId: 1 },
    history: "latest",
    fork: "initial",
  };
  await client.query(
    "insert into pg_temp.pi_durable_owned_records(tenant_id,session_id,id,kind,seq,ordinal,fields,created_seq) values($1,$2,2,'document',1,0,$3,1)",
    [...ids, indexNative("document", document, 1, 0).fields],
  );
  await client.query(
    "insert into pg_temp.pi_durable_current(tenant_id,session_id,id,kind,seq,ordinal,body,delta_count,created_seq,committed_at) values($1,$2,2,'document',1,0,$3,0,1,clock_timestamp())",
    [
      ...ids,
      JSON.stringify({
        type: "document.create",
        record: document,
        content: { kind: "base", version: 1, value: {} },
      }),
    ],
  );
  let previous = 0;
  for (const depth of [8, 20000, 100000]) {
    await client.query(
      `insert into pg_temp.pi_durable_commits(tenant_id,session_id,seq,next_id,fingerprint,write_count,history_write_count,history_fingerprint,current_hashes)
      select $1,$2,n,4,'fixture',1,1,'fixture','{}' from generate_series($3::int,$4::int) n`,
      [...ids, previous + 1, depth],
    );
    for (const table of tables) await client.query("analyze pg_temp." + table);
    previous = depth;
    const query = await captureHead(depth + 1);
    await compare(client, cold, depth, 4);
    if (depth === 8) {
      for (const [base, baseNext, expected, expectedNext] of [
        [6, 3, 8, 4],
        [8, 4, 8, 4],
        [10, 16, 10, 16],
      ]) {
        await setBase(base, baseNext);
        await compare(client, query, expected, expectedNext);
        await compare(client, cold, expected, expectedNext);
      }
      await client.query("delete from pg_temp.pi_durable_bases");
    }
    const variants = [unboundedControl(query), query].map((q, i) => ({
      name: i ? "bounded" : "original",
      query: q,
      clientMs: [],
      planning: [],
      execution: [],
      plan: undefined,
    }));
    for (let i = 0; i < 60; i++) {
      const ordered = i % 2 ? variants.slice().reverse() : variants;
      let first;
      for (const v of ordered) {
        const start = performance.now();
        const result = await client.query(v.query);
        const elapsed = performance.now() - start;
        if (first) assert.deepEqual(result.rows, first);
        else first = result.rows;
        assert.equal(Number(result.rows[0].seq), depth);
        assert.equal(result.rows[0].inputs.length, 1);
        if (i >= 10) v.clientMs.push(elapsed);
        if (i >= 35) {
          const explained = await client.query({
            ...v.query,
            text: "explain (analyze,buffers,timing off,format json) " + v.query.text,
          });
          v.plan = explained.rows[0]["QUERY PLAN"][0];
          v.planning.push(v.plan["Planning Time"]);
          v.execution.push(v.plan["Execution Time"]);
        }
      }
    }
    for (const v of variants) {
      const access = nodes(v.plan.Plan)
        .filter((n) => n["Relation Name"] === "pi_durable_commits")
        .map((n) => ({
          node: n["Node Type"],
          direction: n["Scan Direction"],
          rows: n["Actual Rows"],
          loops: n["Actual Loops"],
          removedByFilter: n["Rows Removed by Filter"] ?? 0,
          localBlocks: n["Local Hit Blocks"] + n["Local Read Blocks"],
        }));
      if (depth >= 20000 && v.name === "bounded")
        assert(
          access.some((n) => n.direction === "Backward" && n.rows === 1),
          "Expected bounded newest-receipt index lookup",
        );
      measurements.push({
        variant: v.name,
        receipts: depth,
        otherReceipts: peers * perPeer,
        queryHash: createHash("sha256").update(v.query.text).digest("hex").slice(0, 16),
        planningMs: stats(v.planning),
        executionMs: stats(v.execution),
        clientMs: stats(v.clientMs),
        receiptAccess: access,
      });
    }
  }
} finally {
  try {
    await client.query("rollback");
  } finally {
    client.release();
    await db.destroy();
  }
}
await writeFile(process.argv[2], JSON.stringify(measurements, null, 2) + "\n");
