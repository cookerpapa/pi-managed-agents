// Verify the real projection transaction and memory adoption independently of the
// RF3 consumer-kill drill. Only the Kafka grant and external send are controlled here.
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { randomUUID } from "node:crypto";
import pg from "pg";
import { checkCapturedIntents } from "./native-intent-pg-check.mjs";
import { testPostgres } from "../../lib/test-postgres.mjs";
import { ReadyProjection } from "./ready-projection.mjs";
import { projectRecord } from "./project-record.mjs";
import { projectionTemplates } from "./projection-statements.mjs";
import { applyMovingSql } from "./ready-projection-sql.mjs";
import { installWorkspaceRoutes } from "./workspace-route-spans.mjs";
import { WorkspaceOperationRouter } from "./workspace-operation-router.mjs";
import { FanoutSender } from "./fanout-network.mjs";
import { fanoutFixture, targets, partitions } from "./fanout-fixture.mjs";
import { decodeWorkspaceFact, operationsFor } from "./workspace-operations.mjs";
import { prepareToolGrants } from "./tool-grant-prefix.mjs";

const server = await testPostgres({});
const db = new pg.Pool({ connectionString: server.url, max: 4 });
const topic = "intent-pg-test",
  fixture = fanoutFixture("f123456789abcdee"),
  ids = [0, partitions, partitions * 2];
const owners = ids.map((i) => fixture.owners[i]),
  current = () => true;
const sender = new FanoutSender();
sender.reset();
let losePgAck = false,
  loseGrantAck = false,
  nextGrant = 3n,
  grantCalls = 0;
const delivered = [];
const store = {
  query: (...args) => db.query(...args),
  async connect() {
    const client = await db.connect();
    return {
      release: () => client.release(),
      query: async (sql, args) => {
        const result = await client.query(sql, args);
        if (losePgAck && sql === applyMovingSql) {
          losePgAck = false;
          throw Object.assign(Error("connection lost after PG commit"), { code: "ECONNRESET" });
        }
        return result;
      },
    };
  },
};
function fresh() {
  const view = new ReadyProjection(store, topic, new Map(), projectionTemplates);
  view.configure(16, "recorded-history");
  const env = {
    pool: store,
    sessions: view.sessions,
    stage: { integrated: true, sink: "single", scene: "plain" },
    metrics: {
      byPartition: Array(partitions).fill(0),
      queries: 0,
      skipped: 0,
      stale: 0,
      duplicate: 0,
    },
    partitions,
    nextOffsets: new Map(),
    account() {},
    send() {},
  };
  const router = new WorkspaceOperationRouter({
    pool: store,
    view,
    env,
    select() {
      throw Error("Unexpected change to the original binding");
    },
    resolve() {
      throw Error("Unexpected replayed external send");
    },
  });
  return { view, env, router };
}
let state = fresh();
const record = (i, round, offset, seed = false) => ({
  topic,
  partition: 0,
  offset: BigInt(offset),
  fact: decodeWorkspaceFact(fixture.frame(1, round * targets + i, seed).value).fact,
});
const seeds = ids.map((i, j) => record(i, 0, j, true));
const round = (n, offset) => ids.map((i, j) => record(i, n, offset + j));
const head = async () =>
  BigInt(
    (
      await db.query(
        "select next_offset from accepted_fact_projection_offsets where topic=$1 and partition=0",
        [topic],
      )
    ).rows[0].next_offset,
  );
async function rows() {
  return (await db.query("select id,projection_tools from runs order by id")).rows;
}
async function seed(r) {
  return state.view.apply([r], current, (x, d) => projectRecord(x, d, state.env));
}
async function handle(records) {
  const n = await prepareToolGrants(
    records,
    current,
    async (count) => {
      grantCalls++;
      for (const r of records.slice(0, count)) {
        const [op] = operationsFor(r.fact);
        const ref = (
          await db.query("select projection_tools->$2 as ref from runs where id=$1", [
            r.fact.scope.runId,
            String(op.taskId),
          ])
        ).rows[0].ref;
        assert.equal(ref.operationId, op.operationId);
        assert.deepEqual(ref.reply, op.reply);
        assert.equal(ref.offset, String(r.offset));
      }
      const first = records.slice(0, count).filter((r) => r.offset < nextGrant).length;
      const next = records[count - 1].offset + 1n;
      if (next > nextGrant) nextGrant = next;
      if (loseGrantAck) {
        loseGrantAck = false;
        throw Error("Kafka grant reply lost");
      }
      return first;
    },
    {
      batchProjection: true,
      limit: 8,
      sender,
      router: state.router,
      targetFor: (r) => r.fact.scope.sessionId,
      prepareSend: (r, { route, op }) => {
        assert.equal(
          route.bindingId,
          owners.find((o) => o.scope.sessionId === r.fact.scope.sessionId).binding,
        );
        return () => {
          delivered.push(op.operationId);
        };
      },
    },
  );
  if (!n)
    await state.router.consume(records[0], current, async () => {
      throw Error("Rejected record attempted a grant");
    });
  assert.equal(sender.count, 0);
  return n || 1;
}
async function reopen(history) {
  state = fresh();
  const floor = await state.view.recover([{ partition: 0, low: 0n, high: await head() }]);
  assert.equal(floor.get(0), 0n);
  for (const r of seeds) await seed(r);
  for (let i = 0; i < history.length;) i += await handle(history.slice(i));
}
try {
  await db.query(await readFile(new URL("./schema.sql", import.meta.url), "utf8"));
  await state.view.install();
  await installWorkspaceRoutes(store);
  await state.view.reset(new Map([[0, 0n]]), 16, "recorded-history");
  for (const [j, o] of owners.entries()) {
    const s = o.scope;
    await db.query(
      "insert into runs(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,model_ref,state,lease_id) values($1,$2,$3,1,$1,'test','fixture','running',$4)",
      [s.runId, s.tenantId, s.sessionId, s.writerId],
    );
    await state.router.spans.activate(s, 1, seeds[j], o.binding, current);
    await seed(seeds[j]);
  }
  const first = round(0, 3),
    second = round(1, 6),
    third = round(2, 9);
  const middle = owners[1].scope.runId;
  await db.query(`update runs set projection_tools='{"9":"conflict"}'::jsonb where id=$1`, [
    middle,
  ]);
  const before = await rows();
  await assert.rejects(handle(first), { code: "PZ002" });
  assert.deepEqual(await rows(), before);
  assert.equal(await head(), 3n);
  assert(state.view.sessions.values().every((s) => s.seq === 1));
  assert.equal(grantCalls, 0);
  assert.equal(sender.count, 0);
  await db.query("update runs set projection_tools='{}' where id=$1", [middle]);

  losePgAck = true;
  await assert.rejects(handle(first), { code: "ECONNRESET" });
  assert.equal(await head(), 6n);
  assert.equal(state.view.next.get(0), 3n);
  assert(state.view.sessions.values().every((s) => s.seq === 1));
  assert.equal(grantCalls, 0);
  assert.equal(delivered.length, 0);
  assert.equal(sender.count, 0);
  await reopen(first);
  const firstEffects = first.map((r) => operationsFor(r.fact)[0].operationId);
  assert.deepEqual(delivered, firstEffects);
  loseGrantAck = true;
  await assert.rejects(handle(second), /Kafka grant reply lost/);
  assert.equal(await head(), 9n);
  assert.equal(nextGrant, 9n);
  assert.equal(sender.count, 0);
  await reopen([...first, ...second]);
  assert.deepEqual(delivered, firstEffects);

  await db.query("update runs set state='cancel_requested' where id=$1", [middle]);
  const references = await rows(),
    grants = grantCalls;
  await assert.rejects(handle(third), { code: "PZ001" });
  assert.deepEqual(await rows(), references);
  assert.equal(await head(), 9n);
  assert.equal(grantCalls, grants);
  assert.equal(sender.count, 0);
  for (const r of third) await handle([r]);
  assert.deepEqual(delivered, [
    ...firstEffects,
    ...[third[0], third[2]].map((r) => operationsFor(r.fact)[0].operationId),
  ]);
  assert.equal(
    (await rows())[1].projection_tools[11].operationId,
    operationsFor(third[1].fact)[0].operationId,
  );
  const last = [record(ids[0], 3, 12), record(ids[2], 3, 13)];
  await db.query("update runs set lease_id=$2 where id=$1", [owners[2].scope.runId, randomUUID()]);
  const prior = await rows(),
    grantsBeforeStale = grantCalls;
  await assert.rejects(handle(last), { code: "PZ001" });
  assert.deepEqual(await rows(), prior);
  assert.equal(await head(), 12n);
  assert.equal(grantCalls, grantsBeforeStale);
  for (const r of last) await handle([r]);
  assert.equal(await head(), 14n);
  assert.equal(delivered.length, 6);
  assert.equal(delivered.at(-1), operationsFor(last[0].fact)[0].operationId);
  assert.equal(new Set(delivered).size, delivered.length);
  await checkCapturedIntents(db, state, sender, seed, topic);
  console.log(
    "Native Bash Task, pi.live and tool.started batched; failed and covered projections emitted no new start.",
  );
  console.log(
    "Actual PG middle-row rollback, lost PG/Kafka ACK replay, operation receipts, cancelled and stale owner isolation passed.",
  );
} finally {
  try {
    await db.end();
  } finally {
    await server.close();
  }
}
