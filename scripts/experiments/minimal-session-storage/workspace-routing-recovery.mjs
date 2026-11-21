// A private PG crash/rewind test for the routing decision. Kafka grant loss
// is modelled separately from the real group exercised by the Guest study.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import pg from "pg";
import { testPostgres } from "../../lib/test-postgres.mjs";
import { ReadyProjection } from "./ready-projection.mjs";
import { projectionTemplates } from "./projection-statements.mjs";
import { WorkspaceOperationRouter } from "./workspace-operation-router.mjs";
import { installWorkspaceRoutes } from "./workspace-route-spans.mjs";
import { decodeWorkspaceFact, encodeWorkspaceFact } from "./workspace-operations.mjs";
const fixture = await testPostgres({}, { crashable: true });
let pool;
const connect = () => {
  pool = new pg.Pool({ connectionString: fixture.url, max: 4 });
  pool.on("error", () => {});
};
connect();
try {
  const s = {
    tenantId: randomUUID(),
    piSessionId: randomUUID(),
    sessionId: randomUUID(),
    turnId: randomUUID(),
    runId: randomUUID(),
    writerId: randomUUID(),
    fencingToken: 1,
  };
  const topic = "test-private-workspace",
    p = (n) => ({ topic, partition: 0, offset: BigInt(n) });
  const ids = [randomUUID(), randomUUID()],
    operationId = randomUUID(),
    taskId = 9;
  const attempted = [],
    cancelled = [];
  const build = (view = new ReadyProjection(pool, topic, new Map(), projectionTemplates)) => {
    const env = {
      pool,
      sessions: view.sessions,
      stage: { integrated: true, sink: "single", scene: "plain" },
      metrics: { byPartition: [0], queries: 0, skipped: 0 },
      partitions: 1,
      nextOffsets: new Map(),
      account() {},
      send() {},
    };
    return {
      view,
      router: new WorkspaceOperationRouter({
        pool,
        view,
        env,
        select: () => {
          throw Error("Replayed tool selected today's Guest");
        },
        resolve: (id) => ({
          submit() {
            attempted.push(id);
          },
          cancel() {
            cancelled.push(id);
            throw Error("Lost Guest cancellation acknowledgement");
          },
        }),
      }),
    };
  };
  await pool.query(await readFile(new URL("./schema.sql", import.meta.url), "utf8"));
  let { view, router } = build();
  await view.install();
  await installWorkspaceRoutes(pool);
  await pool.query(
    "insert into runs(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,model_ref,state,lease_id) values($1,$2,$3,1,$1,'test','model','running',$4)",
    [s.runId, s.tenantId, s.piSessionId, s.writerId],
  );
  await router.spans.activate(s, 1, p(10), ids[0], () => true);
  await router.spans.activate(s, 1, p(40), ids[1], () => true);
  await assert.rejects(
    router.spans.activate(s, 1, p(40), randomUUID(), () => true),
    /boundary cannot change/,
  );
  await assert.rejects(
    router.spans.activate({ ...s, writerId: randomUUID() }, 1, p(60), randomUUID(), () => true),
    /no longer admitted/,
  );
  // Both registered binding transitions must survive an immediate postmaster exit.
  await pool.end();
  pool = undefined;
  await fixture.crash();
  await fixture.restart();
  connect();
  ({ view, router } = build());
  await view.reset(new Map([[0, 0n]]), 16, "recorded-history");
  assert.equal((await router.spans.at(s, 1, p(39))).bindingId, ids[0]);
  assert.equal((await router.spans.at(s, 1, p(40))).bindingId, ids[1]);
  assert.equal(await router.spans.at(s, 2, p(40)), undefined);
  assert.equal(await router.spans.at({ ...s, tenantId: randomUUID() }, 1, p(40)), undefined);
  const ref = {
    topic: "pi-cloud.tool-replies.v4." + randomUUID(),
    partition: 0,
    offset: "0",
    toolName: "bash",
    toolCallId: "call",
    deadlineAt: Date.now() + 60000,
  };
  const native = {
    id: taskId,
    conversationId: 1,
    kind: "pi.tool",
    version: 1,
    input: { assistant: 8, callId: "call" },
    background: false,
    abortRequested: false,
    state: {
      status: "running",
      checkpoint: { phase: "execute", arguments: { command: "true" }, replay: "safe" },
    },
  };
  const logical = {
    action: "execute",
    taskId,
    operationId,
    reply: ref,
    maximumOutputBytes: 51200,
    timeoutMs: 10000,
  };
  const f = {
    kind: "durable_commit",
    scope: s,
    factId: randomUUID(),
    occurredAt: new Date().toISOString(),
    origins: [{ recordId: taskId, sessionId: s.sessionId, turnId: s.turnId, runId: s.runId }],
    commit: { seq: 1, nextId: 10, writes: [{ type: "task", value: native }] },
    tools: [],
    cancellations: [],
    display: [],
    workspaceOperations: [logical],
  };
  const wire = encodeWorkspaceFact(f);
  for (const key of ["bindingId", "activationId", "originalWriterId"]) {
    assert.throws(() =>
      encodeWorkspaceFact({ ...f, workspaceOperations: [{ ...logical, [key]: ids[0] }] }),
    );
    const raw = JSON.parse(wire);
    raw.workspaceOperations[0][key] = ids[0];
    assert.throws(() => decodeWorkspaceFact(JSON.stringify(raw)));
  }
  // Restore a missing candidate PG projection from the earlier Kafka intent.
  // The dispatch offset was already granted. No physical submit may occur.
  await router.consume(
    { ...p(20), fact: decodeWorkspaceFact(wire).fact },
    () => true,
    async () => false,
  );
  const op = { taskId, operationId, conversationId: 1, scope: s };
  router = build(view).router;
  assert.equal((await router.original(s, op, p(60))).bindingId, ids[0]);
  assert.equal(await router.original(s, { ...op, operationId: randomUUID() }, p(60)), undefined);
  assert.equal(
    await router.original(s, { ...op, scope: { ...s, turnId: randomUUID() } }, p(60)),
    undefined,
  );
  assert.deepEqual(attempted, []);
  const newer = structuredClone(f);
  newer.factId = randomUUID();
  newer.commit.seq = 2;
  newer.commit.nextId++;
  newer.commit.writes[0].value.id++;
  newer.origins[0].recordId++;
  newer.workspaceOperations[0].taskId++;
  newer.workspaceOperations[0].operationId = randomUUID();
  await router.consume(
    { ...p(53), fact: decodeWorkspaceFact(encodeWorkspaceFact(newer)).fact },
    () => true,
    async () => true,
  );
  assert.deepEqual(attempted, [ids[1]]);
  const abort = structuredClone(f);
  abort.factId = randomUUID();
  abort.commit.seq = 3;
  abort.commit.nextId = newer.commit.nextId;
  abort.commit.writes[0].value.abortRequested = true;
  abort.commit.writes[0].value.state = { status: "terminal", outcome: { status: "aborted" } };
  abort.workspaceOperations = [{ action: "cancel", operationId, taskId }];
  await router.consume(
    { ...p(60), fact: decodeWorkspaceFact(encodeWorkspaceFact(abort)).fact },
    () => true,
    async () => {
      throw Error("Cancel acquired a new command grant");
    },
  );
  assert.deepEqual(cancelled, [ids[0]]);
  router = build(view).router;
  assert.equal(
    (await router.original(s, op, p(60))).bindingId,
    ids[0],
    "Same-batch settlement removed the original cancel address",
  );

  console.log(
    "PG restart, source-position replay, withheld dispatch, original target, scope isolation and Worker wire checks passed.",
  );
} finally {
  try {
    await pool?.end();
  } finally {
    await fixture.close();
  }
}
