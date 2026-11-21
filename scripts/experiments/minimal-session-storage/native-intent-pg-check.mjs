// Capture unmodified commits from the pinned Harness and use the real fixed Bash
// definition with an inert executor. No provider request or shell escapes the fixture.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { kafkaProducerLane } from "../../../packages/runtime-core/src/kafka-accepted-fact.ts";
import { FamilyPresentation } from "./family-presentation.mjs";
import {
  decodeWorkspaceFact,
  encodeWorkspaceFact,
  operationsFor,
} from "./workspace-operations.mjs";
import { prepareToolGrants } from "./tool-grant-prefix.mjs";
import { nativeBashTrace, nativeBashFact } from "./native-bash-trace.mjs";

async function capture(scope) {
  const frames = await nativeBashTrace(scope);
  const presentation = new FamilyPresentation();
  presentation.bind(1, scope);
  const prefix = [];
  for (const commit of frames) {
    const d = presentation.prepare(commit);
    d.confirmed();
    const f = nativeBashFact(
      scope,
      commit,
      [],
      randomUUID(),
      "pi-cloud.tool-replies.v4." + randomUUID(),
    );
    if (f.workspaceOperations.length) {
      assert.deepEqual(commit.writes.map((w) => w.type).sort(), ["document.change", "task"]);
      const withDisplay = decodeWorkspaceFact(
        encodeWorkspaceFact({ ...f, display: d.display }),
      ).fact;
      assert(
        withDisplay.display.some((s) => s.events.some((e) => e.event.type === "tool.started")),
      );
      return { prefix, intent: decodeWorkspaceFact(encodeWorkspaceFact(f)).fact, withDisplay };
    }
    prefix.push(decodeWorkspaceFact(encodeWorkspaceFact(f)).fact);
  }
  throw Error("Missing native execute checkpoint");
}

export async function checkCapturedIntents(db, state, sender, seed, topic) {
  const { view, router } = state,
    records = [],
    bindings = new Map();
  const displayed = [];
  let commitBefore,
    grantObtained = false;
  router.onToolStart = (event, record) => {
    assert.equal(view.stats.commits, commitBefore + 1);
    assert(!grantObtained);
    assert.equal(event.payload.input.timeout, 10);
    displayed.push(record.fact.factId);
  };
  let off = view.next.get(0);
  for (let i = 0; i < 2; i++) {
    let sessionId;
    do {
      sessionId = randomUUID();
    } while (kafkaProducerLane(sessionId, state.env.partitions) !== 0);
    const runId = randomUUID(),
      scope = {
        tenantId: randomUUID(),
        runId,
        sessionId,
        piSessionId: sessionId,
        turnId: runId,
        writerId: randomUUID(),
        fencingToken: 1,
      };
    const { prefix, intent, withDisplay } = await capture(scope);
    await db.query(
      "insert into runs(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,model_ref,state,lease_id) values($1,$2,$3,1,$1,'test','fixture','running',$4)",
      [runId, scope.tenantId, sessionId, scope.writerId],
    );
    const binding = randomUUID();
    bindings.set(runId, binding);
    await router.spans.activate(
      scope,
      1,
      { topic, partition: 0, offset: off },
      binding,
      () => true,
    );
    for (const f of prefix) await seed({ topic, partition: 0, offset: off++, fact: f });
    const record = { topic, partition: 0, fact: intent };
    assert(router.canGroupOperation(record));
    assert(
      router.canGroupOperation({ ...record, fact: withDisplay }),
      "Matching native tool start must be eligible",
    );
    const other = {
      ...withDisplay,
      workspaceOperations: operationsFor(withDisplay).map(
        ({ action, operationId, taskId, reply, maximumOutputBytes, timeoutMs }) => ({
          action,
          operationId,
          taskId,
          reply,
          maximumOutputBytes,
          timeoutMs,
        }),
      ),
      display: withDisplay.display.map((d) => ({ ...d, coveredThrough: 999 })),
    };
    assert(
      !router.canGroupOperation({
        ...record,
        fact: decodeWorkspaceFact(encodeWorkspaceFact(other)).fact,
      }),
    );
    const wrongTask = structuredClone(other);
    delete wrongTask.display[0].coveredThrough;
    wrongTask.display[0].events[0].event.payload.toolCallId = "task:999";
    assert(
      !router.canGroupOperation({
        ...record,
        fact: decodeWorkspaceFact(encodeWorkspaceFact(wrongTask)).fact,
      }),
    );
    const wrongRef = structuredClone(other);
    delete wrongRef.display[0].coveredThrough;
    wrongRef.display[0].events[0].source.ordinal = withDisplay.commit.writes.findIndex(
      (w) => w.type === "document.change",
    );
    assert(
      !router.canGroupOperation({
        ...record,
        fact: decodeWorkspaceFact(encodeWorkspaceFact(wrongRef)).fact,
      }),
    );
    records.push({ ...record, fact: withDisplay });
  }
  for (const r of records) r.offset = off++;
  const calls = [],
    before = view.stats.commits;
  commitBefore = before;
  const priorRefs = await db.query("select projection_tools from runs where id=$1", [
    records[1].fact.scope.runId,
  ]);
  const op = operationsFor(records[1].fact)[0];
  await db.query(
    "update runs set projection_tools=jsonb_build_object($2::text,'conflict'::text) where id=$1",
    [records[1].fact.scope.runId, String(op.taskId)],
  );
  const options = {
    batchProjection: true,
    limit: 8,
    sender,
    router,
    targetFor: (r) => r.fact.scope.sessionId,
    prepareSend: () => assert.fail("wire prepared after failed PG"),
  };
  await assert.rejects(
    prepareToolGrants(
      records,
      () => true,
      () => assert.fail("grant after failed PG"),
      options,
    ),
    { code: "PZ002" },
  );
  assert.deepEqual(displayed, []);
  assert.equal(view.stats.commits, before);
  await db.query("update runs set projection_tools=$2::jsonb where id=$1", [
    records[1].fact.scope.runId,
    JSON.stringify(priorRefs.rows[0].projection_tools),
  ]);
  const n = await prepareToolGrants(
    records,
    () => true,
    async (count) => {
      grantObtained = true;
      assert.deepEqual(
        displayed,
        records.map((r) => r.fact.factId),
      );
      assert.equal(count, 2);
      assert.equal(
        await db
          .query("select 1 from runs where id=any($1::uuid[]) and projection_tools<>'{}'::jsonb", [
            records.map((r) => r.fact.scope.runId),
          ])
          .then((r) => r.rowCount),
        2,
      );
      return 0;
    },
    {
      batchProjection: true,
      limit: 8,
      sender,
      router,
      targetFor: (r) => r.fact.scope.sessionId,
      prepareSend: (r, { command, op, route }) => {
        assert.equal(route.bindingId, bindings.get(r.fact.scope.runId));
        assert.equal(command.request.args.timeout, 10);
        return () => calls.push(op.operationId);
      },
    },
  );
  assert.equal(n, 2);
  for (const r of records) await router.projectOperation(r, () => true);
  assert.deepEqual(
    displayed,
    records.map((r) => r.fact.factId),
    "replay must not repeat the start",
  );
  assert.equal(view.stats.commits - before, 1);
  assert.deepEqual(
    calls,
    records.map((r) => operationsFor(r.fact)[0].operationId),
  );
  for (const r of records) {
    const change = r.fact.commit.writes.find((w) => w.type === "document.change");
    assert(
      view.sessions
        .get(r.fact.scope.runId)
        .liveDocs[change.id].value.tools.some(
          (s) => s.status === "running" && s.callId === operationsFor(r.fact)[0].reply.toolCallId,
        ),
    );
  }
}
