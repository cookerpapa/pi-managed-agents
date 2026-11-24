// Check the existing public readers, replay and retention at a suspended Bash,
// before writing any seal-time snapshot. All data and truncation bounds are synthetic.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { sql, Kysely, PostgresDialect } from "kysely";
import { kafkaProducerLane } from "@pi-cloud/runtime-core/kafka-accepted-fact";
import {
  AcceptedFamilyCheckpoint,
  saveSealedFamily,
  readSealedFamily,
} from "@pi-cloud/pi-session-postgres/durable";
import { FamilyPresentation } from "./family-presentation.mjs";
import { nativeBashTrace, nativeBashFact } from "./native-bash-trace.mjs";
import { decodeWorkspaceFact, encodeWorkspaceFact } from "./workspace-operations.mjs";
import { normalRootProjection } from "./normal-root-projection.mjs";

export async function checkMemoryWait(api, tenant, install) {
  const { db } = api,
    topic = "wait-crash",
    session = await api.createScrollWindow("Pending operation");
  const run = randomUUID(),
    scope = {
      tenantId: tenant,
      sessionId: session,
      piSessionId: session,
      runId: run,
      turnId: run,
      writerId: randomUUID(),
      fencingToken: 1,
    };
  const partition = kafkaProducerLane(session, 32),
    replyTopic = "pi-cloud.tool-replies.v4." + randomUUID();
  const trace = await nativeBashTrace(scope, undefined, { callId: "original-call" });
  const presenter = new FamilyPresentation();
  presenter.bind(1, scope);
  const wire = [];
  for (const commit of trace) {
    const d = presenter.prepare(commit),
      f = nativeBashFact(scope, commit, d.display, randomUUID(), replyTopic);
    wire.push(encodeWorkspaceFact(f));
    d.confirmed();
    if (f.workspaceOperations.length) break;
  }
  const decode = (i) => ({
    topic,
    partition,
    offset: BigInt(i),
    fact: decodeWorkspaceFact(Buffer.from(wire[i])).fact,
  });
  const high = BigInt(wire.length);
  const fresh = () =>
    normalRootProjection(
      db,
      topic,
      () => ({ bindingId: run, allocationId: run }),
      () => assert.fail("recovery dispatched an operation"),
      undefined,
      { partitions: 32 },
    );
  await db.query(
    "alter table runs add column fencing_token bigint, add column recovery_requested_at timestamptz",
  );
  const reader = new Kysely({ dialect: new PostgresDialect({ pool: db }) });
  await db.query("insert into sessions(id,tenant_id,flow_id) values($1,$2,$1)", [session, tenant]);
  for (const mode of ["baseline", "memory", "combined"]) {
    await install(mode);
    await db.query("delete from messages where run_id=$1", [run]);
    await db.query("delete from inboxes where pi_session_id=$1", [session]);
    await db.query("delete from runs where id=$1", [run]);
    await db.query(
      "insert into runs(id,tenant_id,session_id,lease_id,initial_input,model_ref,idempotency_key,mailbox_position,state) values($1,$2,$3,$4,'wait','fixture',$1,1,'running')",
      [run, tenant, session, scope.writerId],
    );
    let p = fresh();
    await p.view.reset(new Map([[partition, 0n]]), 16, "recorded-history");
    let family = new AcceptedFamilyCheckpoint();
    for (let i = 0; i < wire.length; i++) {
      const r = decode(i);
      assert((await p.nativeStore.projection.project(r.fact, r, () => true)).admitted);
      family.observe(r.fact);
    }
    const original = Object.values(p.view.sessions.get(run).pendingTools)[0];
    assert.equal(original.reply.topic, replyTopic);
    assert.equal(
      original.assistantEntryId,
      trace
        .flatMap((c) => c.writes)
        .find((w) => w.type === "entry" && w.value.kind === "pi.assistant").value.id,
    );
    const protectedReply = async () =>
      (await p.nativeStore.brokerTimedRetention.replyFloors(reader, [replyTopic])).rows;
    const pendingPage = async () =>
      (await api.page(session, {})).turns
        .flatMap((t) => t.transcript?.items ?? [])
        .filter((i) => i.kind === "tool" && i.status === "running");
    const floor = await protectedReply(),
      pending = await pendingPage();
    assert.equal(floor.length, mode === "memory" ? 0 : 1);
    assert.equal(pending.length, mode === "memory" ? 0 : 1);
    p.view.revoke();
    p = fresh();
    family = new AcceptedFamilyCheckpoint();
    const bounds = { partition, low: 0n, high };
    const start = (await p.view.recover([bounds])).get(partition);
    assert.equal(typeof start, "bigint");
    // The native family's replay floor and the Projector layout checkpoint differ.
    for (let i = 0; i < wire.length; i++) {
      const r = decode(i);
      if (BigInt(i) >= start) await p.nativeStore.projection.project(r.fact, r, () => true);
      family.observe(r.fact);
    }
    const restored = p.view.sessions.get(run).pendingTools;
    const wait = restored[original.taskId];
    assert.deepEqual(wait.reply, original.reply);
    assert.equal(wait.operationId, original.operationId);
    // A handoff can publish the recovered references with the native current
    // values. Exercise abort and commit, and use the existing PG page/floor readers.
    async function handoff(tx, abort) {
      const ready = await sql`update runs set projection_tools=${JSON.stringify(restored)}::jsonb
        where id=${run}::uuid and lease_id=${scope.writerId}::uuid and state='running' returning id`.execute(
        tx,
      );
      assert.equal(ready.rows.length, 1);
      await saveSealedFamily(tx, scope, { topic, partition, offset: high }, run, family.snapshot());
      if (abort) throw Error("abort fixture handoff");
    }
    await db.query(
      "update runs set output_seal_id=id,fencing_token=1,recovery_requested_at=now() where id=$1",
      [run],
    );
    await assert.rejects(
      reader.transaction().execute((tx) => handoff(tx, true)),
      /abort fixture/,
    );
    assert.equal((await protectedReply()).length, floor.length);
    // Once the necessary Kafka offset is gone, a new Projector must refuse the rewind.
    const missing = (await fresh().view.recover([{ ...bounds, low: high }])).get(partition);
    assert(missing instanceof Error);
    await reader.transaction().execute((tx) => handoff(tx, false));
    assert.equal((await pendingPage()).length, 1);
    assert.equal((await protectedReply())[0].floor, original.reply.offset);
    const sealed = (
      await db.query("select projection_tools,native_work_handoff from runs where id=$1", [run])
    ).rows[0];
    assert.equal(
      sealed.projection_tools[original.taskId].reply.deadlineAt,
      original.reply.deadlineAt,
    );
    assert.equal(JSON.parse(sealed.native_work_handoff.nativeText).seq, trace[wire.length - 1].seq);
    const successor = { ...scope, writerId: randomUUID(), fencingToken: 2 };
    await db.query("delete from session_leases where pi_session_id=$1", [session]);
    await db.query(
      `insert into session_leases(tenant_id,pi_session_id,lease_id,fencing_token,writer_sealed_at,released_at,writer_seal_offset)
      values($1,$2,$3,1,now(),now(),$5),($1,$2,$4,2,null,null,null)`,
      [tenant, session, scope.writerId, successor.writerId, String(high)],
    );
    await db.query("update runs set lease_id=$2,fencing_token=2 where id=$1", [
      run,
      successor.writerId,
    ]);
    const cold = await readSealedFamily(reader, successor, topic);
    assert.equal(cold.intents.length, 1);
    assert.equal(cold.intents[0].operationId, original.operationId);
    assert.deepEqual(cold.intents[0].reply, original.reply);
    console.log(
      JSON.stringify({
        kind: "wait-fault",
        mode,
        pendingPageBeforeSeal: pending.length,
        replyFloorsBeforeSeal: floor.length,
        originalWaitAfterReplay: true,
        exactResourceReference: wait.allocationSpanStart === original.allocationSpanStart,
        originalDeadlineAfterHandoff: true,
        expiredRequiredOffsetRefused: true,
      }),
    );
  }
  await db.query("delete from messages where run_id=$1", [run]);
  await db.query("delete from runs where id=$1", [run]);
  await install("baseline");
}
