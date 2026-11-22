// Both control intents exist in both arms. Vary only which stop RPC reaches
// Pi before child creation. Use the public Session host and real candidate PG.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import pg from "pg";
import { createDatabase, runMigrations } from "@pi-cloud/database";
import { indexNative } from "../../../packages/pi-session-postgres/src/durable/types.ts";
import { BACKGROUND_CONTEXT as ctx } from "@earendil-works/chord/context";
import { MemoryStorage } from "@earendil-works/pi-durable";
import { createModels } from "@earendil-works/pi-ai/models";
import { DurableAgentSession } from "../../../packages/sandbox-supervisor/src/durable/agent-session.ts";

async function deliver(trace, targets, delivered) {
  class Recorded extends MemoryStorage {
    appended = [];
    async commit(writes, context) {
      const p = this.prepareCommit(writes);
      this.appended.push({ seq: p.seq, nextId: this.nextId, writes: structuredClone(p.writes) });
      return p.apply();
    }
  }
  const store = new Recorded();
  for (const b of trace.batches) store.prepareCommit(b.writes, b.seq).apply();
  store.nextId = trace.batches.at(-1).nextId;
  const session = await DurableAgentSession.open(store, {
    models: createModels(),
    remoteTools: [],
  });
  try {
    // Identical requested addresses and ambiguous responses in each arm. The
    // fixture controls delivery without simulating a network or replaying a tool.
    const acknowledgements = [];
    for (const [i, key] of targets.entries()) {
      if (i === delivered) await session.stopConversation(key);
      acknowledgements.push("unconfirmed");
    }
    assert.deepEqual(acknowledgements, ["unconfirmed", "unconfirmed"]);
    const observed = [];
    for (const key of targets)
      observed.push(
        !!(await store.findDocument(
          { kind: "pi-cloud.conversation-stop", key, scope: { kind: "session" } },
          "current",
          ctx,
        )),
      );
    assert.deepEqual(
      observed,
      targets.map((_, i) => i === delivered),
    );
    return { observed, trace: { ...trace, batches: [...trace.batches, ...store.appended] } };
  } finally {
    await session.close();
  }
}
// Run the migrated legacy view unmodified against the same two pending
// control relationships. Only the switch-specific native field index differs.
async function referenceStates(base, admin, scope, targets, variants) {
  const name = "child_status_" + randomUUID().replaceAll("-", "");
  await admin.query("create database " + name);
  const u = new URL(base);
  u.pathname = "/" + name;
  const db = createDatabase({ connectionString: u.href }),
    client = new pg.Client({ connectionString: u.href });
  try {
    await runMigrations(db, "up");
    await client.connect();
    await client.query("set search_path=pg_catalog");
    const {
      rows: [ref],
    } = await client.query(
      "select pg_get_viewdef('public.subagent_executions'::regclass,true) as definition",
    );
    const relations = new Set(
      [...ref.definition.matchAll(/(?:FROM|JOIN) public\.([a-z_]+)/gi)].map((m) => m[1]),
    );
    for (const required of ["subagent_conversations", "runs", "sessions", "pi_durable_records"])
      assert(relations.has(required));
    await client.query("create schema witness");
    for (const table of relations)
      await client.query(
        "create table witness." + table + " as table public." + table + " with no data",
      );
    await client.query(
      "create view witness.execution_status as " + ref.definition.replaceAll("public.", "witness."),
    );
    await client.query("insert into witness.runs(id,tenant_id,state) values($1,$2,'completed')", [
      scope.runId,
      scope.tenantId,
    ]);
    for (const [i, id] of targets.entries()) {
      const session = randomUUID();
      await client.query(
        "insert into witness.sessions(tenant_id,id,pi_session_id) values($1,$2,$3)",
        [scope.tenantId, session, scope.piSessionId],
      );
      await client.query(
        "insert into witness.subagent_conversations(tenant_id,id,root_run_id,child_session_id,parent_tool_call_id) values($1,$2,$3,$4,$5)",
        [scope.tenantId, id, scope.runId, session, "task:" + (i + 50)],
      );
    }
    const pairs = [];
    for (const variant of variants) {
      await client.query("truncate witness.pi_durable_records");
      for (const b of variant.trace.batches)
        for (const [ordinal, w] of b.writes.entries()) {
          if (w.record?.kind !== "pi-cloud.conversation-stop") continue;
          const v = indexNative("document", w.record, b.seq, ordinal);
          await client.query(
            "insert into witness.pi_durable_records(tenant_id,session_id,id,kind,fields) values($1,$2,$3,'document',$4)",
            [scope.tenantId, scope.piSessionId, w.record.id, v.fields],
          );
        }
      const { rows } = await client.query(
        "select state from witness.execution_status order by array_position($1::uuid[],id)",
        [targets],
      );
      pairs.push(rows.map((r) => r.state));
    }
    return pairs;
  } finally {
    try {
      await client.end();
      await db.destroy();
    } finally {
      await admin.query("drop database " + name);
    }
  }
}
export async function childStopEvidence(base, admin, scope, nativeHistory, pgHistory) {
  const trace = await nativeHistory("read"),
    targets = [randomUUID(), randomUUID()];
  const left = await deliver(trace, targets, 0),
    right = await deliver(trace, targets, 1);
  assert.equal(left.trace.batches.length, right.trace.batches.length);
  const ids = Array.from({ length: left.trace.batches.length + 1 }, () => randomUUID());
  const a = await pgHistory(base, admin, scope, left.trace, ids),
    b = await pgHistory(base, admin, scope, right.trace, ids);
  assert.deepEqual(a.retained, b.retained);
  assert.deepEqual(a.page, b.page);
  const legacy = await referenceStates(base, admin, scope, targets, [left, right]);
  assert.deepEqual(legacy, [
    ["cancelled", "unknown"],
    ["unknown", "cancelled"],
  ]);
  console.log(
    JSON.stringify(
      {
        comparisons: 2,
        legacyStates: legacy,
        nativeStopReceipts: [left.observed, right.observed],
        nativeCommitsPerArm: left.trace.batches.length,
        comparedCandidateTables: Object.keys(a.retained),
        sameRetainedRows: true,
        sameForkPage: true,
        verdict: "settled_candidate_cannot_identify_the_child_whose_stop_was_accepted",
      },
      null,
      2,
    ),
  );
}
