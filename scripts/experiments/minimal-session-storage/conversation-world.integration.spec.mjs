import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import pg from "pg";
import { it } from "vitest";
import { installWorkspaceRoutes } from "./workspace-route-spans.mjs";
import { ForkStorage, setForkLayout } from "./fork-storage.mjs";
import { projectWorld, worldBaseWrite } from "./conversation-world.mjs";
import { installPrefixSql } from "./ready-projection-sql.mjs";
import { projectionTemplates } from "./projection-statements.mjs";

it("copies a lane's current observation once and preserves independent forks through prune and deletion", async () => {
  const schema = "world_" + randomUUID().replaceAll("-", "");
  const pool = new pg.Pool({
    connectionString: process.env.PI_CLOUD_POSTGRES_INTEGRATION_URL,
    options: "-c search_path=" + schema,
  });
  const forks = new ForkStorage(pool),
    tenant = randomUUID(),
    source = randomUUID(),
    writer = randomUUID();
  const read = (file) => readFile(new URL(file, import.meta.url), "utf8");
  let offset = 0;
  const active = async (session) => {
    const run = randomUUID();
    await pool.query(
      "insert into runs(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,model_ref,state,lease_id) values($1,$2,$3,1,$1,'chat','local','running',$4)",
      [run, tenant, session, writer],
    );
    return run;
  };
  const settled = async (session, run) => {
    assert.equal(
      (
        await pool.query("select * from prefix_finish($1,$2,$3,$4,$5)", [
          tenant,
          session,
          run,
          writer,
          "{}",
        ])
      ).rowCount,
      1,
    );
  };
  const project = async (view, session, run, writes, asWriter = writer) => {
    const frame = { offset: String(offset), ops: [], history: true };
    projectWorld(
      view,
      {
        scope: { tenantId: tenant, piSessionId: session, runId: run, writerId: asWriter },
        commit: { writes },
      },
      frame,
    );
    if (!frame.ops.length) return "memory";
    const value = await pool.query(
      "select project_ready('world',0,$1,$2,$3::jsonb,true) as result",
      [offset, offset + 1, JSON.stringify([frame])],
    );
    offset++;
    return value.rows[0].result;
  };
  const state = (generation) => ({
    schemaVersion: 3,
    sandbox: { status: "active", continuityId: generation },
    workspaceBindingSha256: "b".repeat(64),
    environmentSha256: "a".repeat(64),
    toolPolicySha256: "c".repeat(64),
  });
  const last = async (s) => (await forks.rootWorld(tenant, s))?.sandbox.continuityId;
  try {
    await pool.query("create schema " + schema);
    await pool.query(await read("./schema.sql"));
    await installPrefixSql(pool, projectionTemplates);
    await pool.query(await read("./fork-layout.sql"));
    await setForkLayout(pool, true, false);
    await installWorkspaceRoutes(pool);
    await pool.query("insert into accepted_fact_projection_offsets values('world',0,0)");
    await forks.register(tenant, [source]);
    const run = await active(source),
      pt = { lane: 1, run, entry: 4 },
      a = randomUUID(),
      b = randomUUID(),
      child = randomUUID();
    const view = {};
    assert.equal(
      await project(view, source, run, [
        worldBaseWrite(8, state(a)),
        worldBaseWrite(11, state(child), 2),
      ]),
      "applied",
    );
    const change = (v) => ({
      type: "document.change",
      id: 8,
      content: { kind: "delta", version: 1, ops: [["s", ["state"], v]] },
    });
    assert.equal(
      await project({ ...view }, source, run, [change(state(b))], randomUUID()),
      "obsolete",
    );
    assert.equal(
      await project(view, source, run, [
        change({ ...state(b), sandbox: { status: "unavailable", continuityId: b } }),
      ]),
      "memory",
    );
    assert.equal(await last(source), a);
    for (const [lane, id] of [
      [1, 4],
      [2, 9],
    ]) {
      const row = {
        id,
        conversation: lane,
        ordinal: lane,
        role: "assistant",
        content: JSON.stringify({ model: [{ stopReason: "stop" }] }),
      };
      assert.equal(
        (
          await pool.query("select * from prefix_message($1,$2,$3,1,$4,$5)", [
            tenant,
            source,
            run,
            JSON.stringify([row]),
            writer,
          ])
        ).rows[0].writes,
        1,
      );
    }
    await settled(source, run);
    const key = randomUUID(),
      first = await forks.fork(tenant, source, pt, key),
      second = await forks.fork(tenant, source, pt);
    const childBranch = await forks.fork(tenant, source, { lane: 2, run, entry: 9 });
    assert.equal(await last(childBranch), child);
    const firstRun = await active(first);
    const oldSpan = offset;
    await pool.query(
      `insert into workspace_route_spans(run_id,conversation_id,tenant_id,pi_session_id,session_id,turn_id,
       topic,partition,first_offset,binding_id,writer_id,allocation_id)
       values($1,1,$2,$3::uuid::text,$3,$1,'world',0,$4,$5,$6,$7)`,
      [firstRun, tenant, first, oldSpan, randomUUID(), writer, a],
    );
    const reference = {
      operationId: randomUUID(),
      conversationId: 1,
      offset: String(oldSpan),
      allocationSpanStart: String(oldSpan),
    };
    const tool = async (kind) => {
      const op = {
        kind,
        args: [tenant, first, firstRun, writer, 12, reference],
        count: 1,
        confirmUse: kind === "tool-settled",
      };
      assert.equal(
        (
          await pool.query("select project_ready('world',0,$1,$2,$3) as result", [
            offset,
            offset + 1,
            JSON.stringify([{ offset: String(offset), ops: [op] }]),
          ])
        ).rows[0].result,
        "applied",
      );
      offset++;
    };
    await tool("tool");
    assert.equal(await project({}, first, firstRun, [worldBaseWrite(15, state(b))]), "applied");
    await tool("tool-settled"); // A late result from A cannot move B back to A.
    await settled(first, firstRun);
    assert.equal(await forks.fork(tenant, source, pt, key), first);
    assert.equal(await last(first), b);
    assert.equal(await last(second), a);
    const nested = await forks.fork(tenant, first, pt);
    await forks.retire(tenant, source, pt);
    assert.equal(await last(source), a);
    await forks.retire(tenant, source);
    await forks.retire(tenant, first);
    await forks.collect(tenant);
    assert.equal(await last(nested), b);
    assert.equal(await last(second), a);
    for (const id of [second, childBranch, nested]) await forks.retire(tenant, id);
    assert.equal(
      (await pool.query("select count(*)::int n from conversation_worlds")).rows[0].n,
      0,
    );
  } finally {
    try {
      await pool.query("drop schema if exists " + schema + " cascade");
    } finally {
      await forks.db.destroy();
    }
  }
});
