// Use a private PG schema. Race the serving idle-expiry SQL against candidate
// admission in separate transactions, including a committed Run and a retry.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { setTimeout as delay } from "node:timers/promises";
import pg from "pg";
import { Kysely, PostgresDialect } from "kysely";
import { it } from "vitest";
import { claimIdleExpiry } from "../../../packages/sandbox-control/src/resource-idle-expiry.ts";
import { selectedRunEnvironment } from "./admission-retention.mjs";
import { installContinuationEnvironments, selectEnvironment } from "./continuation-cases.mjs";
import { continuationPrompt } from "./continuation-environment.mjs";
import { installRetentionFixture, seedElasticWorkspace } from "./retention-fixture.mjs";

it("orders Run admission with idle expiry, rollback, redelivery and compute scope", async () => {
  const name = "retention_" + randomUUID().replaceAll("-", "");
  const url = process.env.PI_CLOUD_POSTGRES_INTEGRATION_URL;
  assert(url);
  const db = new pg.Pool({ connectionString: url, options: "-c search_path=" + name, max: 6 });
  const ownerDb = new Kysely({ dialect: new PostgresDialect({ pool: db }) });
  const session = randomUUID(),
    tenant = randomUUID(),
    owner = randomUUID(),
    workspace = randomUUID();
  const selection = { workspace, cwd: "/workspace/game", executionMode: "elastic" };
  const expire = (tx) => claimIdleExpiry(tx, "study", owner, "workspace", 1);
  const client = await db.connect();
  try {
    await db.query("create schema " + name);
    await db.query(await readFile(new URL("./schema.sql", import.meta.url), "utf8"));
    await installContinuationEnvironments(db);
    await installRetentionFixture(db);
    await selectEnvironment(db, tenant, session, selection);
    await seedElasticWorkspace(db, tenant, workspace, "unallocated");
    const observe = (key) => selectedRunEnvironment(client, tenant, session, key);
    const due = () =>
      db.query(
        "update workspace_runtimes set idle_expires_at=clock_timestamp()-interval '1 minute' where owner_instance_id=$1",
        [owner],
      );
    const deadline = async () =>
      (
        await db.query(
          "select idle_expires_at from workspace_runtimes where owner_instance_id=$1",
          [owner],
        )
      ).rows[0].idle_expires_at.getTime();
    // Another scope on the same Workspace cannot make this pure chat look warm.
    await seedElasticWorkspace(db, tenant, workspace, "retained", randomUUID());
    await client.query("begin");
    assert.deepEqual((await observe(randomUUID())).elastic, { state: "unallocated" });
    await client.query("commit");
    assert.equal((await db.query("select count(*)::int n from workspace_runtimes")).rows[0].n, 1);

    await seedElasticWorkspace(db, tenant, workspace, "retained", null, owner);
    // All comparisons happen where the allocation is retained. Neither the
    // current allocation nor the previous ID is copied to the model snapshot.
    const allocations = (
      await db.query("select workspace_runtime_id as id,compute_session_id from workspace_runtimes")
    ).rows;
    const same = allocations.find((r) => !r.compute_session_id).id;
    const otherScope = allocations.find((r) => r.compute_session_id).id;
    await seedElasticWorkspace(db, tenant, workspace, "released");
    const old = (
      await db.query(
        "select workspace_runtime_id as id from workspace_runtimes where state='released'",
      )
    ).rows[0].id;
    for (const [id, continuity] of [
      [same, undefined],
      [otherScope, "different"],
      [old, "replaced"],
      [randomUUID(), "different"],
    ]) {
      await client.query("begin");
      await client.query(
        "insert into conversation_worlds(tenant_id,pi_session_id,conversation_id,state) values($1,$2,1,$3)",
        [tenant, session, { sandbox: { status: "active", continuityId: id } }],
      );
      const info = (await observe(randomUUID())).elastic;
      assert.equal(info.continuity, continuity);
      assert(!JSON.stringify(info).includes(id));
      await client.query("rollback");
    }
    const past = await deadline();
    await client.query("begin");
    assert.equal((await observe(randomUUID())).elastic.state, "retained");
    assert.deepEqual(await ownerDb.transaction().execute(expire), []);
    await client.query("rollback");
    assert.equal(await deadline(), past);

    const key = randomUUID();
    await client.query("begin");
    const captured = await observe(key);
    await client.query(
      `insert into runs(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,model_ref,state,environment_snapshot)
       values($1,$2,$3,1,$1,'chat','local','queued',$4)`,
      [key, tenant, session, captured],
    );
    await client.query("commit");
    const until = await deadline();
    assert.equal(new Date(captured.elastic.until).getTime(), until);
    assert(until > Date.now() + 29 * 60000);
    assert.deepEqual(await ownerDb.transaction().execute(expire), []);
    await client.query("begin");
    assert.deepEqual(await observe(key), captured);
    await client.query("commit");
    assert.equal(await deadline(), until);
    assert.match(
      continuationPrompt(null, captured, until + 1),
      /deadline recorded at admission has passed/,
    );

    await due();
    // Hold the expiry transaction open until PG reports the admission reader
    // waiting on it, then commit. A stale allocated row must never be renewed.
    const closing = Promise.withResolvers(),
      release = Promise.withResolvers();
    const expiry = ownerDb.transaction().execute(async (tx) => {
      assert.equal((await expire(tx)).length, 1);
      closing.resolve();
      await release.promise;
    });
    try {
      await Promise.race([closing.promise, expiry]);
      await client.query("begin");
      const waiting = observe(randomUUID());
      try {
        const timeout = Date.now() + 3000;
        for (;;) {
          const wait = await db.query("select cardinality(pg_blocking_pids($1)) as n", [
            client.processID,
          ]);
          if (wait.rows[0].n) break;
          assert(Date.now() < timeout, "expected PG lock wait");
          await delay(10);
        }
      } finally {
        release.resolve();
      }
      await expiry;
      assert.deepEqual((await waiting).elastic, { state: "closing" });
      await client.query("commit");
    } finally {
      release.resolve();
      await expiry;
    }
    assert((await deadline()) < Date.now());
    await db.query(
      "update workspace_runtimes set state='released',idle_expires_at=null where owner_instance_id=$1",
      [owner],
    );
    await client.query("begin");
    assert.deepEqual((await observe(randomUUID())).elastic, { state: "released" });
    assert.deepEqual(await observe(key), captured);
    await client.query("commit");
    assert.equal((await db.query("select count(*)::int n from runs")).rows[0].n, 1);
  } finally {
    await client.query("rollback");
    client.release();
    try {
      await db.query("drop schema if exists " + name + " cascade");
    } finally {
      await ownerDb.destroy();
    }
  }
});

it("bounds the released lookup for a pure chat among 32768 historical allocations", async () => {
  const db = new pg.Client({ connectionString: process.env.PI_CLOUD_POSTGRES_INTEGRATION_URL });
  const schema = "lookup_" + randomUUID().replaceAll("-", "");
  const tenant = randomUUID(),
    session = randomUUID(),
    workspace = randomUUID(),
    other = randomUUID();
  await db.connect();
  try {
    await db.query("begin");
    await db.query("create schema " + schema);
    await db.query("set local search_path=" + schema);
    await installRetentionFixture(db);
    await installContinuationEnvironments(db);
    await db.query(await readFile(new URL("./schema.sql", import.meta.url), "utf8"));
    await seedElasticWorkspace(db, tenant, workspace, "unallocated");
    await seedElasticWorkspace(db, tenant, other, "unallocated");
    await db.query("insert into study_session_environments values($1,$2,$3)", [
      tenant,
      session,
      { workspace, executionMode: "elastic", cwd: "/workspace" },
    ]);
    await db.query(
      `insert into workspace_runtimes(workspace_runtime_id,tenant_id,workspace_id,compute_session_id,sandbox_domain_id,
      owner_instance_id,state,allocation_closed,creation_spec) select gen_random_uuid(),$1,$2,gen_random_uuid(),'study',
      $3,'released',true,'{}'::jsonb from generate_series(1,32768)`,
      [tenant, other, randomUUID()],
    );
    await db.query("analyze workspace_runtimes");
    let plan;
    const inspected = {
      async query(sql, args) {
        if (sql.includes("state='released' limit 1")) {
          const { rows } = await db.query("explain (analyze,buffers,format json) " + sql, args);
          plan = rows[0]["QUERY PLAN"][0].Plan.Plans[0];
        }
        return db.query(sql, args);
      },
    };
    assert.equal(
      (await selectedRunEnvironment(inspected, tenant, session, randomUUID())).elastic.state,
      "unallocated",
    );
    assert.equal(plan["Index Name"], "workspace_compute_released_scope");
    const indexedBlocks = plan["Shared Hit Blocks"] + plan["Shared Read Blocks"];
    await db.query("drop index workspace_compute_released_scope");
    await selectedRunEnvironment(inspected, tenant, session, randomUUID());
    assert.equal(plan["Rows Removed by Filter"], 32768);
    console.log(
      "Released allocation lookup:",
      JSON.stringify({
        indexedBlocks,
        unindexedBlocks: plan["Shared Hit Blocks"] + plan["Shared Read Blocks"],
        unindexedRowsRejected: plan["Rows Removed by Filter"],
      }),
    );
  } finally {
    await db.query("rollback");
    await db.end();
  }
});
