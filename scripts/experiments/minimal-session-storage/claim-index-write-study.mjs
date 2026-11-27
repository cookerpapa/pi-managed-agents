// Compare the index on identical logical lifecycles. Both schemas use the
// current Worker SELECT, so the only difference is index maintenance/access.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import pg from "pg";
import { createDatabase, runMigrations } from "@pi-cloud/database";
import { testPostgres } from "../../lib/test-postgres.mjs";
import { createPrivateTenant } from "../../../packages/control-plane/src/tenant-administration.ts";
import { registerTestWorker } from "../../../packages/control-plane/test/fixtures/registered-worker.ts";
import { installPagingProjection } from "./flow-paging-fixture.mjs";
import { installForkTables, setForkLayout } from "./fork-storage.mjs";
import { installIndependentSessionLeases } from "./independent-session-leases.mjs";
import { IndependentSessionRuntime } from "./independent-session-runtime.mjs";

const size = 64,
  topic = "index-cost",
  fixture = await testPostgres({}),
  admin = createDatabase({ connectionString: fixture.url });
const cache = await mkdtemp(join(tmpdir(), "turn-index-")),
  arms = [];
async function measure(pool, operation) {
  const {
    rows: [before],
  } = await pool.query("select pg_current_wal_insert_lsn() as lsn");
  const start = performance.now();
  await operation();
  const ms = performance.now() - start;
  const {
    rows: [after],
  } = await pool.query("select pg_wal_lsn_diff(pg_current_wal_insert_lsn(),$1)::float8 as bytes", [
    before.lsn,
  ]);
  return { ms, bytes: after.bytes };
}
try {
  await runMigrations(admin, "up");
  const { tenantId } = await createPrivateTenant(admin, {
    slug: "claim-index-cost",
    ownerDisplayName: "Study",
  });
  for (const schema of ["baseline", "indexed"]) {
    const url = new URL(fixture.url);
    url.searchParams.set("options", "-c search_path=" + schema + ",public");
    const pool = new pg.Pool({ connectionString: url.href });
    const runtime = new IndependentSessionRuntime({ pool, topic });
    const workerId = randomUUID(),
      bootId = randomUUID(),
      sessions = Array.from({ length: size }, () => randomUUID());
    const worker = runtime.worker({
      workerId,
      bootId,
      capacity: size,
      leaseDurationMs: 120000,
      historyCache: { diskDirectory: join(cache, schema) },
    });
    arms.push({ schema, pool, runtime, worker, sessions, offset: 0n });
    await pool.query("create schema " + schema);
    await installPagingProjection(pool);
    await installForkTables(pool);
    await setForkLayout(pool, true, false);
    await installIndependentSessionLeases(pool);
    if (schema === "baseline") await pool.query("drop index turns_unsettled_by_session");
    await admin
      .insertInto("workers")
      .values({
        id: workerId,
        supervisor_id: workerId,
        boot_id: bootId,
        state: "ready",
        max_concurrent_sessions: size,
      })
      .execute();
    await registerTestWorker(admin, workerId);
    await runtime.store.register(tenantId, sessions);
  }
  for (let turn = 1; turn <= 10; turn++) {
    for (const arm of turn % 2 ? arms : arms.toReversed()) {
      const { pool, worker, sessions } = arm;
      const enqueue = await measure(pool, async () => {
        const res = await pool.query(
          `insert into turns(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,state)
        select gen_random_uuid(),$1,s,$3,gen_random_uuid(),'queued','queued' from unnest($2::uuid[]) s`,
          [tenantId, sessions, turn],
        );
        assert.equal(res.rowCount, size);
      });
      const jobs = [];
      const claim = await measure(pool, async () => {
        while (jobs.length < size) {
          const batch = await worker.claimBatch(8);
          assert.equal(batch.length, 8);
          jobs.push(...batch);
        }
      });
      const finish = await measure(pool, async () => {
        for (const job of jobs) {
          const fact = {
            kind: "work_finished",
            factId: randomUUID(),
            scope: job.scope,
            agentId: "root",
            nativeSequence: 0,
            baseSequence: 0,
            occurredAt: new Date().toISOString(),
            terminal: { type: "turn.completed", payload: { stopReason: "stop" } },
          };
          assert(
            await arm.runtime.leases.projectBoundary(
              { fact, topic, partition: 0, offset: arm.offset++ },
              async () => {},
            ),
          );
          await job.release();
        }
      });
      const {
        rows: [counts],
      } = await pool.query(
        `select count(*)::int n from turns where mailbox_position=$1
      and state='completed' and output_sealed_at is not null`,
        [turn],
      );
      assert.equal(counts.n, size);
      if (turn > 2)
        console.log(JSON.stringify({ turn, arm: arm.schema, size, enqueue, claim, finish }));
    }
  }
} finally {
  for (const arm of arms) {
    await arm.worker.close();
    await arm.runtime.store.db.destroy();
  }
  await admin.destroy();
  await fixture.close();
  await rm(cache, { recursive: true, force: true });
}
