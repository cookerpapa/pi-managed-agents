// An optional baseline checkout shares the same disposable PG. Compare the
// complete claim, including the confirmed lease and inbox bootstrap.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { resolve, join } from "node:path";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { testPostgres } from "../../lib/test-postgres.mjs";
import { pathToFileURL } from "node:url";
import pg from "pg";
import { Kysely, PostgresDialect } from "kysely";
import { createDatabase, runMigrations } from "@pi-cloud/database";
import { createPrivateTenant } from "../../../packages/control-plane/src/tenant-administration.ts";
import { registerTestWorker } from "../../../packages/control-plane/test/fixtures/registered-worker.ts";
import { IndependentSessionRuntime } from "./independent-session-runtime.mjs";
import { installIndependentSessionLeases, SegmentWorker } from "./independent-session-leases.mjs";
import { installForkTables, setForkLayout } from "./fork-storage.mjs";
import { installPagingProjection } from "./flow-paging-fixture.mjs";

const baseline =
  process.argv[2] &&
  (
    await import(
      pathToFileURL(
        resolve(
          process.argv[2],
          "scripts/experiments/minimal-session-storage/independent-session-leases.mjs",
        ),
      ).href
    )
  ).SegmentWorker;
const fixture = await testPostgres({}),
  endpoint = fixture.url;
const cache = await mkdtemp(join(tmpdir(), "claim-cost-"));
const schema = "claim_" + randomUUID().replaceAll("-", ""),
  url = new URL(endpoint);
url.searchParams.set("options", "-c search_path=" + schema + ",public");
const control = createDatabase({ connectionString: endpoint }),
  pool = new pg.Pool({ connectionString: url.href });
const runtime = new IndependentSessionRuntime({ pool, topic: "cost" });
let queries = 0,
  offset = 0n;
const db = new Kysely({
  dialect: new PostgresDialect({ pool: new pg.Pool({ connectionString: url.href }) }),
  log: (event) => {
    if (event.level === "query") queries++;
  },
});
const arms = [];
try {
  await runMigrations(control, "up");
  const { tenantId } = await createPrivateTenant(control, {
    slug: schema.replace("_", "-"),
    ownerDisplayName: "Claim measurement",
  });
  await pool.query("create schema " + schema);
  await installPagingProjection(pool);
  await installForkTables(pool);
  await setForkLayout(pool, true, false);
  await installIndependentSessionLeases(pool);
  for (const [label, Worker] of [
    ["candidate", SegmentWorker],
    ["baseline", baseline],
  ]) {
    if (!Worker) continue;
    const workerId = randomUUID(),
      bootId = randomUUID();
    await control
      .insertInto("workers")
      .values({
        id: workerId,
        supervisor_id: workerId,
        boot_id: bootId,
        state: "ready",
        max_concurrent_sessions: 32,
      })
      .execute();
    await registerTestWorker(control, workerId);
    arms.push({
      label,
      worker: new Worker({
        database: db,
        workerId,
        bootId,
        capacity: 32,
        leaseDurationMs: 120000,
        historyCache: { diskDirectory: join(cache, workerId) },
      }),
    });
  }
  for (let round = 0; round < 6; round++) {
    for (const size of round % 2 ? [32, 8, 1] : [1, 8, 32]) {
      for (const arm of round % 2 ? arms.toReversed() : arms) {
        const ids = Array.from({ length: size }, () => randomUUID());
        await runtime.store.register(tenantId, ids);
        await pool.query(
          "insert into turns(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,state) select gen_random_uuid(),$1,s,1,s,'cost','queued' from unnest($2::uuid[]) s",
          [tenantId, ids],
        );
        queries = 0;
        const start = performance.now(),
          jobs = await arm.worker.claimBatch(size);
        const ms = performance.now() - start;
        assert.equal(jobs.length, size);
        if (round > 0) console.log(JSON.stringify({ arm: arm.label, size, ms, queries }));
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
            await runtime.leases.projectBoundary(
              { fact, topic: "cost", partition: 0, offset: offset++ },
              async () => {},
            ),
          );
          await job.release();
        }
      }
    }
  }
} finally {
  for (const { worker } of arms) await worker.close();
  await db.destroy();
  await runtime.store.db.destroy();
  await control.destroy();
  await fixture.close();
  await rm(cache, { recursive: true, force: true });
}
