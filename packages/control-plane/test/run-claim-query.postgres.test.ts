import { randomUUID } from "node:crypto";
import { beforeAll, afterAll, it, expect } from "vitest";
import { sql, type Kysely, type CompiledQuery } from "kysely";
import { createDatabase, runMigrations, type Database } from "@pi-cloud/database";
import { ControlPlaneStore } from "../src/control-plane-store.ts";
import { createPrivateTenant } from "../src/tenant-administration.ts";
import { createTestWorker } from "./admit-test-execution.ts";
import type { RunClaimAdmission } from "../../runtime-core/src/run-executor.ts";

let admin: Kysely<Database>, db: Kysely<Database>, databaseName: string;
let tenant: Awaited<ReturnType<typeof createPrivateTenant>>;
let store: ControlPlaneStore;
const candidates: Array<{
  runId: string;
  sessionId: string;
  piSessionId: string;
  partition: number;
}> = [];

beforeAll(async () => {
  const endpoint = process.env.PI_CLOUD_POSTGRES_INTEGRATION_URL;
  if (!endpoint) throw new Error("Use the isolated PG runner");
  admin = createDatabase({ connectionString: endpoint, maxConnections: 1 });
  databaseName = `claim_keys_${randomUUID().replaceAll("-", "")}`;
  await sql`create database ${sql.id(databaseName)}`.execute(admin);
  const url = new URL(endpoint);
  url.pathname = "/" + databaseName;
  db = createDatabase({ connectionString: url.toString(), maxConnections: 4 });
  await runMigrations(db, "up");
  // Many small tenants, then one new hot tenant after ANALYZE: the production
  // failure underestimated its Sessions and combined them before checking ID.
  for (let i = 0; i < 40; i++) {
    const old = await createPrivateTenant(db, {
      slug: `old-${randomUUID()}`,
      ownerDisplayName: "Claim fixture",
    });
    if (i >= 8) continue;
    const oldStore = new ControlPlaneStore({ database: db, ...old });
    const p = await oldStore.createProject({ name: "old", source: { kind: "empty" } });
    const s = await oldStore.createSession(p.projectId, p.workspaceId, "old", "elastic");
    const run = await oldStore.acceptTurn(s.sessionId, randomUUID(), { prompt: "old" });
    await db
      .updateTable("runs")
      .set({ available_at: new Date("2100-01-01T00:00:00Z") })
      .where("id", "=", run.runId)
      .execute();
  }
  for (const table of [
    "runs",
    "sessions",
    "pi_sessions",
    "session_leases",
    "tenant_runtime_policies",
    "agent_revisions",
  ])
    await sql.raw(`analyze ${table}`).execute(db);
  tenant = await createPrivateTenant(db, {
    slug: `hot-${randomUUID()}`,
    ownerDisplayName: "Hot claim fixture",
  });
  store = new ControlPlaneStore({ database: db, ...tenant });
  const p = await store.createProject({ name: "hot", source: { kind: "empty" } });
  for (let i = 0; i < 192; i++) {
    const session = await store.createSession(p.projectId, p.workspaceId, "hot", "elastic");
    const run = await store.acceptTurn(session.sessionId, randomUUID(), {
      prompt: "owned synthetic input",
    });
    const native = await db
      .selectFrom("sessions as s")
      .innerJoin("pi_sessions as p", (j) =>
        j.onRef("p.tenant_id", "=", "s.tenant_id").onRef("p.id", "=", "s.pi_session_id"),
      )
      .select(["s.pi_session_id", sql<number>`(p.session_hash % 32)::int`.as("partition")])
      .where("s.id", "=", session.sessionId)
      .executeTakeFirstOrThrow();
    candidates.push({
      runId: run.runId,
      sessionId: session.sessionId,
      piSessionId: native.pi_session_id,
      partition: native.partition,
    });
  }
}, 60000);
afterAll(async () => {
  await db?.destroy();
  if (admin) {
    await sql`drop database ${sql.id(databaseName)}`.execute(admin);
    await admin.destroy();
  }
});

// Compile the real executor statement without executing admission or the model.
async function capture(admission?: RunClaimAdmission): Promise<CompiledQuery> {
  let query: CompiledQuery | undefined;
  const captured = new Error("Captured claim query");
  const observed = db.withPlugin({
    transformQuery(args) {
      const q = db.getExecutor().compileQuery(args.node, args.queryId);
      if (q.sql.startsWith('with "claim_candidate"')) {
        query = q;
        throw captured;
      }
      return args.node;
    },
    async transformResult(args) {
      return args.result;
    },
  });
  const worker = await createTestWorker(db);
  const executor = worker.executor(
    {
      execute: async () => {
        throw new Error("Must not execute");
      },
    },
    { database: observed },
  );
  await expect(executor.dispatchNext(admission)).rejects.toBe(captured);
  return query!;
}
const key = (c: (typeof candidates)[number]) => `${tenant.tenantId}:${c.piSessionId}`;
const fullPublication = { partitionCount: 32, partitions: Array.from({ length: 32 }, (_, i) => i) };

it("bounds real claim work under skewed tenant statistics without changing the first eligible Run", async () => {
  const query = await capture({
    blockedFamilyKeys: candidates.slice(0, 7).map(key),
    publication: fullPublication,
  });
  const result = await db.transaction().execute((tx) => tx.executeQuery<{ runId: string }>(query));
  expect(result.rows.map((r) => r.runId)).toEqual([candidates[7]!.runId]);
  const explained = await db.transaction().execute((tx) =>
    tx.executeQuery<{
      "QUERY PLAN": Array<{ Plan: Record<string, number> }>;
    }>({ ...query, sql: `explain (analyze,buffers,format json) ${query.sql}` }),
  );
  const plan = explained.rows[0]!["QUERY PLAN"][0]!.Plan;
  // Production did 1.4 million buffer accesses to return one candidate. Bound
  // measured work rather than a timing threshold or one particular index name.
  expect((plan["Shared Hit Blocks"] ?? 0) + (plan["Shared Read Blocks"] ?? 0)).toBeLessThan(20_000);
});

it("checks family and publication eligibility before choosing a winner", async () => {
  const target = candidates.find((c) => c.partition !== candidates[0]!.partition)!;
  const partitionOnly = await capture({
    blockedFamilyKeys: [],
    publication: { partitionCount: 32, partitions: [target.partition] },
  });
  expect((await db.executeQuery<{ runId: string }>(partitionOnly)).rows[0]?.runId).toBe(
    target.runId,
  );
  const allowed = await capture({
    blockedFamilyKeys: [key(candidates[10]!)],
    allowedFamilyKeys: candidates.slice(10, 13).map(key),
    publication: fullPublication,
  });
  expect((await db.executeQuery<{ runId: string }>(allowed)).rows[0]?.runId).toBe(
    candidates[11]!.runId,
  );
  const none = await capture({
    blockedFamilyKeys: [],
    allowedFamilyKeys: [],
    publication: fullPublication,
  });
  expect((await db.executeQuery(none)).rows).toEqual([]);
  const noPartition = await capture({
    blockedFamilyKeys: [],
    publication: { partitionCount: 32, partitions: [] },
  });
  expect((await db.executeQuery(noPartition)).rows).toEqual([]);
});

it("skips a locked earlier Run and keeps the unlocked FIFO order", async () => {
  const query = await capture();
  const entered = Promise.withResolvers<void>(),
    release = Promise.withResolvers<void>();
  const holding = db.transaction().execute(async (tx) => {
    await tx
      .selectFrom("runs")
      .select("id")
      .where("id", "=", candidates[0]!.runId)
      .forUpdate()
      .execute();
    entered.resolve();
    await release.promise;
  });
  try {
    await entered.promise;
    expect((await db.executeQuery<{ runId: string }>(query)).rows[0]?.runId).toBe(
      candidates[1]!.runId,
    );
  } finally {
    release.resolve();
    await holding;
  }
  expect((await db.executeQuery<{ runId: string }>(query)).rows[0]?.runId).toBe(
    candidates[0]!.runId,
  );
});
