// All wake sockets have a PG-registered Worker identity. The pool counts the
// actual queue SELECTs; one successful claim also runs its normal lease SQL.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import pg from "pg";
import { it } from "vitest";
import { Kysely, PostgresDialect } from "kysely";
import { createDatabase, runMigrations } from "@pi-cloud/database";
import { createPrivateTenant } from "../../../packages/control-plane/src/tenant-administration.ts";
import { registerTestWorker } from "../../../packages/control-plane/test/fixtures/registered-worker.ts";
import { IndependentSessionRuntime } from "./independent-session-runtime.mjs";
import { installIndependentSessionLeases } from "./independent-session-leases.mjs";
import { installForkTables, setForkLayout } from "./fork-storage.mjs";
import { installPagingProjection } from "./flow-paging-fixture.mjs";

const endpoint = process.env.PI_CLOUD_POSTGRES_INTEGRATION_URL;
it.skipIf(!endpoint)(
  "routes committed queue wakes to available independent Workers and reconciles lost hints",
  async () => {
    const size = Number(process.env.PMA_SEGMENT_WAKE_WORKERS ?? 32);
    assert(Number.isSafeInteger(size) && size >= 16 && size <= 1000);
    const schema = "wake_" + randomUUID().replaceAll("-", "");
    const dsn = new URL(endpoint);
    dsn.searchParams.set("options", "-c search_path=" + schema + ",public");
    const control = createDatabase({ connectionString: endpoint, maxConnections: 12 });
    const pool = new pg.Pool({ connectionString: dsn.href, max: 12 });
    const runtime = new IndependentSessionRuntime({ pool, topic: "wake", partitions: 1 });
    let reads = 0;
    const workerDb = new Kysely({
      dialect: new PostgresDialect({ pool: new pg.Pool({ connectionString: dsn.href, max: 12 }) }),
      log: (event) => {
        if (event.level === "query" && event.query.sql.includes("for update of r,s skip locked"))
          reads++;
      },
    });
    const token = randomUUID() + randomUUID(),
      abort = new AbortController();
    const workers = [],
      loops = [],
      executions = new Map(),
      gates = [];
    let scheduler,
      failure,
      offset = 0n;
    async function until(condition, label) {
      const end = performance.now() + 20000;
      while (!(await condition())) {
        if (failure) throw failure;
        assert(performance.now() < end, label);
        await delay(10);
      }
    }
    async function startScheduler(port, tickMs) {
      return runtime.serveScheduler({ connectionString: dsn.href, token, port, tickMs });
    }
    try {
      await runMigrations(control, "up");
      const { tenantId: t } = await createPrivateTenant(control, {
        slug: schema.replaceAll("_", "-"),
        ownerDisplayName: "Queue wake test",
      });
      await pool.query("create schema " + schema);
      await installPagingProjection(pool);
      await installForkTables(pool);
      await setForkLayout(pool, true, false);
      await installIndependentSessionLeases(pool);
      const sessions = Array.from({ length: 20 }, () => randomUUID());
      await runtime.store.register(t, sessions);
      async function enqueue(s, position = 1, client = pool) {
        const id = randomUUID();
        await client.query(
          "insert into turns(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,state) values($1,$2,$3,$4,$1,'wake','queued')",
          [id, t, s, position],
        );
        return id;
      }
      async function finish(id) {
        const { job, gate } = executions.get(id);
        const fact = {
          kind: "work_finished",
          factId: randomUUID(),
          scope: job.scope,
          agentId: "root",
          nativeSequence: 0,
          baseSequence: 0,
          terminal: { type: "turn.cancelled", payload: { reason: "user_request", forced: false } },
          occurredAt: new Date().toISOString(),
        };
        assert(
          await runtime.leases.projectBoundary(
            { fact, topic: "wake", partition: 0, offset: offset++ },
            async () => {},
          ),
        );
        executions.get(id).closed = true;
        gate.resolve();
      }
      const registration = Array.from({ length: size }, () => ({
        workerId: randomUUID(),
        bootId: randomUUID(),
        capacity: 1,
      }));
      await control
        .insertInto("workers")
        .values(
          registration.map(({ workerId, bootId }) => ({
            id: workerId,
            supervisor_id: workerId,
            boot_id: bootId,
            state: "ready",
            max_concurrent_sessions: 1,
          })),
        )
        .execute();
      for (let i = 0; i < size; i += 12)
        await Promise.all(
          registration.slice(i, i + 12).map((r) => registerTestWorker(control, r.workerId)),
        );
      // This commit predates the scheduler's LISTEN. Its post-LISTEN probe (or
      // the central reconciliation tick) must cover it without a fleet poll.
      const early = await enqueue(sessions[0]);
      scheduler = await startScheduler(0, 60000);
      const address = scheduler.url;
      for (const r of registration) {
        const w = runtime.worker({ ...r, database: workerDb, leaseDurationMs: 12000 });
        workers.push(w);
        const loop = w
          .serve({
            scheduler: { url: address, token },
            signal: abort.signal,
            execute: async (job) => {
              if (abort.signal.aborted) return;
              assert(!executions.has(job.id), "Run was claimed twice");
              const gate = Promise.withResolvers();
              gates.push(gate);
              executions.set(job.id, { job, gate });
              await gate.promise;
            },
          })
          .catch((error) => {
            failure ??= error;
          });
        loops.push(loop);
        if (workers.length % 32 === 0) await delay(5);
      }
      await until(
        () => executions.has(early) && scheduler.snapshot().waiting === size - 1,
        "fleet startup and existing work",
      );
      await finish(early);
      await until(() => scheduler.snapshot().waiting === size, "slot advertised after completion");
      const channel = (
        await pool.query("select 'pma_segment_ready_'||md5(current_schema()) channel")
      ).rows[0].channel;
      async function listeners() {
        return (
          await pool.query("select pid from pg_stat_activity where application_name=$1", [channel])
        ).rows;
      }
      const original = await listeners();
      assert.equal(original.length, 1);
      const base = reads,
        arrival = scheduler.snapshot().arrival,
        tx = await pool.connect();
      try {
        await tx.query("begin");
        await enqueue(sessions[1], 1, tx);
        await delay(1200);
        assert.equal(reads, base, "idle workers polled PG or saw an uncommitted job");
        await tx.query("rollback");
        await delay(50);
      } finally {
        tx.release();
      }
      assert.equal(scheduler.snapshot().arrival, arrival);
      const idleChecks = reads - base;
      const single = await enqueue(sessions[1]);
      await until(() => executions.has(single), "single commit");
      assert.equal(scheduler.snapshot().arrival - arrival, 1);
      assert.equal(reads - base, 1);
      const singleArrivals = scheduler.snapshot().arrival - arrival;
      const beforeBurst = reads,
        burstBefore = scheduler.snapshot().arrival;
      const batch = await pool.connect(),
        ids = [];
      try {
        await batch.query("begin");
        for (const s of sessions.slice(2, 10)) ids.push(await enqueue(s, 1, batch));
        await batch.query("commit");
      } finally {
        batch.release();
      }
      await until(
        () => ids.every((id) => executions.has(id)),
        "one transaction with multiple Sessions",
      );
      assert.equal(scheduler.snapshot().arrival - burstBefore, ids.length);
      assert.equal(reads - beforeBurst, ids.length);
      const measurement = {
        workers: size,
        pgListeners: original.length,
        idleClaimSelects: idleChecks,
        single: { arrival: singleArrivals, claimSelects: beforeBurst - base },
        batch: {
          jobs: ids.length,
          arrival: scheduler.snapshot().arrival - burstBefore,
          claimSelects: reads - beforeBurst,
        },
      };

      // An early hint for an already-busy Session may be consumed before its
      // predecessor seals. The terminal transaction must signal again.
      const next = await enqueue(sessions[1], 2);
      await until(() => reads > beforeBurst + ids.length, "checked busy Session");
      assert(!executions.has(next));
      await finish(single);
      await until(() => executions.has(next), "successor after seal");
      // Only the listener belonging to this disposable schema is terminated.
      await pool.query("select pg_terminate_backend($1)", [original[0].pid]);
      const duringReconnect = await enqueue(sessions[10]);
      await until(async () => {
        const l = await listeners();
        return l.length === 1 && l[0].pid !== original[0].pid;
      }, "router PG listener restored");
      await until(() => executions.has(duringReconnect), "job during lost LISTEN");

      const port = Number(new URL(address).port);
      await scheduler.close();
      const lost = [
        await enqueue(sessions[11]),
        await enqueue(sessions[12]),
        await enqueue(sessions[13]),
      ];
      scheduler = await startScheduler(port, 250);
      await until(() => lost.every((id) => executions.has(id)), "backlog after router restart");
      assert(scheduler.snapshot().tick >= 1, "missed commits were covered by a directed probe");
      assert((await listeners()).length === 1);
      for (const [id, v] of executions) if (!v.closed) await finish(id);
      assert.equal(
        (
          await pool.query(
            "select count(*)::int n from session_leases where tenant_id=$1 and released_at is null",
            [t],
          )
        ).rows[0].n,
        0,
      );
      await until(() => scheduler.snapshot().waiting === size, "all slots ready after the backlog");
      const idleReads = reads,
        idle = scheduler.snapshot();
      await until(
        () => reads >= idleReads + 3 && scheduler.snapshot().waiting === size,
        "three fleet reconciliation probes",
      );
      assert.equal(reads - idleReads, scheduler.snapshot().tick - idle.tick);
      assert.equal(scheduler.snapshot().arrival, idle.arrival);
      measurement.idleReconciliation = {
        ticks: scheduler.snapshot().tick - idle.tick,
        claimSelects: reads - idleReads,
      };
      process.stdout.write(
        "segment-directed-wake " +
          JSON.stringify({ ...measurement, restoredBacklog: lost.length }) +
          "\n",
      );
      if (failure) throw failure;
    } finally {
      abort.abort();
      gates.forEach((g) => g.resolve());
      await Promise.allSettled(loops);
      await Promise.allSettled(workers.map((w) => w.close()));
      await scheduler?.close();
      await workerDb.destroy();
      try {
        await pool.query("drop schema if exists " + schema + " cascade");
      } finally {
        await runtime.store.db.destroy();
        await control.destroy();
      }
    }
  },
  180000,
);
