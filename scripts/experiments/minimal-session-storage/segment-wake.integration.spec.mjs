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
      async function settle(job) {
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
      }
      async function finish(id) {
        const { job, gate } = executions.get(id);
        await settle(job);
        executions.get(id).closed = true;
        gate.resolve();
      }
      // Both claimants see the same ready Sessions. Each claim transaction can
      // fill a Worker, and a concurrent claimant must skip the locked Sessions.
      const queued = Array.from({ length: 13 }, () => randomUUID());
      await runtime.store.register(t, queued);
      const turnIds = await Promise.all(queued.map((id) => enqueue(id)));
      // A withdrawn-only Turn still has input history, but has no eligible
      // first input. A different Turn in the same claim can have a placed input.
      const placed = randomUUID();
      await pool.query(
        `insert into user_or_agent_messages
        (tenant_id,session_id,id,sender_id,starts_turn,source,mode,request_key,body,fingerprint,turn_id,state)
        values($1,$2,gen_random_uuid(),$1,true,'user','input','withdrawn','test','test',$3,'withdrawn'),
              ($1,$4,$5,$1,true,'user','input','placed','test','test',$6,'placed')`,
        [t, queued[0], turnIds[0], queued[1], placed, turnIds[1]],
      );
      const bulk = [];
      try {
        for (let i = 0; i < 2; i++) {
          const workerId = randomUUID(),
            bootId = randomUUID();
          await control
            .insertInto("workers")
            .values({
              id: workerId,
              supervisor_id: workerId,
              boot_id: bootId,
              state: "ready",
              max_concurrent_sessions: 8,
            })
            .execute();
          await registerTestWorker(control, workerId);
          bulk.push(
            runtime.worker({
              database: workerDb,
              workerId,
              bootId,
              capacity: 8,
              leaseDurationMs: 30000,
            }),
          );
        }
        // Vary the starting epochs to verify lease-to-Session mapping, including
        // when RETURNING uses a different order from the queue.
        await pool.query(
          `insert into pi_sessions(tenant_id,id,lease_epoch)
          select $1,id,ordinality from unnest($2::text[]) with ordinality as s(id,ordinality)`,
          [t, queued],
        );
        const assertRolledBack = async () => {
          const {
            rows: [counts],
          } = await pool.query(
            `select
            (select count(*)::int from turns where tenant_id=$1 and state='queued') queued,
            (select count(*)::int from turns where ready_at is not null and tenant_id=$1) ready,
            (select count(*)::int from session_leases where tenant_id=$1) leases`,
            [t],
          );
          assert.deepEqual(counts, { queued: 13, ready: 13, leases: 0 });
        };
        // A failure after grant creation must roll back the entire claim.
        await pool.query(
          "update supervisor_connections set state='expired',close_reason='heartbeat_timeout',closed_at=clock_timestamp() where worker_id=$1",
          [bulk[0].workerId],
        );
        await assert.rejects(bulk[0].claimBatch(8), /management connection/);
        await assertRolledBack();
        await pool.query(
          "update supervisor_connections set state='active',close_reason=null,closed_at=null where worker_id=$1",
          [bulk[0].workerId],
        );
        const {
          rows: [first],
        } = await pool.query(
          "select session_id from turns where ready_at is not null and tenant_id=$1 order by ready_at,id limit 1",
          [t],
        );
        await pool.query(
          "update pi_sessions set lease_epoch=9007199254740991 where tenant_id=$1 and id=$2",
          [t, first.session_id],
        );
        await assert.rejects(bulk[0].claimBatch(8), /grant every reserved Session/);
        await assertRolledBack();
        await pool.query("update pi_sessions set lease_epoch=$3 where tenant_id=$1 and id=$2", [
          t,
          first.session_id,
          queued.indexOf(first.session_id) + 1,
        ]);
        const before = reads;
        const batches = await Promise.all(bulk.map((w) => w.claimBatch(32)));
        assert.deepEqual(
          batches.map((batch) => batch.length).sort((a, b) => a - b),
          [5, 8],
        );
        assert.deepEqual(
          batches
            .flat()
            .map((job) => job.id)
            .sort(),
          turnIds.toSorted(),
        );
        for (const job of batches.flat()) {
          assert.equal(job.has_inputs, job.id === turnIds[0] || job.id === turnIds[1]);
          assert.equal(job.first_message, job.id === turnIds[1] ? placed : null);
        }
        assert.equal(new Set(batches.flat().map((job) => job.scope.writerId)).size, 13);
        assert.equal(reads - before, 2, "one candidate selection per Worker");
        for (const job of batches.flat()) {
          assert.equal(job.scope.fencingToken, queued.indexOf(job.logical_id) + 2);
          await settle(job);
          await job.release();
        }
      } finally {
        for (const w of bulk) await w.close();
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
      assert.equal(
        (await pool.query("select ready_at from turns where id=$1", [next])).rows[0].ready_at,
        null,
      );
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
