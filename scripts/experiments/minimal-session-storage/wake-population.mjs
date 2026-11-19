import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import { distribution } from "../../lib/runtime-load-scheduler.mjs";
import { claimBatchSql } from "./fixture.mjs";
import { connectClaimWorker } from "./wake-channel.mjs";
import { openPool } from "./actor.mjs";

async function until(condition, title, failed) {
  const deadline = performance.now() + 60000;
  while (!condition()) {
    if (failed()) throw failed();
    assert(performance.now() < deadline, title);
    await delay(10);
  }
}

// These are real management sockets and real SKIP LOCKED claims. A cheap PG
// completion replaces Pi/Kafka here so the host can run 1,000 waiting clients.
export async function runWakePopulation(router, claimPool, topic, label, publish) {
  const intake = await openPool(3);
  const clients = [],
    seen = new Set();
  let error,
    finished = 0;
  const failed = () => error;
  const wakeMode = label.startsWith("broadcast") ? "broadcast" : "directed";
  const stage = { label, batch: 8, wakeMode, tickMs: 200, reofferMs: 4000, probeUnlisted: true };
  router.configure(stage);
  const timer = setInterval(() => {
    if (error) return;
    maxPoolWait = Math.max(maxPoolWait, claimPool.waitingCount);
  }, 20);
  let maxPoolWait = 0;
  const claim = async (limit) => (await claimPool.query(claimBatchSql, [limit])).rows;
  async function execute(row) {
    assert(!seen.has(row.id), "Duplicate Run claim");
    seen.add(row.id);
    const result = await claimPool.query(
      "update runs set state='completed',settled_at=clock_timestamp() where id=$1 and lease_id=$2 and state='running'",
      [row.id, row.lease_id],
    );
    assert.equal(result.rowCount, 1);
    finished++;
  }
  const totals = () => {
    const result = { queries: 0, empty: 0, rows: 0, arrival: 0, tick: 0 };
    for (const client of clients)
      for (const key of Object.keys(result)) result[key] += client.stats[key];
    return result;
  };
  const results = [];
  try {
    for (let i = 0; i < 1000; i++) {
      clients.push(
        connectClaimWorker({
          url: "ws://127.0.0.1:" + router.port + "/" + topic,
          id: "virtual-" + i,
          stage,
          slots: 8,
          claim,
          execute,
          fail: (e) => {
            error ??= e;
          },
        }),
      );
      if (i % 32 === 31) await delay(12);
    }
    const idle = () => clients.every((c) => c.stats.registrations && !c.busy);
    await until(
      () => router.snapshot().online === 1000 && router.snapshot().waiting >= 999 && idle(),
      "fleet bootstrap",
      failed,
    );
    for (const [name, batches] of [
      ["single", 1],
      ["arrival", 171],
      ...(label === "directed-repeat" ? [["steady-30s", 171 * 30]] : []),
    ]) {
      const tenant = randomUUID();
      const before = totals(),
        base = finished,
        target = batches * 7;
      const expectedNotifications = router.snapshot().notifications + batches;
      maxPoolWait = 0;
      const start = performance.now();
      const publishing = await publish({ label, tenant, batches });
      await until(() => finished === base + target, "fleet completions", failed);
      const completeMs = performance.now() - start;
      // A broadcast may have queued sockets or empty queries even after every
      // job finished. Wait for all deliveries, offers and physical queries.
      await until(
        () => {
          const s = router.snapshot();
          return (
            s.notifications === expectedNotifications &&
            idle() &&
            s.waiting >= 999 &&
            totals().arrival === s.arrival
          );
        },
        "fleet delivery drain",
        failed,
      );
      const after = totals();
      const rows = (
        await intake.query(
          "select id,state,lease_id,started_at,created_at from runs where tenant_id=$1",
          [tenant],
        )
      ).rows;
      assert.equal(rows.length, target);
      assert.equal(new Set(rows.map((r) => r.lease_id)).size, target);
      assert(rows.every((r) => r.state === "completed" && seen.has(r.id)));
      const counts = Object.fromEntries(Object.keys(before).map((k) => [k, after[k] - before[k]]));
      assert.equal(counts.rows, target);
      if (wakeMode === "broadcast") assert.equal(counts.arrival, batches * 1000);
      else assert(counts.arrival <= batches && counts.arrival > 0);
      results.push({
        name,
        connections: clients.length,
        batches,
        claimed: target,
        ...counts,
        maxPoolWait,
        publishing,
        completeMs: Math.round(completeMs),
        queueMs: distribution(rows.map((r) => r.started_at - r.created_at)),
      });
    }
  } finally {
    clearInterval(timer);
    clients.forEach((c) => c.close());
    try {
      await until(
        () => clients.every((c) => !c.busy),
        "client close",
        () => undefined,
      );
    } finally {
      await intake.end();
    }
  }
  if (error) throw error;
  return results;
}
