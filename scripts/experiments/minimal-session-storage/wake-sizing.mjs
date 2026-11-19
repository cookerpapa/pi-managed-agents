import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { runLoad, distribution } from "../../lib/runtime-load-scheduler.mjs";
import { claimBatchSql, claimHintedBatchSql } from "./fixture.mjs";

export async function runSizingTrials(wave) {
  const run = (label, spec) =>
    wave({
      label,
      slots: 128,
      omitNotify: true,
      wakeMode: "directed",
      batch: 16,
      tickMs: 200,
      probeUnlisted: true,
      ...spec,
    });
  for (const [i, batch] of [8, 16, 32, 64, 64, 32, 16, 8].entries()) {
    await run("arriving-" + i, { batch, concurrency: 128, count: 512 });
    const result = await run("backlog-" + i, {
      batch,
      concurrency: 256,
      count: 256,
      offline: ["w0", "w1"],
      joinAfterAdmission: true,
      joinBoth: true,
    });
    assert(result.workerResources.every((w) => w.claimStats && w.claimStats.maximumBatch <= batch));
  }
  for (const [i, slots] of [64, 128, 256, 256, 128, 64].entries()) {
    const result = await run("resident-" + i, {
      slots,
      concurrency: slots,
      count: slots,
      offline: ["w0"],
      initialBytes: 32768,
      residentMs: slots === 256 ? 10000 : 2000,
    });
    assert.equal(result.claimDistribution.w1, slots);
    const worker = result.workerResources.find((w) => w.worker === "w1");
    assert.equal(worker.claimStats.maximumRunning, slots);
    assert.equal(worker.residence.simultaneousModelCalls, slots);
    assert(
      worker.residence.atBarrier && worker.residence.peakRss >= worker.residence.atBarrier.rss,
    );
  }
}

const population = 8192,
  samples = 192,
  batch = 16;
async function seed(db, tenant) {
  await db.query(
    `insert into runs (id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,model_ref,state,created_at)
    select id,$1,id,1,id,'claim fixture','none','queued',statement_timestamp()+i*interval '1 microsecond'
    from (select i,gen_random_uuid() id from generate_series(1,$2::int) i) jobs`,
    [tenant, population],
  );
  await db.query("analyze runs");
}

// Only this invocation's scratch database is in scope. Each timed query commits
// real Running rows, so later samples see exactly the work earlier clients took.
export async function measureClaimPreference(db) {
  const evidence = [];
  for (const variant of ["warmup", "fifo", "hit", "miss", "miss", "hit", "fifo"]) {
    const tenant = randomUUID();
    await seed(db, tenant);
    try {
      const targets = (
        await db.query(
          "select id from runs where tenant_id=$1 order by created_at desc,id desc limit $2",
          [tenant, samples],
        )
      ).rows.map((r) => r.id);
      const stale = (await db.query(claimBatchSql, [1])).rows[0];
      assert.equal(stale.tenant_id, tenant);
      const sql = variant === "hit" || variant === "miss" ? claimHintedBatchSql : claimBatchSql;
      const params = (i) =>
        sql === claimBatchSql
          ? [batch]
          : [batch, tenant, variant === "hit" ? targets[i] : stale.id];
      const client = await db.connect();
      let indexes;
      try {
        await client.query("begin");
        const plan = (
          await client.query("explain (analyze, buffers, format json) " + sql, params(0))
        ).rows[0]["QUERY PLAN"][0].Plan;
        function scan(node) {
          return [
            ...(node["Index Name"] ? [node["Index Name"]] : []),
            ...(node.Plans ?? []).flatMap(scan),
          ];
        }
        indexes = [...new Set(scan(plan))];
      } finally {
        await client.query("rollback");
        client.release();
      }
      const seen = new Set(),
        timings = [];
      const load = await runLoad({
        mode: "closed",
        concurrency: 8,
        count: samples,
        work: async (i) => {
          const start = performance.now();
          const { rows } = await db.query(sql, params(i));
          timings.push(performance.now() - start);
          assert.equal(rows.length, batch);
          if (variant === "hit") assert(rows.some((r) => r.id === targets[i]));
          for (const row of rows) {
            assert(row.tenant_id === tenant && row.state === "running" && row.lease_id);
            assert(!seen.has(row.id));
            seen.add(row.id);
          }
          return { success: true };
        },
      });
      assert(
        load.results.every((r) => r.success),
        load.results.find((r) => !r.success)?.error,
      );
      assert.equal(seen.size, samples * batch);
      if (variant !== "warmup")
        evidence.push({
          variant,
          population,
          samples,
          concurrency: 8,
          batch,
          queryMs: distribution(timings),
          elapsedMs: Math.round(load.elapsedMs),
          indexes,
        });
    } finally {
      await db.query("delete from runs where tenant_id=$1", [tenant]);
      await db.query("vacuum analyze runs");
    }
  }
  return evidence;
}
