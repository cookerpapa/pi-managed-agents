import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import pg from "pg";
import { studyReplicaConsistency, checkStudySse } from "./replica-study.mjs";
import { testPostgres } from "../../lib/test-postgres.mjs";
import { withMatchedPageFixture } from "./matched-page-cost.mjs";
// Time equal amounts of useful reading while both databases are warm and projection is idle.
async function placementCpu(primary, standby, arm) {
  const observer = new pg.Pool({ connectionString: standby, max: 1 });
  const cpu = async (db) =>
    Number(
      /^usage_usec (\d+)/m.exec(
        (await db.query("select pg_read_file('/sys/fs/cgroup/cpu.stat') body")).rows[0].body,
      )[1],
    );
  try {
    const cursor = arm.queries[1],
      expected = await arm.page(cursor);
    const n = 96;
    for (const route of [
      "primary",
      "standby",
      "standby",
      "primary",
      "standby",
      "primary",
      "primary",
      "standby",
    ]) {
      const page = route === "primary" ? arm.page : arm.split.page;
      await Promise.all(Array.from({ length: 8 }, () => page(cursor)));
      const counts = await arm.split.counts(),
        before = await Promise.all([cpu(primary), cpu(observer)]),
        begin = performance.now();
      await Promise.all(
        Array.from({ length: 8 }, async () => {
          for (let i = 0; i < n / 8; i++) assert.deepEqual(await page(cursor), expected);
        }),
      );
      const after = await Promise.all([cpu(primary), cpu(observer)]),
        ms = performance.now() - begin;
      const diff = (i) => +((after[i] - before[i]) / 1000 / n).toFixed(3);
      console.log(
        JSON.stringify({
          mode: "cpu-per-page",
          route,
          pages: n,
          primaryMs: diff(0),
          standbyMs: diff(1),
          replicaReads: (await arm.split.counts()).replica - counts.replica,
          ms: +ms.toFixed(1),
        }),
      );
    }
  } finally {
    await observer.end();
  }
}
const own = await testPostgres({}, { standby: true }),
  tenant = randomUUID();
try {
  await withMatchedPageFixture(
    own.url,
    tenant,
    async (api) => {
      await placementCpu(api.db, own.standbyUrl, api.arms.branch);
      await studyReplicaConsistency(own.url, own.standbyUrl, tenant, api);
      const { split, page, queries, id } = api.arms.branch;
      const expected = await page(queries[1]);
      await own.stopStandby();
      const before = await split.counts(),
        start = performance.now();
      assert.deepEqual(await split.page(queries[1]), expected);
      const elapsed = performance.now() - start,
        after = await split.counts();
      assert.equal(after.unavailableFallback - before.unavailableFallback, 1);
      assert.deepEqual(await split.page({}), await page({}));
      const recent = await page({});
      await checkStudySse(
        split,
        id,
        (snapshot, stop) => {
          assert.deepEqual(snapshot.conversation, recent);
          stop.abort();
        },
        () => assert.fail(),
      );
      assert.deepEqual(await split.counts(), after);
      console.log(
        JSON.stringify({
          mode: "standby-offline",
          fallbackMs: +elapsed.toFixed(2),
          historyEqual: true,
          latestAndRelayedSsePrimary: true,
        }),
      );
    },
    undefined,
    own.standbyUrl,
  );
} finally {
  await own.close();
}
