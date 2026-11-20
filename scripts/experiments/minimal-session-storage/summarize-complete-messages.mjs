import assert from "node:assert/strict";
import { readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";

const [output, ...directories] = process.argv.slice(2);
assert(output && directories.length);
const studies = [];
for (const directory of directories) {
  const read = async (file) => JSON.parse(await readFile(join(directory, file), "utf8"));
  const report = await read("run/report.json"),
    cleanup = await read("fixture-cleanup.json");
  const m = report.completeMessages;
  assert(
    report.cleaned &&
      !report.error &&
      cleanup.temporaryPodsRemoved &&
      cleanup.temporaryServiceRemoved &&
      cleanup.temporaryAuthRemoved,
  );
  assert(
    m.count === 200 * m.runs && m.settings.fsync === "on" && m.settings.synchronous_commit === "on",
  );
  for (const t of m.trials) {
    assert.equal(t.verifiedEntries, m.count + 2 * m.runs);
    assert(t.finalLag === 0 && t.maxBatch <= 16 && t.rps > 0);
    assert.equal(
      t.transactions,
      t.policy === "recorded-history" ? t.asyncTransactions : t.syncTransactions,
    );
    assert(Math.abs((t.rps * t.elapsedMs) / 1000 - m.count) < 0.01);
    if (typeof t.persistUsage === "boolean") {
      assert.equal(t.usageRows, t.persistUsage ? m.runs : 0);
      assert.equal(t.usageUpserts, t.persistUsage ? m.runs * 102 : 0);
      assert(t.walBytes > 0 && t.syncTransactions === 0);
    }
    if (t.layout) {
      assert(t.policy === "recorded-history");
      assert.equal(t.concurrentForks > 0, t.layout === "busy");
      assert.equal(t.existingForks > 0, ["busy", "fanout"].includes(t.layout));
    }
  }
  if (report.forkStudy) {
    assert(Object.values(report.forkStudy.correctness).every((x) => x === true));
    for (const shape of report.forkStudy.results)
      for (const key of ["forkMs", "firstPageMs", "nestedForkMs"])
        assert.equal(shape[key].count, shape.samples);
  }
  const { placement, trials, ...measures } = m;
  studies.push({
    ...measures,
    ...(report.forkStudy ? { forkStudy: report.forkStudy } : {}),
    trials: trials.map(({ rate, rps, ...t }) => ({
      input: rate === "preloaded" ? "preloaded" : "paced",
      ...(rate === "preloaded" ? {} : { offeredMessagesPerSecond: rate }),
      completeMessagesPerSecond: rps,
      ...t,
    })),
    actorHashes: report.actorHashes,
    cpus: {
      pg: placement.pg.effective,
      projector: placement.projector.effective,
      sender: placement.support.effective,
    },
    cleanup,
  });
}
await writeFile(output, JSON.stringify({ studies }, null, 2) + "\n");
