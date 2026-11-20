import assert from "node:assert/strict";
import { readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";

// Export only measurements. Native inputs, process logs and credentials stay
// in the operator's private fixture directory.
const [output, ...directories] = process.argv.slice(2);
assert(output && directories.length);
const studies = [];
const round = (n) => Number(n.toFixed(2));
for (const directory of directories) {
  const read = async (file) => JSON.parse(await readFile(join(directory, file), "utf8"));
  const report = await read("run/report.json"),
    cleanup = await read("fixture-cleanup.json");
  assert(
    report.cleaned &&
      !report.error &&
      Object.keys(cleanup).length === 3 &&
      Object.values(cleanup).every(Boolean),
  );
  const paths = report.pgPaths;
  function trial(source, r) {
    const measure = r.measurement,
      cfg = measure.commitSetting;
    assert(
      cfg.sync === "on" &&
        cfg.fsync === "on" &&
        cfg.verifiedConnections === 12 &&
        cfg.delayUs === 0,
    );
    const replay = source === "replay",
      n = replay ? r.digest.completed : r.completed;
    const txn = Object.values(measure.byKind).reduce((count, k) => count + k.operations, 0);
    const p = r.prefix;
    if (replay) {
      assert.deepEqual(r.digest, paths.tape.digest);
      assert.equal(r.frames, n * 7);
    }
    assert.equal(measure.applied, n * 7);
    assert.equal(p?.logicalWrites ?? txn, 4 * n);
    if (p) {
      assert.equal(txn, p.commits);
      assert(p.maxRecords <= r.prefixWidth);
    }
    const wave = replay ? undefined : report.tests.find((t) => t.label === r.label);
    if (wave) assert.equal(wave.endKafkaLag, 0);
    return {
      source,
      rate: r.rate,
      width: r.prefixWidth,
      completed: n,
      offered: wave?.count ?? n,
      dropped: r.dropped ?? 0,
      runsPerSecond: round(r.runsPerSecond),
      terminalP95Ms: round(r.terminalMs.p95),
      ...(r.source ? { sourceLateP95Ms: round(r.source.lateMs.p95) } : {}),
      ...(wave
        ? {
            dropReasons: wave.dropReasons,
            finalKafkaLag: wave.endKafkaLag,
            submitToClaimP95Ms: round(wave.submitToClaimMs.p95),
            nativeKafkaAckSumP95Ms: round(wave.nativeKafkaAckSumMs.p95),
            finishToHandlerP95Ms: round(wave.finishToHandlerMs.p95),
          }
        : {}),
      peakBacklog: r.peakBacklog,
      drainMs: round(r.drainMs),
      transactions: txn,
      logicalWrites: 4 * n,
      ...(p
        ? {
            multiWriteTransactions: txn - (p.transactionsByWrites[1] ?? 0),
            maxRecords: p.maxRecords,
            holdMsPerWrite: round(p.holdMs / (4 * n)),
            poolWaitMsPerWrite: round(p.poolMs / (4 * n)),
            meanTxnHoldMs: round(measure.byKind.stable_prefix.hold.mean),
            meanPoolWaitMs: round(measure.byKind.stable_prefix.pool.mean),
          }
        : {}),
      walSyncs: r.observations.wal.syncs,
      calibrationMs: round(r.observations.calibrationMs.p50),
      withinTarget: (r.dropped ?? 0) === 0 && r.terminalMs.p95 <= 1000 && r.drainMs <= 1000,
    };
  }
  studies.push({
    postgres: report.settings,
    verifiedConnections: 12,
    partitions: 32,
    replicationFactor: 3,
    tapeRuns: paths.tape.runs,
    tapeRecords: paths.tape.frames,
    tapeSha256: paths.tape.sha256,
    settings: paths.pairs[0].measurement.commitSetting,
    scenarios: report.tests.filter((x) => x.label.startsWith("prefix-")).map((x) => x.label),
    trials: [
      ...paths.pairs.map((r) => trial("replay", r)),
      ...paths.live.filter((r) => r.label !== "path-capture").map((r) => trial("full", r)),
    ],
    cleanup,
  });
}
await writeFile(output, JSON.stringify({ studies }, null, 2) + "\n");
