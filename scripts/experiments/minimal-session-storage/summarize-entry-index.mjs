// Print a shareable result without cluster addresses, credentials or native payloads.
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { join } from "node:path";

const [dir, appendFile, readsFile] = process.argv.slice(2);
assert(
  dir && appendFile && readsFile,
  "Expected the cluster output directory, PG-only JSON and cold-read JSONL",
);
const json = async (path) => JSON.parse(await readFile(path, "utf8"));
const report = await json(join(dir, "run/report.json"));
const cleanup = await json(join(dir, "fixture-cleanup.json"));
assert(report.cleaned && report.projectorResourcesRemoved && Object.values(cleanup).every(Boolean));
const { partitions, targets, trials } = report.fanout;
const [warm, ...compared] = trials;
assert(warm.liveSource && warm.entryIndex === undefined && compared.length === 12);
assert.deepEqual(
  compared.map((t) => t.entryIndex),
  [false, true, true, false, true, false, false, true, false, true, true, false],
);
const mib = (n) => n / 1048576;
const round = (x, d = 2) => +x.toFixed(d);
function median(xs) {
  const a = xs.toSorted((x, y) => x - y),
    m = Math.floor(a.length / 2);
  assert(a.length);
  return a.length % 2 ? a[m] : (a[m - 1] + a[m]) / 2;
}
const pct = (base, next) => 100 * (next / base - 1);
const arms = compared.map((t) => {
  const r = t.result;
  const rows = Object.values(t.mix.entries).reduce((a, b) => a + b, 0);
  assert(
    t.mix.hash === warm.mix.hash &&
      t.mix.records === 191488 &&
      rows === 44032 &&
      r.handled === 191488 &&
      t.count === 20480 &&
      r.acks === t.count &&
      r.samples.at(-1).lag === 0,
  );
  assert(t.storage.entryIndexBytes > 0 === t.entryIndex);
  const seconds = r.elapsedMs / 1000;
  return {
    mode: t.preload ? "backlog" : "paced",
    index: t.entryIndex,
    elapsedMs: round(r.elapsedMs),
    recordsPerSec: t.mix.records / seconds,
    historyRowsPerSec: rows / seconds,
    runsPerSec: targets / seconds,
    toolPgP95UpperMs: r.phases.pg.p95UpperMs,
    pgTransactions: r.pgTransactions.total,
    walBytes: t.storage.walBytes,
    entryIndexBytes: t.storage.entryIndexBytes,
    messageTableBytes: t.storage.messageTableBytes,
    allMessageIndexBytes: t.storage.allMessageIndexBytes,
    finalLag: r.samples.at(-1).lag,
    ...(!t.preload
      ? {
          offered: t.rate,
          inputRecordsPerSec: round(t.source.rate),
          peakLag: r.peakLag,
          drainAfterInputMs: round(
            Math.max(0, Math.max(r.processedAt, r.lastAckAt) - t.source.acknowledgedAt),
          ),
        }
      : {}),
  };
});
function paired(rows, metric) {
  assert(rows.length % 2 === 0);
  const pairs = [];
  for (let i = 0; i < rows.length; i += 2) {
    const [base, indexed] = [rows[i], rows[i + 1]].sort(
      (a, b) => Number(a.index) - Number(b.index),
    );
    assert(!base.index && indexed.index);
    pairs.push(round(pct(base[metric], indexed[metric])));
  }
  return pairs;
}
function group(rows, metrics) {
  return Object.fromEntries(
    metrics.map((k) => {
      const values = (index) => rows.filter((r) => r.index === index).map((r) => r[k]);
      const a = median(values(false)),
        b = median(values(true));
      return [k, { off: round(a), on: round(b), changePct: a === 0 ? null : round(pct(a, b)) }];
    }),
  );
}
const byMode = {};
for (const mode of ["backlog", "paced"]) {
  const rows = arms.filter((r) => r.mode === mode);
  byMode[mode] = {
    medians: group(rows, [
      "runsPerSec",
      "recordsPerSec",
      "historyRowsPerSec",
      "toolPgP95UpperMs",
      "walBytes",
      "entryIndexBytes",
      "allMessageIndexBytes",
      "messageTableBytes",
      ...(mode === "paced" ? ["drainAfterInputMs"] : []),
    ]),
    pairedThroughputChangesPct: paired(rows, "runsPerSec"),
  };
}
const append = await json(appendFile);
assert(append.seedRows === 132096 && append.appendedRows === 44032 && append.trials.length === 8);
const reads = (await readFile(readsFile, "utf8"))
  .trim()
  .split("\n")
  .map((s) => JSON.parse(s));
const cold = reads.find((s) => s.children === 500);
assert(cold?.indexTimings && cold.withTemporaryIndex && cold.baselineRepeat);
console.log(
  JSON.stringify(
    {
      workload: {
        runs: targets,
        partitions,
        connections: report.projectorConnections,
        callsPerRun: 20,
        resultBytes: 1024,
        mockAckMs: 100,
        kafkaRecords: warm.mix.records,
        completeHistoryRows: 44032,
        entryKinds: warm.mix.entries,
        traceHash: warm.mix.hash,
        pg: report.settings.postgres,
      },
      mixed: {
        arms: arms.map((a) => ({
          ...a,
          runsPerSec: round(a.runsPerSec, 3),
          recordsPerSec: round(a.recordsPerSec),
          historyRowsPerSec: round(a.historyRowsPerSec),
          toolPgP95UpperMs: round(a.toolPgP95UpperMs, 3),
        })),
        byMode,
      },
      append: {
        ...append,
        medians: group(append.trials, ["ms", "walBytes", "entryIndexBytes"]),
        pairedTimeChangesPct: paired(append.trials, "ms"),
      },
      read500: cold,
      indexedMiBPer44032Rows: round(mib(byMode.backlog.medians.entryIndexBytes.on)),
      fixtureCleanup: cleanup,
    },
    null,
    2,
  ),
);
