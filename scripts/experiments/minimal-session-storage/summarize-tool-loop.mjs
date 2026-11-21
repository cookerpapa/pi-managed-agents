// Read only the isolated fixture report. Retain the unit of every rate and
// distinguish live producer progress, simultaneous replay and backlog drain.
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { join } from "node:path";

const root = process.argv[2];
assert(root, "Expected the run-cpu-placement output directory");
const json = async (name) => JSON.parse(await readFile(join(root, name), "utf8"));
const report = await json("run/report.json"),
  cleanup = await json("fixture-cleanup.json");
assert(report.cleaned && report.projectorResourcesRemoved && Object.values(cleanup).every(Boolean));
const { trials, partitions, targets } = report.fanout;
assert(
  trials[0]?.mix?.intents === 200 * targets &&
    trials[0]?.mix?.entries?.["pi.compaction"] === targets,
  "Expected the 200-call native tool-loop study",
);
const first = trials[0].mix,
  complete = Object.values(first.entries).reduce((a, n) => a + n, 0);
const calls = first.intents;
assert(trials.some((t) => t.liveSource) && trials.some((t) => t.preload));
const rounded = (v) => Math.round(v * 10) / 10;
const rates = trials.map((t) => {
  const { result: r, source: s } = t;
  assert(
    t.mix.hash === first.hash &&
      r.handled === first.records &&
      r.acks === calls &&
      r.displayed === calls &&
      r.samples.at(-1).lag === 0,
  );
  const duration = (r.processedAt - r.startAt) / 1000;
  assert(duration > 0 && r.pgTransactions.total === r.pgTransactions.sync + r.pgTransactions.async);
  const last = s.acknowledgedAt,
    period = last - r.startAt;
  function inputSample(fraction) {
    const at = r.startAt + period * fraction;
    return r.samples.reduce((best, sample) =>
      Math.abs(at - sample.at) < Math.abs(at - best.at) ? sample : best,
    );
  }
  const lagAtInputFraction = (f) => inputSample(f).lag;
  return {
    mode: t.preload ? "backlog" : t.liveSource ? "live" : "paced",
    targetRecordsPerSec: t.preload || t.liveSource ? null : t.rate,
    actualInputRecordsPerSec: t.preload ? null : rounded(s.rate),
    projectorSeconds: rounded(duration),
    recordsPerSec: rounded(first.records / duration),
    completeHistoryRowsPerSec: rounded(complete / duration),
    toolPairsPerSec: rounded(calls / duration),
    callAndResultMessagesPerSec: rounded((2 * calls) / duration),
    inputWindowLag: t.preload ? null : [0.25, 0.5, 0.75, 1].map(lagAtInputFraction),
    peakLag: r.peakLag,
    finalLag: r.samples.at(-1).lag,
    drainMsAfterLastProducerAck: t.preload ? null : Math.round(Math.max(0, r.processedAt - last)),
    syncTransactions: r.pgTransactions.sync,
    asyncTransactions: r.pgTransactions.async,
    toolsInMultiItemGrants: calls - (r.grantSizes[1] ?? 0),
  };
});
console.log(
  JSON.stringify(
    {
      partitions,
      targets,
      projectorConnections: report.projectorConnections,
      runs: first.runs,
      toolCalls: calls,
      entries: first.entries,
      records: first.records,
      complete,
      rates,
    },
    null,
    2,
  ),
);
