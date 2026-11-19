import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile, writeFile } from "node:fs/promises";
import { basename, join } from "node:path";
import { percentile } from "../../lib/runtime-load-scheduler.mjs";

// Publish aggregates and source provenance only. No run IDs, transcripts or management addresses.
const [output, ...directories] = process.argv.slice(2);
assert(output && directories.length);
const measurements = [],
  sources = [];
for (const directory of directories) {
  const r = JSON.parse(await readFile(join(directory, "report.json"), "utf8"));
  assert(
    r.mode === "wake-throughput" &&
      r.cleaned &&
      r.sameSessionClaimOrder &&
      r.wakePoliciesRemoved &&
      !r.error,
  );
  const sample = basename(directory);
  sources.push({
    sample,
    baseRevision: r.revision,
    actorBundles: r.actorHashes,
    postgres: r.settings,
  });
  for (const t of r.tests.filter((t) => t.integrated)) {
    assert.equal(t.completed + t.dropped, t.count);
    assert.equal(t.endKafkaLag, 0);
    assert(t.verified.sequenceVerified);
    const l = t.locality,
      workers = t.workerResources.flatMap((w) => w.claimStats ?? []);
    const sum = (k) => workers.reduce((n, w) => n + w[k], 0);
    assert.equal(l.hits + l.cold + l.fresh, t.completed);
    assert.equal(sum("rows"), t.completed);
    const cal = percentile(
      t.samples.flatMap((s) => (s.calibrationMs === undefined ? [] : [s.calibrationMs])),
      0.5,
    );
    measurements.push({
      sample,
      label: t.label,
      projection: t.sink,
      cache: t.cacheMode,
      cohort: t.cohort ?? 0,
      seedKiB: (t.seedBytes ?? 0) / 1024,
      halfFresh: !!t.halfFresh,
      concurrency: t.concurrency,
      offeredPerSec: t.rate ?? null,
      secondsOffered: t.rate ? t.count / t.rate : null,
      planned: t.count,
      completed: t.completed,
      unissued: t.dropped,
      dropReasons: t.dropReasons ?? {},
      runsPerSecond: t.runsPerSecond,
      elapsedMs: t.elapsedMs,
      p50Ms: t.submitToTerminalMs.p50,
      p95Ms: t.submitToTerminalMs.p95,
      p99Ms: t.submitToTerminalMs.p99,
      lateHalfP95Ms: t.secondHalfMs?.p95 ?? null,
      claimP95Ms: t.submitToClaimMs.p95,
      firstModelP95Ms: l.firstModelMs.p95,
      finishTransitP95Ms: t.finishToHandlerMs.p95,
      finishPgP95Ms: t.finishPgMs.p95,
      peakKafkaLag: t.peakKafkaLag,
      drainMs: t.drainAfterOfferMs ?? null,
      endLag: t.endKafkaLag,
      fresh: l.fresh,
      cold: l.cold,
      hits: l.hits,
      readBytes: l.pgReadBytes,
      preferredWakes: t.router.preferred,
      claimedTargets: sum("targetedRows"),
      claimStatements: sum("queries"),
      emptyClaims: sum("empty"),
      workerShares: t.claimDistribution,
      peakRunsPerWorker: workers.map((w) => w.maximumRunning),
      kafkaBytesPerRun: Math.round(t.bytesToKafka / t.completed),
      projectorStatements: t.projection.queries,
      hostHashP50Ms: cal === null ? null : Math.round(cal * 100) / 100,
      ...(t.verifyFull ? { historyPaths: l.path } : {}),
    });
  }
}
const currentSource = {};
for (const name of [
  "actor",
  "run",
  "worker",
  "projector",
  "wake-runtime",
  "wake-channel",
  "fixture",
  "session-prefix",
  "locality-load",
])
  currentSource[name] = createHash("sha256")
    .update(await readFile(new URL(name + ".mjs", import.meta.url)))
    .digest("hex");
const header = {
  workload:
    "Two Worker Pods, native Pi with zero intentional model delay, 4 KiB answers. Session Kafka RF3/minISR2, one Projector, PG.",
  batch: 32,
  workerSlots: 128,
  wakeMs: 5,
  backupMs: 200,
  minCacheKiB: 256,
  cacheMiBPerWorker: 96,
  cacheTtlSeconds: 300,
  sources,
  currentSource,
  schemaSha256: createHash("sha256")
    .update(await readFile(new URL("schema.sql", import.meta.url)))
    .digest("hex"),
};
const lines = [
  "{",
  ...Object.entries(header).map(
    ([k, v]) => "  " + JSON.stringify(k) + ": " + JSON.stringify(v) + ",",
  ),
  '  "measurements": [',
  measurements.map((m) => "    " + JSON.stringify(m)).join(",\n"),
  "  ]",
  "}",
  "",
];
await writeFile(output, lines.join("\n"));
