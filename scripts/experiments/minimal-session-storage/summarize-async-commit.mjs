// Export aggregate ACK timings, verified database settings and recovery counts.
import assert from "node:assert/strict";
import { readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { asyncReplayCases, asyncLiveCases } from "./pg-group-commit.mjs";

const [output, ...directories] = process.argv.slice(2);
assert(output && directories.length);
const studies = [];
for (const directory of directories) {
  const read = async (name) => JSON.parse(await readFile(join(directory, name), "utf8"));
  const a = await read("run/report.json"),
    p = a.pgPaths,
    crash = await read("crash-recovery.json"),
    cleanup = await read("fixture-cleanup.json");
  assert(
    a.cleaned &&
      !a.error &&
      Object.values(cleanup).length === 3 &&
      Object.values(cleanup).every((v) => v === true),
  );
  assert.deepEqual(p.asyncCommit.initial, p.asyncCommit.restored);
  assert.equal(p.asyncCommit.restored.sync, "on");
  assert(crash.immediateStop && crash.trials.length === 6);
  assert.deepEqual(
    crash.trials.map((t) => t.mode),
    ["on", "off", "off", "on", "on", "off"],
  );
  for (const t of crash.trials) {
    assert.equal(t.acknowledged, 32);
    assert(t.lost >= 0 && t.recovered >= 0 && t.lost + t.recovered === 32);
    if (t.mode === "on") assert.equal(t.lost, 0);
  }
  const round = (n) => (Number.isFinite(n) ? Math.round(100 * n) / 100 : null);
  const bands = new Map();
  function describe(r, path) {
    const m = r.measurement,
      k = Object.values(m.byKind),
      o = r.observations;
    const n = r.completed ?? r.digest.completed,
      writes = k.reduce((v, s) => v + s.operations, 0),
      cfg = m.commitSetting;
    assert.equal(writes, n * 4);
    assert.equal(m.applied, n * 7);
    assert.equal(cfg.key, r.commitSetting);
    assert.equal(cfg.verifiedConnections, 12);
    assert.equal(cfg.sync, r.commitSetting === "async" ? "off" : "on");
    assert.equal(cfg.delayUs, 0);
    assert.equal(cfg.siblings, 5);
    assert.equal(cfg.fsync, "on");
    assert.equal(cfg.method, p.asyncCommit.initial.method);
    assert.equal(r.placement.layout, "open");
    const wal = m.postAckFlush;
    assert(wal.initialGapBytes >= 0 && wal.coveredMs >= wal.firstProbeMs);
    if (path !== "full") {
      assert.deepEqual(r.digest, p.tape.digest);
      assert.equal(r.frames, p.tape.frames);
      assert.equal(r.source.frames, p.tape.frames);
    }
    const weighted = (key) => round(k.reduce((v, s) => v + s[key].mean * s.operations, 0) / writes);
    const offer = r.offeredAt ?? m.started,
      offerEnd = offer + (1000 * (n + (r.dropped ?? 0))) / r.rate,
      seen = new Set();
    for (const s of o.samples) {
      const sec = Math.floor((s.at - m.started) / 1000),
        from = m.started + 1000 * sec,
        b = m.seconds[sec];
      if (
        s.calibrationMs === undefined ||
        r.rate !== 260 ||
        from < offer + 1000 ||
        from + 1000 > offerEnd ||
        seen.has(sec) ||
        !b?.operations
      )
        continue;
      seen.add(sec);
      const band = s.calibrationMs < 3 ? "fast" : s.calibrationMs >= 6 ? "slow" : "middle";
      const id = [path, cfg.sync, band].join("/");
      const z = bands.get(id) ?? {
        seconds: 0,
        writes: 0,
        hold: 0,
        queue: 0,
        hash: 0,
        rounds: new Set(),
      };
      z.seconds++;
      z.writes += b.operations;
      z.hold += b.operations * b.hold.mean;
      z.queue += b.operations * b.pool.mean;
      z.hash += s.calibrationMs;
      z.rounds.add(m.tag);
      bands.set(id, z);
    }
    return {
      path,
      rate: r.rate,
      sync: cfg.sync,
      completed: n,
      notIssued: r.dropped ?? 0,
      writes,
      ackRps: round(r.runsPerSecond),
      ackP95Ms: round(r.terminalMs.p95),
      ackP99Ms: round(r.terminalMs.p99),
      peakBacklog: r.peakBacklog,
      ackDrainMs: round(r.drainMs),
      meetsAckTarget: (r.dropped ?? 0) === 0 && r.terminalMs.p95 <= 1000 && r.drainMs <= 1000,
      meanHoldMs: weighted("hold"),
      meanCheckoutMs: weighted("pool"),
      meanQueryMs: weighted("query"),
      hostHashMedianMs: round(o.calibrationMs.p50),
      observedWalGapBytes: wal.initialGapBytes,
      firstProbeMsAfterMeasurement: round(wal.firstProbeMs),
      fixedLsnCoveredMsAfterMeasurement: round(wal.coveredMs),
      pgAndProjectorMask: r.placement.pg.effective,
      verifiedWriterConnections: cfg.verifiedConnections,
      sampledPgQueries: o.server.length,
    };
  }
  const peak = p.asyncCommit.peak === true;
  const plan = asyncReplayCases(false, peak);
  assert.equal(p.pairs.length, plan.length);
  const rows = p.pairs.map((r, i) => {
    assert.deepEqual([r.rate, r.path, r.diagnostics, r.placement.layout, r.commitSetting], plan[i]);
    return describe(r, r.path);
  });
  const live = p.live.filter((r) => r.label.startsWith("path-full"));
  assert.deepEqual(
    live.map((r) => [r.rate, r.commitSetting]),
    asyncLiveCases(false, peak),
  );
  rows.push(...live.map((r) => describe(r, "full")));
  const groups = [];
  for (const [name, rs] of Map.groupBy(rows, (r) => [r.path, r.rate, r.sync].join("/"))) {
    const range = (key) => ({
      min: Math.min(...rs.map((r) => r[key])),
      max: Math.max(...rs.map((r) => r[key])),
    });
    groups.push({
      name,
      trials: rs.length,
      passed: rs.filter((r) => r.meetsAckTarget).length,
      completed: rs.reduce((s, r) => s + r.completed, 0),
      notIssued: rs.reduce((s, r) => s + r.notIssued, 0),
      ackRps: range("ackRps"),
      ackP95Ms: range("ackP95Ms"),
      meanHoldMs: range("meanHoldMs"),
      peakBacklog: range("peakBacklog"),
      ackDrainMs: range("ackDrainMs"),
      walGapBytes: range("observedWalGapBytes"),
      walCoveredAfterEndMs: range("fixedLsnCoveredMsAfterMeasurement"),
    });
  }
  const hostSpeed = [...bands].map(([key, z]) => ({
    key,
    rounds: z.rounds.size,
    seconds: z.seconds,
    hashMs: round(z.hash / z.seconds),
    writesPerSecond: round(z.writes / z.seconds),
    meanHoldMs: round(z.hold / z.writes),
    meanCheckoutMs: round(z.queue / z.writes),
  }));
  studies.push({
    peak,
    capturedRuns: p.tape.runs,
    frames: p.tape.frames,
    projectionWrites: p.tape.operations,
    traceSha256: p.tape.sha256,
    pg: a.settings,
    server: p.asyncCommit,
    groups,
    rows,
    hostSpeed,
    crash,
    cleanup,
  });
}
await writeFile(output, JSON.stringify({ studies }, null, 2) + "\n");
