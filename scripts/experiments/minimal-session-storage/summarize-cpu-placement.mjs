// Publish aggregate evidence without the private trace or the fixture's credentials.
import assert from "node:assert/strict";
import { readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { distribution } from "../../lib/runtime-load-scheduler.mjs";
import { cpuReplayCases } from "./pg-path-trials.mjs";
const [output, ...directories] = process.argv.slice(2);
assert(output && directories.length);
const round = (n) => (Number.isFinite(n) ? Math.round(n * 100) / 100 : null);
const mean = (xs) => (xs.length ? xs.reduce((n, x) => n + x, 0) / xs.length : null);
const bands = new Map();
function noteSeconds(r, stratum, mode, tapeRuns) {
  if (!["primary", "full"].includes(stratum) || r.rate !== 260) return;
  const m = r.measurement,
    start = r.offeredAt ?? m.started,
    end =
      start + ((r.completed === undefined ? tapeRuns : r.completed + r.dropped) * 1000) / r.rate,
    seen = new Set();
  for (const s of r.observations.samples) {
    const hash = s.calibrationMs;
    if (hash === undefined) continue;
    const sec = Math.floor((s.at - m.started) / 1000),
      begin = m.started + 1000 * sec;
    const b = m.seconds[sec];
    if (begin < start + 1000 || begin + 1000 > end || seen.has(sec) || !b?.operations) continue;
    seen.add(sec);
    const band = hash < 3 ? "<3" : hash < 6 ? "3–6" : hash < 7 ? "6–7" : hash < 8 ? "7–8" : "8+";
    const key = [stratum, mode, band].join("/");
    const g = bands.get(key) ?? {
      seconds: 0,
      writes: 0,
      records: 0,
      hold: 0,
      checkout: 0,
      hash: 0,
      rounds: new Set(),
    };
    g.seconds++;
    g.writes += b.operations;
    g.records += b.records;
    g.hold += b.operations * b.hold.mean;
    g.checkout += b.operations * b.pool.mean;
    g.hash += hash;
    g.rounds.add(m.tag);
    bands.set(key, g);
  }
}
function summarize(r, stratum, tapeRuns) {
  const m = r.measurement,
    o = r.observations,
    k = Object.values(m.byKind),
    p = r.placement;
  const n = r.digest?.completed ?? r.completed,
    writes = k.reduce((v, g) => v + g.operations, 0);
  assert.equal(writes, n * 4);
  assert.equal(m.applied, n * 7);
  const shape = p.layout === "ab" || p.layout === "ba" ? "split" : p.layout;
  const pg = o.pgCpu.filter((s) => s.at >= m.started && s.at <= m.ended);
  assert.equal(pg.length, p.pgSamples);
  assert(pg.length > 0 && pg.every((s) => s.allowed === p.pg.effective));
  assert.equal(m.samples.length, p.projectorSamples);
  assert(m.samples.length > 0 && m.samples.every((s) => s.allowed === p.projector.effective));
  const hold = (key) => round(k.reduce((v, g) => v + g.operations * g[key].mean, 0) / writes);
  noteSeconds(r, stratum, shape, tapeRuns);
  return {
    stratum,
    layout: p.layout,
    path: r.path ?? "full",
    offered: r.rate,
    completed: n,
    notSubmitted: r.dropped ?? 0,
    rps: round(r.runsPerSecond),
    p95Ms: round(r.terminalMs.p95),
    peakBacklog: r.peakBacklog,
    drainMs: round(r.drainMs),
    holdMs: hold("hold"),
    checkoutMs: hold("pool"),
    hostHashMedianMs: round(o.calibrationMs.p50),
    probeReadMedianMs: round(distribution(m.readPings.map((p) => p[1])).p50),
    loopUtilization: round(mean(m.samples.map((s) => s.loopUtilization))),
    pgMask: p.pg.effective,
    projectorMask: p.projector.effective,
    supportMask: p.support.effective,
    pgSamples: p.pgSamples,
    projectorSamples: p.projectorSamples,
    pgQuota: p.pg.quota,
    projectorQuota: p.projector.quota,
    pgThrottledUs: pg.at(-1).values[1] - pg[0].values[1],
    projectorThrottledUs: m.samples.reduce((v, s) => v + s.throttledUs, 0),
    sampledQueries: o.server.length,
    meetsOneSecond: (r.dropped ?? 0) === 0 && r.terminalMs.p95 <= 1000 && r.drainMs <= 1000,
  };
}
const series = [];
for (const directory of directories) {
  const a = JSON.parse(await readFile(join(directory, "run/report.json"), "utf8"));
  const clean = JSON.parse(await readFile(join(directory, "fixture-cleanup.json"), "utf8"));
  assert(
    a.cleaned && Object.values(clean).length === 3 && Object.values(clean).every((x) => x === true),
  );
  const p = a.pgPaths,
    reverse = p.cpu.reverse === true,
    expected = cpuReplayCases(false, reverse);
  assert.equal(p.pairs.length, expected.length);
  const rows = p.pairs.map((r, i) => {
    assert.deepEqual([r.rate, r.path, r.diagnostics, r.placement.layout], expected[i]);
    assert.deepEqual(r.digest, p.tape.digest);
    assert.equal(r.frames, p.tape.frames);
    assert.equal(r.source.frames, p.tape.frames);
    const stratum =
      r.placement.layout === "open"
        ? "allCpus"
        : reverse || i < 8
          ? "primary"
          : r.path === "direct"
            ? "direct"
            : r.rate === 390
              ? "higherRate"
              : "narrow";
    return summarize(r, stratum, p.tape.runs);
  });
  const live = p.live.filter((r) => r.label.startsWith("path-full"));
  assert.deepEqual(
    live.map((r) => r.placement.layout),
    reverse ? ["ba", "shared", "shared", "ab"] : ["shared", "ab", "ba", "shared"],
  );
  rows.push(...live.map((r) => summarize(r, "full", p.tape.runs)));
  series.push({
    reverse,
    plan: {
      ...p.cpu.plan,
      siblings: Object.fromEntries(p.cpu.plan.siblings.map((group, i) => [i, group.join(",")])),
    },
    environment: p.cpu.environment,
    images: p.cpu.images,
    pg: a.settings,
    capturedRuns: p.tape.runs,
    frames: p.tape.frames,
    writes: p.tape.operations,
    traceHash: p.tape.sha256,
    rows,
    cleanup: true,
  });
}
const clockBands = [...bands].map(([group, b]) => ({
  group,
  seconds: b.seconds,
  rounds: b.rounds.size,
  hostHashMs: round(b.hash / b.seconds),
  writesPerSecond: round(b.writes / b.seconds),
  recordsPerSecond: round(b.records / b.seconds),
  holdMs: round(b.hold / b.writes),
  checkoutMs: round(b.checkout / b.writes),
}));
await writeFile(output, JSON.stringify({ series, clockBands }, null, 2) + "\n");
