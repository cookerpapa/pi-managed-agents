// Public aggregates only. The input files may contain backend PIDs and per-call timing.
import assert from "node:assert/strict";
import { readFile, writeFile } from "node:fs/promises";
import { distribution } from "../../lib/runtime-load-scheduler.mjs";

const [output, ...files] = process.argv.slice(2);
assert(output && files.length);
const round = (n) => (Number.isFinite(n) ? Math.round(n * 100) / 100 : null);
const mean = (a) => (a.length ? a.reduce((s, n) => s + n, 0) / a.length : null);
function times(a) {
  if (!a.length) return null;
  const d = distribution(a);
  return { n: a.length, mean: round(mean(a)), p50: round(d.p50), p95: round(d.p95) };
}
function perQuery(server) {
  const idle = server.filter((q) => q.afterIdle !== undefined);
  return {
    n: server.length,
    client: times(server.map((q) => q.client)),
    protocol: times(server.map((q) => q.server)),
    executeLogToClient: times(server.map((q) => q.afterExecute)),
    idleWitness: idle.length
      ? {
          n: idle.length,
          client: times(idle.map((q) => q.client)),
          sentToServerIdle: times(idle.map((q) => q.toIdle)),
          executeLogToServerIdle: times(idle.map((q) => q.executeToIdle)),
          serverIdleToClient: times(idle.map((q) => q.afterIdle)),
        }
      : null,
  };
}
function waitShares(samples) {
  const waits = new Map();
  for (const s of samples)
    for (const [k, n] of Object.entries(s.events)) {
      if (k.startsWith("active/")) waits.set(k.slice(7), (waits.get(k.slice(7)) ?? 0) + n);
    }
  const n = [...waits.values()].reduce((s, x) => s + x, 0);
  const pct = (re) =>
    round((100 * [...waits].filter(([k]) => re.test(k)).reduce((s, [, v]) => s + v, 0)) / (n || 1));
  return {
    activeSamples: n,
    walWriteLockPct: pct(/^LWLock\.WALWrite$/i),
    walSyncPct: pct(/^IO\.WalSync$/i),
    heavyweightLockPct: pct(/^Lock\./),
    runnablePct: pct(/^on_cpu_or_runnable$/),
  };
}
function timeline(raw, m, obs, rate) {
  if (!raw?.length) return [];
  const peak = raw.reduce((a, b) => (a.backlog >= b.backlog ? a : b));
  const early = raw.find((s) => s.at >= m.started + 1250);
  const firstSecond = raw.find((s) => s.backlog >= rate * 7);
  const chosen = [
    ["early", early],
    ["oneSecondOfArrivals", firstSecond],
    ["peak", peak],
    ["last", raw.at(-1)],
  ];
  return chosen
    .filter(([, s]) => s)
    .map(([point, s]) => {
      const sec = Math.max(0, Math.floor((s.at - m.started) / 1000)),
        stat = m.seconds[sec];
      const inSec = (x) => Math.floor((x.at - m.started) / 1000) === sec;
      const logs = obs.samples.filter(inSec);
      return {
        point,
        second: sec,
        backlog: s.backlog,
        poolWaiting: s.poolWaiting,
        operations: stat?.operations ?? 0,
        avgHoldMs: round(stat?.hold.mean),
        avgCheckoutMs: round(stat?.pool.mean),
        calibrationMs: round(mean(logs.flatMap((l) => l.calibrationMs ?? []))),
        loopUtilization: round(mean(m.samples.filter(inSec).map((x) => x.loopUtilization))),
        ...waitShares(logs),
        server: perQuery(obs.server.filter(inSec)),
      };
    });
}
function summary(r, archive, includeTimeline) {
  const s = archive.tests.find((t) => t.label === r.label),
    m = r.measurement,
    o = r.observations;
  const n = r.digest?.completed ?? r.completed,
    rate = r.rate ?? s.rate;
  const kinds = Object.values(m.byKind);
  const writes = kinds.reduce((a, g) => a + g.operations, 0);
  assert.equal(writes, 4 * n);
  assert.equal(m.applied, 7 * n);
  const wm = (k) => round(kinds.reduce((a, g) => a + g.operations * g[k].mean, 0) / writes);
  const raw = r.path
    ? m.samples
    : r.offeredAt
      ? s.samples.map((v) => ({
          at: v.at + r.offeredAt,
          backlog: v.lag,
          poolWaiting: v.poolWaiting,
        }))
      : [];
  return {
    path: r.path ?? r.label,
    rate,
    diagnostics: m.diagnostics,
    completed: n,
    dropped: r.dropped ?? 0,
    writes,
    runsPerSecond: round(r.runsPerSecond ?? s.runsPerSecond),
    p95Ms: round(r.terminalMs.p95),
    p99Ms: round(r.terminalMs.p99),
    peakBacklog: r.peakBacklog,
    drainMs: round(r.drainMs),
    sourceLateP95Ms: round(r.source?.lateMs.p95),
    avgCheckoutMs: wm("pool"),
    avgHoldMs: wm("hold"),
    avgQueryMs: wm("query"),
    holdByWrite: Object.fromEntries(
      Object.entries(m.byKind).map(([k, g]) => [k, round(g.hold.mean)]),
    ),
    pgTiming: perQuery(o.server),
    ...waitShares(o.samples),
    walSyncs: o.wal.syncs,
    walSyncMsPerSync: o.wal.syncs && m.diagnostics ? round(o.wal.syncMs / o.wal.syncs) : null,
    walMiB: round(o.wal.bytes / 2 ** 20),
    readOnlyPingMs: times(m.readPings.map((p) => p[1])),
    hashMs: round(o.calibrationMs.p50),
    loopBusy: round(mean(m.samples.map((s) => s.loopUtilization))),
    loopDelayP95OfQuarterSecondP99Ms: round(
      distribution(m.samples.map((s) => s.loopDelayP99Ms)).p95,
    ),
    projectorThrottledUs: m.samples.reduce((s, x) => s + x.throttledUs, 0),
    pgThrottledUs: o.pgCpu.length > 1 ? o.pgCpu.at(-1).values[1] - o.pgCpu[0].values[1] : null,
    ...(includeTimeline ? { timeline: timeline(raw, m, o, rate) } : {}),
  };
}
const sets = [];
for (const f of files) {
  const a = JSON.parse(await readFile(f, "utf8"));
  assert(a.cleaned && a.wakePoliciesRemoved);
  const p = a.pgPaths;
  assert(p.tape.frames === 7 * p.tape.runs && p.tape.operations === 4 * p.tape.runs);
  for (const c of p.pairs) {
    assert.deepEqual(c.digest, p.tape.digest);
    assert.equal(c.frames, p.tape.frames);
    assert.equal(c.source.frames, p.tape.frames);
  }
  const slow = p.pairs
    .filter((c) => c.rate === 260 && c.path === "projector" && c.diagnostics)
    .sort((a, b) => b.terminalMs.p95 - a.terminalMs.p95)[0];
  const slowLive = p.live
    .filter((c) => c.label.startsWith("path-full") && c.measurement.diagnostics)
    .sort((a, b) => b.terminalMs.p95 - a.terminalMs.p95)[0];
  sets.push({
    capturedRuns: p.tape.runs,
    frames: p.tape.frames,
    writes: p.tape.operations,
    sha256: p.tape.sha256,
    rows: p.tape.digest,
    pgSettings: a.settings,
    actorHashes: a.actorHashes,
    cleanup: true,
    paths: p.pairs.map((r) => summary(r, a, r === slow)),
    live: p.live.map((r) => summary(r, a, r === slowLive)),
  });
}
await writeFile(output, JSON.stringify({ sets }, null, 2) + "\n");
