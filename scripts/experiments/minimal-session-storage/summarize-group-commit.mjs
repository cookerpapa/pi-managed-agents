// Publish aggregates; keep per-backend timings and synthetic SQL bindings private.
import assert from "node:assert/strict";
import { readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { commitProfiles, groupReplayCases, groupLiveCases } from "./pg-group-commit.mjs";
const [output, ...directories] = process.argv.slice(2);
assert(output && directories.length);
const studies = [];
for (const directory of directories) {
  const read = async (f) => JSON.parse(await readFile(join(directory, f), "utf8"));
  const report = await read("run/report.json"),
    cleaned = await read("fixture-cleanup.json");
  assert(
    report.cleaned &&
      !report.error &&
      Object.keys(cleaned).length === 3 &&
      Object.values(cleaned).every((v) => v === true),
  );
  const s = report.pgPaths;
  assert.deepEqual(s.groupCommit.initial, s.groupCommit.restored);
  const round = (n) => (Number.isFinite(n) ? Math.round(n * 100) / 100 : null);
  const bins = new Map();
  function row(r, kind, key) {
    const m = r.measurement,
      cfg = m.commitSetting,
      obs = r.observations,
      wal = obs.wal;
    const n = r.digest?.completed ?? r.completed,
      ops = Object.values(m.byKind),
      writes = ops.reduce((sum, g) => sum + g.operations, 0);
    assert.equal(n * 4, writes);
    assert.equal(m.applied, n * 7);
    assert.equal(cfg.key, key);
    assert.equal(cfg.verifiedConnections, 12);
    assert.equal(cfg.fsync, "on");
    assert.equal(cfg.sync, "on");
    assert.equal(cfg.method, s.groupCommit.initial.method);
    assert.deepEqual({ delayUs: cfg.delayUs, siblings: cfg.siblings }, commitProfiles[key]);
    assert.equal(r.placement.layout, "open");
    const pg = obs.pgCpu.filter((v) => v.at >= m.started && v.at <= m.ended);
    assert(pg.length && pg.every((v) => v.allowed === r.placement.pg.effective));
    assert(
      m.samples.length && m.samples.every((v) => v.allowed === r.placement.projector.effective),
    );
    assert.equal(r.placement.pg.effective, r.placement.projector.effective);
    if (kind === "replay") {
      assert.deepEqual(r.digest, s.tape.digest);
      assert.equal(r.source.frames, s.tape.frames);
    }
    const weighted = (k) =>
      round(ops.reduce((sum, g) => sum + g.operations * g[k].mean, 0) / writes);
    // Host-speed controls compare only interior offer seconds with an independent calibration.
    const begin = r.offeredAt ?? m.started,
      end = begin + (1000 * (n + (r.dropped ?? 0))) / r.rate,
      seen = new Set();
    for (const sample of obs.samples) {
      if (sample.calibrationMs === undefined || r.rate !== 260) continue;
      const i = Math.floor((sample.at - m.started) / 1000),
        from = m.started + i * 1000,
        b = m.seconds[i];
      if (from < begin + 1000 || from + 1000 > end || seen.has(i) || !b?.operations) continue;
      seen.add(i);
      const band =
        sample.calibrationMs < 3 ? "fast" : sample.calibrationMs >= 6 ? "slow" : "middle";
      const k = [kind, key, band].join("/");
      const z = bins.get(k) ?? {
        seconds: 0,
        writes: 0,
        hold: 0,
        checkout: 0,
        hash: 0,
        rounds: new Set(),
      };
      z.seconds++;
      z.writes += b.operations;
      z.hold += b.operations * b.hold.mean;
      z.checkout += b.operations * b.pool.mean;
      z.hash += sample.calibrationMs;
      z.rounds.add(m.tag);
      bins.set(k, z);
    }
    return {
      kind,
      key,
      rate: r.rate,
      completed: n,
      notIssued: r.dropped ?? 0,
      logicalProjectorWrites: writes,
      effectiveRps: round(r.runsPerSecond),
      terminalP95Ms: round(r.terminalMs.p95),
      drainMs: round(r.drainMs),
      peakBacklog: r.peakBacklog,
      meetsTarget: (r.dropped ?? 0) === 0 && r.terminalMs.p95 <= 1000 && r.drainMs <= 1000,
      holdMs: weighted("hold"),
      checkoutMs: weighted("pool"),
      hostHashMedianMs: round(obs.calibrationMs.p50),
      walSyncs: wal.syncs,
      walWrites: wal.writes,
      walMiB: round(wal.bytes / 2 ** 20),
      syncsPer1000ProjectionWrites: kind === "replay" ? round((1000 * wal.syncs) / writes) : null,
      meanTimedSyncMs: wal.syncs ? round(wal.syncMs / wal.syncs) : null,
      verifiedSettings: cfg,
      pgCpuMask: r.placement.pg.effective,
      projectorCpuMask: r.placement.projector.effective,
      sampledQueries: obs.server.length,
      sourceLateP95Ms: round(r.source?.lateMs.p95),
      pgThrottleUs: pg.at(-1).values[1] - pg[0].values[1],
      projectorThrottleUs: m.samples.reduce((sum, v) => sum + v.throttledUs, 0),
    };
  }
  const confirm = s.groupCommit.confirm === true;
  const cases = groupReplayCases(false, confirm);
  assert.equal(s.pairs.length, cases.length);
  const rounds = s.pairs.map((r, i) => {
    assert.deepEqual(
      [r.rate, r.path, r.diagnostics, r.placement.layout, r.commitSetting],
      cases[i],
    );
    return row(r, "replay", r.commitSetting);
  });
  const live = s.live.filter((r) => r.label.startsWith("path-full"));
  assert.deepEqual(
    live.map((r) => [r.rate, r.commitSetting]),
    groupLiveCases(false, confirm),
  );
  rounds.push(...live.map((r) => row(r, "full", r.commitSetting)));
  const groups = [];
  for (const [name, values] of Map.groupBy(rounds, (r) => [r.kind, r.rate, r.key].join("/"))) {
    const tail = values.map((v) => v.terminalP95Ms);
    groups.push({
      name,
      rounds: values.length,
      passed: values.filter((v) => v.meetsTarget).length,
      p95RangeMs: { min: Math.min(...tail), max: Math.max(...tail) },
      notIssued: values.reduce((sum, v) => sum + v.notIssued, 0),
      syncsPer1000ProjectionWrites:
        values[0].kind === "replay"
          ? round(
              (1000 * values.reduce((sum, v) => sum + v.walSyncs, 0)) /
                values.reduce((sum, v) => sum + v.logicalProjectorWrites, 0),
            )
          : null,
      maxPeakBacklog: Math.max(...values.map((v) => v.peakBacklog)),
    });
  }
  const hostSpeed = [...bins].map(([name, g]) => ({
    name,
    rounds: g.rounds.size,
    seconds: g.seconds,
    hashMs: round(g.hash / g.seconds),
    writesPerSecond: round(g.writes / g.seconds),
    meanHoldMs: round(g.hold / g.writes),
    meanCheckoutMs: round(g.checkout / g.writes),
  }));
  studies.push({
    confirmation: confirm,
    capturedRuns: s.tape.runs,
    records: s.tape.frames,
    writes: s.tape.operations,
    traceSha256: s.tape.sha256,
    settings: report.settings,
    server: s.groupCommit,
    cleanup: cleaned,
    groups,
    rounds,
    hostSpeed,
  });
}
await writeFile(output, JSON.stringify({ studies }, null, 2) + "\n");
