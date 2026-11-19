/** Export content-free results; private actor messages and application logs stay local. */
import assert from "node:assert/strict";
import { readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
const [directory, output] = process.argv.slice(2);
assert(directory && output);
const read = async (...parts) => JSON.parse(await readFile(join(directory, ...parts), "utf8"));
const evidence = {
  sourceRevision: null,
  settings: null,
  scope: {
    workers: 2,
    projectors: 1,
    kafkaPartitions: 32,
    kafkaReplicas: 3,
    minimumIsr: 2,
    projectorPool: 12,
    outputBytesPerFinalAnswer: 4096,
    plainModelDelayMs: 0,
    workerStorage: "Pi MemoryStorage applying after Kafka acknowledgements",
    fullColdRecovery: false,
  },
  sources: {},
  completeControls: [],
  correctness: {},
  waves: [],
  deepInbox: [],
  intakeLocks: [],
  capacity: [],
};
for (const n of ["full-control", "full-control-repeat"]) {
  const r = await read(n, "load/report.json"),
    o = await read(n, "observation-private.json");
  assert(
    r.accepted &&
      r.cleanupConfirmed &&
      !r.realProviderRequests &&
      o.phase === "restored" &&
      o.loadExit === 0,
  );
  const m = r.measurements[0],
    rs = m.load.results;
  assert(
    rs.length === 192 &&
      m.summary.completed === 192 &&
      m.summary.concurrency === 64 &&
      r.postLoad.lastKafkaLag === 0,
  );
  const models = m.samples.map((v) => v.finishedAt - v.startedAt).sort((a, b) => a - b);
  const runs = rs.map((v) => v.settlementMs).sort((a, b) => a - b);
  evidence.completeControls.push({
    case: n,
    completed: rs.length,
    totalMs: Math.round(m.summary.elapsedMs),
    runsPerSecond: +m.summary.successfulRunsPerSecond.toFixed(2),
    timeToPgMs: { min: runs[0], p50: runs[95], p95: runs[182], max: runs.at(-1) },
    modelMs: { min: models[0], p50: models[95], p95: models[182], max: models.at(-1) },
    configuredFirstByteMs: m.profile.firstByteMs,
    configuredChunkMs: m.profile.chunkMs,
    peakOffsetLag: m.resources.maximumObservedKafkaLag,
    finalLag: r.postLoad.lastKafkaLag,
    ...(n === "full-control-repeat"
      ? { pgNotifyObservation: await read(n, "notify-observations.json") }
      : {}),
  });
}
const pct = (a, b) => (b ? +((100 * a) / b).toFixed(2) : null);
for (const [folder, group] of [
  ["measured-matrix", "matrix"],
  ["measured-diagnose", "diagnose"],
  ["measured-inbox", "inbox"],
  ["measured-capacity", "capacity"],
  ["intake", "intake"],
  ["boundary", "boundary"],
  ["acceptance", "acceptance"],
]) {
  const r = await read(folder, "report.json");
  assert(r.cleaned && !r.error && r.sameSessionClaimOrder);
  evidence.settings = r.settings;
  evidence.sourceRevision ??= r.revision;
  evidence.sources[group] = r.actorHashes;
  for (const x of r.tests) {
    const perRun =
      x.scene === "plain"
        ? 2
        : x.scene === "steer"
          ? 5
          : 3 + (x.depth ?? 1) - Number(Boolean(x.mutate));
    assert.equal(x.verified.messages, (x.sink === "terminal-only" ? 0 : perRun) * x.completed);
    assert.equal(x.modelCalls, x.replay ? 0 : (x.scene === "plain" ? 1 : 2) * x.completed);
    assert(
      x.verified.sequenceVerified &&
        x.verified.nonemptyInboxes === 0 &&
        x.endKafkaLag === 0 &&
        x.completed + x.dropped === x.count,
    );
    if (x.label.startsWith("correct-") || x.label === "quiet-admission") {
      if (group === "acceptance")
        evidence.correctness[x.label] = {
          completed: x.completed,
          messages: x.verified.messages,
          sqlChecks: x.verified.sequenceVerified,
          inboxes: x.verified.inboxes,
          rechecks: x.idleRechecks,
          obsolete: x.projection.stale,
          duplicate: x.projection.duplicate,
          projectedFinalLag: x.endKafkaLag,
        };
      continue;
    }
    if (x.label === "warmup") continue;
    const w = {
      group,
      stage: x.label,
      scene: x.scene,
      sink: x.sink,
      replay: x.replay ?? null,
      offerRate: x.rate ?? null,
      offered: x.count,
      completed: x.completed,
      unissued: x.dropped,
      modelDelayMs: x.modelDelayMs ?? 0,
      elapsedMs: x.elapsedMs,
      runsPerSecond: x.runsPerSecond,
      timeToPgMs: x.submitToTerminalMs,
      admitPoolP50: x.admissionPoolMs.p50,
      admitSqlP50: x.admissionSqlMs.p50,
      claimP50: x.claimCallMs.p50,
      workerP50: x.claimedWorkMs.p50,
      nativeRecordsPerRun: x.nativeRecordsPerRun.p50,
      workerKafkaAckSumP50: x.nativeKafkaAckSumMs.p50,
      finishKafkaAckP50: x.finishProducerAckMs.p50,
      finishSentToHandlerP50: x.finishToHandlerMs.p50,
      finishPgP50: x.finishPgMs.p50,
      finishPgP95: x.finishPgMs.p95,
      lastAnswerPgToFinishPgP50:
        x.sink === "terminal-only" ? null : x.answerCommitToRunCommitMs.p50,
      extraInputPgP95: x.extraInputConfirmedMs.p95,
      pgCallsPerRun: x.projection.queries / x.completed,
      peakKafkaLag: x.peakKafkaLag,
      drainedLag: x.endKafkaLag,
      verifiedMessages: x.verified.messages,
      workerCounts: Object.values(x.claimDistribution),
      modelCalls: x.modelCalls,
    };
    evidence.waves.push(w);
    if (x.rate) {
      const c = x.samples
        .filter((s) => Number.isFinite(s.calibrationMs))
        .map((s) => s.calibrationMs)
        .sort((a, b) => a - b);
      evidence.capacity.push({
        stage: x.label,
        rate: x.rate,
        seconds: x.count / x.rate,
        completed: x.completed,
        unissued: x.dropped,
        achieved: x.runsPerSecond,
        p95Ms: x.submitToTerminalMs.p95,
        nearEndLag: x.endOfOffer.lag,
        nearEndSampleMs: x.endOfOffer.at,
        peakKafkaLag: x.peakKafkaLag,
        nearEndQueuedRuns: x.endOfOffer.queued,
        nearEndRunningRuns: x.endOfOffer.running,
        drainMs: x.drainAfterOfferMs,
        endLag: x.endKafkaLag,
        generatorDelayP95: x.generatorDelayMs.p95,
        projectorELU: +x.projection.loop.utilization.toFixed(3),
        wslFourMiBHashMedianMs: c.length ? +c[Math.ceil(c.length / 2) - 1].toFixed(2) : null,
      });
    }
    if (x.label.startsWith("depth-"))
      evidence.deepInbox.push({
        items: x.depth,
        inputBytesPerItem: x.inputBytes,
        totalItems: x.depth * x.completed,
        completed: x.completed,
        snapshotsPerRun: x.projection.inboxSnapshots / x.completed,
        jsonBytesPerRun: Math.round(x.projection.inboxBytes / x.completed),
        inputAckP50: x.extraInputConfirmedMs.p50,
        inputAckP95: x.extraInputConfirmedMs.p95,
        runP95: x.submitToTerminalMs.p95,
      });
    if (x.label.startsWith("intake-")) {
      let active = 0,
        locked = 0;
      for (const s of x.samples)
        for (const v of s.waits ?? []) {
          active += v.n;
          if (
            v.locktype === "object" &&
            v.class === "pg_database" &&
            String(v.objid) === "0" &&
            v.mode === "AccessExclusiveLock"
          )
            locked += v.n;
        }
      evidence.intakeLocks.push({
        stage: x.label,
        workerWake: x.wakeMode ?? "pg",
        notifyInAdmission: !x.omitNotify,
        notifications: x.wakeCount,
        completed: x.completed,
        runsPerSecond: x.runsPerSecond,
        sqlP50: x.admissionSqlMs.p50,
        poolP50: x.admissionPoolMs.p50,
        nonIdleIntakeSamples: active,
        blockedOnNotify: locked,
        blockedPercent: pct(locked, active),
      });
    }
  }
}
await writeFile(output, JSON.stringify(evidence, null, 2) + "\n");
