import assert from "node:assert/strict";
import { readFile, writeFile } from "node:fs/promises";

const [output, ...inputs] = process.argv.slice(2);
assert(output && inputs.length, "Usage: summarize-runtime-load.mjs output.json report.json ...");
const result = { format: "pma.runtime-capacity-summary.v1", evaluations: [] };
for (const path of inputs) {
  const r = JSON.parse(await readFile(path, "utf8"));
  assert.equal(r.format, "pma.runtime-capacity.v1");
  const role = (name) =>
    r.inventory
      .map((i) => i.name)
      .sort((a, b) => b.length - a.length)
      .find((n) => name === n || name.startsWith(n + "-")) ??
    (name.startsWith("pma-load-") ? "simulated-provider" : "other");
  const evaluation = {
    mode: r.mode,
    at: r.at,
    driverRevision: r.driverRevision,
    dirty: r.dirty,
    serving: r.serving,
    hardware: r.hardware,
    inventory: r.inventory,
    accepted: r.accepted,
    cleanupConfirmed: r.cleanupConfirmed,
    realProviderRequests: r.realProviderRequests,
    metering: r.metering,
    idleBaseline: r.idleBaseline?.resources,
    postLoad: r.postLoad,
    history: r.history && {
      seedRuns: r.history.seedRuns,
      seedOutputBytes: r.history.seedOutputBytes,
      outcomes: r.history.outcomes,
      timings: Object.fromEntries(
        ["warm", "cold", "after"].map((k) => [
          k,
          {
            settlementMs: r.history[k].settlementMs,
            firstTextMs: r.history[k].firstTextMs,
            historyMs: r.history[k].historyMs,
          },
        ]),
      ),
    },
    stages: [],
  };
  for (const m of r.measurements) {
    const samples = m.metrics ?? [],
      processes = new Map(),
      containers = new Map();
    for (const s of samples) {
      for (const p of s.processes ?? []) {
        if (p.unavailable) continue;
        const values = new Map(p.metrics.map((v) => [v.name + v.labels, v.value]));
        for (const v of p.metrics)
          if (/^(process_|nodejs_)/.test(v.name)) values.set(v.name, v.value);
        const item = { at: s.at, metrics: p.metrics, values };
        const previous = processes.get(p.uid);
        if (previous) {
          previous.last = item;
          previous.samples.push(item);
        } else
          processes.set(p.uid, { role: role(p.name), first: item, last: item, samples: [item] });
      }
      for (const c of s.containers ?? []) {
        const a = containers.get(role(c.name)) ?? { peakWorkingSetMiB: 0, peakCpuCores: 0 };
        a.peakWorkingSetMiB = Math.max(
          a.peakWorkingSetMiB,
          (c.memory?.workingSetBytes ?? 0) / 1024 ** 2,
        );
        a.peakCpuCores = Math.max(a.peakCpuCores, (c.cpu?.usageNanoCores ?? 0) / 1e9);
        containers.set(role(c.name), a);
      }
    }
    const components = {},
      durations = {};
    for (const p of processes.values()) {
      const d = (name) => {
        const value = p.last.values.get(name) - p.first.values.get(name);
        return Number.isFinite(value) && value >= 0 ? value : null;
      };
      const dt = (p.last.at - p.first.at) / 1000;
      const c = (components[p.role] ??= {
        averageCpuCores: 0,
        peakInstanceRssMiB: 0,
        peakEventLoopP99Ms: 0,
      });
      c.averageCpuCores += dt > 0 ? (d("process_cpu_seconds_total") ?? 0) / dt : 0;
      for (const s of p.samples) {
        c.peakInstanceRssMiB = Math.max(
          c.peakInstanceRssMiB,
          (s.values.get("process_resident_memory_bytes") ?? 0) / 1024 ** 2,
        );
        c.peakEventLoopP99Ms = Math.max(
          c.peakEventLoopP99Ms,
          (s.values.get("nodejs_eventloop_lag_p99_seconds") ?? 0) * 1000,
        );
      }
      for (const v of p.last.metrics.filter(
        (v) => v.name.startsWith("pi_cloud_") && v.name.endsWith("_sum"),
      )) {
        const count = d(v.name.replace(/_sum$/, "_count") + v.labels),
          sum = d(v.name + v.labels);
        if (!count || sum === null) continue;
        const key = p.role + ":" + v.name.replace(/_sum$/, "") + v.labels;
        const a = (durations[key] ??= { count: 0, sumSeconds: 0 });
        a.count += count;
        a.sumSeconds += sum;
      }
    }
    const profile = { ...m.profile };
    delete profile.toolName;
    const kafka = [
      ...new Map(
        samples
          .filter((s) => s.kafka && !s.kafka.unavailable)
          .map((s) => [s.kafka.observedAt, s.kafka]),
      ).values(),
    ];
    const provider = m.provider && {
      ...m.provider,
      averageCpuCores: m.provider.cpuSeconds / (m.summary.elapsedMs / 1000),
      sampledRequests: m.samples.length,
      modelInFlightPeak: Math.max(0, ...samples.map((s) => s.provider?.inflight ?? 0)),
      imageParts: m.samples.reduce((n, s) => n + (s.imageCount ?? 0), 0),
    };
    evaluation.stages.push({
      profile,
      summary: m.summary,
      expectedOutcomesPassed: m.expectedOutcomesPassed,
      recovery: m.recovery?.summary,
      resources: {
        ...m.resources,
        kafkaSamples: kafka.length,
        firstKafkaLag: kafka[0]?.lag,
        lastObservedKafkaLag: kafka.at(-1)?.lag,
        lastKafkaObservationAgeMs: kafka.length
          ? samples.at(-1).finishedAt - kafka.at(-1).observedAt
          : null,
      },
      provider,
      components,
      containers: Object.fromEntries(containers),
      stageTimings: Object.fromEntries(
        Object.entries(durations).map(([key, v]) => [
          key,
          {
            count: v.count,
            meanMs: (v.sumSeconds / v.count) * 1000,
          },
        ]),
      ),
      historyReadsMs: m.historyReadsMs,
      historyPages: m.historyPages,
      // IDs, transcripts, configuration receipts and request bodies remain private.
      evidence: m.expectedOutcomesPassed ? m.evidence : undefined,
    });
  }
  result.evaluations.push(evaluation);
}
await writeFile(output, JSON.stringify(result, null, 2) + "\n");
console.log(JSON.stringify({ evaluations: result.evaluations.length, output }));
