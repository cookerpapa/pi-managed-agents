import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";

const [podsDirectory, fleetDirectory, output] = process.argv.slice(2);
assert(podsDirectory && fleetDirectory && output);
const read = async (directory) =>
  JSON.parse(await readFile(join(directory, "report.json"), "utf8"));
const [pods, fleet] = await Promise.all([read(podsDirectory), read(fleetDirectory)]);
assert(pods.mode === "wake" && fleet.mode === "wake-fleet");
assert(pods.cleaned && pods.wakePoliciesRemoved && fleet.cleaned && !pods.error && !fleet.error);
assert(pods.sameSessionClaimOrder && fleet.sameSessionClaimOrder);
assert.deepEqual(pods.actorHashes, fleet.actorHashes);

const hashes = {};
for (const file of [
  "run.mjs",
  "wake-trials.mjs",
  "wake-channel.mjs",
  "wake-runtime.mjs",
  "wake-population.mjs",
  "worker.mjs",
  "projector.mjs",
  "fixture.mjs",
  "actor.mjs",
  "schema.sql",
]) {
  hashes[file] = createHash("sha256")
    .update(await readFile(new URL(file, import.meta.url)))
    .digest("hex");
}
const tests = pods.tests
  .filter((t) => t.router)
  .map((t) => {
    assert(
      t.completed === t.count &&
        t.dropped === 0 &&
        t.endKafkaLag === 0 &&
        t.verified.sequenceVerified,
    );
    const workers = t.workerResources.flatMap((w) => w.claimStats ?? []);
    const sum = (key) => workers.reduce((n, w) => n + w[key], 0);
    assert.equal(sum("rows"), t.completed);
    return {
      label: t.label,
      variant: t.wakeMode,
      batch: t.batch,
      coalesceMs: t.windowMs ?? 5,
      tickMs: t.tickMs,
      probeUnlisted: t.probeUnlisted,
      offeredRate: t.rate ?? null,
      completed: t.completed,
      dropped: t.dropped,
      concurrency: t.concurrency,
      notifications: t.wakeCount,
      router: t.router,
      claimQueries: sum("queries"),
      emptyClaims: sum("empty"),
      maxBatch: Math.max(...workers.map((w) => w.maximumBatch)),
      ignoredWhileFull: sum("fullHints"),
      share: t.claimDistribution,
      claimReasons: t.claimReasons,
      queueMs: t.verified.queueMs,
      submitToClaimMs: t.submitToClaimMs,
      submitToTerminalMs: t.submitToTerminalMs,
      runPerSec: t.runsPerSecond,
      endKafkaLag: t.endKafkaLag,
    };
  });
for (const list of Object.values(fleet.population))
  for (const r of list)
    assert(r.connections === 1000 && r.rows === 7 * r.batches && r.claimed === r.rows);
assert(fleet.population["directed-repeat"].some((r) => r.name === "steady-30s"));
const evidence = {
  baseRevision: pods.revision,
  sourceSha256: hashes,
  actorBundleSha256: pods.actorHashes,
  postgres: pods.settings,
  managedWorkerPods: 2,
  kafkaPartitions: 32,
  replicationFactor: 3,
  fleet: {
    virtualWorkers: 1000,
    sockets: "loopback WebSocket",
    claimAndCompletionPoolMax: 12,
    poolConnectionsReservedForListen: 1,
    runArrivalPerNotification: 7,
    targetNotificationsPerSecond: 171,
    nativePiAndKafka: false,
  },
  managed: tests,
  population: fleet.population,
  cleanup: {
    podRun: pods.cleaned,
    fleetRun: fleet.cleaned,
    temporaryPoliciesRemoved: pods.wakePoliciesRemoved,
  },
};
await writeFile(output, JSON.stringify(evidence, null, 2) + "\n");
