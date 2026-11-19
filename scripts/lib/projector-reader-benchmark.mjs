// Executes only through run-projector-reader-diagnostic.mjs in the maintained Tool host.
// The buffered control is a fixed-size experiment, not a production reader:
// it deliberately tests no rebalance, EOF recovery or external-effect semantics.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import native from "@confluentinc/kafka-javascript";
import { KafkaPartitionReader } from "/app/packages/event-log/src/kafka-partition-reader.ts";
import { KafkaWorkerProducer } from "/app/packages/event-log/src/kafka-worker-producer.ts";
const brokers = process.env.PI_CLOUD_KAFKA_BROKERS.split(",");
const compareBatch = process.argv[2] === "batch";
const id = randomUUID(),
  topic = "pma-reader-diagnosis-" + id,
  partitions = 32,
  perPartition = compareBatch ? 256 : 64,
  total = partitions * perPartition;
const admin = new native.KafkaJS.Kafka({
  kafkaJS: { brokers, clientId: "diag-" + id, logLevel: native.KafkaJS.logLevel.NOTHING },
}).admin();
const producer = new KafkaWorkerProducer({ brokers, clientId: "diag-producer-" + id, topic });
const groups = [];
const report = {
  at: new Date().toISOString(),
  version: native.librdkafkaVersion,
  partitions,
  total,
  results: [],
};
let created = false;
const logger = {
  info() {},
  warn() {},
  debug() {},
  setLogLevel() {},
  namespace() {
    return this;
  },
  error(e) {
    console.error(JSON.stringify({ readerError: String(e) }));
  },
};
const stats = (seen, started, first, cpu) => {
  const now = performance.now(),
    used = process.cpuUsage(cpu);
  return {
    seen,
    wallMs: now - started,
    deliveryMs: now - first,
    recordsPerSecond: seen / ((now - first) / 1000),
    cpuMs: (used.user + used.system) / 1000,
  };
};
async function maintained(waitMs, batchRead = false) {
  const groupId = "diag-maintained-" + randomUUID();
  groups.push(groupId);
  let seen = 0,
    first = 0;
  let nativeStatistics;
  const counts = new Map(),
    done = Promise.withResolvers();
  const started = performance.now(),
    cpu = process.cpuUsage();
  const reader = new KafkaPartitionReader({
    brokers,
    clientId: groupId,
    groupId,
    topic,
    batchRead,
    logger,
    onStatistics: (value) => {
      nativeStatistics = value;
    },
    onPosition() {},
    rebalance: async (error, assignments, functions) => {
      if (error.code === native.CODES.ERRORS.ERR__ASSIGN_PARTITIONS)
        functions.assign(assignments.map((a) => ({ ...a, offset: 0 })));
      else functions.unassign(assignments);
    },
  });
  let timer;
  try {
    await reader.connect();
    await reader.subscribe({ topics: [topic] });
    await reader.run({
      eachBatch: async ({ batch, resolveOffset }) => {
        first ||= performance.now();
        for (const m of batch.messages) {
          const value = JSON.parse(m.value.toString());
          assert.equal(value.n, counts.get(batch.partition) ?? 0);
          counts.set(batch.partition, value.n + 1);
          if (waitMs) await delay(waitMs);
          resolveOffset(m.offset);
          seen++;
          if (seen === total) done.resolve();
        }
      },
    });
    await Promise.race([
      done.promise,
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(Error("reader timeout " + seen)), 120000);
      }),
    ]);
    const measurement = stats(seen, started, first, cpu);
    await delay(1100); // Capture a final native counter sample outside the timed interval.
    return {
      kind: "maintained",
      batchRead,
      waitMs,
      ...measurement,
      nativeStatistics,
      buffer: reader.statistics(),
    };
  } finally {
    clearTimeout(timer);
    await reader.disconnect();
  }
}
async function controlled(pauseEach, waitMs) {
  const groupId = "diag-control-" + randomUUID();
  groups.push(groupId);
  const reader = new native.KafkaConsumer({
    "metadata.broker.list": brokers.join(","),
    "client.id": groupId,
    "group.id": groupId,
    "enable.auto.commit": false,
    "enable.auto.offset.store": false,
    "isolation.level": "read_committed",
    "allow.auto.create.topics": false,
    "queued.max.messages.kbytes": 32 * 1024,
    "queued.min.messages": 1,
    "fetch.queue.backoff.ms": 5,
    "fetch.wait.max.ms": 25,
    "fetch.max.bytes": 1024 * 1024,
    "statistics.interval.ms": 100,
  });
  reader.setDefaultConsumeTimeout(25);
  reader.on("event.error", () => {});
  let nativeStats;
  reader.on("event.stats", (event) => {
    nativeStats = JSON.parse(event.message);
  });
  const counts = new Map(),
    tails = new Map(),
    finishAt = new Map(),
    gaps = [],
    polls = [];
  let received = 0,
    seen = 0,
    first = 0,
    active = 0,
    maxActive = 0,
    workMs = 0;
  const started = performance.now(),
    cpu = process.cpuUsage();
  try {
    await new Promise((res, rej) => reader.connect({}, (e) => (e ? rej(e) : res())));
    reader.assign(
      Array.from({ length: partitions }, (_, partition) => ({ topic, partition, offset: 0 })),
    );
    while (received < total) {
      const polling = performance.now();
      const values = await new Promise((res, rej) =>
        reader.consume(1, (e, v) => (e ? rej(e) : res(v))),
      );
      polls.push(performance.now() - polling);
      for (const m of values) {
        first ||= performance.now();
        received++;
        const part = m.partition;
        if (pauseEach) reader.pause([{ topic, partition: part }]);
        const task = (tails.get(part) ?? Promise.resolve()).then(async () => {
          const at = performance.now();
          if (finishAt.has(part)) gaps.push(at - finishAt.get(part));
          active++;
          maxActive = Math.max(maxActive, active);
          const value = JSON.parse(m.value.toString());
          assert.equal(value.n, counts.get(part) ?? 0);
          counts.set(part, value.n + 1);
          if (waitMs) await delay(waitMs);
          workMs += performance.now() - at;
          active--;
          finishAt.set(part, performance.now());
          seen++;
          if (pauseEach) reader.resume([{ topic, partition: part }]);
        });
        tails.set(part, task);
      }
      if (!values.length) await delay(5);
      if (performance.now() - started > 120000) throw Error("controlled timeout");
    }
    await Promise.all(tails.values());
    const dist = (a) => {
      a = a.sort((x, y) => x - y);
      return {
        count: a.length,
        mean: a.reduce((n, x) => n + x, 0) / a.length,
        p95: a[Math.ceil(a.length * 0.95) - 1],
        max: a.at(-1),
      };
    };
    return {
      kind: pauseEach ? "controlled-pause" : "controlled-buffered",
      waitMs,
      maxActive,
      averageActive: workMs / (performance.now() - first),
      gapsMs: dist(gaps),
      pollMs: dist(polls),
      nativeStats: nativeStats && {
        rxmsgs: nativeStats.rxmsgs,
        rxmsgBytes: nativeStats.rxmsg_bytes,
        rxbytes: nativeStats.rx_bytes,
        fetchRequests: Object.values(nativeStats.brokers).reduce(
          (n, b) => n + (b.req?.Fetch ?? 0),
          0,
        ),
      },
      ...stats(seen, started, first, cpu),
    };
  } finally {
    await new Promise((res) => reader.disconnect(() => res()));
  }
}
try {
  await admin.connect();
  await admin.createTopics({
    topics: [
      {
        topic,
        numPartitions: partitions,
        replicationFactor: 3,
        configEntries: [{ name: "min.insync.replicas", value: "2" }],
      },
    ],
  });
  created = true;
  await delay(1200);
  await producer.start();
  const padding = "x".repeat(2048);
  for (let base = 0; base < total; base += 2048) {
    await Promise.all(
      Array.from({ length: Math.min(2048, total - base) }, (_, j) => {
        const i = base + j;
        const partition = i % partitions,
          n = Math.floor(i / partitions);
        return producer.append(partition, {
          factId: randomUUID(),
          sessionId: "partition-" + partition,
          value: JSON.stringify({ n, padding }),
        });
      }),
    );
  }
  if (compareBatch) {
    // The same preloaded offsets, partition ordering checks and zero-delay handler in every pass.
    for (const batchRead of [false, true, true, false]) {
      report.results.push(await maintained(0, batchRead));
    }
  } else {
    report.results.push(await maintained(10));
    for (const [pauseEach, ms] of [
      [true, 10],
      [false, 10],
      [false, 50],
      [true, 50],
    ]) {
      report.results.push(await controlled(pauseEach, ms));
    }
  }
} finally {
  await producer.close();
  if (created) {
    await admin.deleteTopics({ topics: [topic] });
    const deadline = Date.now() + 15000;
    while ((await admin.listTopics()).includes(topic)) {
      assert(Date.now() < deadline, "Owned Topic deletion not confirmed");
      await delay(250);
    }
  }
  if (created) {
    const ownedGroups = async () =>
      (await admin.listGroups()).groups.map((g) => g.groupId).filter((id) => groups.includes(id));
    const present = await ownedGroups();
    if (present.length) await admin.deleteGroups(present);
    assert.equal((await ownedGroups()).length, 0, "Owned groups remain after cleanup");
  }
  await admin.disconnect();
  console.log(JSON.stringify({ report, cleanupConfirmed: created }));
}
