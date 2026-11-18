// Invoked inside the managed Session runtime Pod by the isolated cost experiment.
import assert from "node:assert/strict";
import { randomUUID, createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { setTimeout as delay } from "node:timers/promises";
import { performance } from "node:perf_hooks";
import native from "@confluentinc/kafka-javascript";
import { KafkaWorkerProducer, KafkaLogConsumer } from "@pi-cloud/event-log";
import {
  encodeSessionLogFact,
  decodeSessionLogFact,
} from "@pi-cloud/runtime-core/kafka-accepted-fact";
import { createDatabase } from "@pi-cloud/database";
import { sql } from "kysely";

const name = process.argv[1];
assert.match(name, /^pma_cost_[0-9a-f]{14}_seed$/);
const u = new URL((await readFile(process.env.DATABASE_URL_FILE, "utf8")).trim());
u.pathname = "/" + name;
const db = createDatabase({ connectionString: u.toString(), maxConnections: 12 });
const buffered = [];
for await (const chunk of process.stdin) buffered.push(chunk);
const original = JSON.parse(Buffer.concat(buffered).toString()),
  id = randomUUID();
const brokers = process.env.PI_CLOUD_KAFKA_BROKERS.split(","),
  topic = "pma-raw-sink-" + id,
  group = topic + "-reader";
const admin = new native.KafkaJS.Kafka({
  kafkaJS: { brokers, clientId: topic, logLevel: native.KafkaJS.logLevel.NOTHING },
}).admin();
const producer = new KafkaWorkerProducer({ topic, brokers, clientId: topic + "-producer" });
const digest = (s) => createHash("sha256").update(s).digest("hex");
const expected = new Map(),
  results = [];
let done,
  first,
  last,
  seen = 0,
  target,
  created = false;
const reader = new KafkaLogConsumer({
  topic,
  brokers,
  groupId: group,
  clientId: group,
  groupRecovery: true,
  replayOffsets: async (bounds) => new Map(bounds.map((p) => [p.partition, p.low])),
  decode: (b) => {
    const payload = b.toString(),
      f = decodeSessionLogFact(payload).fact;
    assert.equal(f.kind, "durable_commit");
    return { payload, session: f.scope.piSessionId, seq: f.commit.seq, tenant: f.scope.tenantId };
  },
  handler: async ({ fact }) => {
    first ??= performance.now();
    try {
      await db.transaction().execute((tx) =>
        sql`insert into assay_append values(${fact.tenant}::uuid,${fact.session},${fact.seq},${fact.payload})
      on conflict(tenant_id,session_id,seq) do nothing`.execute(tx),
      );
      last = performance.now();
      if (++seen === target) done.resolve();
    } catch (error) {
      done.reject(error);
      throw error;
    }
  },
});
try {
  assert.equal((await sql`select current_database() n`.execute(db)).rows[0].n, name);
  await sql`create unique index assay_key on assay_append(tenant_id,session_id,seq)`.execute(db);
  await admin.connect();
  await admin.createTopics({
    topics: [
      {
        topic,
        numPartitions: 32,
        replicationFactor: 3,
        configEntries: [{ name: "min.insync.replicas", value: "2" }],
      },
    ],
  });
  created = true;
  await producer.start();
  await reader.start();
  await reader.waitUntilAssigned(45000);
  for (const wave of [32, 192, 192, 192]) {
    const records = Array.from({ length: wave }, (_, index) => {
      let src = JSON.stringify(original);
      for (const old of new Set([
        original.scope.sessionId,
        original.scope.runId,
        original.scope.turnId,
        original.scope.writerId,
        original.factId,
        original.modelEvidence[0].context.invocationId,
      ]))
        src = src.replaceAll(old, randomUUID());
      const f = JSON.parse(src),
        value = encodeSessionLogFact(f);
      expected.set(f.scope.piSessionId, digest(value));
      return { partition: index % 32, factId: f.factId, sessionId: f.scope.piSessionId, value };
    });
    done = Promise.withResolvers();
    seen = 0;
    target = wave;
    first = undefined;
    const timer = setTimeout(() => done.reject(new Error("Raw sink wave timed out")), 60000);
    const start = performance.now();
    let ack;
    try {
      await Promise.all([
        Promise.all(records.map(({ partition, ...r }) => producer.append(partition, r))).then(
          () => {
            ack = performance.now();
          },
        ),
        done.promise,
      ]);
    } finally {
      clearTimeout(timer);
    }
    const elapsed = Math.max(last, ack) - start,
      deliveries = last - first;
    if (wave === 192)
      results.push({
        count: wave,
        payloadBytes: Buffer.byteLength(records[0].value),
        allProducerAcksMs: Math.round(ack - start),
        firstDeliveryToLastPgCommitMs: Math.round(deliveries),
        startToAllAcksAndPgMs: Math.round(elapsed),
        recordsPerSecond: Math.round((wave / elapsed) * 100000) / 100,
      });
  }
  const offsets = await admin.fetchTopicOffsets(topic);
  await reader.waitUntilInitialReplay(
    offsets.toSorted((a, b) => a.partition - b.partition).map((p) => BigInt(p.high ?? p.offset)),
    30000,
  );
  const { rows } = await sql`select session_id,payload from assay_append`.execute(db);
  assert.equal(rows.length, expected.size);
  for (const r of rows) assert.equal(digest(r.payload), expected.get(r.session_id));
} finally {
  try {
    await reader.close();
  } finally {
    try {
      await producer.close();
    } finally {
      try {
        if (created) {
          await admin.deleteTopics({ topics: [topic] });
          const deadline = Date.now() + 30000;
          while ((await admin.listTopics()).includes(topic)) {
            assert(Date.now() < deadline, "Owned topic still present");
            await delay(200);
          }
          if ((await admin.listGroups()).groups.some((g) => g.groupId === group))
            await admin.deleteGroups([group]);
          assert(
            !(await admin.listGroups()).groups.some((g) => g.groupId === group),
            "Owned group still present",
          );
        }
      } finally {
        await admin.disconnect();
        await db.destroy();
      }
    }
  }
}
console.log(
  JSON.stringify({
    rawKafkaPg: true,
    partitions: 32,
    replicas: 3,
    minIsr: 2,
    connections: 12,
    poll: 1,
    verifiedRows: expected.size,
    topicAndGroupRemoved: true,
    results,
  }),
);
