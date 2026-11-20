import assert from "node:assert/strict";
import { performance } from "node:perf_hooks";
import { setTimeout as delay } from "node:timers/promises";
import native from "@confluentinc/kafka-javascript";
// Bundle the candidate reader from this checkout into the isolated actor.
import { KafkaLogConsumer } from "../../../packages/event-log/src/kafka-log-consumer.ts";
import { decodeSessionLogFact } from "@pi-cloud/runtime-core/kafka-accepted-fact";
import { distribution } from "../../lib/runtime-load-scheduler.mjs";
import { brokers, topic, actor, send, fail, listen, openPool } from "./actor.mjs";
import { installTailProbe } from "./tail-probe.mjs";
import { PgPathStudy } from "./pg-path-study.mjs";
import { ReadyProjection, isMovingStudy, recoveryConnectionError } from "./ready-projection.mjs";
import { projectRecord } from "./project-record.mjs";
import { projectionTemplates } from "./projection-statements.mjs";
import { isCommitStudy } from "./pg-group-commit.mjs";
import { MessageRateMeter } from "./complete-message-rate.mjs";

let resetAssignment;
const pool = await openPool(
    Number(process.argv[7] ?? 12),
    __PG_PATH_STUDY__
      ? {
          idleTimeoutMillis: 0,
          onError: (e) => {
            if (resetAssignment && isMovingStudy(process.argv[8]) && recoveryConnectionError(e))
              resetAssignment();
            else fail(e);
          },
        }
      : {},
  ),
  group = topic + "-projector",
  startedAt = performance.now();
const admin = new native.KafkaJS.Kafka({
  kafkaJS: { brokers, clientId: topic, logLevel: native.KafkaJS.logLevel.NOTHING },
}).admin();
await admin.connect();
const tailProbe = process.argv[5] === "tail" ? installTailProbe(native, group, topic) : undefined;
const pathStudy = __PG_PATH_STUDY__
  ? new PgPathStudy(
      pool,
      topic.slice(-16),
      ["timing", "cpu", "cpu-smoke"].includes(process.argv[8]) || isCommitStudy(process.argv[8]),
    )
  : undefined;
const sessions = new Map(),
  nextOffsets = new Map();
let cpuAt = process.cpuUsage(),
  eluAt = performance.eventLoopUtilization();
let stage,
  metrics = {},
  created = false;
const blank = () => ({
  byKind: {},
  byPartition: Array(32).fill(0),
  acknowledgedInputs: 0,
  inboxSnapshots: 0,
  inboxBytes: 0,
  skipped: 0,
  stale: 0,
  duplicate: 0,
  firstReceipt: null,
  lastCommit: null,
  queries: 0,
  peakWaiting: 0,
});
metrics = blank();
function account(kind, start, poolMs, pgMs, wireMs) {
  const m = (metrics.byKind[kind] ??= { wall: [], pool: [], pg: [], wire: [] });
  m.wall.push(performance.now() - start);
  m.pool.push(poolMs);
  m.pg.push(pgMs);
  m.wire.push(wireMs);
}
function summary() {
  const byKind = {};
  for (const [k, g] of Object.entries(metrics.byKind)) {
    byKind[k] = { count: g.wall.length };
    for (const [key, values] of Object.entries(g))
      byKind[k][key] = {
        ...distribution(values),
        sumMs: Math.round(values.reduce((a, b) => a + b, 0)),
      };
  }
  return {
    ...metrics,
    byKind,
    reader: reader.statistics(),
    processCpu: process.cpuUsage(cpuAt),
    loop: performance.eventLoopUtilization(eluAt),
    elapsed: performance.now() - startedAt,
  };
}
const prefixStudy = __READY_STUDY__
  ? new ReadyProjection(pool, topic, sessions, projectionTemplates)
  : undefined;
let messageMeter;
const environment = {
  pool,
  sessions,
  nextOffsets,
  account,
  send: (kind, data) => {
    if (!messageMeter || kind !== "settled") send(kind, data);
  },
  pathStudy,
  tailProbe,
  partitions: 32,
  get stage() {
    return stage;
  },
  get metrics() {
    return metrics;
  },
};
const processRecord = (record, draft) => projectRecord(record, draft, environment);
const reader = new KafkaLogConsumer({
  brokers,
  topic,
  groupId: group,
  clientId: group,
  groupRecovery: true,
  batchRead: process.argv[6] !== "single",
  replayOffsets: async (bounds) =>
    prefixStudy?.width
      ? prefixStudy.recover(bounds)
      : new Map(bounds.map((b) => [b.partition, b.low])),
  ...(prefixStudy
    ? {
        prepareReady: async (records, current) => {
          try {
            if (messageMeter && messageMeter.at === undefined) await messageMeter.gate.promise;
            return await prefixStudy.apply(
              records,
              current,
              processRecord,
              pathStudy?.mode ? (f, sql, args) => pathStudy.note(f, sql, args) : undefined,
              () =>
                pathStudy?.mode ? pathStudy.measure.acquire(() => "stable_prefix") : pool.connect(),
            );
          } catch (e) {
            if (current() && prefixStudy.policy !== "fixed" && recoveryConnectionError(e))
              reader.reloadRecoveryState();
            throw e;
          }
        },
        onRevoked: () => prefixStudy.revoke(),
      }
    : {}),
  decode: (v) => {
    const f = decodeSessionLogFact(v).fact;
    pathStudy?.decoded(f, v);
    return f;
  },
  handler: async (record) => {
    try {
      if (pathStudy?.mode) pathStudy.before(record);
      if (!prefixStudy?.width) await processRecord(record);
      if (pathStudy?.mode) pathStudy.applied(record.fact);
      messageMeter?.apply(record.fact);
    } catch (e) {
      fail(e);
      throw e;
    }
  },
});
resetAssignment = () => reader.reloadRecoveryState();
async function resetPrefix(width, policy) {
  if (!prefixStudy) return;
  const ends = await reader.captureEndOffsets();
  await reader.waitUntilInitialReplay(ends);
  await prefixStudy.reset(new Map(ends.entries()), width ?? 0, policy);
}
async function lag() {
  const ends = await admin.fetchTopicOffsets(topic);
  let sum = 0,
    max = 0;
  for (const p of ends) {
    const d = Math.max(
      0,
      Number(BigInt(p.high ?? p.offset) - (nextOffsets.get(p.partition) ?? 0n)),
    );
    sum += d;
    max = Math.max(max, d);
  }
  return {
    lag: sum,
    partitionLagMax: max,
    poolWaiting: pool.waitingCount,
    queued: reader.statistics().queuedRecords,
  };
}
async function cleanup() {
  try {
    try {
      if (messageMeter) {
        messageMeter.gate.resolve();
        await messageMeter.stop();
      }
    } finally {
      await reader.close();
    }
  } finally {
    try {
      if (created) {
        await admin.deleteTopics({ topics: [topic] });
        const end = Date.now() + 30000;
        while ((await admin.listTopics()).includes(topic)) {
          assert(Date.now() < end);
          await delay(150);
        }
        if ((await admin.listGroups()).groups.some((g) => g.groupId === group))
          await admin.deleteGroups([group]);
        assert(!(await admin.listGroups()).groups.some((g) => g.groupId === group));
      }
      send("cleaned", { actor, topicAndGroupRemoved: created });
    } finally {
      await admin.disconnect();
      await pathStudy?.measure.close();
      await pool.end();
    }
  }
}
try {
  await admin.createTopics({
    topics: [
      {
        topic,
        numPartitions: 32,
        replicationFactor: 3,
        configEntries: [
          { name: "min.insync.replicas", value: "2" },
          ...(isMovingStudy(process.argv[8])
            ? [
                { name: "retention.ms", value: "604800000" },
                { name: "cleanup.policy", value: "delete" },
                { name: "retention.bytes", value: "-1" },
                { name: "message.timestamp.type", value: "LogAppendTime" },
              ]
            : []),
        ],
      },
    ],
  });
  created = true;
  if (prefixStudy) await prefixStudy.install();
  await reader.start();
  await reader.waitUntilAssigned(45000);
  listen(
    {
      stage: async (m) => {
        assert.equal(sessions.size, 0);
        stage = m;
        await resetPrefix(m.prefixWidth, m.projectionPolicy);
        cpuAt = process.cpuUsage();
        eluAt = performance.eventLoopUtilization();
        metrics = blank();
        if (tailProbe) await tailProbe.configure(m, pool);
        if (m.pgPath) await pathStudy.live(m.pgPath, m.pathDiagnostics, m.commitSetting);
        send("configured", { stage: m.stage });
      },
      sample: async (m) => send("sample", { n: m.n, ...(await lag()) }),
      prepareMessages: async (m) => {
        assert(!sessions.size && !messageMeter);
        stage = { integrated: true, scene: "plain", sink: "single" };
        metrics = blank();
        await resetPrefix(16, m.policy);
        prefixStudy.persistUsage = m.persistUsage;
        prefixStudy.bulkUsage = m.bulkUsage;
        messageMeter = new MessageRateMeter(m.owners, m.rate, !m.rate);
        send("messagesReady");
      },
      releaseMessages: (m) => {
        messageMeter.start(m.start ?? performance.timeOrigin + performance.now(), lag);
        send("messagesReleased");
      },
      finishMessages: async () => {
        const result = await messageMeter.done.promise;
        await messageMeter.stop();
        result.sampledPeakLag = messageMeter.peakLag;
        const ends = await reader.captureEndOffsets();
        await reader.waitUntilInitialReplay(ends);
        assert.equal(sessions.size, 0);
        messageMeter = undefined;
        send("messagesFinished", { result, stats: prefixStudy.stats, lag: (await lag()).lag });
      },
      freezePaths: async () => send("pathTape", { tape: await pathStudy.freeze() }),
      preparePaths: async (m) => {
        stage = { integrated: true, scene: "plain", sink: "single" };
        await resetPrefix(m.prefixWidth, m.projectionPolicy);
        const start = await pathStudy.prepare(m.path, m.rate, m.diagnostics, m.commitSetting);
        send("pathReady", { start });
      },
      runPaths: async (m) => {
        const source = m.path === "direct" ? await pathStudy.direct() : undefined;
        const prefix = prefixStudy?.width ? prefixStudy.stats : undefined;
        send("pathResult", { source, result: { ...(await pathStudy.result(prefix)), prefix } });
      },
      results: async (m) => {
        const ends = await reader.captureEndOffsets();
        await reader.waitUntilInitialReplay(ends, 30000);
        assert.equal(sessions.size, 0);
        send("results", {
          stage: m.stage,
          stats: summary(),
          ...(prefixStudy?.width ? { prefix: prefixStudy.stats } : {}),
          tail: tailProbe?.results(),
          ...(pathStudy?.mode ? { pgPath: await pathStudy.endLive() } : {}),
          ...(await lag()),
        });
      },
    },
    cleanup,
  );
  send("ready", { actor });
} catch (e) {
  try {
    await cleanup();
  } finally {
    throw e;
  }
}
