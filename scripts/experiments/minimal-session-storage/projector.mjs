import assert from "node:assert/strict";
import { performance } from "node:perf_hooks";
import { setTimeout as delay } from "node:timers/promises";
import native from "@confluentinc/kafka-javascript";
// Bundle the candidate reader from this checkout into the isolated actor.
import { KafkaLogConsumer } from "../../../packages/event-log/src/kafka-log-consumer.ts";
import { decodeSessionLogFact } from "@pi-cloud/runtime-core/kafka-accepted-fact";
import { distribution } from "../../lib/runtime-load-scheduler.mjs";
import { brokers, topic, actor, send, fail, listen, openPool } from "./actor.mjs";
import { continuationGuestRoutes } from "./continuation-guest-routes.mjs";
import { installTailProbe } from "./tail-probe.mjs";
import { PgPathStudy } from "./pg-path-study.mjs";
import { ReadyProjection, isMovingStudy, recoveryConnectionError } from "./ready-projection.mjs";
import { projectRecord } from "./project-record.mjs";
import { projectionTemplates } from "./projection-statements.mjs";
import { isCommitStudy } from "./pg-group-commit.mjs";
import { WorkspaceOperationRouter } from "./workspace-operation-router.mjs";
import { decodeWorkspaceFact, operationsFor } from "./workspace-operations.mjs";
import { partitions, targets } from "./fanout-fixture.mjs";
import { FanoutProjector } from "./fanout-projector.mjs";
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
  byPartition: Array(partitions).fill(0),
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
  if (stage?.fanout) return; // The fan-out recorder keeps fixed-size phase histograms.
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
    if (kind !== "settled" || (!messageMeter && !stage?.fanout)) send(kind, data);
  },
  pathStudy,
  tailProbe,
  partitions,
  get stage() {
    return stage;
  },
  get metrics() {
    return metrics;
  },
};
const processRecord = (record, draft) => projectRecord(record, draft, environment);
const workspaceProfile = [
  "workspace-fanout",
  "workspace-rate",
  "workspace-rate-confirm",
  "workspace-rate-input",
  "workspace-continuation",
  "workspace-guest-continuation",
  "workspace-guest-mix",
].includes(process.argv[8]);
const ownedReplyTopics = new Set();
const guestRoutes = ["workspace-guest-continuation", "workspace-guest-mix"].includes(
  process.argv[8],
)
  ? continuationGuestRoutes(pool, send)
  : undefined;
const fanout =
  process.argv[8] === "workspace-fanout"
    ? new FanoutProjector(topic.slice(-16), lag, () => reader.statistics())
    : undefined;
fanout?.observeCommits(native, group);
const workspaceRouter = workspaceProfile
  ? new WorkspaceOperationRouter({
      pool,
      view: prefixStudy,
      env: environment,
      ...(fanout ? { onToolStart: (event, r) => fanout.toolStarted(event, r) } : {}),
      select(scope) {
        assert(guestRoutes && stage.guestTools, "History selected a resource");
        return guestRoutes.select(scope);
      },
      resolve(binding) {
        if (fanout?.spec) return fanout.resolve(binding);
        assert(guestRoutes && stage.guestTools, "History dispatched a tool");
        return guestRoutes.resolve(binding);
      },
    })
  : undefined;
let workspaceMode = "batch";
const preparedHistory = new WeakSet();
const reader = new KafkaLogConsumer({
  brokers,
  topic,
  groupId: group,
  clientId: group,
  groupRecovery: true,
  orderedEffects: workspaceProfile,
  ...(fanout && __GRANT_STUDY__ ? { effectPrefixLimit: __FANOUT_PREFIX_LIMIT__ } : {}),
  batchRead: process.argv[6] !== "single",
  replayOffsets: async (bounds) =>
    prefixStudy?.width
      ? prefixStudy.recover(bounds)
      : new Map(bounds.map((b) => [b.partition, b.low])),
  ...(prefixStudy
    ? {
        prepareReady: async (records, current, grantPrefix) => {
          try {
            if (fanout?.spec)
              return await fanout.prepare(records, current, grantPrefix, workspaceRouter);
            if (messageMeter && messageMeter.at === undefined) await messageMeter.gate.promise;
            if (workspaceRouter && workspaceMode !== "direct") {
              if (workspaceMode === "single") return 1;
              const n = await workspaceRouter.prepareHistory(records, current);
              for (let i = 0; i < n; i++) preparedHistory.add(records[i].fact);
              return n || 1;
            }
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
    const f = (workspaceProfile ? decodeWorkspaceFact : decodeSessionLogFact)(v).fact;
    pathStudy?.decoded(f, v);
    return f;
  },
  handler: async (record, current, grant) => {
    try {
      if (fanout?.spec)
        return await fanout.handle(record, current, grant, workspaceRouter, nextOffsets);
      if (pathStudy?.mode) pathStudy.before(record);
      if (!prefixStudy?.width) await processRecord(record);
      else if (
        workspaceRouter &&
        workspaceMode !== "direct" &&
        !preparedHistory.delete(record.fact)
      ) {
        if (record.fact.kind === "durable_commit") {
          const op = stage.guestQuarters !== undefined && operationsFor(record.fact)[0];
          const timing = op ? { received: performance.timeOrigin + performance.now() } : undefined;
          await workspaceRouter.consume(
            record,
            current,
            grant,
            timing
              ? (key) => {
                  timing[key] = performance.timeOrigin + performance.now();
                  if (key === "granted")
                    send("toolTiming", { id: record.fact.scope.runId, op: op.operationId, timing });
                }
              : undefined,
          );
        } else await prefixStudy.apply([record], current, processRecord);
      }
      if (
        stage?.guestQuarters !== undefined &&
        record.fact.kind === "durable_commit" &&
        record.fact.commit.writes.some(
          (w) => w.type === "entry" && w.value.model?.some((m) => m.role === "toolResult"),
        )
      ) {
        send("resultProjected", {
          id: record.fact.scope.runId,
          at: performance.timeOrigin + performance.now(),
        });
      }
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
    ...(fanout ? { endSum: ends.reduce((n, p) => n + Number(p.high ?? p.offset), 0) } : {}),
    partitionLagMax: max,
    poolWaiting: pool.waitingCount,
    queued: reader.statistics().queuedRecords,
  };
}
async function verifyGroupPosition(ends, allowPeriodic) {
  const deadline = Date.now() + 5000;
  for (;;) {
    const offsets = (await admin.fetchOffsets({ groupId: group, topics: [topic] }))[0].partitions;
    if (
      offsets.length === partitions &&
      offsets.every((row) => BigInt(row.offset) === ends[row.partition])
    )
      break;
    assert(
      allowPeriodic && Date.now() < deadline,
      "Committed offsets did not cover verified input",
    );
    await delay(80);
  }
}
async function cleanup() {
  try {
    try {
      if (messageMeter) {
        messageMeter.gate.resolve();
        await messageMeter.stop();
      }
    } finally {
      await fanout?.close();
      await reader.close();
    }
  } finally {
    try {
      if (created) {
        const existing = await admin.listTopics();
        const owned = [topic, ...ownedReplyTopics].filter((t) => existing.includes(t));
        if (owned.length) await admin.deleteTopics({ topics: owned });
        const end = Date.now() + 30000;
        while ((await admin.listTopics()).some((t) => owned.includes(t))) {
          assert(Date.now() < end);
          await delay(150);
        }
        const ownedGroups = new Set([
          group,
          ...[...ownedReplyTopics].map(
            (t) => "tool-reply-" + t.slice("pi-cloud.tool-replies.v4.".length),
          ),
        ]);
        const remaining = (await admin.listGroups()).groups
          .map((g) => g.groupId)
          .filter((id) => ownedGroups.has(id));
        if (remaining.length) await admin.deleteGroups(remaining);
        assert(!(await admin.listGroups()).groups.some((g) => ownedGroups.has(g.groupId)));
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
        numPartitions: partitions,
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
      fanoutConnect: async (m) => {
        const before = process.memoryUsage(),
          at = performance.now();
        await fanout.sender.connect(m.url);
        send("fanoutConnected", {
          before,
          after: process.memoryUsage(),
          elapsedMs: performance.now() - at,
        });
      },
      fanoutDirect: async (m) => send("fanoutDirectDone", { result: await fanout.direct(m) }),
      fanoutReset: async () => {
        await resetPrefix(16, "recorded-history");
        stage = { fanout: true, integrated: true, sink: "single", scene: "plain" };
        metrics = blank();
        send("fanoutResetDone");
      },
      fanoutWarm: async (m) => {
        const ends = await reader.captureEndOffsets();
        await reader.waitUntilInitialReplay(ends);
        assert.equal(sessions.size, m.mixed ? 0 : targets);
        await fanout.warm(workspaceRouter, topic);
        // A complete mixed trace starts before any new seed records or grants.
        if (!m.mixed) await verifyGroupPosition(ends, true);
        send("fanoutWarmDone");
      },
      fanoutBegin: (m) => {
        fanout.pgAtStart = {
          poolMs: prefixStudy.stats.poolMs,
          holdMs: prefixStudy.stats.holdMs,
          commits: prefixStudy.stats.commits,
          sync: prefixStudy.stats.syncTransactions,
          async: prefixStudy.stats.asyncTransactions,
        };
        fanout.begin(m);
        send("fanoutBegun");
      },
      fanoutRelease: (m) => {
        fanout.release(m.start);
        send("fanoutReleased");
      },
      fanoutFinish: async () => {
        const periodicTail = fanout.spec.mode === "pg-control" || fanout.spec.mixed;
        const result = await fanout.finish();
        result.pgTransactions = {
          poolLimit: pool.options.max,
          openConnections: pool.totalCount,
          total: prefixStudy.stats.commits - fanout.pgAtStart.commits,
          poolMs: prefixStudy.stats.poolMs - fanout.pgAtStart.poolMs,
          holdMs: prefixStudy.stats.holdMs - fanout.pgAtStart.holdMs,
          sync: prefixStudy.stats.syncTransactions - fanout.pgAtStart.sync,
          async: prefixStudy.stats.asyncTransactions - fanout.pgAtStart.async,
        };
        const ends = await reader.captureEndOffsets();
        await reader.waitUntilInitialReplay(ends);
        await verifyGroupPosition(ends, periodicTail);
        send("fanoutFinished", { result, ends: ends.map(String) });
      },
      guestAccepted: (m) => guestRoutes.accepted(m),
      stage: async (m) => {
        for (const boot of m.guestBoots ?? [])
          ownedReplyTopics.add("pi-cloud.tool-replies.v4." + boot);
        assert.equal(sessions.size, 0);
        stage = m;
        workspaceMode = m.workspaceMode ?? "batch";
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
        workspaceMode = m.workspaceMode ?? "batch";
        messageMeter = new MessageRateMeter(m.owners, m.rate, !m.rate, workspaceProfile);
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
