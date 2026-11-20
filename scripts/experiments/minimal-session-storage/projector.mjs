import assert from "node:assert/strict";
import { performance } from "node:perf_hooks";
import { isDeepStrictEqual } from "node:util";
import { setTimeout as delay } from "node:timers/promises";
import native from "@confluentinc/kafka-javascript";
// Bundle the candidate reader from this checkout into the isolated actor.
import { KafkaLogConsumer } from "../../../packages/event-log/src/kafka-log-consumer.ts";
import {
  decodeSessionLogFact,
  kafkaProducerLane,
} from "@pi-cloud/runtime-core/kafka-accepted-fact";
import { applyImmutableBatches } from "@earendil-works/chord/delta";
import { distribution } from "../../lib/runtime-load-scheduler.mjs";
import { brokers, topic, actor, send, fail, listen, openPool } from "./actor.mjs";
import { EMPTY_HEAD, openingHead, priorState } from "./session-prefix.mjs";
import { installTailProbe } from "./tail-probe.mjs";
import { PgPathStudy } from "./pg-path-study.mjs";
import { isCommitStudy } from "./pg-group-commit.mjs";

const pool = await openPool(
    Number(process.argv[7] ?? 12),
    __PG_PATH_STUDY__ ? { idleTimeoutMillis: 0 } : {},
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
const messageSql = (
  guarded,
) => `insert into messages(tenant_id,pi_session_id,entry_id,conversation_id,run_id,role,content,source_input_id,commit_seq,ordinal)
 select $1,$2,v.id,v.conversation,$3,v.role,v.content,v.input,$4,v.ordinal from
 jsonb_to_recordset($5::jsonb) v(id bigint,conversation bigint,role text,content text,input uuid,ordinal int) ${guarded ? "cross join owner" : ""}
 on conflict(tenant_id,pi_session_id,entry_id) do update set entry_id=excluded.entry_id
 where (messages.run_id,messages.commit_seq,messages.ordinal,messages.role,messages.content,messages.source_input_id)
 is not distinct from (excluded.run_id,excluded.commit_seq,excluded.ordinal,excluded.role,excluded.content,excluded.source_input_id) returning entry_id`;
const inboxSql = (
  guarded,
) => `insert into inboxes select $1,$2,1,$3,$4,$5,$6::jsonb ${guarded ? "from owner" : ""}
 on conflict(tenant_id,pi_session_id,conversation_id) do update set commit_seq=excluded.commit_seq,ordinal=excluded.ordinal,items=excluded.items
 where inboxes.doc_id=excluded.doc_id and (inboxes.commit_seq,inboxes.ordinal)<(excluded.commit_seq,excluded.ordinal) returning doc_id`;
const insertMessages = messageSql(false),
  upsertInbox = inboxSql(false);
function singleSql(mutation, run, lease) {
  return `with owner as materialized (select id from runs where tenant_id=$1 and session_id=$2 and id=${run} and lease_id=${lease} and state='running' for share), changed as (${mutation}) select exists(select 1 from owner) as accepted, (select count(*)::int from changed) as writes`;
}
const oneMessage = singleSql(messageSql(true), "$3", "$6"),
  oneInbox = singleSql(inboxSql(true), "$7", "$8");
async function processRecord({ fact: f, partition, offset }) {
  const t = performance.now(),
    wireMs = Date.now() - Date.parse(f.occurredAt),
    { tenantId, piSessionId, runId, writerId } = f.scope;
  assert.equal(kafkaProducerLane(piSessionId, 32), partition);
  let s = sessions.get(runId);
  const trace = tailProbe?.begin(partition, offset, f);
  const measure = (kind, poolMs = 0, pgMs = 0, io) => {
    trace?.complete(io);
    account(kind, t, poolMs, pgMs, wireMs);
    metrics.byPartition[partition]++;
    nextOffsets.set(partition, offset + 1n);
  };
  metrics.firstReceipt ??= Date.now();
  if (f.kind === "work_finished") {
    if (s && s.writer !== writerId) {
      metrics.stale++;
      measure("obsolete");
      return;
    }
    assert.equal(f.terminal.type, "turn.completed");
    if (!s) {
      const prior = await pool.query(
        "select state,lease_id from runs where tenant_id=$1 and session_id=$2 and id=$3",
        [tenantId, piSessionId, runId],
      );
      metrics.queries++;
      if (prior.rows[0]?.lease_id !== writerId) {
        metrics.stale++;
        measure("obsolete");
        return;
      }
      assert.equal(
        prior.rows[0].state,
        "completed",
        "Fresh completion lacks a projected native head",
      );
      metrics.duplicate++;
      measure("duplicate");
      return;
    }
    assert.equal(f.nativeSequence, s.seq);
    assert(s.lastAnswerCommit !== undefined, "Completion lacks a preceding complete answer");
    const sqlStart = performance.now(),
      c = await (pathStudy?.mode ? pathStudy.acquire(f) : pool.connect()),
      taken = performance.now();
    let result;
    try {
      let head;
      if (stage?.integrated) {
        assert.equal(f.baseSequence, s.baseSeq);
        assert(s.inboxId && s.value.items.length === 0, "Run finished before Inbox settlement");
        if (s.floorChanged) {
          const {
            rows: [covered],
          } = await c.query(
            `select count(*)::int n,coalesce(sum(octet_length(content)),0)::bigint bytes
            from messages where tenant_id=$1 and pi_session_id=$2 and conversation_id=1 and entry_id>=$3`,
            [tenantId, piSessionId, s.floor],
          );
          metrics.queries++;
          s.count = covered.n;
          s.bytes = Number(covered.bytes);
        }
        head = {
          version: f.factId,
          seq: s.seq,
          nextId: s.nextId,
          floor: s.floor,
          count: s.count,
          bytes: s.bytes,
        };
      }
      metrics.queries++;
      result = await c.query(
        `update runs set state='completed',settled_at=clock_timestamp()${head ? ",history_head=$5::jsonb" : ""}
     where tenant_id=$1 and session_id=$2 and id=$3 and lease_id=$4 and state='running' returning id`,
        [tenantId, piSessionId, runId, writerId, ...(head ? [JSON.stringify(head)] : [])],
      );
    } finally {
      c.release();
    }
    const end = performance.now(),
      at = Date.now();
    measure(
      "work_finished",
      taken - sqlStart,
      end - taken,
      trace ? [sqlStart, taken, end].map((v) => performance.timeOrigin + v) : undefined,
    );
    if (result.rowCount === 0) {
      metrics.duplicate++;
      return;
    }

    sessions.delete(runId);
    metrics.lastCommit = at;
    if (pathStudy?.mode !== "projector")
      send("settled", {
        id: runId,
        at,
        terminalSinceAnswerMs: end - s.lastAnswerCommit,
        finishTransitMs: wireMs,
        finishHandlerMs: end - t,
        ...(trace ? { tail: trace.terminal } : {}),
      });
    return;
  }
  assert.equal(f.kind, "durable_commit");
  const batch = f.commit;
  if (!s) {
    // If the final record already retired the view, an old Kafka delivery must
    // not re-open it. An active replacement needs native recovery beyond this assay.
    let prior = EMPTY_HEAD,
      inboxId = null,
      items = [];
    if (batch.seq !== 1) {
      if (stage?.integrated) {
        const {
          rows: [row],
        } = await pool.query(openingHead, [runId, tenantId, piSessionId]);
        metrics.queries++;
        if (row?.state !== "running" || row.lease_id !== writerId) {
          metrics.stale++;
          measure("obsolete");
          return;
        }
        prior = priorState(row);
        inboxId = Number(row.inbox_id);
        items = row.inbox_items;
      } else {
        const prior = await pool.query(
          "select state,lease_id from runs where tenant_id=$1 and session_id=$2 and id=$3",
          [tenantId, piSessionId, runId],
        );
        metrics.queries++;
        if (prior.rows[0]?.state !== "running" || prior.rows[0]?.lease_id !== writerId) {
          metrics.stale++;
          measure("obsolete");
          return;
        }
        throw new Error("Active native suffix requires recovery before projection");
      }
    }
    s = {
      writer: writerId,
      seq: prior.seq,
      baseSeq: prior.seq,
      count: prior.count,
      bytes: prior.bytes,
      nextId: prior.nextId,
      floor: prior.floor,
      inboxId,
      value: { items },
      lastFact: null,
    };
  }
  if (s.writer !== writerId) {
    metrics.stale++;
    measure("obsolete");
    return;
  }
  if (batch.seq === s.seq) {
    assert.equal(f.factId, s.lastFact);
    assert(isDeepStrictEqual(batch, s.lastBatch));
    metrics.duplicate++;
    measure("duplicate");
    return;
  }
  assert.equal(batch.seq, s.seq + 1, "Native commit sequence gap");
  let inboxId = s.inboxId,
    value = s.value,
    inboxOrdinal = -1;
  const messages = [],
    inputs = new Map();
  for (const w of batch.writes)
    if (w.type === "submission" && w.value.requestId && w.value.entry !== undefined)
      inputs.set(w.value.entry, w.value.requestId);
  for (const [ordinal, w] of batch.writes.entries()) {
    if (w.type === "document.create" && w.record.kind === "pi.live") s.liveId = w.record.id;
    if (w.type === "document.create" && w.record.kind === "pi.inbox") {
      assert.equal(w.content.kind, "base");
      assert.equal(w.record.scope.conversationId, 1);
      inboxId = w.record.id;
      value = w.content.value;
      inboxOrdinal = ordinal;
    } else if (w.type === "document.change" && w.id === inboxId) {
      value =
        w.content.kind === "base" ? w.content.value : applyImmutableBatches(value, [w.content.ops]);
      inboxOrdinal = ordinal;
    }
    if (w.type === "entry") {
      const role = w.value.model?.[0]?.role;
      if (stage?.integrated || ["user", "assistant", "toolResult"].includes(role))
        messages.push({
          id: w.value.id,
          conversation: w.value.conversationId,
          role: role ?? w.value.kind,
          content: JSON.stringify(w.value),
          input: inputs.get(w.value.id) ?? null,
          ordinal,
        });
      if (stage?.integrated && w.value.head !== undefined && (s.markerId ?? 0) < w.value.id) {
        s.markerId = w.value.id;
        s.floor = w.value.head;
        s.floorChanged = true;
      }
    }
  }
  const lastAssistant = messages.some((m) => m.role === "assistant");
  const kind =
    inboxOrdinal >= 0 && messages.length
      ? "inbox+messages"
      : inboxOrdinal >= 0
        ? "inbox"
        : messages.length
          ? "messages"
          : batch.writes.length &&
              batch.writes.every((w) => w.type === "document.change" && w.id === s.liveId)
            ? "live-only"
            : "native-memory";
  let poolMs = 0,
    pgMs = 0;
  let io;
  if ((inboxOrdinal >= 0 || messages.length) && stage?.sink !== "terminal-only") {
    metrics.peakWaiting = Math.max(metrics.peakWaiting, pool.waitingCount);
    const acq = performance.now(),
      c = await (pathStudy?.mode ? pathStudy.acquire(f) : pool.connect()),
      q = performance.now();
    poolMs = q - acq;
    const query = (...args) => {
      metrics.queries++;
      return c.query(...args);
    };
    try {
      if (
        stage.sink === "single" &&
        ((messages.length === 1 && inboxOrdinal < 0) || (!messages.length && inboxOrdinal >= 0))
      ) {
        const items = inboxOrdinal < 0 ? null : JSON.stringify(value.items);
        const {
          rows: [proof],
        } = await query(
          items === null ? oneMessage : oneInbox,
          items === null
            ? [tenantId, piSessionId, runId, batch.seq, JSON.stringify(messages), writerId]
            : [tenantId, piSessionId, inboxId, batch.seq, inboxOrdinal, items, runId, writerId],
        );
        if (!proof.accepted) {
          metrics.stale++;
          measure("obsolete", poolMs, performance.now() - q);
          return;
        }
        assert.equal(proof.writes, 1, "Conflicting immutable message or Inbox position");
        if (items !== null) {
          metrics.inboxSnapshots++;
          metrics.inboxBytes += Buffer.byteLength(items);
        }
      } else {
        await query("begin");
        const owner = await query(
          "select id from runs where tenant_id=$1 and session_id=$2 and id=$3 and lease_id=$4 and state='running' for share",
          [tenantId, piSessionId, runId, writerId],
        );
        if (!owner.rowCount) {
          await query("rollback");
          metrics.stale++;
          measure("obsolete", poolMs, performance.now() - q);
          return;
        }
        if (messages.length)
          assert.equal(
            (
              await query(insertMessages, [
                tenantId,
                piSessionId,
                runId,
                batch.seq,
                JSON.stringify(messages),
              ])
            ).rowCount,
            messages.length,
          );
        if (inboxOrdinal >= 0) {
          const items = JSON.stringify(value.items);
          assert.equal(
            (
              await query(upsertInbox, [
                tenantId,
                piSessionId,
                inboxId,
                batch.seq,
                inboxOrdinal,
                items,
              ])
            ).rowCount,
            1,
          );
          metrics.inboxSnapshots++;
          metrics.inboxBytes += Buffer.byteLength(items);
        }
        await query("commit");
      }
    } catch (e) {
      await c.query("rollback");
      throw e;
    } finally {
      c.release();
    }
    pgMs = performance.now() - q;
    if (trace) io = [acq, q, q + pgMs].map((v) => performance.timeOrigin + v);
  } else metrics.skipped++;
  s.seq = batch.seq;
  if (stage?.integrated) {
    s.nextId = batch.nextId;
    s.count += messages.length;
    for (const m of messages) s.bytes += Buffer.byteLength(m.content);
  }
  s.lastFact = f.factId;
  s.lastBatch = batch;
  s.value = value;
  s.inboxId = inboxId;
  if (lastAssistant) s.lastAnswerCommit = performance.now();
  sessions.set(runId, s);
  if (lastAssistant && stage?.holdFinish) send("answerCommitted", { id: runId });
  measure(kind, poolMs, pgMs, io);
  if (
    stage?.scene !== "plain" &&
    (inboxOrdinal >= 0 || messages.some((m) => m.role === "user" && m.input !== runId))
  ) {
    metrics.acknowledgedInputs++;
    send("confirmed", {
      id: runId,
      seq: batch.seq,
      at: Date.now(),
      depth: value.items.length,
      placed: messages.some((m) => m.role === "user" && m.input !== runId),
    });
  }
}
const reader = new KafkaLogConsumer({
  brokers,
  topic,
  groupId: group,
  clientId: group,
  groupRecovery: true,
  batchRead: process.argv[6] !== "single",
  replayOffsets: async (bounds) => new Map(bounds.map((b) => [b.partition, b.low])),
  decode: (v) => {
    const f = decodeSessionLogFact(v).fact;
    pathStudy?.decoded(f, v);
    return f;
  },
  handler: async (record) => {
    try {
      if (pathStudy?.mode) pathStudy.before(record);
      await processRecord(record);
      if (pathStudy?.mode) pathStudy.applied(record.fact);
    } catch (e) {
      fail(e);
      throw e;
    }
  },
});
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
    await reader.close();
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
        configEntries: [{ name: "min.insync.replicas", value: "2" }],
      },
    ],
  });
  created = true;
  await reader.start();
  await reader.waitUntilAssigned(45000);
  listen(
    {
      stage: async (m) => {
        assert.equal(sessions.size, 0);
        stage = m;
        cpuAt = process.cpuUsage();
        eluAt = performance.eventLoopUtilization();
        metrics = blank();
        if (tailProbe) await tailProbe.configure(m, pool);
        if (m.pgPath) await pathStudy.live(m.pgPath, m.pathDiagnostics, m.commitSetting);
        send("configured", { stage: m.stage });
      },
      sample: async (m) => send("sample", { n: m.n, ...(await lag()) }),
      freezePaths: async () => send("pathTape", { tape: await pathStudy.freeze() }),
      preparePaths: async (m) => {
        stage = { integrated: true, scene: "plain", sink: "single" };
        const start = await pathStudy.prepare(m.path, m.rate, m.diagnostics, m.commitSetting);
        send("pathReady", { start });
      },
      runPaths: async (m) => {
        const source = m.path === "direct" ? await pathStudy.direct() : undefined;
        send("pathResult", { source, result: await pathStudy.result() });
      },
      results: async (m) => {
        const ends = await reader.captureEndOffsets();
        await reader.waitUntilInitialReplay(ends, 30000);
        assert.equal(sessions.size, 0);
        send("results", {
          stage: m.stage,
          stats: summary(),
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
