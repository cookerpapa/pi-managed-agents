import assert from "node:assert/strict";
import { distribution } from "../../lib/runtime-load-scheduler.mjs";

const wall = () => performance.timeOrigin + performance.now();
const overlap = (a, b, c, d) => Math.max(0, Math.min(b, d) - Math.max(a, c));
const shape = (q) =>
  q.startsWith("with owner")
    ? q.includes("insert into messages")
      ? "message"
      : q.includes("insert into inboxes")
        ? "inbox"
        : null
    : q.startsWith("update runs set state='completed'")
      ? "finish"
      : null;

// Only installed in a separate kubectl-exec experiment process and only for its
// randomly named group. The application's imported reader and its ordering stay intact.
export function installTailProbe(native, group, topic) {
  const original = native.KafkaConsumer,
    arrivals = new Map(),
    sampled = new Map();
  let stage, counts, history, sql, templates, nativeSamples, previousPollEnd, startMs, windows;
  function slice() {
    if (!stage?.switchMs) return;
    const i = Math.floor((wall() - startMs) / stage.switchMs);
    return (windows[i] ??= {
      from: startMs + i * stage.switchMs,
      size: [1, 32, 32, 1][i % 4],
      messages: 0,
      calls: 0,
      done: 0,
    });
  }
  native.KafkaConsumer = class extends original {
    constructor(conf, ...args) {
      const owned = conf["group.id"] === group;
      super(owned ? { ...conf, "statistics.interval.ms": 500 } : conf, ...args);
      this.traced = owned;
      if (owned)
        this.on("event.stats", ({ message }) => {
          if (!stage) return;
          const s = JSON.parse(message),
            parts = Object.values(s.topics?.[topic]?.partitions ?? {}).filter(
              (p) => p.partition >= 0,
            );
          if (!parts.length) return;
          const diff = (a, b) =>
            parts.reduce((n, p) => n + (p[a] >= 0 && p[b] >= 0 ? Math.max(0, p[a] - p[b]) : 0), 0);
          nativeSamples.push({
            at: Date.now(),
            parts: parts.length,
            maximumReportedFetchQueue: Math.max(...parts.map((p) => p.fetchq_cnt)),
            fetchedNotPassed: diff("next_offset", "app_offset"),
            stableNotFetched: diff("ls_offset", "next_offset"),
            messages: s.rxmsgs,
            fetches: Object.values(s.brokers).reduce((n, b) => n + (b.req?.Fetch ?? 0), 0),
          });
        });
    }
    consume(n, callback) {
      if (!this.traced) return super.consume(n, callback);
      const bucket = slice(),
        size = bucket?.size ?? stage?.pollSize ?? n;
      if (size !== 1) this.setDefaultIsTimeoutOnlyForFirstMessage(true);
      const start = performance.now(),
        between = previousPollEnd === undefined ? 0 : start - previousPollEnd;
      return super.consume(size, (err, values) => {
        const now = performance.now(),
          stamp = wall();
        previousPollEnd = now;
        if (!err && stage && values) {
          counts.calls++;
          counts.messages += values.length;
          if (bucket) {
            bucket.calls++;
            bucket.messages += values.length;
          }
          counts.empty += Number(values.length === 0);
          counts.inCallbackWaitMs += now - start;
          counts.betweenMs += between;
          for (const m of values)
            if (m.topic === topic) arrivals.set(m.partition + ":" + m.offset, stamp);
        }
        callback(err, values);
      });
    }
    pause(partitions) {
      if (this.traced && stage) counts.pauses += partitions.length;
      return super.pause(partitions);
    }
  };
  return {
    async configure(s, pool) {
      assert.equal(arrivals.size, 0, "undrained native deliveries");
      stage = s.tail ? s : undefined;
      startMs = wall();
      windows = [];
      previousPollEnd = undefined;
      counts = { calls: 0, messages: 0, empty: 0, pauses: 0, inCallbackWaitMs: 0, betweenMs: 0 };
      history = Array.from({ length: 32 }, () => []);
      sql = [];
      templates = {};
      nativeSamples = [];
      if (!stage?.sampleServer) return;
      const c = await pool.connect();
      try {
        if (!sampled.has(c.processID)) {
          await c.query(
            "set log_statement='none'; set log_min_duration_statement=-1; set log_min_duration_sample=-1; set log_parameter_max_length=0; set log_parameter_max_length_on_error=0; set log_duration=on",
          );
          const call = c.query.bind(c);
          sampled.set(c.processID, true);
          c.query = (q, ...params) => {
            const kind = typeof q === "string" && stage?.sampleServer && shape(q);
            if (!kind) return call(q, ...params);
            const spans = sql,
              t = wall();
            templates[kind] = q;
            return call(q, ...params).then((result) => {
              spans.push([c.processID, kind, t, wall()]);
              return result;
            });
          };
          c.once("end", () => sampled.delete(c.processID));
        }
      } finally {
        c.release();
      }
    },
    begin(partition, offset, f) {
      if (!stage) return;
      const key = partition + ":" + offset,
        received = arrivals.get(key);
      assert(received !== undefined, "missing native receive timestamp");
      arrivals.delete(key);
      const start = wall();
      let terminal;
      if (f.kind === "work_finished") {
        const spans = history[partition];
        let other = 0,
          own = 0,
          pg = 0,
          pool = 0;
        for (let i = spans.length - 1; i >= 0; i--) {
          const h = spans[i];
          if (h.end < received) break;
          const time = overlap(received, start, h.start, h.end);
          if (h.run === f.scope.runId) own += time;
          else other += time;
          if (h.io) {
            pool += overlap(received, start, h.io[0], h.io[1]);
            pg += overlap(received, start, h.io[1], h.io[2]);
          }
        }
        terminal = { created: Date.parse(f.occurredAt), received, start, own, other, pg, pool };
      }
      return {
        terminal,
        complete(io) {
          history[partition].push({ run: f.scope.runId, start, end: wall(), io });
          const bucket = slice();
          if (bucket && terminal) bucket.done++;
        },
      };
    },
    results() {
      if (!stage) return;
      assert.equal(arrivals.size, 0);
      return { counts, nativeSamples, sql, templates, windows };
    },
  };
}

// Phase shares are computed on the same Runs. ACK and consumption can overlap.
export function summarizeTerminal(items) {
  const rows = items
    .map((e) => {
      const t = e.terminal.tail,
        end = e.terminal.at;
      return {
        total: end - e.sentAt,
        claim: e.claimedAt - e.sentAt,
        worker: t.created - e.claimedAt,
        untilReceive: t.received - t.created,
        local: t.start - t.received,
        terminal: end - t.start,
        afterAck: t.received - e.work.finishAckAt,
        own: t.own,
        other: t.other,
        priorPg: t.pg,
        priorPool: t.pool,
        between: Math.max(0, t.start - t.received - t.other - t.own),
      };
    })
    .sort((a, b) => a.total - b.total);
  const summarize = (rs) =>
    Object.fromEntries(
      Object.keys(rows[0]).map((k) => [
        k,
        {
          ...distribution(rs.map((r) => r[k])),
          mean: rs.reduce((sum, r) => sum + r[k], 0) / rs.length,
        },
      ]),
    );
  const p95 = Math.ceil(rows.length * 0.95) - 1;
  return {
    all: summarize(rows),
    slowestFivePercent: summarize(rows.slice(p95)),
    example: rows[p95],
    slowCount: rows.length - p95,
  };
}

// Use PG timestamps for both sides of the actual preceding mailbox boundary.
// Seed snapshots were settled before this stage and never block its first Run.
export function precedingRunDelay(rows, records) {
  const keyed = new Map(
    rows.map((r) => [r.tenant_id + "/" + r.session_id + "/" + r.mailbox_position, r]),
  );
  const events = new Map(records.map((r) => [r.id, r]));
  const samples = rows
    .map((r) => {
      const prev = keyed.get(
        r.tenant_id + "/" + r.session_id + "/" + (Number(r.mailbox_position) - 1),
      );
      assert(prev || Number(r.mailbox_position) === 2);
      const wait = r.started_at - r.created_at,
        previous = prev ? Math.max(0, prev.settled_at - r.created_at) : 0;
      assert(previous <= wait + 1);
      const ack = prev ? events.get(prev.id).work.finishAckAt : 0;
      const afterAck = prev
        ? Math.max(0, Number(prev.settled_at) - Math.max(Number(r.created_at), ack))
        : 0;
      return { wait, previous, ready: wait - previous, afterAck };
    })
    .sort((a, b) => a.wait - b.wait);
  const at = Math.ceil(samples.length * 0.95) - 1,
    tail = samples.slice(at);
  return {
    count: samples.length,
    hadPredecessorWait: samples.filter((s) => s.previous > 0).length,
    example: samples[at],
    slowestFivePercentMean: Object.fromEntries(
      Object.keys(samples[0]).map((k) => [k, tail.reduce((n, r) => n + r[k], 0) / tail.length]),
    ),
  };
}
