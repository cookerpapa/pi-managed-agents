import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { monitorEventLoopDelay } from "node:perf_hooks";
import pg from "pg";
import { verifyCommitClients, isCommitStudy, isAsyncStudy } from "./pg-group-commit.mjs";
import { setTimeout as delay } from "node:timers/promises";
import { distribution } from "../../lib/runtime-load-scheduler.mjs";
import { wall } from "./pg-path-replay.mjs";

function cpuCounters() {
  const fields = Object.fromEntries(
    readFileSync("/sys/fs/cgroup/cpu.stat", "utf8")
      .trim()
      .split("\n")
      .map((l) => l.split(" ")),
  );
  const pressure = readFileSync("/sys/fs/cgroup/cpu.pressure", "utf8").match(/some .*?total=(\d+)/);
  return [Number(fields.usage_usec), Number(fields.throttled_usec ?? 0), Number(pressure[1])];
}
const dist = (a) => ({
  ...distribution(a),
  mean: a.length ? a.reduce((s, n) => s + n, 0) / a.length : null,
});
const blank = () => ({ pool: [], query: [], hold: [], terminal: [], operations: 0, records: 0 });
const summarize = (b) => ({
  records: b.records,
  operations: b.operations,
  pool: dist(b.pool),
  query: dist(b.query),
  hold: dist(b.hold),
  terminal: dist(b.terminal),
});

// Observe a fixed cluster-wide WAL position after the measured ACK interval.
// Polling is read-only and never forces a flush.
export async function observeFlush(client, end) {
  const {
    rows: [first],
  } =
    await client.query(`with s as materialized (select pg_current_wal_insert_lsn() target,pg_current_wal_flush_lsn() flushed)
    select target::text, greatest(0,pg_wal_lsn_diff(target,flushed))::float8 bytes from s`);
  const firstProbeMs = wall() - end;
  let remaining = first.bytes,
    polls = 0;
  while (remaining > 0) {
    assert(wall() - end < 15000, "Post-trial WAL flush did not reach its captured target");
    await delay(5);
    const {
      rows: [row],
    } = await client.query(
      "select pg_wal_lsn_diff($1::pg_lsn,pg_current_wal_flush_lsn())::float8 bytes",
      [first.target],
    );
    remaining = row.bytes;
    polls++;
  }
  return { initialGapBytes: first.bytes, firstProbeMs, coveredMs: wall() - end, polls };
}

export class PgPathMeasure {
  constructor(pool, token, tagIdle) {
    this.pool = pool;
    this.token = token;
    this.tagIdle = tagIdle;
    this.round = 0;
    this.canary = new pg.Client({
      connectionString: pool.options.connectionString,
      application_name: "pg-path-read-canary",
      statement_timeout: 5000,
      connectionTimeoutMillis: 5000,
    });
  }
  async withClients(fn) {
    const acquired = await Promise.allSettled(
      Array.from({ length: 12 }, () => this.pool.connect()),
    );
    const clients = acquired.flatMap((r) => (r.status === "fulfilled" ? [r.value] : []));
    try {
      const error = acquired.find((r) => r.status === "rejected");
      if (error) throw error.reason;
      await fn(clients);
    } finally {
      for (const c of clients) c.release();
    }
  }
  async start(start, diagnostics, commitSetting) {
    this.tag = this.token + "_" + ++this.round;
    this.startAt = start;
    this.bySecond = new Map();
    this.byKind = new Map();
    this.spans = [];
    this.maxWaiting = 0;
    this.applied = 0;
    this.samples = [];
    this.diagnostics = diagnostics;
    this.readPings = [];
    if ((diagnostics || isAsyncStudy(process.argv[8])) && !this.connected) {
      await this.canary.connect();
      this.connected = true;
    }
    await this.withClients(async (clients) => {
      this.commitSetting = commitSetting
        ? await verifyCommitClients(clients, commitSetting)
        : undefined;
      // pg-pool checks out its most recently returned idle connection first.
      const c = clients.at(-1);
      this.pid = c.processID;
      if (!diagnostics) return;
      await Promise.all(
        clients.map((c) =>
          c.query(
            "set track_io_timing=on; set track_wal_io_timing=on; select pg_stat_force_next_flush()",
          ),
        ),
      );
      await c.query(
        "set log_statement='none'; set log_min_duration_statement=-1; set log_min_duration_sample=-1; set log_parameter_max_length=0; set log_parameter_max_length_on_error=0; set log_duration=on",
      );
      await c.query("do $$ begin raise log 'pma_path_" + this.tag + "_begin'; end $$");
    });
    this.delay = monitorEventLoopDelay({ resolution: 10 });
    this.delay.enable();
    let elu = performance.eventLoopUtilization(),
      cpu = cpuCounters(),
      processCpu = process.cpuUsage(),
      lastPing = 0;
    const sample = () => {
      const current = performance.eventLoopUtilization(),
        now = cpuCounters(),
        usage = process.cpuUsage();
      if (wall() >= this.startAt)
        this.samples.push({
          at: wall(),
          applied: this.applied,
          poolWaiting: this.pool.waitingCount,
          loopUtilization: performance.eventLoopUtilization(current, elu).utilization,
          loopDelayP99Ms: this.delay.count ? this.delay.percentile(99) / 1e6 : 0,
          cpuUs: now[0] - cpu[0],
          processCpuUs: usage.user + usage.system - processCpu.user - processCpu.system,
          throttledUs: now[1] - cpu[1],
          cpuPressureUs: now[2] - cpu[2],
          ...(["cpu", "cpu-smoke", "prefix-smoke", "prefix-study", "prefix-confirm"].includes(
            process.argv[8],
          ) || isCommitStudy(process.argv[8])
            ? {
                allowed: readFileSync("/sys/fs/cgroup/cpuset.cpus.effective", "utf8").trim(),
              }
            : {}),
        });
      cpu = now;
      elu = current;
      processCpu = usage;
      this.delay.reset();
      const at = wall();
      if (diagnostics && !this.pendingPing && at >= this.startAt && at - lastPing >= 500) {
        lastPing = at;
        this.pendingPing = this.canary
          .query("select 1")
          .then(() => this.readPings.push([at, wall() - at]))
          .catch((e) => {
            this.pingError = e;
          })
          .finally(() => {
            this.pendingPing = undefined;
          });
      }
    };
    this.sample = sample;
    this.timer = setInterval(sample, 250);
  }
  bin(at) {
    const s = Math.max(0, Math.floor((at - this.startAt) / 1000));
    if (!this.bySecond.has(s)) this.bySecond.set(s, blank());
    return this.bySecond.get(s);
  }
  record(terminalAge) {
    const b = this.bin(wall());
    b.records++;
    this.applied++;
    if (terminalAge !== undefined) b.terminal.push(terminalAge);
  }
  async acquire(note) {
    const checkout = wall(),
      c = await this.pool.connect(),
      taken = wall();
    let kind, sent, returned;
    this.maxWaiting = Math.max(this.maxWaiting, this.pool.waitingCount);
    return {
      query: async (sql, values) => {
        if (sql === "rollback") return c.query(sql);
        assert.equal(kind, undefined, "Expected one real autocommit write per checkout");
        kind = note(sql, values);
        const sampled = this.diagnostics && c.processID === this.pid;
        const text =
          sampled && this.tagIdle ? `/*pmap_${this.tag}_${this.spans.length}*/${sql}` : sql;
        sent = wall();
        const r = await c.query(text, values);
        returned = wall();
        if (sampled) this.spans.push([kind, sent, returned]);
        return r;
      },
      release: () => {
        c.release();
        if (returned === undefined) return;
        const released = wall();
        if (!this.byKind.has(kind)) this.byKind.set(kind, blank());
        for (const b of [this.bin(released), this.byKind.get(kind)]) {
          b.operations++;
          b.pool.push(taken - checkout);
          b.query.push(returned - sent);
          b.hold.push(released - taken);
        }
      },
    };
  }
  async end() {
    const end = wall();
    clearInterval(this.timer);
    this.sample();
    this.delay.disable();
    await this.pendingPing;
    if (this.pingError) throw this.pingError;
    const flush = isAsyncStudy(process.argv[8]) ? await observeFlush(this.canary, end) : undefined;
    if (this.diagnostics)
      await this.withClients(async (clients) => {
        const c = clients.find((c) => c.processID === this.pid);
        assert(c, "Sampled PG connection was replaced");
        await c.query("do $$ begin raise log 'pma_path_" + this.tag + "_end'; end $$");
        await c.query("set log_duration=off");
        await Promise.all(
          clients.map((c) =>
            c.query(
              "reset track_io_timing; reset track_wal_io_timing; select pg_stat_force_next_flush()",
            ),
          ),
        );
      });
    return {
      tag: this.tag,
      pid: this.pid,
      diagnostics: this.diagnostics,
      ...(this.commitSetting ? { commitSetting: this.commitSetting } : {}),
      ...(flush ? { postAckFlush: flush } : {}),
      started: this.startAt,
      ended: end,
      spans: this.spans,
      samples: this.samples,
      readPings: this.readPings,
      maxWaiting: this.maxWaiting,
      applied: this.applied,
      byKind: Object.fromEntries([...this.byKind].map(([k, b]) => [k, summarize(b)])),
      seconds: Object.fromEntries([...this.bySecond].map(([k, b]) => [k, summarize(b)])),
    };
  }
  async close() {
    clearInterval(this.timer);
    this.delay?.disable();
    if (this.connected) await this.canary.end();
  }
}
