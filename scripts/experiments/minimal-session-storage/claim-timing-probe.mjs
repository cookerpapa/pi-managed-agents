// Mounted only by the synthetic serving check. Instrument the same compiled
// Worker entry on both revisions, without changing normal Worker traffic.
import { AsyncLocalStorage } from "node:async_hooks";
import { performance } from "node:perf_hooks";
import pg from "pg";
import { SegmentWorker } from "./independent-session-leases.mjs";

const context = new AsyncLocalStorage();
const backendPositions = new WeakMap();
const startup = pg.Client.prototype.getStartupConf;
pg.Client.prototype.getStartupConf = function () {
  const position = { next: 0 };
  this.on("notice", (n) => {
    if (/^duration: [0-9.]+ ms$/.test(n.message)) position.next++;
  });
  backendPositions.set(this, position);
  const config = startup.call(this);
  config.options =
    (config.options ?? "") +
    " -c client_min_messages=log -c log_duration=on -c log_lock_waits=on -c deadlock_timeout=10ms -c log_parameter_max_length=0";
  return config;
};
function stage(sql) {
  if (sql.includes("from ready_agent_sessions q join sessions")) return "selectCandidates";
  if (sql.includes('from "workers"') && sql.includes("for update")) return "lockWorker";
  if (sql.includes("count(*)::int used")) return "countLeases";
  if (sql.includes("insert into pi_sessions")) return "preparePhysical";
  if (sql.includes("select lease_epoch from pi_sessions")) return "lockPhysical";
  if (sql.includes('from "workers"') && sql.includes("for share")) return "recheckWorker";
  if (sql.includes('from "session_leases"') && sql.includes("for no key update"))
    return "checkPriorLease";
  if (sql.includes("insert into session_leases")) return "grantLeases";
  if (sql.includes("update turns")) return "attachTurns";
  if (sql.includes("delete from ready_agent_sessions")) return "dequeue";
  if (sql.includes("remaining_ms")) return "confirmAuthority";
  if (sql.includes("steer_notice")) return "checkInbox";
  if (sql.startsWith("set local")) return "setDurability";
  return ["begin", "commit", "rollback"].includes(sql) ? sql : "other";
}

const connect = pg.Pool.prototype.connect;
pg.Pool.prototype.connect = function (...args) {
  const trace = context.getStore();
  if (!trace || trace.closed || args.length) return connect.apply(this, args);
  const start = performance.now();
  return connect.call(this).finally(() => {
    trace.poolCheckoutMs += performance.now() - start;
  });
};

const query = pg.Client.prototype.query;
pg.Client.prototype.query = function (statement, ...args) {
  const trace = context.getStore();
  if (!trace || trace.closed || typeof statement !== "string")
    return query.call(this, statement, ...args);
  const label = stage(statement.toLowerCase().trim());
  const part = (trace.stages[label] ??= {
    calls: 0,
    clientMs: 0,
    serverMs: 0,
    lockMs: 0,
    notices: 0,
  });
  const position = backendPositions.get(this);
  const offset = position.next;
  const start = performance.now();
  trace.firstQuery ??= start;
  part.calls++;
  const notice = (n) => {
    const duration = /^duration: ([0-9.]+) ms$/.exec(n.message);
    if (duration) {
      part.serverMs += Number(duration[1]);
      part.notices++;
    }
    const lock = /acquired .* after ([0-9.]+) ms/.exec(n.message);
    if (lock) part.lockMs += Number(lock[1]);
  };
  this.on("notice", notice);
  return query.call(this, statement, ...args).finally(() => {
    this.off("notice", notice);
    const end = performance.now();
    trace.queries.push([
      label,
      this.processID,
      offset,
      position.next,
      performance.timeOrigin + start,
      performance.timeOrigin + end,
    ]);
    part.clientMs += end - start;
    if (label === "commit") trace.commitEnd = end;
  });
};
const claim = SegmentWorker.prototype.claimBatch;
SegmentWorker.prototype.claimBatch = function (...args) {
  const trace = { stages: {}, queries: [], poolCheckoutMs: 0 },
    start = performance.now(),
    initialElu = performance.eventLoopUtilization();
  return context.run(trace, async () => {
    try {
      const jobs = await claim.apply(this, args);
      if (jobs.length) {
        const end = performance.now();
        console.log(
          JSON.stringify({
            measurement: "sessionClaimTiming",
            size: jobs.length,
            totalMs: end - start,
            poolCheckoutMs: trace.poolCheckoutMs,
            beforeSqlMs: trace.firstQuery - start,
            postCommitMs: end - trace.commitEnd,
            loopActiveMs: performance.eventLoopUtilization(initialElu).active,
            stages: trace.stages,
            queries: trace.queries,
          }),
        );
      }
      return jobs;
    } finally {
      trace.closed = true;
    }
  });
};
await import("../../../packages/supervisor-host/src/independent-session-worker-main.mjs");
