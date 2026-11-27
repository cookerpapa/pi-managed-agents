// Mounted only by the synthetic serving check. Instrument the same compiled
// Worker entry on both revisions, without changing normal Worker traffic.
import { AsyncLocalStorage } from "node:async_hooks";
import { createHash } from "node:crypto";
import { performance } from "node:perf_hooks";
import pg from "pg";
import { SegmentWorker } from "./independent-session-leases.mjs";

import { SegmentMailReader } from "./segment-mail-reader.mjs";
import { SegmentHistoryReader } from "./segment-history-reader.mjs";
const activity = new AsyncLocalStorage();
for (const [port, methods, role] of [
  [SegmentMailReader, ["selectAt", "lastPlaced", "releaseBeforePlacement"], "inbox"],
  [SegmentHistoryReader, ["contextPrefix", "hydrate"], "history"],
]) {
  for (const method of methods) {
    const original = port.prototype[method];
    port.prototype[method] = function (...args) {
      return activity.run(role, () => original.apply(this, args));
    };
  }
}
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

// Inspect only the measured Worker's own pools. A checkout can also take
// time when opening a connection or when the JS continuation is delayed.
const poolClients = new WeakMap(),
  clientWork = new WeakMap();
function trackPool(pool) {
  if (poolClients.has(pool)) return poolClients.get(pool);
  const held = new Set();
  poolClients.set(pool, held);
  pool.on("acquire", (client) => {
    held.add(client);
    clientWork.delete(client);
  });
  pool.on("release", (_error, client) => {
    held.delete(client);
    clientWork.delete(client);
  });
  return held;
}
function operation(sql) {
  if (sql.includes("user_or_agent_messages")) return "inbox";
  if (/flow_segments|message_entries|from sessions where|history_head/.test(sql)) return "history";
  if (/session_leases|from "workers"/.test(sql)) return "authority";
  return "other";
}
const connect = pg.Pool.prototype.connect;
pg.Pool.prototype.connect = function (...args) {
  const trace = context.getStore();
  const held = trackPool(this);
  const work = activity.getStore();
  if (!trace || trace.closed || args.length) {
    const result = connect.apply(this, args);
    return work && !args.length
      ? result.then((c) => {
          clientWork.set(c, work);
          return c;
        })
      : result;
  }
  const checkout = {
    phase: trace.firstQuery === undefined ? "transaction" : "bootstrap",
    total: this.totalCount,
    idle: this.idleCount,
    queued: this.waitingCount,
    limit: this.options.max,
    holders: {},
  };
  for (const client of held) {
    const work = clientWork.get(client) ?? "between";
    checkout.holders[work] = (checkout.holders[work] ?? 0) + 1;
  }
  const start = performance.now();
  return connect.call(this).finally(() => {
    checkout.elapsedMs = performance.now() - start;
    trace.checkouts.push(checkout);
    trace.poolCheckoutMs += checkout.elapsedMs;
  });
};

const query = pg.Client.prototype.query;
pg.Client.prototype.query = function (statement, ...args) {
  const trace = context.getStore();
  const text = typeof statement === "string" ? statement : statement?.text;
  if (text && !/^(begin|start transaction|commit|rollback|set )/i.test(text.trim())) {
    clientWork.set(
      this,
      trace && !trace.closed ? "claim" : (activity.getStore() ?? operation(text.toLowerCase())),
    );
  }
  if (!trace || trace.closed || typeof statement !== "string")
    return query.call(this, statement, ...args);
  const label = stage(statement.toLowerCase().trim());
  const measured = () => timedQuery(this, statement, args, trace, label);
  if (label === "selectCandidates" && process.env.PI_CLOUD_TEST_SELECTOR_CPU === "1") {
    // PG emits per-protocol-message rusage only during the studied SELECT.
    // Keep both SET commands outside the correlated query-duration range.
    return query
      .call(this, "set local log_statement_stats=on")
      .then(measured)
      .then(async (value) => {
        await query.call(this, "set local log_statement_stats=off");
        return value;
      });
  }
  return measured();
};
function timedQuery(client, statement, args, trace, label) {
  const part = (trace.stages[label] ??= {
    calls: 0,
    clientMs: 0,
    serverMs: 0,
    lockMs: 0,
    notices: 0,
  });
  const position = backendPositions.get(client);
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
    const phase = /^(PARSE|BIND|EXECUTE) MESSAGE STATISTICS$/.exec(n.message);
    if (phase) {
      const usage = /([0-9.]+) s user, ([0-9.]+) s system, ([0-9.]+) s elapsed/.exec(
        n.detail ?? "",
      );
      if (usage) {
        const stats = ((part.cpu ??= {})[phase[1]] ??= {
          userMs: 0,
          systemMs: 0,
          wallMs: 0,
          count: 0,
        });
        stats.count++;
        stats.userMs += 1000 * Number(usage[1]);
        stats.systemMs += 1000 * Number(usage[2]);
        stats.wallMs += 1000 * Number(usage[3]);
      }
    }
    const lock = /acquired .* after ([0-9.]+) ms/.exec(n.message);
    if (lock) part.lockMs += Number(lock[1]);
  };
  client.on("notice", notice);
  const request =
    label === "selectCandidates" && process.env.PI_CLOUD_TEST_PREPARE_SELECTOR === "1"
      ? {
          name: "selector_" + createHash("sha256").update(statement).digest("hex").slice(0, 24),
          text: statement,
        }
      : statement;
  return query.call(client, request, ...args).finally(() => {
    client.off("notice", notice);
    const end = performance.now();
    trace.queries.push([
      label,
      client.processID,
      offset,
      position.next,
      performance.timeOrigin + start,
      performance.timeOrigin + end,
    ]);
    part.clientMs += end - start;
    if (label === "commit") trace.commitEnd = end;
  });
}
const claim = SegmentWorker.prototype.claimBatch;
SegmentWorker.prototype.claimBatch = function (...args) {
  const trace = { stages: {}, queries: [], poolCheckoutMs: 0, checkouts: [] },
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
            checkouts: trace.checkouts,
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
