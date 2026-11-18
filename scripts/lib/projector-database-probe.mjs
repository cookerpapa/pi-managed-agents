import { AsyncLocalStorage } from "node:async_hooks";
import { createHash } from "node:crypto";

/** Temporary operator experiment only. No values/results/connection strings are
 * recorded, and timing never changes a query's success or failure. */
export function installProjectorDatabaseProbe(pg, emit) {
  const contexts = new AsyncLocalStorage();
  const statements = new Map();
  const pools = new WeakMap();
  const clients = new WeakMap();
  const originalConnect = pg.Pool.prototype.connect;
  const originalQuery = pg.Client.prototype.query;
  let poolId = 0;
  function report(value) {
    try {
      emit(value);
    } catch {
      // Diagnostic sinks cannot turn success into a retry.
    }
  }
  function statement(input) {
    const text = typeof input === "string" ? input : input?.text;
    if (typeof text !== "string") return { id: "unobserved", command: "other" };
    let value = statements.get(text);
    if (value) return value;
    if (statements.size >= 1024) return { id: "overflow", command: "other" };
    const sanitized = text
      .replace(/'(?:[^']|'')*'/g, "'?'")
      .replace(/\s+/g, " ")
      .trim();
    value = {
      id: createHash("sha256").update(text).digest("hex").slice(0, 16),
      command: /^(\w+)/.exec(sanitized)?.[1].toLowerCase() ?? "other",
    };
    statements.set(text, value);
    report({ event: "diag.sql.statement", ...value, sql: sanitized.slice(0, 16000) });
    return value;
  }
  function span(context, started, attributes, error) {
    if (!context || context.finished) return;
    if (context.spans.length >= 1024) {
      context.truncated++;
      return;
    }
    context.spans.push({
      start: started - context.started,
      end: performance.now() - context.started,
      ...attributes,
      ...(error ? { error: String(error.code ?? error.name ?? "error") } : {}),
    });
  }
  pg.Pool.prototype.connect = function (...args) {
    const context = contexts.getStore();
    if (!context) return Reflect.apply(originalConnect, this, args);
    const started = performance.now();
    let pool = pools.get(this);
    if (!pool) {
      pool = ++poolId;
      pools.set(this, pool);
      report({
        event: "diag.sql.pool",
        pool,
        maximum: this.options.max,
        checkoutTimeout: this.options.statement_timeout ?? null,
      });
    }
    const attributes = {
      type: "acquire",
      pool,
      waiting: this.waitingCount,
      total: this.totalCount,
      idle: this.idleCount,
    };
    const done = (error, client) => {
      try {
        if (client) clients.set(client, pool);
        span(context, started, { ...attributes, pid: client?.processID }, error);
      } catch {
        // See report().
      }
    };
    const callback = args.at(-1);
    if (typeof callback === "function") {
      args[args.length - 1] = function (error, client, release) {
        done(error, client);
        return Reflect.apply(callback, this, [error, client, release]);
      };
      return Reflect.apply(originalConnect, this, args);
    }
    return Reflect.apply(originalConnect, this, args).then(
      (client) => {
        done(undefined, client);
        return client;
      },
      (error) => {
        done(error);
        throw error;
      },
    );
  };
  pg.Client.prototype.query = function (...args) {
    const context = contexts.getStore();
    if (!context) return Reflect.apply(originalQuery, this, args);
    const started = performance.now();
    let attributes;
    try {
      attributes = {
        type: "query",
        ...statement(args[0]),
        pool: clients.get(this),
        pid: this.processID,
      };
    } catch {
      return Reflect.apply(originalQuery, this, args);
    }
    const done = (error, result) => {
      try {
        span(context, started, { ...attributes, rows: result?.rowCount }, error);
      } catch {
        // See report().
      }
    };
    const callback = args.at(-1);
    if (typeof callback === "function") {
      args[args.length - 1] = function (error, result) {
        done(error, result);
        return Reflect.apply(callback, this, [error, result]);
      };
      return Reflect.apply(originalQuery, this, args);
    }
    let request;
    try {
      request = Reflect.apply(originalQuery, this, args);
    } catch (error) {
      done(error);
      throw error;
    }
    if (!request || typeof request.then !== "function") return request;
    return request.then(
      (result) => {
        done(undefined, result);
        return result;
      },
      (error) => {
        done(error);
        throw error;
      },
    );
  };
  return {
    async run(record, work) {
      const context = { started: performance.now(), spans: [], truncated: 0, finished: false };
      const at = Date.now();
      let failed = false;
      try {
        return await contexts.run(context, work);
      } catch (error) {
        failed = true;
        throw error;
      } finally {
        context.finished = true;
        report({
          event: "diag.sql.projection",
          at,
          durationMs: performance.now() - context.started,
          kind: record.fact.kind,
          runId: record.fact.scope.runId,
          sessionId: record.fact.scope.piSessionId,
          partition: record.partition,
          offset: String(record.offset),
          seq: record.fact.commit?.seq,
          writes: record.fact.commit?.writes?.map((w) =>
            [w.type, w.value?.kind].filter(Boolean).join(":"),
          ),
          failed,
          truncated: context.truncated,
          spans: context.spans,
        });
      }
    },
    uninstall() {
      pg.Pool.prototype.connect = originalConnect;
      pg.Client.prototype.query = originalQuery;
    },
  };
}
