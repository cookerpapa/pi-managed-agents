// Capture one authenticated HTTP page outside the load trial. Replay only its
// read statements with the same parameters on this disposable database.
import assert from "node:assert/strict";
import pg from "pg";
import { cpuMillis } from "./page-backend-cpu.mjs";

function label(sql) {
  if (sql.includes("from unnest(") && sql.includes("assistant_bytes")) return "locate page entries";
  if (sql.includes("as proposal") && sql.includes("as human"))
    return "load bodies and tool dependencies";
  if (sql.includes("left join lateral(select seq,type,payload")) return "load terminal metadata";
  if (sql.includes("flow_read_ranges") && sql.includes("prior_positions+r.mailbox_position"))
    return sql.includes("limit") ? "select visible runs" : "resolve cursor run";
  if (sql.includes("flow_session_tips")) return "session metadata";
  if (sql.includes("session_row") && sql.includes("project")) return "session metadata";
  if (sql.includes(") root_turns"))
    return sql.includes("order by mailbox_position") ? "select visible runs" : "resolve cursor run";
  if (sql.includes("input_text,null::text")) return "load run headers";
  if (sql.includes("environment_versions")) return "project environment";
  return "other read";
}
// Sample the actual Session API backend around each SELECT. The observer
// reads Linux scheduler counters on its own connection; its CPU is excluded.
async function tracePageCpu(diag, read, query) {
  const original = pg.Client.prototype.query,
    totals = new Map();
  const stamp = async (pid) =>
    Number(
      (
        await original.call(
          diag,
          "select split_part(pg_read_file('/proc/'||$1::text||'/schedstat'),' ',1) ns",
          [pid],
        )
      ).rows[0].ns,
    );
  try {
    pg.Client.prototype.query = function (...args) {
      const text = typeof args[0] === "string" ? args[0] : args[0]?.text;
      if (
        this === diag ||
        !this.connectionParameters.options?.includes("flow_web") ||
        !text ||
        !/^\s*(select|with)\s/i.test(text)
      )
        return original.apply(this, args);
      return (async () => {
        const before = await stamp(this.processID);
        const rows = await original.apply(this, args);
        const elapsed = (await stamp(this.processID)) - before;
        const key = label(text),
          o = totals.get(key) ?? { ns: 0, calls: 0 };
        assert(elapsed >= 0);
        o.ns += elapsed;
        o.calls++;
        totals.set(key, o);
        return rows;
      })();
    };
    for (let i = 0; i < 48; i++) await read(query);
  } finally {
    pg.Client.prototype.query = original;
  }
  assert(totals.size >= 5);
  return [...totals].map(([stage, o]) => ({
    stage,
    calls: o.calls,
    msPerPage: +(o.ns / 48 / 1e6).toFixed(3),
  }));
}
export async function profilePage(url, read, query, cpuLabel) {
  const original = pg.Client.prototype.query,
    statements = [];
  const diag = new pg.Client({ connectionString: url });
  try {
    await diag.connect();
    for (let i = 0; i < 6; i++) await read(query);
    pg.Client.prototype.query = function (...args) {
      const cfg = typeof args[0] === "string" ? { text: args[0], values: args[1] } : args[0];
      const promise = original.apply(this, args);
      if (
        this.connectionParameters.options?.includes("flow_web") &&
        cfg?.text &&
        /^\s*(select|with)\s/i.test(cfg.text)
      ) {
        const record = { text: cfg.text, values: cfg.values ?? [], start: performance.now() };
        statements.push(record);
        promise.then(
          () => {
            record.ms = performance.now() - record.start;
          },
          () => {},
        );
      }
      return promise;
    };
    const start = performance.now();
    await read(query);
    const httpMs = performance.now() - start;
    pg.Client.prototype.query = original;
    const result = [];
    for (const s of statements) {
      const r = (
        await diag.query("explain(analyze,buffers,format json,timing off) " + s.text, s.values)
      ).rows[0]["QUERY PLAN"][0];
      function all(p) {
        return [p, ...(p.Plans ?? []).flatMap(all)];
      }
      const nodes = all(r.Plan);
      let cpuMs, planChoice;
      if (cpuLabel) {
        const cfg = { name: "statement_" + result.length, text: s.text, values: s.values };
        for (let i = 0; i < 8; i++) await diag.query(cfg);
        const cpu = async () =>
          cpuMillis((await diag.query("select pg_read_file('/proc/self/stat') stat")).rows[0].stat);
        const blocks = [];
        for (let pass = 0; pass < 2; pass++) {
          const before = await cpu();
          for (let i = 0; i < 96; i++) await diag.query(cfg);
          blocks.push(((await cpu()) - before) / 96);
        }
        cpuMs = blocks;
        if (["select visible runs", "resolve cursor run"].includes(label(s.text))) {
          const counts = async () =>
            (
              await diag.query(
                "select generic_plans::int generic,custom_plans::int custom from pg_prepared_statements where name=$1",
                [cfg.name],
              )
            ).rows[0];
          const originalRows = (await diag.query(cfg)).rows;
          assert(
            cfg.values.every(
              (v) => ["string", "number", "boolean"].includes(typeof v) || v === null,
            ),
          );
          const planSql = (
            await diag.query(
              `select format('explain(analyze,buffers,timing off,format json) execute %I (%s)',
             $1::text,string_agg(quote_nullable(v),',' order by i)) stmt from unnest($2::text[]) with ordinality a(v,i)`,
              [cfg.name, cfg.values],
            )
          ).rows[0].stmt;

          planChoice = { automatic: await counts(), trials: [] };
          try {
            await diag.query("set statement_timeout='3s'");
            for (const setting of [
              "force_generic_plan",
              "force_custom_plan",
              "force_custom_plan",
              "force_generic_plan",
            ]) {
              await diag.query("set plan_cache_mode=" + setting);
              const verify = await diag.query(cfg);
              assert.deepEqual(verify.rows, originalRows);
              const before = await cpu();
              for (let i = 0; i < 64; i++) await diag.query(cfg);
              const cpuMs = ((await cpu()) - before) / 64;
              const plans = [];
              for (let i = 0; i < 7; i++)
                plans.push((await diag.query(planSql)).rows[0]["QUERY PLAN"][0]);
              const median = (key) => plans.map((p) => p[key]).sort((a, b) => a - b)[3];
              planChoice.trials.push({
                setting,
                cpuMs,
                planningMs: median("Planning Time"),
                executionMs: median("Execution Time"),
                sharedHits: plans[3].Plan["Shared Hit Blocks"],
              });
            }
          } finally {
            await diag.query("reset plan_cache_mode");
            await diag.query("reset statement_timeout");
          }
        }
      }
      result.push({
        ...(cpuMs ? { cpuMs } : {}),
        ...(planChoice ? { planChoice } : {}),
        label: label(s.text),
        clientMs: +s.ms.toFixed(2),
        planMs: r["Planning Time"],
        execMs: r["Execution Time"],
        sharedHits: r.Plan["Shared Hit Blocks"],
        maxVisitedRows: Math.max(
          ...nodes
            .filter((n) => n["Relation Name"])
            .map(
              (n) => n["Actual Loops"] * (n["Actual Rows"] + (n["Rows Removed by Filter"] ?? 0)),
            ),
        ),
      });
    }
    assert(result.length >= 5);
    console.log(
      JSON.stringify({
        mode: "page-plan",
        ...(cpuLabel
          ? { layout: cpuLabel, apiStatementCpu: await tracePageCpu(diag, read, query) }
          : {}),
        page: query.before ? "older" : "latest",
        httpMs: +httpMs.toFixed(2),
        sql: result,
      }),
    );
  } finally {
    pg.Client.prototype.query = original;
    await diag.end();
  }
}
