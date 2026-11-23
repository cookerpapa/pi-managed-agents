// Capture one authenticated HTTP page outside the load trial. Replay only its
// read statements with the same parameters on this disposable database.
import assert from "node:assert/strict";
import pg from "pg";

function label(sql) {
  if (sql.includes("from unnest(") && sql.includes("assistant_bytes")) return "locate page entries";
  if (sql.includes("as proposal") && sql.includes("as human"))
    return "load bodies and tool dependencies";
  if (sql.includes("left join lateral(select seq,type,payload")) return "load terminal metadata";
  if (sql.includes("flow_read_ranges") && sql.includes("prior_positions+r.mailbox_position"))
    return sql.includes("limit") ? "select visible runs" : "resolve cursor run";
  if (sql.includes("flow_session_tips")) return "session metadata";
  if (sql.includes("input_text,null::text")) return "load run headers";
  return "other read";
}
export async function profilePage(url, read, query) {
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
      result.push({
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
