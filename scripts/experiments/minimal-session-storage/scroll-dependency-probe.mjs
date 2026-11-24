// Read-only ablation. Keep the historical page and parameters fixed and force
// a dependency lookup to execute only for a non-null assistant Entry pointer.
import assert from "node:assert/strict";
import pg from "pg";
// The normal reader supplies the exact-Entry form. Derive the earlier controls
// from its predicates to retain the real tenant, lane, bindings and page shape.
function probeSql(text, mode) {
  const assistant =
    /left join lateral\(select a\.entry_id,a\.run_id,a\.content from messages a\s+where p\.assistant_id is not null and ([^;]+?) limit 1\) a on a\.run_id=r\.id/;
  if (!assistant.test(text)) return;
  if (mode === "exact") return text;
  const main =
    /left join lateral\(select m\.run_id,m\.content,m\.created_at,m\.source_input_id from messages m\s+where ([^;]+?) limit 1\) m on m\.run_id=r\.id/;
  return text
    .replace(main, (_, pred) => "left join messages m on m.run_id=r.id and " + pred)
    .replace(assistant, (_, pred) =>
      mode === "original"
        ? "left join messages a on a.run_id=r.id and " + pred
        : "left join lateral(select a.entry_id,a.content from messages a where p.assistant_id is not null and a.run_id=r.id and " +
          pred +
          " limit 1) a on true",
    );
}
// Fixture child only. Change just the two page SELECTs and use separate named
// plans so the ordinary Kysely reader and response validation stay in circuit.
export function applyDependencyProbe(mode) {
  const original = pg.Client.prototype.query;
  const counts = { locate: 0, bodies: 0 };
  pg.Client.prototype.query = function (...args) {
    const cfg = args[0],
      text = typeof cfg === "string" ? cfg : cfg?.text;
    if (
      typeof text === "string" &&
      /^\s*select\b/i.test(text) &&
      this.connectionParameters.options?.includes("flow_web")
    ) {
      const revised = probeSql(text, mode);
      if (revised) counts[text.includes("order by t.turn_index") ? "locate" : "bodies"]++;
      if (revised)
        args[0] =
          typeof cfg === "string"
            ? revised
            : {
                ...cfg,
                text: revised,
                ...(cfg.name ? { name: cfg.name + ("_dep_" + mode) } : {}),
              };
    }
    return original.apply(this, args);
  };
  return () => {
    pg.Client.prototype.query = original;
    return counts;
  };
}
export async function profileDependencyJoins(diag, statements, shape, repeats = 48) {
  const ns = async () =>
    Number(
      (await diag.query("select split_part(pg_read_file('/proc/self/schedstat'),' ',1) t")).rows[0]
        .t,
    );
  for (const [idx, s] of statements.entries()) {
    const gated = probeSql(s.text, "gated");
    if (!gated) continue;
    const metadata = s.text.includes("order by t.turn_index");
    const prepared = [probeSql(s.text, "original"), gated, s.text].map((text, i) => ({
      text,
      name: "dep_" + idx + "_" + i,
      values: s.values,
    }));
    const oracle = (await diag.query(prepared[0])).rows;
    for (const q of prepared)
      for (let i = 0; i < 8; i++) assert.deepEqual((await diag.query(q)).rows, oracle);
    const blocks = [[], [], []];
    for (const a of [0, 1, 2, 2, 1, 0, 1, 2, 0, 0, 2, 1]) {
      const t = await ns();
      for (let i = 0; i < repeats; i++) await diag.query(prepared[a]);
      blocks[a].push(+(((await ns()) - t) / (repeats * 1e6)).toFixed(3));
    }
    function nodeWalk(p) {
      return [p, ...(p.Plans ?? []).flatMap(nodeWalk)];
    }
    const plans = [];
    for (const q of prepared) {
      // EXPLAIN with the actual bindings gives a representative custom plan.
      const p = (
        await diag.query("explain(analyze,buffers,timing off,format json) " + q.text, q.values)
      ).rows[0]["QUERY PLAN"][0].Plan;
      plans.push({
        hits: p["Shared Hit Blocks"],
        reads: p["Shared Read Blocks"],
        messageIndexes: nodeWalk(p)
          .filter((x) => x["Relation Name"] === "messages")
          .map((x) => ({
            alias: x.Alias,
            index: x["Index Name"],
            loops: x["Actual Loops"],
            rows: x["Actual Rows"],
            filtered: x["Rows Removed by Filter"] ?? 0,
          })),
        dependencyLookups: nodeWalk(p)
          .filter((x) => x["Alias"] === "a" && x["Relation Name"] === "messages")
          .map((x) => ({
            loops: x["Actual Loops"],
            index: x["Index Name"],
            rows: x["Actual Rows"],
          })),
      });
    }
    console.log(
      JSON.stringify({
        kind: "dependency-probe",
        shape,
        sql: metadata ? "locate" : "bodies",
        returnedRows: oracle.length,
        originalCpuMs: blocks[0],
        gatedCpuMs: blocks[1],
        exactEntryCpuMs: blocks[2],
        plans,
      }),
    );
  }
}
