// Read-only ablation. Keep the historical page and parameters fixed and force
// a dependency lookup to execute only for a non-null assistant Entry pointer.
import assert from "node:assert/strict";
import pg from "pg";
function probeSql(text, exact = false) {
  const begin = text.lastIndexOf("left join messages a on ");
  if (begin < 0) return;
  const order = text.indexOf("order by t.turn_index", begin);
  const end = order < 0 ? text.length : order;
  const pred = text.slice(begin + "left join messages a on ".length, end).trim();
  assert(pred.includes("p.assistant_id"));
  let gated =
    text.slice(0, begin) +
    "left join lateral (select a.entry_id, a.content from messages a where p.assistant_id is not null and " +
    pred +
    " limit 1) a on true " +
    text.slice(end);
  if (exact) {
    // Entry IDs are unique within their tenant and physical Pi stream. Look
    // up that ID first, then validate run membership on the joined row.
    assert(gated.includes("and a.run_id=r.id "));
    gated = gated
      .replace("select a.entry_id, a.content from", "select a.entry_id, a.content, a.run_id from")
      .replace("and a.run_id=r.id ", "")
      .replace("limit 1) a on true", "limit 1) a on a.run_id=r.id");
    const main = /left join messages m on (not p.pending and [^\n]+)\s+left join lateral/;
    if (order < 0) {
      assert(main.test(gated));
      gated = gated.replace(main, (_, condition) => {
        assert(condition.includes("and m.run_id=r.id "));
        return (
          "left join lateral(select m.* from messages m where " +
          condition.replace("and m.run_id=r.id ", "") +
          " limit 1) m on m.run_id=r.id left join lateral"
        );
      });
    }
  }
  return gated;
}
// Fixture child only. Change just the two page SELECTs and use separate named
// plans so the ordinary Kysely reader and response validation stay in circuit.
export function applyDependencyProbe(exact = false) {
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
      const revised = probeSql(text, exact);
      if (revised) counts[text.includes("order by t.turn_index") ? "locate" : "bodies"]++;
      if (revised)
        args[0] =
          typeof cfg === "string"
            ? revised
            : {
                ...cfg,
                text: revised,
                ...(cfg.name ? { name: cfg.name + (exact ? "_exact_dep" : "_dep") } : {}),
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
    const gated = probeSql(s.text);
    if (!gated) continue;
    const metadata = s.text.includes("order by t.turn_index");
    const prepared = [s.text, gated, probeSql(s.text, true)].map((text, i) => ({
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
