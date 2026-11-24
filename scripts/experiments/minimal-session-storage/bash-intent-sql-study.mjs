// Replay identical PG calls with the original per-partition order in rotating
// blocks so a long host slowdown is shared by all layouts. No browser or Kafka
// waiting is included in these PG-leg rates.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import pg from "pg";
import { entryWithCoverage } from "./projection-statements.mjs";
import { bashLogFrames, setBashLogIndexes, installBashIntentLog } from "./bash-intent-log.mjs";
import { installPagingProjection } from "./flow-paging-fixture.mjs";
import { installFlowHistory } from "./flow-history.mjs";
import { installWorkspaceRoutes } from "./workspace-route-spans.mjs";

export async function compareBashWriteSql(url, source, tape, scopes, topic) {
  const arms = [],
    spans = (await source.query("select * from workspace_route_spans where topic=$1", [topic]))
      .rows;
  assert.equal(spans.length, scopes.length);
  try {
    for (const mode of [
      "combined",
      "intents",
      "coverage",
      "coverage-no-index",
      "coverage-sql",
      "folded",
    ]) {
      const schema = "write_" + randomUUID().replaceAll("-", ""),
        pool = new pg.Pool({
          connectionString: url,
          options: "-c track_functions=all -c search_path=" + schema,
          max: 12,
        });
      const arm = { schema, pool, mode, timeMs: 0, wal: 0, transactions: 0, cpuMs: 0 };
      arms.push(arm);
      await pool.query("create schema " + schema);
      await installPagingProjection(pool);
      await installFlowHistory(pool, true, false, "base");
      await installWorkspaceRoutes(pool);
      await installBashIntentLog(pool);
      await setBashLogIndexes(
        pool,
        mode === "combined"
          ? false
          : mode === "coverage-no-index"
            ? "intents"
            : mode === "coverage-sql" || mode === "folded"
              ? "coverage"
              : mode,
      );
      if (mode === "coverage-sql")
        await pool.query(
          `create or replace function log_covered_entry(uuid,uuid,uuid,bigint,text,uuid,int,bigint) returns table(accepted boolean,writes int) language sql volatile as $body$ ${entryWithCoverage} $body$`,
        );
      await pool.query(
        `insert into runs(id,tenant_id,session_id,lease_id,mailbox_position,initial_input,idempotency_key,model_ref,state)
        select "runId","tenantId","piSessionId","writerId",1,'Write measurement prompt',"runId",'fixture','running' from jsonb_to_recordset($1) v("runId" uuid,"tenantId" uuid,"piSessionId" uuid,"writerId" uuid)`,
        [JSON.stringify(scopes)],
      );
      await pool.query(
        "insert into workspace_route_spans select * from jsonb_populate_recordset(null::workspace_route_spans,$1)",
        [JSON.stringify(spans)],
      );
      await pool.query(
        "insert into accepted_fact_projection_offsets select $1,p,n from unnest($2::int[],$3::bigint[]) x(p,n)",
        [topic, tape.map((_, i) => i), tape.map((l) => l[0].params[2])],
      );
      arm.tape = tape.map((lane) =>
        lane.map(({ text, params }) => {
          if (mode === "combined") return { text, params };
          const values = params.slice();
          values[4] = JSON.stringify(
            bashLogFrames(
              JSON.parse(params[4]),
              mode === "folded" ? "folded" : mode === "intents" ? "intents" : "coverage",
            ),
          );
          return { text, params: values };
        }),
      );
    }
    const meter = async (pool) =>
      (
        await pool.query(`select pg_current_wal_insert_lsn()::text lsn,
     substring(pg_read_file('/sys/fs/cgroup/cpu.stat') from 'usage_usec ([0-9]+)')::float8/1000 ms`)
      ).rows[0];
    const length = Math.max(...tape.map((p) => p.length)),
      block = 12;
    for (let start = 0, i = 0; start < length; start += block, i++) {
      const order = Array.from({ length: arms.length }, (_, j) => arms[(i + j) % arms.length]);
      if (Math.floor(i / arms.length) % 2) order.reverse();
      for (const arm of order) {
        const before = await meter(arm.pool),
          begin = performance.now();
        await Promise.all(
          arm.tape.map(async (lane) => {
            for (const { text, params } of lane.slice(start, start + block)) {
              const r = await arm.pool.query(text, params);
              assert.equal(r.rows[0].outcome, "applied");
              arm.transactions++;
            }
          }),
        );
        arm.timeMs += performance.now() - begin;
        const after = await meter(arm.pool);
        arm.cpuMs += after.ms - before.ms;
        arm.wal += Number(
          (await arm.pool.query("select pg_wal_lsn_diff($1,$2) bytes", [after.lsn, before.lsn]))
            .rows[0].bytes,
        );
      }
    }
    for (const { mode, pool, timeMs, wal, transactions, cpuMs } of arms) {
      const {
        rows: [counts],
      } = await pool.query(
        "select count(*) filter(where role='toolResult')::int results,count(*) filter(where role='pma.bash-intent')::int intents from messages",
      );
      assert.equal(counts.results, scopes.length * 20);
      assert.equal(counts.intents, mode === "combined" ? 0 : counts.results);
      assert.equal((await pool.query("select from pending_bash_intents limit 1")).rowCount, 0);
      const offsets = (
        await pool.query("select partition,next_offset from accepted_fact_projection_offsets")
      ).rows;
      for (const row of offsets)
        assert.equal(row.next_offset, tape[row.partition].at(-1).params[3]);
      const clients = await Promise.all(Array.from({ length: 12 }, () => pool.connect()));
      try {
        await Promise.all(clients.map((c) => c.query("select pg_stat_force_next_flush()")));
      } finally {
        for (const c of clients) c.release();
      }
      const functions = (
        await pool.query(
          `select funcname,calls::int,round(total_time::numeric,1)::float total_ms from pg_stat_user_functions where schemaname=current_schema() and calls>0 order by funcname`,
        )
      ).rows;
      console.log(
        JSON.stringify({
          kind: "bash-pg-replay",
          functions,
          mode,
          transactions,
          results: counts.results,
          intents: counts.intents,
          pgLegVisiblePerSecond: Math.round((scopes.length * 41 * 1000) / timeMs),
          pgCpuMs: Math.round(cpuMs),
          walBytes: wal,
          wallMs: Math.round(timeMs),
        }),
      );
    }
  } finally {
    for (const { pool, schema } of arms) {
      try {
        await pool.query("drop schema if exists " + schema + " cascade");
      } finally {
        await pool.end();
      }
    }
  }
}
