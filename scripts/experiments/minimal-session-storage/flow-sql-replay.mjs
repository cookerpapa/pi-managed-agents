// Replay the exact captured client calls in short, counterbalanced blocks. No producer, Guest or JS native fold runs inside the measured interval.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import pg from "pg";
import { installPrefixSql } from "./ready-projection-sql.mjs";
import { projectionTemplates } from "./projection-statements.mjs";
import { installFlowHistory, seedFlowHeads } from "./flow-history.mjs";

export async function compareFlowSql(url, tape, runs, visible) {
  const dbs = [];
  try {
    for (const mode of ["baseline", "flow", "flow-without-active-index"]) {
      const schema = "sql_flow_" + randomUUID().replaceAll("-", "");
      const pool = new pg.Pool({
        connectionString: url,
        options: "-c search_path=" + schema,
        max: 12,
      });
      const arm = { pool, schema, mode, ms: 0, walBytes: 0, requests: 0 };
      dbs.push(arm);
      await pool.query("create schema " + schema);
      await pool.query(await readFile(new URL("./schema.sql", import.meta.url), "utf8"));
      await pool.query(
        "alter table runs add output_first_topic text,add output_first_partition int,add output_first_offset bigint,add output_display_seq bigint default 0,add output_display_native_seq bigint default 0",
      );
      await installPrefixSql(pool, projectionTemplates);
      if (mode !== "baseline") {
        await installFlowHistory(pool, true);
        if (mode === "flow-without-active-index") await pool.query("drop index flow_unsettled");
      }
      await pool.query(
        "insert into accepted_fact_projection_offsets select 'comparison',p,0 from generate_series(0,$1::int-1)p",
        [tape.length],
      );
      await pool.query(
        `insert into runs(id,tenant_id,session_id,lease_id,initial_input,idempotency_key,model_ref,state,mailbox_position)
     select "runId","tenantId","sessionId","writerId",'Inspect',"runId",'fixture','running',1 from jsonb_to_recordset($1::jsonb)t("runId" uuid,"tenantId" uuid,"sessionId" uuid,"writerId" uuid)`,
        [JSON.stringify(runs)],
      );
      if (mode !== "baseline") await seedFlowHeads(pool, runs[0].tenantId);
      await pool.query("analyze runs");
    }
    const orders = [
      [0, 1, 2],
      [2, 1, 0],
      [1, 2, 0],
      [0, 2, 1],
      [2, 0, 1],
      [1, 0, 2],
    ];
    const length = Math.max(...tape.map((l) => l.length));
    for (let start = 0, block = 0; start < length; start += 12, block++) {
      for (const n of orders[block % orders.length]) {
        const arm = dbs[n],
          pool = arm.pool;
        const {
          rows: [{ lsn }],
        } = await pool.query("select pg_current_wal_insert_lsn()::text lsn");
        const begin = performance.now();
        await Promise.all(
          tape.map(async (lane) => {
            for (const { text, params } of lane.slice(start, start + 12)) {
              const res = await pool.query(text, params);
              assert.equal(res.rows[0].outcome, "applied");
              arm.requests++;
            }
          }),
        );
        arm.ms += performance.now() - begin;
        arm.walBytes += Number(
          (
            await pool.query(
              "select pg_wal_lsn_diff(pg_current_wal_insert_lsn(),$1::pg_lsn) bytes",
              [lsn],
            )
          ).rows[0].bytes,
        );
      }
    }
    for (const arm of dbs) {
      const clients = await Promise.all(Array.from({ length: 12 }, () => arm.pool.connect()));
      try {
        await Promise.all(clients.map((c) => c.query("select pg_stat_force_next_flush()")));
      } finally {
        for (const c of clients) c.release();
      }
      const {
        rows: [stats],
      } = await arm.pool.query(
        "select n_tup_upd,n_tup_hot_upd from pg_stat_user_tables where schemaname=$1 and relname='runs'",
        [arm.schema],
      );
      assert.equal(
        (
          await arm.pool.query(
            "select count(*)::int n from message_entries where role='toolResult'",
          )
        ).rows[0].n,
        runs.length * 20,
      );
      console.log(
        JSON.stringify({
          layout: arm.mode,
          requests: arm.requests,
          visibleRecordsPerSecond: Math.round((visible * 1000) / arm.ms),
          ms: Math.round(arm.ms),
          walBytes: arm.walBytes,
          ...stats,
        }),
      );
    }
  } finally {
    for (const { pool, schema } of dbs)
      try {
        await pool.query("drop schema if exists " + schema + " cascade");
      } finally {
        await pool.end();
      }
  }
}
