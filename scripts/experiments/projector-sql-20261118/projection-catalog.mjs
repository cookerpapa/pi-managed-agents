/** Read only catalog definitions and operator-sanitized SQL shapes. */
import assert from "node:assert/strict";
import { readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import pg from "pg";
import { runtimeDirectory, serviceAddress } from "../../lib/production-kubernetes.mjs";
const [traceDirectory, output] = process.argv.slice(2);
assert(traceDirectory && output);
const groups = {
  admission:
    "sessions pi_sessions runs session_leases run_transitions turn_control_requests accepted_fact_projection_offsets session_terminal_events outbox",
  native:
    "pi_durable_commits pi_durable_owned_records pi_durable_current pi_durable_document_revisions pi_durable_write_refs pi_durable_bases",
  history:
    "pi_history_streams pi_history_records pi_history_bodies pi_history_nodes pi_history_blocks pi_history_frames pi_history_views pi_history_view_nodes",
  attachments:
    "pi_history_record_attachments pi_history_header_attachments session_attachment_objects session_attachment_references session_upload_pins session_uploads",
  accounting: "model_invocations model_metering_events model_execution_writers",
  branches:
    "subagent_conversations subagent_supervisor_requests conversation_fork_operations conversation_prune_operations",
  routing:
    "sandbox_tool_bindings execution_target_bindings execution_targets workspaces workspace_runtimes development_environments development_environment_operations",
  recovery: "workers worker_retirements supervisor_connections",
  views:
    "active_execution_scopes conversation_turns pi_durable_records pi_durable_inherited_entries pi_durable_writes pi_durable_log pi_durable_record_bodies pi_history_live_turns pi_history_snapshots pi_history_turns pi_history_visible_blocks subagent_executions model_billing_events",
};
const url = new URL((await readFile(runtimeDirectory + "/secrets/database-url", "utf8")).trim());
url.hostname = serviceAddress("postgres");
const client = new pg.Client({
  connectionString: url.toString(),
  options: "-c default_transaction_read_only=on",
  application_name: "pma-catalog-review",
});
const inventory = {};
const names = Object.values(groups).join(" ").split(" ");
await client.connect();
try {
  const all = await client.query(`select c.relname,c.relkind,
     array(select a.attname||' '||format_type(a.atttypid,a.atttypmod)||case when a.attnotnull then ' NOT NULL' else '' end from pg_attribute a where a.attrelid=c.oid and a.attnum>0 and not a.attisdropped order by a.attnum) cols,
     coalesce((select json_agg(json_build_object('name',idx.relname,'definition',pg_get_indexdef(i.indexrelid),'unique',i.indisunique,'primary',i.indisprimary,'predicate',pg_get_expr(i.indpred,i.indrelid),
       'foreignKeyUsers',array(select k.conrelid::regclass::text||'.'||k.conname from pg_constraint k where k.conindid=i.indexrelid and k.contype='f' order by 1))
       order by idx.relname) from pg_index i join pg_class idx on idx.oid=i.indexrelid where i.indrelid=c.oid),'[]') indexes,
     coalesce((select json_agg(pg_get_constraintdef(co.oid) order by co.conname) from pg_constraint co where co.conrelid=c.oid and co.contype='f'),'[]') foreign_keys,
     coalesce((select json_agg(pg_get_triggerdef(t.oid) order by t.tgname) from pg_trigger t where t.tgrelid=c.oid and not t.tgisinternal),'[]') triggers,
     case when c.relkind='v' then pg_get_viewdef(c.oid,true) end view
   from pg_class c join pg_namespace n on n.oid=c.relnamespace where n.nspname='public' and c.relkind in ('r','v') order by 1`);
  const logs = await readFile(join(traceDirectory, "runtime.log"), "utf8");
  const report = JSON.parse(await readFile(join(traceDirectory, "load/report.json"), "utf8"));
  const receipt = JSON.parse(
    await readFile(join(traceDirectory, "observation-private.json"), "utf8"),
  );
  assert(
    report.accepted &&
      report.cleanupConfirmed &&
      !report.realProviderRequests &&
      receipt.phase === "restored",
  );
  const measurement = report.measurements[0],
    runs = new Set(measurement.load.results.map((r) => r.runId));
  const start = Math.min(...measurement.load.results.map((r) => r.submittedAt));
  const templates = new Map(),
    records = [];
  for (const line of logs.split("\n")) {
    if (!line.startsWith("{")) continue;
    const r = JSON.parse(line);
    if (r.event === "diag.sql.statement") templates.set(r.id, r.sql);
    if (r.event === "diag.sql.projection" || r.event === "diag.sql.handler") records.push(r);
  }
  const owned = new Set(
    records
      .filter((r) => r.event === "diag.sql.projection" && runs.has(r.runId))
      .map((r) => r.sessionId),
  );
  assert.equal(owned.size, 192);
  const ends = records.filter(
    (r) => r.event === "diag.sql.handler" && r.kind === "work_finished" && runs.has(r.runId),
  );
  assert.equal(ends.length, runs.size);
  const end = Math.max(...ends.map((r) => r.at + r.durationMs));
  const queries = new Map();
  for (const record of records) {
    if (!owned.has(record.sessionId) || record.at >= end || record.at + record.durationMs < start)
      continue;
    const family =
      (record.event === "diag.sql.handler" ? "handler/" : "projection/") +
      (record.kind === "durable_commit"
        ? record.writes.join("+")
        : [record.kind, record.subtype].filter(Boolean).join("."));
    for (const s of record.spans) {
      if (s.type !== "query") continue;
      const value = queries.get(s.id) ?? {
        id: s.id,
        count: 0,
        callMs: 0,
        shapes: {},
        sql: templates.get(s.id),
      };
      assert(value.sql && !s.error);
      value.count++;
      value.callMs += s.end - s.start;
      value.shapes[family] = (value.shapes[family] ?? 0) + 1;
      queries.set(s.id, value);
    }
  }
  const scanned = new Set();
  for (const q of queries.values()) {
    q.callMs = Math.round(q.callMs);
    q.relations = all.rows
      .filter((r) => new RegExp("\\b" + r.relname + "\\b", "i").test(q.sql))
      .map((r) => r.relname);
    for (const name of q.relations) scanned.add(name);
  }
  for (const n of scanned) if (!names.includes(n)) names.push(n);
  for (const r of all.rows.filter((r) => names.includes(r.relname))) {
    const { relname, relkind, ...rest } = r;
    inventory[relname] = {
      group:
        Object.entries(groups).find(([, v]) => v.split(" ").includes(relname))?.[0] ?? "observed",
      kind: relkind,
      ...rest,
    };
  }
  assert.equal(Object.keys(inventory).length, new Set(names).size);
  const version = (
    await client.query("select name from kysely_migration order by name desc limit 1")
  ).rows[0].name;
  await writeFile(
    output,
    JSON.stringify(
      {
        schema: version,
        runs: runs.size,
        relations: inventory,
        queries: [...queries.values()].sort((a, b) => b.count - a.count),
      },
      null,
      2,
    ) + "\n",
    { mode: 0o600 },
  );
  console.log(
    "relations",
    Object.keys(inventory).length,
    "indexes",
    Object.values(inventory).flatMap((r) => r.indexes).length,
    "SQL shapes",
    queries.size,
  );
} finally {
  await client.end();
}
