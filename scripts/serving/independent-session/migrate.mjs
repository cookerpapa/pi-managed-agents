import pg from "pg";
import { createDatabase, runMigrations } from "@pi-cloud/database";
import { installation, schema, readSchema } from "./config.mjs";
import { installSessionTables } from "../../experiments/minimal-session-storage/segment-schema.mjs";
import { installPrefixSql } from "../../experiments/minimal-session-storage/ready-projection-sql.mjs";
import { projectionTemplates } from "../../experiments/minimal-session-storage/projection-statements.mjs";
import {
  installForkTables,
  installForkLayout,
  installForkAttachmentChecks,
} from "../../experiments/minimal-session-storage/fork-storage.mjs";
import {
  createUnsettledTurnIndex,
  installIndependentSessionLeases,
  installTurnQueue,
} from "../../experiments/minimal-session-storage/independent-session-leases.mjs";
import { installWorkspaceRoutes } from "../../experiments/minimal-session-storage/workspace-route-spans.mjs";
import { pathToFileURL } from "node:url";
export async function migrate(config) {
  const c = new pg.Client({ connectionString: config.url });
  await c.connect();
  try {
    await c.query("select pg_advisory_lock(379052,1)");
    const {
      rows: [known],
    } = await c.query(
      "select to_regclass('pma_segments.installation') marker,to_regclass('public.sessions') existing",
    );
    if (known.marker) {
      const {
        rows: [row],
      } = await c.query("select * from pma_segments.installation");
      if (
        ![1, 2].includes(row?.version) ||
        row.topic !== config.topic ||
        row.partitions !== config.partitions
      )
        throw Error("Mismatched installation");
      // The advisory lock serializes installers. Both Worker revisions can use
      // the same data while PG builds this additive index outside a transaction.
      await c.query("set search_path=pma_segments,public");
      const {
        rows: [index],
      } = await c.query(`select i.indisvalid,
        i.indrelid='pma_segments.turns'::regclass as on_turns
        from pg_class c left join pg_index i on i.indexrelid=c.oid
        where c.oid=to_regclass('pma_segments.turns_unsettled_by_session')`);
      if (index && !index.on_turns) throw Error("Unexpected object at Turn claim index name");
      if (!index?.indisvalid) {
        // Cancellation can leave an invalid index with the final name.
        if (index) await c.query("drop index concurrently pma_segments.turns_unsettled_by_session");
        await createUnsettledTurnIndex(c, true);
      }
      if (row.version === 1) {
        await c.query("begin");
        await c.query("lock table turns,sessions in access exclusive mode");
        if (
          (await c.query("select 1 from public.session_leases where released_at is null limit 1"))
            .rowCount ||
          (
            await c.query(
              "select 1 from public.supervisor_connections where state='active' and expires_at>clock_timestamp() limit 1",
            )
          ).rowCount
        )
          throw Error("Drain old Session Workers before the matched Turn queue upgrade");
        await c.query(`create temporary table prior_ready_heads on commit drop as
          select r.id from ready_agent_sessions q join sessions s on s.tenant_id=q.tenant_id and s.id=q.session_id
          join turns r on r.tenant_id=s.tenant_id and r.session_id=s.flow_id
          where r.state='queued' and r.lease_id is null and r.output_seal_id is null and not exists
          (select from turns p where p.tenant_id=r.tenant_id and p.session_id=r.session_id and p.mailbox_position<r.mailbox_position
          and (p.state not in ('completed','failed','timed_out','cancelled') or (p.output_seal_id is not null and p.output_sealed_at is null)))`);
        await c.query(`update user_or_agent_messages m set starts_turn=true from (
          select distinct on(r.id) r.id as turn_id,p.id from turns r join user_or_agent_messages p on p.turn_id=r.id
          where r.state='queued' and r.started_at is null and p.state='pending' and not exists(
            select from user_or_agent_messages z where z.turn_id=r.id and z.state<>'withdrawn' and z.starts_turn)
          order by r.id,p.position) first where m.turn_id=first.turn_id and m.id=first.id`);
        await c.query(`drop trigger independent_turn_ready on pma_segments.turns;
          drop trigger independent_successor_ready on pma_segments.turns;
          drop function pma_segments.notify_segment_work();
          drop trigger finish_agent_session_inputs on pma_segments.turns`);
        await installTurnQueue(c);
        const mismatch = await c.query(`select 1 from prior_ready_heads p full join
          (select id from turns where ready_at is not null) n using(id) where p.id is null or n.id is null limit 1`);
        if (mismatch.rowCount)
          throw Error("Turn queue backfill differs from eligible Session heads");
        await c.query("drop table pma_segments.ready_agent_sessions");
        await c.query("update installation set version=2");
        await c.query("commit");
      }
      return;
    }
    if (
      known.existing &&
      (
        await c.query(
          "select 1 where exists(select from public.sessions) or exists(select from public.workers)",
        )
      ).rowCount
    )
      throw Error("Use a fresh isolated Session database");
    const publicDb = createDatabase({ connectionString: config.url });
    try {
      await runMigrations(publicDb, "up");
    } finally {
      await publicDb.destroy();
    }
    await c.query("begin");
    await c.query(
      "create schema " +
        schema +
        ";create schema " +
        readSchema +
        ";set local search_path=" +
        schema +
        ",public",
    );
    await installSessionTables(c);
    await installPrefixSql(c, projectionTemplates);
    await installForkTables(c);
    await installForkLayout(c);
    await installForkAttachmentChecks(c);
    await installWorkspaceRoutes(c);
    await installIndependentSessionLeases(c);
    await c.query("create view " + readSchema + ".sessions as select * from public.sessions");
    await c.query(
      "create table session_terminal_events (like public.session_terminal_events including defaults including constraints including indexes)",
    );
    await c.query(
      "create table installation(singleton boolean primary key default true check(singleton),version int not null,topic text not null,partitions int not null)",
    );
    await c.query("insert into installation(version,topic,partitions) values(2,$1,$2)", [
      config.topic,
      config.partitions,
    ]);
    await c.query("commit");
  } catch (e) {
    await c.query("rollback").catch(() => {});
    throw e;
  } finally {
    await c.end();
  }
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href)
  await migrate(await installation());
