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
      if (row?.version !== 1 || row.topic !== config.topic || row.partitions !== config.partitions)
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
    await c.query("insert into installation(version,topic,partitions) values(1,$1,$2)", [
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
