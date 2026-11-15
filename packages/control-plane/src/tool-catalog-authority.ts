import { isDeepStrictEqual } from "node:util";
import { sql, type Kysely } from "kysely";
import type { Database } from "@pi-cloud/database";
import {
  parseToolCatalogPublication,
  type ToolCatalogPublication,
  type ControlToolCatalog,
  type ToolCatalogWatermark,
} from "@pi-cloud/protocol";

type Row = {
  scope: string;
  mcp: ToolCatalogPublication["mcp"] | null;
  generation: string;
  release_id: string;
  catalog: string;
  replayable: string[];
  published_generation: string | null;
};

function publication(row: Row): ToolCatalogPublication {
  return parseToolCatalogPublication({
    version: 2,
    scope: row.scope,
    ...(row.mcp ? { mcp: row.mcp } : {}),
    generation: Number(row.generation),
    releaseId: row.release_id,
    catalog: JSON.parse(row.catalog),
    replayable: row.replayable,
  });
}

/** One selected release, not a second tool scheduler. Only deployment activation
 * changes this current value; restarting an old executable cannot select itself. */
export class ToolCatalogAuthority {
  constructor(readonly database: Kysely<Database>) {}

  async read(scope = "platform"): Promise<ToolCatalogPublication | undefined> {
    const { rows } = await sql<Row>`select scope,mcp,generation::text,release_id,catalog,replayable,
      published_generation::text from tool_catalog_publication where scope=${scope}`.execute(
      this.database,
    );
    return rows[0] ? publication(rows[0]) : undefined;
  }

  async minimumGenerations(): Promise<readonly ToolCatalogWatermark[]> {
    const { rows } = await sql<{ scope: string; generation: string }>`
      select scope,generation::text from tool_catalog_publication`.execute(this.database);
    return rows.map((row) => ({ scope: row.scope, generation: Number(row.generation) }));
  }

  async select(input: {
    scope?: string;
    mcp?: ToolCatalogPublication["mcp"];
    releaseId: string;
    catalog: ControlToolCatalog;
    replayable: readonly string[];
    expectedGeneration: number;
  }): Promise<ToolCatalogPublication> {
    const scope = input.scope ?? "platform";
    parseToolCatalogPublication({
      version: 2,
      scope,
      ...(input.mcp ? { mcp: input.mcp } : {}),
      generation: 1,
      releaseId: input.releaseId,
      catalog: input.catalog,
      replayable: [...input.replayable],
    });
    if (!Number.isSafeInteger(input.expectedGeneration) || input.expectedGeneration < 0)
      throw new Error("Invalid expected catalog generation");
    return this.database.transaction().execute(async (tx) => {
      // Serializes only rare release selections. No lock spans Kafka or HTTP.
      await sql`select pg_advisory_xact_lock(713013,16)`.execute(tx);
      const { rows } =
        await sql<Row>`select scope,mcp,generation::text,release_id,catalog,replayable,
        published_generation::text from tool_catalog_publication where scope=${scope} for update`.execute(
          tx,
        );
      const old = rows[0];
      if (scope !== "platform") {
        const current = await sql`select 1 from tool_catalog_publication p join mcp_connections c
          on c.id=${scope.slice(4)}::uuid where p.scope='platform' and p.release_id=${input.releaseId}
          and c.revision=${input.mcp!.revision} and c.tenant_id=${input.mcp!.tenantId}::uuid
          and c.owner_user_id=${input.mcp!.ownerUserId}::uuid and c.visibility=${input.mcp!.visibility}
          and (not ${input.mcp!.enabled} or (c.enabled and c.deleted_at is null
            and c.status='ready' and c.checked_revision=c.revision))`.execute(tx);
        if (!current.rows.length) throw new Error("MCP connection or Tool release changed");
      }
      if (
        old?.release_id === input.releaseId &&
        (scope === "platform" ||
          (isDeepStrictEqual(old.mcp, input.mcp) &&
            (JSON.parse(old.catalog) as ControlToolCatalog).revision === input.catalog.revision))
      ) {
        if (
          (JSON.parse(old.catalog) as ControlToolCatalog).revision !== input.catalog.revision ||
          JSON.stringify([...old.replayable].sort()) !==
            JSON.stringify([...input.replayable].sort())
        )
          throw new Error("Tool release identity changed its catalog");
        return publication(old);
      }
      if (Number(old?.generation ?? 0) !== input.expectedGeneration)
        throw new Error("Tool catalog selection changed; refresh before activation");
      const generation = input.expectedGeneration + 1;
      await sql`insert into tool_catalog_publication(scope,generation,release_id,catalog,replayable,mcp)
        values(${scope},${generation},${input.releaseId},${JSON.stringify(input.catalog)},
          ${JSON.stringify([...input.replayable].sort())}::jsonb,${input.mcp ? JSON.stringify(input.mcp) : null}::jsonb)
        on conflict(scope) do update set generation=excluded.generation,
          release_id=excluded.release_id,catalog=excluded.catalog,replayable=excluded.replayable,mcp=excluded.mcp,
          published_generation=null,updated_at=clock_timestamp()`.execute(tx);
      return parseToolCatalogPublication({
        version: 2,
        scope,
        ...(input.mcp ? { mcp: input.mcp } : {}),
        generation,
        releaseId: input.releaseId,
        catalog: input.catalog,
        replayable: [...input.replayable].sort(),
      });
    });
  }

  async current(value: ToolCatalogPublication): Promise<boolean> {
    const { rows } = await sql`select 1 from tool_catalog_publication where scope=${value.scope}
      and generation=${value.generation} and release_id=${value.releaseId}
      and catalog::jsonb->>'revision'=${value.catalog.revision}
      ${
        value.mcp
          ? sql`and exists(select 1 from mcp_connections c
        join tool_catalog_publication p on p.scope='platform' and p.release_id=${value.releaseId}
        where c.id=${value.scope.slice(4)}::uuid and c.revision=${value.mcp.revision})`
          : sql``
      }`.execute(this.database);
    return rows.length === 1;
  }

  async confirmPublished(value: ToolCatalogPublication): Promise<void> {
    const result = await sql`update tool_catalog_publication
      set published_generation=generation,updated_at=clock_timestamp()
      where scope=${value.scope} and generation=${value.generation}
        and release_id=${value.releaseId} and catalog::jsonb->>'revision'=${value.catalog.revision}`.execute(
      this.database,
    );
    if (result.numAffectedRows !== 1n)
      throw new Error("Catalog changed before publication confirmation");
  }
}
