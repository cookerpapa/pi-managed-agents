import { controlToolCatalog } from "./control-tool-registry.ts";
import { createHash } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import { sql, type Kysely, type Transaction } from "kysely";
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

type Selection = {
  scope?: string;
  mcp?: ToolCatalogPublication["mcp"];
  releaseId: string;
  catalog: ControlToolCatalog;
  replayable: readonly string[];
  expectedGeneration: number;
  versioned?: {
    endpoint: string;
    graceMs: number;
    availability?: { name: string; disabled: boolean };
  };
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

function versionEndpoint(raw: string) {
  const url = new URL(raw);
  if (
    !["http:", "https:"].includes(url.protocol) ||
    url.username ||
    url.password ||
    url.pathname !== "/" ||
    url.hash ||
    url.search
  )
    throw Error("Invalid operator Tool host address");
  return url.origin;
}

async function changeAvailability(
  tx: Transaction<Database>,
  input: Selection,
  old: Row | undefined,
  change: { name: string; disabled: boolean },
) {
  if (!old || old.release_id !== input.releaseId)
    throw Error("Tool release changed before the policy update");
  if (Number(old.generation) !== input.expectedGeneration) {
    const existing = await sql<{
      disabled: boolean;
    }>`select disabled from tool_host_revocations where tool_name=${change.name}`.execute(tx);
    if (
      Number(old.generation) === input.expectedGeneration + 1 &&
      existing.rows[0]?.disabled === change.disabled
    )
      return publication(old);
    throw Error("Tool policy generation changed");
  }
  if (
    !input.catalog.tools.some(
      (tool) => tool.name === change.name && tool.environment.kind !== "target",
    ) &&
    !(await sql`select from tool_host_revocations where tool_name=${change.name}`.execute(tx)).rows
      .length &&
    !(
      await sql`select from tool_host_versions where (expires_at is null or expires_at>now())
                 and exists(select from jsonb_array_elements(publication->'catalog'->'tools') tool where tool->>'name'=${change.name} and tool#>>'{environment,kind}'<>'target') limit 1`.execute(
        tx,
      )
    ).rows.length
  )
    throw Error("Unknown ordinary tool");
  if (change.disabled)
    await sql`insert into tool_host_revocations values(${change.name},true,${old.generation})
              on conflict(tool_name) do update set disabled=true,revoked_through=greatest(tool_host_revocations.revoked_through,excluded.revoked_through)`.execute(
      tx,
    );
  else
    await sql`update tool_host_revocations set disabled=false where tool_name=${change.name}`.execute(
      tx,
    );
  return undefined;
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

  async select(input: Selection): Promise<ToolCatalogPublication> {
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
      const route = input.versioned && {
        ...input.versioned,
        endpoint: versionEndpoint(input.versioned.endpoint),
      };
      const manifest =
        route &&
        createHash("sha256")
          .update(JSON.stringify([input.catalog.revision, [...input.replayable].sort()]))
          .digest("hex");
      if (route) await sql`set local synchronous_commit=on`.execute(tx);
      if (
        scope === "platform" &&
        !route &&
        old?.release_id !== input.releaseId &&
        (await sql`select 1 from tool_host_versions where expires_at is null limit 1`.execute(tx))
          .rows.length
      )
        throw Error("A versioned Tool host requires a matched release-specific activation");
      if (route) {
        if (scope !== "platform" || !Number.isSafeInteger(route.graceMs) || route.graceMs <= 0)
          throw Error("Invalid ordinary Tool release");
        const reused = await sql`select from tool_host_versions where
          ((endpoint=${route.endpoint} and release_id<>${input.releaseId}) or
           (release_id=${input.releaseId} and manifest_hash<>${manifest!}))
          and (expires_at is null or expires_at>clock_timestamp()) limit 1`.execute(tx);
        if (reused.rows.length) throw Error("Retained Tool release address or manifest conflict");
        if (route.availability) {
          const applied = await changeAvailability(tx, input, old, route.availability);
          if (applied) return applied;
        }
        const rules = await sql<{
          tool_name: string;
        }>`select tool_name from tool_host_revocations where disabled`.execute(tx);
        const excluded = new Set(rules.rows.map((r) => r.tool_name));
        input = {
          ...input,
          catalog: controlToolCatalog(input.catalog.tools.filter((t) => !excluded.has(t.name))),
          replayable: input.replayable.filter((n) => !excluded.has(n)),
        };
      }
      if (
        old?.release_id === input.releaseId &&
        ((scope === "platform" && !route) ||
          (isDeepStrictEqual(old.mcp ?? undefined, input.mcp) &&
            (JSON.parse(old.catalog) as ControlToolCatalog).revision === input.catalog.revision))
      ) {
        if (
          (JSON.parse(old.catalog) as ControlToolCatalog).revision !== input.catalog.revision ||
          JSON.stringify([...old.replayable].sort()) !==
            JSON.stringify([...input.replayable].sort())
        )
          throw new Error("Tool release identity changed its catalog");
        if (route) {
          // First matched activation may attach the retained address to an already selected manifest.
          const {
            rows: [known],
          } = await sql<{
            endpoint: string;
          }>`select endpoint from tool_host_versions where generation=${old.generation}`.execute(
            tx,
          );
          if (known && known.endpoint !== route.endpoint)
            throw Error("Tool release requires its original endpoint");
          if (!known)
            await sql`insert into tool_host_versions(generation,release_id,endpoint,manifest_hash,publication)
            values(${old.generation},${old.release_id},${route.endpoint},${manifest!},${JSON.stringify(publication(old))}::jsonb)`.execute(
              tx,
            );
        }
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
      const selected = parseToolCatalogPublication({
        version: 2,
        scope,
        ...(input.mcp ? { mcp: input.mcp } : {}),
        generation,
        releaseId: input.releaseId,
        catalog: input.catalog,
        replayable: [...input.replayable].sort(),
      });
      if (route) {
        await sql`update tool_host_versions set expires_at=clock_timestamp()+${route.graceMs}::double precision*interval '1 millisecond' where expires_at is null`.execute(
          tx,
        );
        await sql`delete from tool_host_versions where expires_at<=now()`.execute(tx);
        await sql`insert into tool_host_versions(generation,release_id,endpoint,manifest_hash,publication)
          values(${generation},${selected.releaseId},${route.endpoint},${manifest!},${JSON.stringify(selected)}::jsonb)`.execute(
          tx,
        );
      }
      return selected;
    });
  }

  async versionEndpoint(generation: number, releaseId: string) {
    const {
      rows: [row],
    } = await sql<{ endpoint: string }>`select endpoint from tool_host_versions
      where generation=${generation} and release_id=${releaseId} and (expires_at is null or expires_at>clock_timestamp())`.execute(
      this.database,
    );
    return row?.endpoint;
  }
  async retention(releaseId: string, endpoint: string) {
    const {
      rows: [row],
    } = await sql<{
      current: boolean;
      until: Date | null;
    }>`select coalesce(bool_or(expires_at is null),false) as current,max(expires_at) as until
      from tool_host_versions where release_id=${releaseId} and endpoint=${versionEndpoint(endpoint)}
      and (expires_at is null or expires_at>clock_timestamp())`.execute(this.database);
    return { current: row!.current, acceptUntil: row!.until?.toISOString() ?? null };
  }
  async versionCatalog(generation: number, releaseId: string) {
    const {
      rows: [row],
    } = await sql<{
      publication: ToolCatalogPublication;
    }>`select publication from tool_host_versions
      where generation=${generation} and release_id=${releaseId} and (expires_at is null or expires_at>clock_timestamp())`.execute(
      this.database,
    );
    return row?.publication;
  }
  async admitOrdinary(
    generation: number,
    releaseId: string,
    name: string,
  ): Promise<"accepted" | "disabled" | "expired"> {
    const {
      rows: [r],
    } = await sql<{ disabled: boolean; available: boolean }>`select
      exists(select from tool_host_revocations where tool_name=${name} and (disabled or revoked_through>=${generation})) as disabled,
      exists(select from tool_host_versions where generation=${generation} and release_id=${releaseId}
        and (expires_at is null or expires_at>clock_timestamp()) and publication->'catalog'->'tools' @> ${JSON.stringify([{ name }])}::jsonb) as available`.execute(
      this.database,
    );
    return r?.disabled ? "disabled" : r?.available ? "accepted" : "expired";
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
