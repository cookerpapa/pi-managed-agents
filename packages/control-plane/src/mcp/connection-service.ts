import { createHash } from "node:crypto";
import { sql, type Kysely, type Transaction } from "kysely";
import type { Database } from "@pi-cloud/database";
import {
  parseMcpConnectionChange,
  type McpConnectionChange,
  type McpConnectionResource,
  type McpConnectionTool,
} from "@pi-cloud/protocol";
import { ControlPlaneStoreError } from "../control-plane-store-error.ts";
import { TenantRequestContextError } from "../tenant-request-context.ts";
import type { TenantRequestIdentity } from "../tenant-identity.ts";
import type { McpCredentialVault } from "./credential-vault.ts";

type Actor = Pick<TenantRequestIdentity, "tenantId" | "userId" | "role">;
export type McpConnectionRow = {
  id: string;
  tenant_id: string;
  owner_user_id: string;
  name: string;
  endpoint: string;
  visibility: "personal" | "organization";
  enabled: boolean;
  revision: string;
  last_change_hash: string;
  allowed_tools: string[] | null;
  credential: string | null;
  credential_revision: string;
  status: "pending" | "ready" | "error" | "disabled" | "deleted";
  checked_revision: string | null;
  checked_at: Date | string | null;
  next_check_at: Date | string;
  last_error: string | null;
  tools: McpConnectionTool[];
  deleted_at: Date | string | null;
  publication_ready?: boolean;
};
const notFound = (): never => {
  throw new ControlPlaneStoreError("not_found", "MCP connection was not found");
};
const conflict = (): never => {
  throw new ControlPlaneStoreError("conflict", "MCP connection changed; refresh before saving");
};
const requireOwner = (actor: Actor) => {
  if (actor.role !== "owner")
    throw new TenantRequestContextError(
      "authorization_denied",
      "Only a tenant owner can manage MCP connections",
    );
};

export class McpConnectionService {
  constructor(
    readonly database: Kysely<Database>,
    readonly vault?: McpCredentialVault,
  ) {}

  resource(row: McpConnectionRow, actor: Actor): McpConnectionResource {
    const status = row.deleted_at
      ? "disabled"
      : !row.enabled
        ? "disabled"
        : row.checked_revision !== row.revision ||
            (row.status === "ready" && row.publication_ready === false)
          ? "pending"
          : row.status === "deleted"
            ? "disabled"
            : row.status;
    return {
      id: row.id,
      name: row.name,
      endpoint: row.endpoint,
      ownerUserId: row.owner_user_id,
      visibility: row.visibility,
      enabled: row.enabled,
      revision: row.revision,
      authentication: row.credential ? "bearer" : "none",
      status,
      error: status === "error" ? row.last_error : null,
      checkedAt: row.checked_at ? new Date(row.checked_at).toISOString() : null,
      toolCount: row.tools.length,
      allowedTools: row.allowed_tools,
      canEdit: actor.role === "owner" && row.owner_user_id === actor.userId,
    };
  }

  async list(actor: Actor, after?: string) {
    const { rows } = await sql<McpConnectionRow>`select c.*,
      coalesce(p.generation=p.published_generation and p.release_id=a.release_id,false) as publication_ready
      from mcp_connections c left join tool_catalog_publication p on p.scope='mcp:'||c.id::text
      left join tool_catalog_publication a on a.scope='platform'
      where c.tenant_id=${actor.tenantId}::uuid and c.deleted_at is null
      and (c.owner_user_id=${actor.userId}::uuid or c.visibility='organization')
      and (${after ?? null}::uuid is null or c.id>${after ?? null}::uuid)
      order by c.id limit 101`.execute(this.database);
    const visible = rows.slice(0, 100);
    return {
      connections: visible.map((row) => this.resource(row, actor)),
      next: rows.length > 100 ? visible.at(-1)!.id : null,
    };
  }

  async read(actor: Actor, id: string) {
    const { rows } = await sql<McpConnectionRow>`select c.*,
      coalesce(p.generation=p.published_generation and p.release_id=a.release_id,false) as publication_ready
      from mcp_connections c left join tool_catalog_publication p on p.scope='mcp:'||c.id::text
      left join tool_catalog_publication a on a.scope='platform'
      where c.tenant_id=${actor.tenantId}::uuid and c.id=${id}::uuid and c.deleted_at is null
      and (c.owner_user_id=${actor.userId}::uuid or c.visibility='organization')`.execute(
      this.database,
    );
    const row = rows[0];
    if (!row) return notFound();
    return { connection: this.resource(row, actor), tools: row.tools };
  }

  async save(actor: Actor, id: string, raw: McpConnectionChange) {
    requireOwner(actor);
    const input = parseMcpConnectionChange(raw);
    const allowed = input.allowedTools === null ? null : [...input.allowedTools].sort();
    const hash = createHash("sha256")
      .update(JSON.stringify({ actor: actor.userId, id, ...input, allowedTools: allowed }))
      .digest("hex");
    await this.database.transaction().execute(async (tx) => {
      // Only same-connection administration is serialized; no network I/O holds this lock.
      await sql`select pg_advisory_xact_lock(hashtextextended(${"mcp:" + id},0))`.execute(tx);
      const { rows } =
        await sql<McpConnectionRow>`select * from mcp_connections where id=${id}::uuid for update`.execute(
          tx,
        );
      const old = rows[0];
      if (old && (old.tenant_id !== actor.tenantId || old.owner_user_id !== actor.userId))
        return notFound();
      if (old?.deleted_at) return conflict();
      if (old?.last_change_hash === hash) return;
      if ((old?.revision ?? null) !== input.expectedRevision) return conflict();
      if (old?.credential && old.endpoint !== input.endpoint && input.credential === undefined)
        throw new ControlPlaneStoreError(
          "invalid_request",
          "Changing the endpoint requires replacing or removing its credential",
        );
      const revision = String(BigInt(old?.revision ?? "0") + 1n);
      let credential = old?.credential ?? null,
        credentialRevision = old?.credential_revision ?? revision;
      if (input.credential) {
        credentialRevision = revision;
        if (input.credential.kind === "none") credential = null;
        else {
          if (!this.vault)
            throw new ControlPlaneStoreError(
              "control_plane_misconfigured",
              "MCP credential storage is not configured",
            );
          credential = this.vault.seal(
            actor.tenantId,
            id,
            credentialRevision,
            input.credential.token,
          );
        }
      }
      await sql`insert into mcp_connections(id,tenant_id,owner_user_id,name,endpoint,visibility,enabled,revision,last_change_hash,allowed_tools,credential,credential_revision,status)
        values(${id}::uuid,${actor.tenantId}::uuid,${actor.userId}::uuid,${input.name},${input.endpoint},${input.visibility},${input.enabled},${revision}::bigint,${hash},${allowed === null ? null : JSON.stringify(allowed)}::jsonb,${credential},${credentialRevision}::bigint,${input.enabled ? "pending" : "disabled"})
        on conflict(id) do update set name=excluded.name,endpoint=excluded.endpoint,visibility=excluded.visibility,
          enabled=excluded.enabled,revision=excluded.revision,last_change_hash=excluded.last_change_hash,
          allowed_tools=excluded.allowed_tools,credential=excluded.credential,credential_revision=excluded.credential_revision,
          status=excluded.status,last_error=null,next_check_at=clock_timestamp(),updated_at=clock_timestamp()`.execute(
        tx,
      );
      await this.#notify(tx, id);
    });
    return this.read(actor, id);
  }

  async refresh(actor: Actor, id: string, expectedRevision: string) {
    await this.#change(actor, id, expectedRevision, false);
    return this.read(actor, id);
  }
  async remove(actor: Actor, id: string, expectedRevision: string) {
    await this.#change(actor, id, expectedRevision, true);
    return { removed: true };
  }
  async #change(actor: Actor, id: string, expectedRevision: string, remove: boolean) {
    requireOwner(actor);
    const hash = createHash("sha256")
      .update(JSON.stringify({ actor: actor.userId, id, expectedRevision, remove }))
      .digest("hex");
    await this.database.transaction().execute(async (tx) => {
      const { rows } =
        await sql<McpConnectionRow>`select * from mcp_connections where id=${id}::uuid
        and tenant_id=${actor.tenantId}::uuid and owner_user_id=${actor.userId}::uuid for update`.execute(
          tx,
        );
      const old = rows[0];
      if (!old) return notFound();
      if (old.last_change_hash === hash) return;
      if (old.deleted_at || old.revision !== expectedRevision) return conflict();
      await sql`update mcp_connections set revision=revision+1,last_change_hash=${hash},
        enabled=case when ${remove} then false else enabled end,
        credential=case when ${remove} then null else credential end,
        deleted_at=case when ${remove} then clock_timestamp() else null end,
        status=case when ${remove} then 'deleted' when enabled then 'pending' else 'disabled' end,
        last_error=null,next_check_at=clock_timestamp(),updated_at=clock_timestamp()
        where id=${id}::uuid`.execute(tx);
      await this.#notify(tx, id);
    });
  }
  async #notify(tx: Transaction<Database>, id: string) {
    await sql`select pg_notify('pi_cloud_mcp_connections',${id})`.execute(tx);
  }
}
