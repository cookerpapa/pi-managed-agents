import { sql, type Kysely } from "kysely";

export async function up(database: Kysely<unknown>): Promise<void> {
  await sql`
    create table mcp_connections (
      id uuid primary key,
      tenant_id uuid not null references tenants(id),
      owner_user_id uuid not null,
      name text not null check(char_length(name) between 1 and 128),
      endpoint text not null check(char_length(endpoint) between 8 and 2048),
      visibility text not null check(visibility in ('personal','organization')),
      enabled boolean not null default true,
      revision bigint not null default 1 check(revision>0),
      last_change_hash text not null,
      allowed_tools jsonb,
      credential text,
      credential_revision bigint not null default 1 check(credential_revision>0),
      status text not null default 'pending' check(status in ('pending','ready','error','disabled','deleted')),
      checked_revision bigint,
      checked_at timestamptz,
      next_check_at timestamptz not null default clock_timestamp(),
      last_error text,
      tools jsonb not null default '[]',
      deleted_at timestamptz,
      created_at timestamptz not null default clock_timestamp(),
      updated_at timestamptz not null default clock_timestamp(),
      foreign key(tenant_id,owner_user_id) references users(tenant_id,id),
      check(allowed_tools is null or (jsonb_typeof(allowed_tools)='array' and octet_length(allowed_tools::text)<=65536)),
      check(octet_length(tools::text)<=1048576),
      check(credential is null or octet_length(credential)<=16384)
    );
    create index mcp_connections_tenant on mcp_connections(tenant_id,id) where deleted_at is null;
    create index mcp_connections_reconcile on mcp_connections(next_check_at,id);
    alter table tool_catalog_publication drop constraint tool_catalog_publication_scope_check;
    alter table tool_catalog_publication add column mcp jsonb;
    alter table tool_catalog_publication add constraint tool_catalog_scope_valid check(
      (scope='platform' and mcp is null) or
      (scope ~ '^mcp:[0-9a-f-]{36}$' and mcp is not null and jsonb_typeof(mcp)='object')
    );
  `.execute(database);
}

export async function down(): Promise<void> {
  throw new Error("Scoped MCP catalogs require a matched development upgrade");
}
