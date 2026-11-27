import { sql, type Kysely } from "kysely";
export async function up(db: Kysely<unknown>) {
  await sql`create table tool_host_versions (
   generation bigint primary key, release_id text not null, endpoint text not null, manifest_hash text not null,
   publication jsonb not null, expires_at timestamptz
 );
 create unique index tool_host_one_current on tool_host_versions((true)) where expires_at is null;
 create index tool_host_versions_expiry on tool_host_versions(expires_at) where expires_at is not null;
 create table tool_host_revocations (
   tool_name text primary key, disabled boolean not null, revoked_through bigint not null
 )`.execute(db);
}
export async function down(db: Kysely<unknown>) {
  await sql`drop table tool_host_versions, tool_host_revocations`.execute(db);
}
