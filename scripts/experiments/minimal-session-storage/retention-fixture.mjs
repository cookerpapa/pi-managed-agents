import { readFile } from "node:fs/promises";
import { randomUUID } from "node:crypto";

// Only the resource columns exercised by admission and the real PG expiry claim.
// In this private database, the fixture supplies the resource owner's lifecycle evidence.
export async function installRetentionFixture(db) {
  await db.query(`create table workspaces(
    id uuid primary key, tenant_id uuid not null, sandbox_domain_id text not null, deleted_at timestamptz);
    create table workspace_runtimes(workspace_runtime_id uuid primary key, tenant_id uuid not null,
    workspace_id uuid not null references workspaces, compute_session_id uuid, sandbox_domain_id text not null,
    owner_instance_id uuid not null, state text not null, runtime_capsule text, allocation_closed boolean not null,
    creation_spec jsonb not null, idle_expires_at timestamptz, failure_code text, updated_at timestamptz);
    create unique index workspace_compute_live_unique on workspace_runtimes(tenant_id,workspace_id,compute_session_id)
      nulls not distinct where state<>'released'`);
  await db.query(
    await readFile(new URL("./admission-retention-index.sql", import.meta.url), "utf8"),
  );
}

export async function seedElasticWorkspace(
  db,
  tenant,
  workspace,
  outcome,
  compute = null,
  owner = randomUUID(),
) {
  await db.query("insert into workspaces values($1,$2,'study',null) on conflict do nothing", [
    workspace,
    tenant,
  ]);
  if (outcome === "unallocated") return;
  const state = {
    retained: "allocated",
    closing: "cleaning",
    starting: "materializing",
    released: "released",
    unknown: "unknown",
  }[outcome];
  await db.query(
    `insert into workspace_runtimes(workspace_runtime_id,tenant_id,workspace_id,compute_session_id,
    sandbox_domain_id,owner_instance_id,state,runtime_capsule,allocation_closed,creation_spec,idle_expires_at)
    values(gen_random_uuid(),$1,$2,$3,'study',$4,$5,case when $5='allocated' then 'fixture' end,$6,
    '{"retentionWindowMs":1800000}',clock_timestamp()-interval '1 minute')`,
    [tenant, workspace, compute, owner, state, ["released", "cleaning"].includes(state)],
  );
}
