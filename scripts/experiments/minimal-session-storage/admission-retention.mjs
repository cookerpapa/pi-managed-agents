// Resource-owner operation inside the caller's admission transaction. Fields and
// lock order follow the serving allocation repository and its idle expiry claim.
async function retainElasticForAdmission(tx, tenant, workspace, computeSessionId) {
  const {
    rows: [resource],
  } = await tx.query(
    "select sandbox_domain_id from workspaces where tenant_id=$1 and id=$2 and deleted_at is null for share",
    [tenant, workspace],
  );
  if (!resource) throw Error("Selected Workspace no longer exists");
  const scope = [tenant, workspace, resource.sandbox_domain_id];
  if (computeSessionId != null) scope.push(computeSessionId);
  const where = `tenant_id=$1 and workspace_id=$2 and sandbox_domain_id=$3
    and ${computeSessionId == null ? "compute_session_id is null" : "compute_session_id=$4::uuid"}`;
  const {
    rows: [allocation],
  } = await tx.query(
    `select workspace_runtime_id,state,allocation_closed from workspace_runtimes
       where ${where} and state<>'released' for update`,
    scope,
  );
  if (!allocation) {
    const { rowCount } = await tx.query(
      `select 1 from workspace_runtimes where ${where} and state='released' limit 1`,
      scope,
    );
    return { state: rowCount ? "released" : "unallocated" };
  }
  if (allocation.state === "unknown") return { state: "unknown" };
  if (allocation.allocation_closed || allocation.state === "cleaning") return { state: "closing" };
  if (["reserved", "materializing"].includes(allocation.state)) return { state: "starting" };
  if (allocation.state !== "allocated") return { state: "unknown" };
  const {
    rows: [renewed],
  } = await tx.query(
    `update workspace_runtimes set idle_expires_at=greatest(idle_expires_at,
       statement_timestamp()+(creation_spec->>'retentionWindowMs')::bigint*interval '1 millisecond')
       where workspace_runtime_id=$1 and runtime_capsule is not null and idle_expires_at is not null
         and (creation_spec->>'retentionWindowMs')::bigint>0
       returning idle_expires_at`,
    [allocation.workspace_runtime_id],
  );
  return renewed
    ? { state: "retained", until: renewed.idle_expires_at.toISOString() }
    : { state: "unknown" };
}

// The caller locks the Session selection before checking the key. Both row locks
// and a new deadline are released by the same commit or rollback as the Run.
export async function selectedRunEnvironment(tx, tenant, session, key) {
  const {
    rows: [selection],
  } = await tx.query(
    "select value from study_session_environments where tenant=$1 and session=$2 for update",
    [tenant, session],
  );
  if (!selection) throw Error("Missing admitted Session environment");
  const env = selection.value;
  if (env.executionMode !== "elastic") return env;
  const {
    rows: [prior],
  } = await tx.query(
    "select environment_snapshot from runs where tenant_id=$1 and session_id=$2 and idempotency_key=$3",
    [tenant, session, key],
  );
  if (prior) return prior.environment_snapshot;
  const { computeSessionId, ...modelEnvironment } = env;
  return {
    ...modelEnvironment,
    elastic: await retainElasticForAdmission(tx, tenant, env.workspace, computeSessionId),
  };
}
