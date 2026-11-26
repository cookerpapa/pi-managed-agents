import { randomUUID, createHash } from "node:crypto";
import { sql } from "kysely";
import { CLOUD_TOOL_NAMES, parseCloudToolCapabilitySnapshot } from "@pi-cloud/protocol";
import { ControlPlaneStoreError } from "../../../packages/control-plane/src/control-plane-store-error.ts";
import {
  supportedSessionModelSelection,
  ensureSelectableModelProfile,
} from "../../../packages/control-plane/src/model-profile-catalog.ts";
import { catalogModel } from "../../../packages/control-plane/src/runtime-model-catalog.ts";
import { resolveTurnModelSnapshot } from "../../../packages/control-plane/src/turn-model-snapshot.ts";
import { resolveWorkspaceBindingTarget } from "../../../packages/control-plane/src/workspace-binding-target.ts";

const invalid = (message) => new ControlPlaneStoreError("invalid_request", message);
const conflict = (message) => new ControlPlaneStoreError("conflict", message);
async function sessionForUpdate(tx, tenant, session) {
  const {
    rows: [row],
  } =
    await sql`select * from sessions where tenant_id=${tenant}::uuid and id=${session}::uuid for no key update`.execute(
      tx,
    );
  if (!row) throw new ControlPlaneStoreError("not_found", "Unknown Session");
  return row;
}
function resource(id, m) {
  return {
    sessionId: id,
    modelProfileId: m.profileId,
    provider: m.provider,
    modelId: m.modelId,
    displayName: catalogModel(m.provider, m.modelId)?.displayName ?? m.modelId,
    thinkingLevel: m.thinkingLevel,
    fastMode: m.serviceTier === "fast",
  };
}
function logicalGrant(workspaceId, machineId, cwd, tools, computeSessionId) {
  const volumeRoot = machineId ? "/home/user" : "/workspace";
  return {
    workspaceId,
    volumeRoot,
    cwd,
    tools: parseCloudToolCapabilitySnapshot(tools),
    compute: "shared",
    executionMode: machineId ? "development_environment" : "elastic",
    computeSessionId: computeSessionId ?? machineId ?? workspaceId,
    developmentEnvironmentId: machineId ?? null,
  };
}

export async function initializeRoots(
  tx,
  tenant,
  ids,
  { ownerUserId = null, workspaceId, workingDirectory } = {},
) {
  const policy = await tx
    .selectFrom("tenant_runtime_policies")
    .select("default_model_profile_id")
    .where("tenant_id", "=", tenant)
    .where("enabled", "=", true)
    .executeTakeFirstOrThrow();
  const fallback = await resolveTurnModelSnapshot(tx, tenant, policy.default_model_profile_id);
  let workspace = null;
  if (workspaceId) {
    const {
      target,
      machine,
      workingDirectory: cwd,
    } = await resolveWorkspaceBindingTarget(
      tx,
      tenant,
      workspaceId,
      ownerUserId,
      ownerUserId,
      workingDirectory,
      true,
    );
    workspace = logicalGrant(target.id, machine?.id, cwd, CLOUD_TOOL_NAMES);
  }
  await sql`update sessions set desired_model=${JSON.stringify(fallback)}::jsonb,desired_workspace=${workspace && JSON.stringify(workspace)}::jsonb,owner_user_id=${ownerUserId}::uuid where tenant_id=${tenant}::uuid and id=any(${ids}::uuid[])`.execute(
    tx,
  );
  // Import a public window's configured defaults when attaching a pre-existing Session.
  const { rows } =
    await sql`select id,desired_model_profile_id,desired_thinking_level,desired_service_tier,created_by_user_id,workspace_id,working_directory,
    compute_session_id,development_environment_id,tool_capabilities from public.sessions where tenant_id=${tenant}::uuid and id=any(${ids}::uuid[]) for share`.execute(
      tx,
    );
  for (const s of rows) {
    const m = await resolveTurnModelSnapshot(
      tx,
      tenant,
      s.desired_model_profile_id,
      s.desired_thinking_level,
      s.desired_service_tier,
    );
    const g = logicalGrant(
      s.workspace_id,
      s.development_environment_id,
      s.working_directory,
      s.tool_capabilities,
      s.compute_session_id,
    );
    await sql`update sessions set desired_model=${JSON.stringify(m)}::jsonb,desired_workspace=${JSON.stringify(g)}::jsonb,owner_user_id=${s.created_by_user_id}::uuid
      where tenant_id=${tenant}::uuid and id=${s.id}::uuid`.execute(tx);
  }
}

export class SessionDefaults {
  constructor(db) {
    this.db = db;
  }
  async model(tenant, session) {
    const {
      rows: [row],
    } =
      await sql`select desired_model from sessions where tenant_id=${tenant}::uuid and id=${session}::uuid`.execute(
        this.db,
      );
    if (!row) throw new ControlPlaneStoreError("not_found", "Unknown Session");
    return resource(session, row.desired_model);
  }
  async selectModel(tenant, session, selection) {
    if (!supportedSessionModelSelection(selection)) throw invalid("Unsupported model selection");
    return this.db.transaction().execute(async (tx) => {
      await sql`set local synchronous_commit=on`.execute(tx);
      // Use the serving model profile/credential admission order.
      await tx
        .selectFrom("tenant_runtime_policies")
        .select("tenant_id")
        .where("tenant_id", "=", tenant)
        .where("enabled", "=", true)
        .forUpdate()
        .executeTakeFirstOrThrow();
      const s = await sessionForUpdate(tx, tenant, session);
      if (s.parent_session_id) throw conflict("Delegated Session settings belong to its grant");
      const profile = await ensureSelectableModelProfile({
        transaction: tx,
        tenantId: tenant,
        selection,
        idGenerator: randomUUID,
        now: new Date(),
      });
      const m = await resolveTurnModelSnapshot(
        tx,
        tenant,
        profile.profileId,
        selection.thinkingLevel,
        selection.fastMode ? "fast" : null,
      );
      await sql`update sessions set desired_model=${JSON.stringify(m)}::jsonb where tenant_id=${tenant}::uuid and id=${session}::uuid`.execute(
        tx,
      );
      return resource(session, m);
    });
  }
  async rebind({ tenant, session, sender }, key, { workspaceId, workingDirectory }) {
    if (typeof key !== "string" || !key.length || key.length > 256)
      throw invalid("Missing Workspace operation identity");
    const hash = createHash("sha256")
      .update(JSON.stringify([sender, workspaceId, workingDirectory ?? null]))
      .digest("hex");
    return this.db.transaction().execute(async (tx) => {
      await sql`set local synchronous_commit=on`.execute(tx);
      const s = await sessionForUpdate(tx, tenant, session);
      const {
        rows: [old],
      } =
        await sql`select fingerprint,receipt from session_workspace_changes where tenant_id=${tenant}::uuid and session_id=${session}::uuid and request_key=${key}`.execute(
          tx,
        );
      if (old) {
        if (old.fingerprint !== hash) throw conflict("Workspace operation identity changed");
        return { ...old.receipt, replayed: true };
      }
      if (s.parent_session_id) throw conflict("Delegated Session settings belong to its grant");
      const busy =
        await sql`select 1 from turns where tenant_id=${tenant}::uuid and session_id=${s.flow_id}::uuid and
        (state in ('queued','running','settling','cancel_requested') or (output_seal_id is not null and output_sealed_at is null))
        union all select 1 from session_leases where tenant_id=${tenant}::uuid and pi_session_id=${session} and released_at is null limit 1`.execute(
          tx,
        );
      if (busy.rows.length)
        throw conflict(
          "Wait for this Session's Turn to seal before changing Workspace or directory",
        );
      const {
        machine,
        target,
        workingDirectory: cwd,
      } = await resolveWorkspaceBindingTarget(
        tx,
        tenant,
        workspaceId,
        sender,
        s.owner_user_id,
        workingDirectory,
        true,
      );
      const grant = logicalGrant(
        target.id,
        machine?.id,
        cwd,
        s.desired_workspace?.tools ?? CLOUD_TOOL_NAMES,
      );
      const receipt = {
        operationId: randomUUID(),
        sessionId: session,
        projectId: target.projectId,
        workspaceId,
        workspaceName: target.workspaceName,
        workspaceState: "attached",
        replayed: false,
        boundAt: new Date().toISOString(),
      };
      await sql`update sessions set desired_workspace=${JSON.stringify(grant)}::jsonb where tenant_id=${tenant}::uuid and id=${session}::uuid`.execute(
        tx,
      );
      await sql`insert into session_workspace_changes values(${tenant}::uuid,${session}::uuid,${key},${hash},${JSON.stringify(receipt)}::jsonb)`.execute(
        tx,
      );
      return receipt;
    });
  }
}
