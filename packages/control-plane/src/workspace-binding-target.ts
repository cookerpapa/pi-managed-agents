import type { Transaction } from "kysely";
import type { Database } from "@pi-cloud/database";
import { validMachineDirectory } from "@pi-cloud/protocol";
import { ControlPlaneStoreError } from "./control-plane-store-error.ts";

export async function resolveWorkspaceBindingTarget(
  transaction: Transaction<Database>,
  tenantId: string,
  workspaceId: string,
  ownerUserId: string | undefined,
  sessionOwnerId: string | null,
  directory?: string,
  elasticSubdirectory = false,
) {
  if (directory !== undefined && !validMachineDirectory(directory))
    throw new ControlPlaneStoreError(
      "invalid_request",
      "Conversation working directory is invalid",
    );
  // Match machine lifecycle/admission: machine before Workspace. A release
  // intent may leave the machine marked running until its external call ends.
  const machine = await transaction
    .selectFrom("development_environments")
    .select(["id", "owner_user_id", "profile_key", "state"])
    .where("tenant_id", "=", tenantId)
    .where("workspace_id", "=", workspaceId)
    .where("state", "!=", "released")
    .forShare()
    .executeTakeFirst();
  if (machine) {
    if (machine.owner_user_id !== ownerUserId || sessionOwnerId !== ownerUserId)
      throw new ControlPlaneStoreError("not_found", "Target development machine was not found");
    if (!["ready"].includes(machine.state))
      throw new ControlPlaneStoreError("conflict", "Cloud development machine is not ready");
    const releasing = await transaction
      .selectFrom("development_environment_operations")
      .select("id")
      .where("tenant_id", "=", tenantId)
      .where("environment_id", "=", machine.id)
      .where("action", "=", "release")
      .where("result_state", "!=", "released")
      .limit(1)
      .executeTakeFirst();
    if (releasing)
      throw new ControlPlaneStoreError("conflict", "Cloud development machine is being released");
  }
  const target = await transaction
    .selectFrom("workspaces as workspace")
    .innerJoin("projects as project", (join) =>
      join
        .onRef("project.tenant_id", "=", "workspace.tenant_id")
        .onRef("project.id", "=", "workspace.project_id"),
    )
    .select([
      "workspace.id",
      "workspace.project_id as projectId",
      "project.name as workspaceName",
      "workspace.workspace_kind as workspaceKind",
    ])
    .where("workspace.tenant_id", "=", tenantId)
    .where("workspace.id", "=", workspaceId)
    .where("workspace.workspace_kind", "in", ["user", "development_environment"])
    .where("workspace.deleted_at", "is", null)
    .where("project.deleted_at", "is", null)
    .forUpdate("workspace")
    .executeTakeFirst();
  if (target === undefined) {
    throw new ControlPlaneStoreError("not_found", "Target Workspace was not found");
  }
  const machineTarget = target.workspaceKind === "development_environment";
  if (machineTarget !== (machine !== undefined))
    throw new ControlPlaneStoreError(
      "conflict",
      "Target Workspace execution resource is unavailable",
    );
  const workingDirectory = directory ?? (machineTarget ? "/home/user" : "/workspace");
  if (
    !machineTarget &&
    workingDirectory !== "/workspace" &&
    !(elasticSubdirectory && workingDirectory.startsWith("/workspace/"))
  )
    throw new ControlPlaneStoreError("invalid_request", "Elastic Workspace requires /workspace");
  if (machine && !["starter", "standard", "performance"].includes(machine.profile_key))
    throw new ControlPlaneStoreError(
      "control_plane_misconfigured",
      "Development machine profile is invalid",
    );

  return { machine, target, machineTarget, workingDirectory };
}
