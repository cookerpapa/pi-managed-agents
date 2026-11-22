import { posix } from "node:path";
import {
  parseCloudToolCapabilitySnapshot,
  type CloudToolCapabilitySnapshot,
} from "@pi-cloud/protocol";

type WorkspaceRequest = {
  compute?: "shared" | "ephemeral";
  cwd?: string;
  tools?: CloudToolCapabilitySnapshot;
};
export type DelegatedWorkspaceGrant = {
  workspaceId: string;
  volumeRoot: string;
  cwd: string;
  compute: "shared" | "ephemeral";
  computeSessionId: string;
  tools: CloudToolCapabilitySnapshot;
} | null;

function within(path: string, root: string) {
  return (
    posix.isAbsolute(path) &&
    posix.normalize(path) === path &&
    !/[\x00-\x1f\x7f]/.test(path) &&
    (path === root || path.startsWith(root === "/" ? "/" : root + "/"))
  );
}

/** Logical resource scope only. Never carry provider addresses or physical VM identity. */
export function childWorkspaceGrant(
  parent: DelegatedWorkspaceGrant,
  request: WorkspaceRequest | null | undefined,
  agentId: string,
): DelegatedWorkspaceGrant {
  if (request === null) return null;
  const tools = parseCloudToolCapabilitySnapshot(request?.tools ?? parent?.tools ?? []);
  if (tools.some((t) => !parent?.tools.includes(t)))
    throw Error("Requested child tools exceed caller permissions");
  if (!parent || !tools.length) {
    if (request?.cwd || request?.compute === "ephemeral")
      throw Error("Child has no workspace access");
    return null;
  }
  const cwd = request?.cwd ?? parent.cwd;
  if (!within(cwd, parent.volumeRoot))
    throw Error("Child working directory is outside the workspace");
  const compute = request?.compute ?? "shared";
  return {
    workspaceId: parent.workspaceId,
    volumeRoot: parent.volumeRoot,
    cwd,
    compute,
    computeSessionId: compute === "ephemeral" ? agentId : parent.computeSessionId,
    tools,
  };
}

export function continuedWorkspaceGrant(
  previous: DelegatedWorkspaceGrant,
  caller: DelegatedWorkspaceGrant,
): DelegatedWorkspaceGrant {
  if (
    !previous ||
    !caller ||
    previous.workspaceId !== caller.workspaceId ||
    !within(previous.cwd, caller.volumeRoot)
  )
    return null;
  if (previous.compute === "shared" && previous.computeSessionId !== caller.computeSessionId)
    return null;
  const tools = parseCloudToolCapabilitySnapshot(previous.tools).filter((t) =>
    caller.tools.includes(t),
  );
  return tools.length
    ? {
        workspaceId: previous.workspaceId,
        volumeRoot: within(previous.volumeRoot, caller.volumeRoot)
          ? previous.volumeRoot
          : caller.volumeRoot,
        cwd: previous.cwd,
        compute: previous.compute,
        computeSessionId: previous.computeSessionId,
        tools,
      }
    : null;
}
