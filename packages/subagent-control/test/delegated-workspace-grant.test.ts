import { expect, it } from "vitest";
import {
  childWorkspaceGrant,
  continuedWorkspaceGrant,
  type DelegatedWorkspaceGrant,
} from "../src/delegated-workspace-grant.ts";

it("intersects only compatible workspace and compute grants on an explicit continuation", () => {
  const caller: NonNullable<DelegatedWorkspaceGrant> = {
    workspaceId: "workspace",
    volumeRoot: "/workspace",
    cwd: "/workspace/repo",
    compute: "shared",
    computeSessionId: "root-machine",
    tools: ["read", "edit", "bash"],
  };
  const read = childWorkspaceGrant(caller, { tools: ["read"] }, "A");
  expect(continuedWorkspaceGrant(read, caller)).toEqual(read);
  expect(continuedWorkspaceGrant(read, { ...caller, tools: ["bash"] })).toBeNull();
  expect(continuedWorkspaceGrant(read, { ...caller, workspaceId: "other" })).toBeNull();
  expect(continuedWorkspaceGrant(read, { ...caller, computeSessionId: "other" })).toBeNull();
  const narrower = { ...caller, volumeRoot: caller.cwd };
  expect(continuedWorkspaceGrant(narrower, caller)?.volumeRoot).toBe(narrower.volumeRoot);
  expect(continuedWorkspaceGrant(caller, narrower)?.volumeRoot).toBe(narrower.volumeRoot);
  expect(continuedWorkspaceGrant(null, caller)).toBeNull();
  const privateGrant = childWorkspaceGrant(
    caller,
    { compute: "ephemeral", tools: ["read", "edit"] },
    "A",
  );
  expect(continuedWorkspaceGrant(privateGrant, { ...caller, tools: ["read"] })).toEqual({
    ...privateGrant,
    tools: ["read"],
  });
  expect(() => childWorkspaceGrant(caller, { tools: ["write"] }, "A")).toThrow();
  expect(() => childWorkspaceGrant(caller, { cwd: "/workspace/../elsewhere" }, "A")).toThrow();
});
