import { createHash } from "node:crypto";

/** A name-based UUIDv8 for a native child invocation, independent of its delivery or owner. */
export function delegatedRunId(
  scope: { tenantId: string; piSessionId: string; runId: string },
  taskId: number,
): string {
  if (!Number.isSafeInteger(taskId) || taskId < 1)
    throw new TypeError("Invalid delegation task ID");
  return uuid([
    "pi-cloud.delegated-run.v1",
    scope.tenantId.toLowerCase(),
    scope.piSessionId.toLowerCase(),
    scope.runId.toLowerCase(),
    taskId,
  ]);
}

export function namedAgentId(
  tenant: string,
  session: string,
  parent: string,
  name: string,
): string {
  return uuid([
    "pi-cloud.named-agent.v1",
    tenant.toLowerCase(),
    session.toLowerCase(),
    parent.toLowerCase(),
    name.normalize("NFC"),
  ]);
}

function uuid(parts: readonly (string | number)[]): string {
  const bytes = createHash("sha256").update(JSON.stringify(parts)).digest();
  bytes[6] = (bytes[6]! & 15) | 0x80;
  bytes[8] = (bytes[8]! & 63) | 0x80;
  const hex = bytes.subarray(0, 16).toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
