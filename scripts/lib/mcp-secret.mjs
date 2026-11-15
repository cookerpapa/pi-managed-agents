import { randomBytes } from "node:crypto";
import { constants } from "node:fs";
import { open, stat } from "node:fs/promises";
import { resolve } from "node:path";
import { readPrivateRuntimeFile } from "./runtime-file-policy.mjs";

/** Existing deployments gain one role-scoped key; an existing key is never rotated. */
export async function ensureMcpCredentialMasterKey(runtimeDirectory) {
  const path = resolve(runtimeDirectory, "secrets/mcp-credential-master-key");
  try {
    const value = (await readPrivateRuntimeFile(path, 256, "MCP credential key")).trim();
    if (!/^[A-Za-z0-9_-]{43}$/.test(value) || Buffer.from(value, "base64url").length !== 32)
      throw new Error("Invalid MCP credential master key");
    return false;
  } catch (error) {
    if (error.code !== "ENOENT") throw error;
  }
  const owner = await stat(resolve(runtimeDirectory, "secrets/cli-proxy-api-key"));
  const file = await open(
    path,
    constants.O_CREAT | constants.O_EXCL | constants.O_WRONLY | constants.O_NOFOLLOW,
    0o600,
  );
  try {
    await file.chown(owner.uid, owner.gid);
    await file.writeFile(`${randomBytes(32).toString("base64url")}\n`);
    await file.sync();
  } finally {
    await file.close();
  }
  return true;
}
