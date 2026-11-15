import { mkdtemp, mkdir, writeFile, readFile, stat, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it } from "vitest";
import { ensureMcpCredentialMasterKey } from "./mcp-secret.mjs";

it("creates an upgrade key once and refuses to overwrite damaged credentials", async () => {
  const root = await mkdtemp(join(tmpdir(), "pma-mcp-key-"));
  try {
    await mkdir(join(root, "secrets"), { mode: 0o700 });
    await writeFile(join(root, "secrets/cli-proxy-api-key"), "preserved", { mode: 0o600 });
    expect(await ensureMcpCredentialMasterKey(root)).toBe(true);
    const path = join(root, "secrets/mcp-credential-master-key"),
      first = await readFile(path, "utf8");
    expect(Buffer.from(first.trim(), "base64url")).toHaveLength(32);
    expect((await stat(path)).mode & 0o777).toBe(0o600);
    expect(await ensureMcpCredentialMasterKey(root)).toBe(false);
    expect(await readFile(path, "utf8")).toBe(first);
    await writeFile(path, "damaged");
    await expect(ensureMcpCredentialMasterKey(root)).rejects.toThrow("Invalid MCP");
    expect(await readFile(path, "utf8")).toBe("damaged");
    expect(await readFile(join(root, "secrets/cli-proxy-api-key"), "utf8")).toBe("preserved");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
