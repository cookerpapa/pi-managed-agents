import { it, expect } from "vitest";
import { mkdtemp, mkdir, writeFile, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { scanDatabaseDownloadRequired } from "./scan-database-policy.mjs";

it("reuses an old database without rewriting its date, and refreshes only on request or missing cache", async () => {
  const root = await mkdtemp(join(tmpdir(), "pma-scan-cache-"));
  try {
    expect(await scanDatabaseDownloadRequired(root, "vulnerability")).toBe(true);
    for (const [kind, directory, filename] of [
      ["vulnerability", "db", "trivy.db"],
      ["java", "java-db", "trivy-java.db"],
    ]) {
      await mkdir(join(root, directory));
      const metadata = JSON.stringify({
        Version: 2,
        UpdatedAt: "2026-01-01T00:00:00Z",
        NextUpdate: "2026-01-02T00:00:00Z",
      });
      await writeFile(join(root, directory, "metadata.json"), metadata);
      await writeFile(join(root, directory, filename), "opaque fixture");
      expect(await scanDatabaseDownloadRequired(root, kind)).toBe(false);
      expect(await scanDatabaseDownloadRequired(root, kind, true)).toBe(true);
      expect(await readFile(join(root, directory, "metadata.json"), "utf8")).toBe(metadata);
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
