import { stat, readFile } from "node:fs/promises";
import { join } from "node:path";

/** Local/private releases reuse the available database. NextUpdate is upstream
 * advice, not a publication deadline. Keep its original dates in scan evidence. */
export async function scanDatabaseDownloadRequired(cache, kind, refresh = false) {
  if (refresh) return true;
  const directory = join(cache, kind === "java" ? "java-db" : "db");
  try {
    const [metadata, file] = await Promise.all([
      readFile(join(directory, "metadata.json"), "utf8").then(JSON.parse),
      stat(join(directory, kind === "java" ? "trivy-java.db" : "trivy.db")),
    ]);
    if (!file.isFile() || file.size === 0 || !Number.isInteger(metadata.Version))
      throw new Error(
        "Invalid cached scan database; explicitly refresh the vulnerability databases",
      );
    return false;
  } catch (error) {
    if (error.code === "ENOENT") return true;
    throw error;
  }
}
