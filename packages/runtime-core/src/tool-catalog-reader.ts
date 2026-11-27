import { sql, type Kysely } from "kysely";
import type { Database } from "@pi-cloud/database";
import type { ToolCatalogWatermark, ToolCatalogPublication } from "@pi-cloud/protocol";

/** Cold view watermark and immutable definitions needed to resume an interrupted request. */
export class ToolCatalogReader {
  constructor(readonly database: Kysely<Database>) {}
  async minimumGenerations(): Promise<readonly ToolCatalogWatermark[]> {
    const { rows } = await sql<{ scope: string; generation: string }>`
      select scope,generation::text from tool_catalog_publication`.execute(this.database);
    return rows.map((row) => ({ scope: row.scope, generation: Number(row.generation) }));
  }

  async versionCatalog(generation: number, releaseId: string) {
    const {
      rows: [row],
    } = await sql<{
      publication: ToolCatalogPublication;
    }>`select publication from tool_host_versions
      where generation=${generation} and release_id=${releaseId} and (expires_at is null or expires_at>clock_timestamp())`.execute(
      this.database,
    );
    return row?.publication;
  }
}
