import { sql, type Kysely } from "kysely";
import type { Database } from "@pi-cloud/database";
import type { ConversationTreeEntryResource } from "@pi-cloud/protocol";

/** Page only adopted input Entries in this window's retained history. */
export async function placedInputNavigation(
  database: Kysely<Database>,
  schema: string,
  tenant: string,
  session: string,
  limit: number,
  before?: string,
  after?: string | null,
) {
  const { rows } = await sql<{
    entry_id: string;
    turn_id: string;
    source: "user" | "agent";
    preview: string;
    created_at: Date;
  }>`
    select page.* from ${sql.id(schema, "flow_read_ranges")}(${tenant}::uuid,${session}::uuid) f
    cross join lateral (
      select m.entry_id, m.run_id as turn_id, m.input_display->>'source' as source,
        left(coalesce(nullif(coalesce(m.input_display->>'text',m.navigation_caption),''),m.input_display#>>'{uploads,0,name}',''),240) as preview,m.created_at
      from ${sql.id(schema, "messages")} m
      join ${sql.id(schema, "turns")} t on t.tenant_id=m.tenant_id and t.id=m.run_id and t.session_id=m.pi_session_id
      where m.tenant_id=${tenant}::uuid and m.pi_session_id=f.physical and m.conversation_id=f.source_lane
        and m.input_display is not null
        and t.mailbox_position<=f.through_position and (f.cut_seq is null or m.commit_seq<=f.cut_seq)
        and m.entry_id between greatest(coalesce(f.entry_lo,1),${after ?? 0}::bigint+1)
            and least(coalesce(f.entry_hi,9007199254740991),${before ?? "9007199254740992"}::bigint-1)
      order by m.entry_id desc limit ${limit + 1}
    ) page order by page.entry_id desc limit ${limit + 1}`.execute(database);
  const hasMore = rows.length > limit;
  const entries: ConversationTreeEntryResource[] = rows
    .slice(0, limit)
    .reverse()
    .map((r, n, all) => ({
      entryId: String(r.entry_id),
      parentEntryId: n ? String(all[n - 1]!.entry_id) : null,
      turnId: r.turn_id,
      role: "user",
      source: r.source,
      text: r.preview,
      finalAssistant: false,
      createdAt: r.created_at.toISOString(),
    }));
  return {
    entries,
    ...(hasMore && entries[0] ? { earlierEntryId: entries[0].entryId } : {}),
    ...(before ? { hasLaterEntries: true as const } : {}),
  };
}
