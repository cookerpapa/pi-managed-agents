import { sql, type Kysely } from "kysely";
import type { Database } from "@pi-cloud/database";
import type { ConversationTreeEntryResource, NavigationChildren } from "@pi-cloud/protocol";

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

/** One index probe per input on the visible page, independent of family size. */
export async function childrenAtInputs(
  db: Kysely<Database>,
  schema: string,
  tenant: string,
  parent: string,
  entries: string[],
  limit: number,
  after?: string,
) {
  const pages = new Map<string, NavigationChildren>();
  if (!entries.length) return pages;
  const cursor = after?.split(".");
  const { rows } = await sql<{
    entry: string;
    id: string;
    agent_name: string;
    cursor: string;
  }>`select a.entry::text,child.id,child.agent_name,
    concat_ws('.',child.parent_call_seq,child.parent_call_entry,child.parent_call_block) as cursor
    from unnest(${entries}::bigint[]) a(entry) cross join lateral (
      select id,agent_name,parent_call_seq,parent_call_entry,parent_call_block from ${sql.id(schema, "sessions")}
      where tenant_id=${tenant}::uuid and parent_session_id=${parent}::uuid and parent_input_entry=a.entry
      ${cursor ? sql`and (parent_call_seq,parent_call_entry,parent_call_block)>(${cursor[0]}::bigint,${cursor[1]}::bigint,${cursor[2]}::bigint)` : sql``}
      order by parent_call_seq,parent_call_entry,parent_call_block limit ${limit + 1}
    ) child order by a.entry,child.parent_call_seq,child.parent_call_entry,child.parent_call_block`.execute(
    db,
  );
  let lastCursor = "";
  for (const row of rows) {
    const page = pages.get(row.entry) ?? { items: [] };
    if (page.items.length === limit) page.next = lastCursor;
    else page.items.push({ sessionId: row.id, name: row.agent_name });
    pages.set(row.entry, page);
    lastCursor = row.cursor;
  }
  return pages;
}
