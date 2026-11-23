import { sql, type Kysely } from "kysely";
import type { Database } from "@pi-cloud/database";
import { estimateStoredContext } from "@pi-cloud/pi-session-postgres/durable";

type Range = { stream: string; lane: string; lo: string; hi: string; seq: string };
type Entry = Parameters<typeof estimateStoredContext>[0][number];
const tip = String(Number.MAX_SAFE_INTEGER);
const first = "1";
const lesser = (a: string, b: string) => (BigInt(a) < BigInt(b) ? a : b);

export type ProjectedContextAddress = {
  tenant: string;
  publicRoot: string;
  physical: string;
  fork: boolean;
  // One immutable Run per delegated invocation, or the historical child Session address.
  invocation?: string;
  childSession?: string;
};

// The caller holds a repeatable-read snapshot across address resolution and paging.
// Fork ranges carry both an Entry-ID interval and a commit ceiling.
export async function readProjectedContextEstimate(
  db: Kysely<Database>,
  address: ProjectedContextAddress,
  model: Parameters<typeof estimateStoredContext>[1],
) {
  const { tenant, publicRoot, physical } = address;
  const ranges: Range[] = [];
  let cut = tip,
    invocation = address.invocation,
    child = address.childSession;
  let includeRoot = true;
  const seen = new Set<string>();
  while (invocation || child) {
    const id = (invocation ?? child)!;
    if (seen.has(id)) throw new Error("Cyclic context ancestry");
    seen.add(id);
    const {
      rows: [edge],
    } = await (
      invocation
        ? sql<{ lane: string | null; at: string | null; parent: string | null }>`
        select c.native_lane as lane,c.context_at as at,
          case when p.root_run_id is not null then p.id end as parent
        from runs c join runs p on p.tenant_id=c.tenant_id and p.session_id=c.session_id and p.id=coalesce(c.previous_run_id,c.caller_run_id)
        where c.tenant_id=${tenant}::uuid and c.session_id=${publicRoot}::uuid and c.id=${id}::uuid and c.root_run_id is not null`
        : sql<{ lane: string | null; at: string | null; parent: string | null }>`
        select s.pi_conversation_id as lane,e.pi_context_base_entry_id as at,
          nullif(e.parent_session_id,e.root_session_id) as parent
        from subagent_conversations e join sessions s on s.id=e.child_session_id and s.tenant_id=e.tenant_id
        where e.tenant_id=${tenant}::uuid and e.root_session_id=${publicRoot}::uuid and e.child_session_id=${id}::uuid`
    ).execute(db);
    if (!edge) throw new Error("Missing projected conversation ancestry");
    if (edge.lane !== null)
      ranges.push({ stream: physical, lane: edge.lane, lo: first, hi: cut, seq: tip });
    if (edge.at === null) {
      includeRoot = false;
      break;
    }
    cut = lesser(cut, edge.at);
    if (invocation) invocation = edge.parent ?? undefined;
    else child = edge.parent ?? undefined;
  }
  if (includeRoot) {
    const { rows } =
      await sql<Range>`select stream,lane,lo,hi,seq from message_root_ranges(${tenant}::uuid,${publicRoot}::uuid,${physical}::uuid,${address.fork})`.execute(
        db,
      );
    if (!rows.length) throw new Error("Projected root history directory is unavailable");
    for (const r of rows)
      if (BigInt(r.lo) <= BigInt(cut)) ranges.push({ ...r, hi: lesser(r.hi, cut) });
  }
  // Select at most one page of coordinates before fetching bodies. Once a head
  // is visible, Pi keeps the summary and entries starting at the head's floor.
  const entries: Entry[] = [];
  let before = tip,
    floor = first,
    foundHead = false;
  while (ranges.length && BigInt(before) >= BigInt(floor)) {
    const { rows } = await sql<{ content: string; entry_id: string }>`with page as materialized (
      select k.entry_id,k.pi_session_id from jsonb_to_recordset(${JSON.stringify(ranges)}::jsonb) s(stream uuid,lane bigint,lo bigint,hi bigint,seq bigint)
      cross join lateral(select m.entry_id,m.pi_session_id from message_entries m where m.tenant_id=${tenant}::uuid and m.pi_session_id=s.stream
        and m.conversation_id=s.lane and m.entry_id between greatest(s.lo,${floor}::bigint) and least(s.hi,${before}::bigint) and m.commit_seq<=s.seq
        order by m.entry_id desc limit 64) k order by k.entry_id desc limit 64)
      select p.entry_id,m.content from page p join message_entries m on m.tenant_id=${tenant}::uuid and m.pi_session_id=p.pi_session_id and m.entry_id=p.entry_id
      order by p.entry_id desc`.execute(db);
    for (const row of rows) {
      if (BigInt(row.entry_id) < BigInt(floor)) break;
      const entry: Entry = JSON.parse(row.content);
      if (entry.id !== Number(row.entry_id))
        throw new Error("Projected context Entry identity changed");
      entries.push(entry);
      if (entry.head !== undefined && !foundHead) {
        floor = String(entry.head);
        foundHead = true;
      }
    }
    if (rows.length < 64) break;
    before = String(BigInt(rows.at(-1)!.entry_id) - 1n);
  }
  return estimateStoredContext(entries, model);
}
