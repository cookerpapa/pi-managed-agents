import { sql, type Kysely } from "kysely";
import type { Database } from "@pi-cloud/database";
import {
  HISTORY_PAGE_BYTES,
  HISTORY_PAGE_RECORDS,
  type ConversationTurnTranscriptResource,
} from "@pi-cloud/protocol";
import { historyHeaders, type FrozenRecord } from "./frozen-history.ts";
import {
  projectDurableTranscript,
  terminalMetadata,
  type NativeHistoryRow,
} from "./native-transcript-presentation.ts";
import { encodeHistoryPosition, type HistoryPosition } from "./history-position.ts";
import type { CanonicalHistoryWindow, HistoryWindowInput } from "./canonical-history-window.ts";

export async function readFrozenHistoryWindow(
  db: Kysely<Database>,
  input: HistoryWindowInput,
): Promise<CanonicalHistoryWindow> {
  const before = input.direction === "before",
    order = sql.raw(before ? "desc" : "asc");
  const comparison = sql.raw(
    before ? (input.inclusive ? "<=" : "<") : input.inclusive ? ">=" : ">",
  );
  const edge = before ? Number.MAX_SAFE_INTEGER : 0,
    ordinalEdge = before ? 2147483647 : 0;
  type Point = {
    turn_id: string;
    origin_session_id: string;
    kind: string;
    record_id: string;
    seq: string;
    ordinal: number;
    entry_id: string | null;
    assistant_id: string | null;
    entry_record_id: string | null;
    assistant_record_id: string | null;
    body_id: string;
    body_bytes: number;
    pi_session_id: string;
    assistant_bytes: number;
  };
  // Fix visibility/order and the candidate count before resolving dependencies.
  // These are immutable references, including when a Fork outlives its source.
  const points = await sql<Point>`with page as materialized (
    select h.turn_id,h.origin_session_id,h.kind,h.record_id::text,h.seq,h.ordinal,
      h.entry_id::text,h.assistant_id::text,h.entry_record_id,h.assistant_record_id,
      h.body_id,h.body_bytes::int,t.pi_session_id,t.turn_index
    from unnest(${input.turns.map((t) => t.turnId)}::uuid[],${input.turns.map((t) => t.piSessionId)}::text[]) with ordinality t(turn_id,pi_session_id,turn_index)
    join pi_history_snapshots h on h.tenant_id=${input.tenantId}::uuid and h.session_id=${input.sessionId}::uuid and h.turn_id=t.turn_id
    where (h.seq,h.ordinal) ${comparison} (case when h.turn_id=${input.position?.turnId ?? null}::uuid then ${input.position?.sequence ?? edge} else ${edge} end::bigint,
      case when h.turn_id=${input.position?.turnId ?? null}::uuid then ${input.position?.ordinal ?? ordinalEdge} else ${ordinalEdge} end::int)
      or (${!before && !input.position} and h.kind='turn')
    order by t.turn_index,h.seq ${order},h.ordinal ${order} limit ${(input.maxRecords ?? HISTORY_PAGE_RECORDS) + 1}
  ) select p.turn_id,p.origin_session_id,p.kind,p.record_id,p.seq::text,p.ordinal,
    p.entry_id,p.assistant_id,p.entry_record_id,p.assistant_record_id,p.body_id,p.body_bytes,p.pi_session_id,
    coalesce(ab.payload_bytes+octet_length(pi_native_entry_prefix(ag.record_id,ag.entry_conversation_id,ag.entry_task_id)),0)::int as assistant_bytes
  from page p
  left join lateral (
    select record_id,entry_conversation_id,entry_task_id,entry_body_id from pi_history_records
    where tenant_id=${input.tenantId}::uuid and id=p.assistant_record_id limit 1
  ) ag on true
  left join lateral (
    select payload_bytes from pi_history_bodies
    where tenant_id=${input.tenantId}::uuid and id=ag.entry_body_id limit 1
  ) ab on true
  order by p.turn_index,p.seq ${order},p.ordinal ${order}`.execute(db);
  const selected: Point[] = [];
  let bytes = 0;
  for (const p of points.rows) {
    const size = p.body_bytes + p.assistant_bytes + 1024;
    if (
      selected.length >= (input.maxRecords ?? HISTORY_PAGE_RECORDS) ||
      bytes + size > (input.maxBytes ?? HISTORY_PAGE_BYTES)
    )
      break;
    bytes += size;
    selected.push(p);
  }
  if (!selected.length && points.rows.length && !input.allowEmpty)
    throw new Error("History record exceeds the frozen presentation page boundary");
  const ids = [...new Set(selected.map((p) => p.turn_id))];
  const headers = await historyHeaders(db, input.tenantId, input.sessionId, ids);
  const records = selected.filter((p) => p.kind === "record");
  // Resolve only the selected immutable keys. LIMIT 1 keeps each keyed lookup
  // bounded even when tenant cardinality is underestimated; no view is reopened.
  const bodies = records.length
    ? (
        await sql<{
          body_id: string;
          turn_id: string;
          seq: string;
          ordinal: number;
          payload_text: string;
          body: string | null;
          assistant_body: string | null;
        }>`
    select k.body_id,k.turn_id,k.seq::text,k.ordinal,b.payload_text,
      case when eg.id is not null then concat(pi_native_entry_prefix(eg.record_id,eg.entry_conversation_id,eg.entry_task_id),substr(eb.payload_text,2),'}') end as body,
      case when ag.id is not null then concat(pi_native_entry_prefix(ag.record_id,ag.entry_conversation_id,ag.entry_task_id),substr(ab.payload_text,2),'}') end as assistant_body
    from unnest(${records.map((p) => p.turn_id)}::uuid[],${records.map((p) => p.seq)}::bigint[],${records.map((p) => p.ordinal)}::int[],
      ${records.map((p) => p.body_id)}::uuid[],${records.map((p) => p.entry_record_id)}::uuid[],${records.map((p) => p.assistant_record_id)}::uuid[])
      k(turn_id,seq,ordinal,body_id,entry_record_id,assistant_record_id)
    join lateral (
      select payload_text from pi_history_bodies where tenant_id=${input.tenantId}::uuid and id=k.body_id limit 1
    ) b on true
    left join lateral (
      select id,record_id,entry_conversation_id,entry_task_id,entry_body_id from pi_history_records
      where tenant_id=${input.tenantId}::uuid and id=k.entry_record_id limit 1
    ) eg on true
    left join lateral (
      select payload_text from pi_history_bodies where tenant_id=${input.tenantId}::uuid and id=eg.entry_body_id limit 1
    ) eb on true
    left join lateral (
      select id,record_id,entry_conversation_id,entry_task_id,entry_body_id from pi_history_records
      where tenant_id=${input.tenantId}::uuid and id=k.assistant_record_id limit 1
    ) ag on true
    left join lateral (
      select payload_text from pi_history_bodies where tenant_id=${input.tenantId}::uuid and id=ag.entry_body_id limit 1
    ) ab on true`.execute(db)
      ).rows
    : [];
  const byBody = new Map(bodies.map((b) => [`${b.turn_id}:${b.seq}:${b.ordinal}`, b]));
  const transcripts = new Map<string, ConversationTurnTranscriptResource>(),
    positions: HistoryPosition[] = [],
    visibleHeaders = new Set<string>();
  const ordered = before ? [...selected].reverse() : selected;
  for (const p of ordered) {
    const header = headers.get(p.turn_id);
    if (!header) throw new Error("Frozen Turn header is missing");
    const terminal = header.terminal
      ? terminalMetadata({ ...header.terminal, occurred_at: new Date(header.terminal.occurred_at) })
      : {
          throughSequence: Number(header.output_display_seq),
          terminalSequence: null,
          stopReason: null,
          failure: null,
          cancellation: null,
          occurredAt: new Date(0).toISOString(),
        };
    const position: HistoryPosition = {
      version: 2,
      sessionId: input.sessionId,
      originSessionId: p.origin_session_id,
      turnId: p.turn_id,
      sequence: Number(p.seq),
      ordinal: p.ordinal,
      storageId: p.pi_session_id,
    };
    positions.push(position);
    const transcript = transcripts.get(p.turn_id) ?? projectDurableTranscript([], terminal);
    transcripts.set(p.turn_id, transcript);
    if (p.kind === "turn") {
      visibleHeaders.add(p.turn_id);
      continue;
    }
    const body = byBody.get(`${p.turn_id}:${p.seq}:${p.ordinal}`);
    if (!body) throw new Error("Frozen history descriptor is missing");
    const descriptor = JSON.parse(body.payload_text) as FrozenRecord;
    let native = body.body ?? descriptor.body;
    if (!native) throw new Error("Frozen history Entry reference is missing");
    if (p.entry_id && descriptor.fields.task) {
      const parsed = JSON.parse(native);
      parsed.value.byTaskId = Number(descriptor.fields.task);
      native = JSON.stringify(parsed);
    }
    const row: NativeHistoryRow = {
      id: p.record_id,
      seq: p.seq,
      fields: descriptor.fields,
      body: native,
      committed_at: new Date(descriptor.committedAt),
      task_kind: descriptor.taskKind,
    };
    const dependency: NativeHistoryRow | undefined =
      p.assistant_id && body.assistant_body
        ? {
            id: p.assistant_id,
            seq: p.seq,
            fields: {},
            body: body.assistant_body,
            committed_at: row.committed_at,
          }
        : undefined;
    if (p.assistant_id && !dependency)
      throw new Error("Frozen Tool assistant reference is missing");
    const humans = new Map(
      descriptor.human
        ? [
            [
              p.record_id,
              {
                ...descriptor.human,
                uploads: descriptor.human.uploads.map((u) => ({
                  ...u,
                  sessionId: input.sessionId,
                })),
              },
            ],
          ]
        : [],
    );
    const projected = projectDurableTranscript(
      [...(dependency ? [dependency] : []), row],
      terminal,
      new Set(dependency ? [dependency.id] : []),
      humans,
      input.sessionId,
    );
    transcript.items.push(
      ...projected.items.map((item) => ({
        ...item,
        historyCursor: encodeHistoryPosition(position),
      })),
    );
  }
  return {
    transcripts,
    positions,
    headers: visibleHeaders,
    more: selected.length < points.rows.length,
    bytes,
    lastContinues:
      selected.length > 0 && points.rows[selected.length]?.turn_id === selected.at(-1)?.turn_id,
  };
}
