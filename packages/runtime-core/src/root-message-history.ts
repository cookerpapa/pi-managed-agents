import {
  firstJsonObject,
  humanPresentationSliceCharacters,
  type StoredHumanInput,
} from "@pi-cloud/pi-session-postgres/durable";
// Matched history reader for root Runs and explicitly scoped child Conversations.
import { sql, type Kysely } from "kysely";
import type { Database } from "@pi-cloud/database";
import { HISTORY_PAGE_RECORDS, parseUserUploads, type UserUpload } from "@pi-cloud/protocol";
import type { HistoryHeader, HistoryTurnRow } from "./frozen-history.ts";
import {
  renderHistoryPoints,
  selectHistoryPoints,
  type HistoryWindowInput,
  type HistoryWindowPoint,
} from "./canonical-history-window.ts";
import { terminalMetadata, type NativeHistoryRow } from "./native-transcript-presentation.ts";

export const rootMessageTurns = sql<HistoryTurnRow>`(select r.tenant_id,r.session_id,r.session_id as origin_session_id,r.turn_id,r.id as run_id,s.pi_session_id,
  r.mailbox_position,r.operation_kind,r.state,r.stop_reason,r.output_display_seq,r.output_display_native_seq,
  (r.output_seal_id is not null and r.output_sealed_at is null) as waiting_for_seal,r.created_at,false as frozen
  from runs r join sessions s on s.tenant_id=r.tenant_id and s.id=r.session_id where r.pruned_at is null and s.pruned_at is null
    and (r.state<>'queued' or r.started_at is not null or r.operation_kind='compact') and not(r.operation_kind='chat' and r.state='cancelled' and r.started_at is null)) root_turns`;

// The Session ID identifies the public window. The message stream belongs to
// the Run's writing flow, which may outlive the window that created it.
export const flowRootMessageTurns = (tenant: string, session: string) =>
  sql<HistoryTurnRow>`(select r.tenant_id,${session}::uuid as session_id,${session}::uuid as origin_session_id,
    r.id as turn_id,r.id as run_id,r.session_id::text as pi_session_id,
    f.entry_lo as flow_lo,f.entry_hi as flow_hi,f.cut_seq as flow_seq,f.source_lane as flow_lane,case when r.mailbox_position=f.through_position then f.tail_coverage end as partial_coverage,
    (f.prior_positions+r.mailbox_position) as mailbox_position,r.operation_kind,
    case when f.tail_coverage is not null and r.mailbox_position=f.through_position then 'running' else r.state end as state,
    case when f.tail_coverage is null or r.mailbox_position<>f.through_position then r.stop_reason end as stop_reason,
    coalesce(case when r.mailbox_position=f.through_position then f.tail_coverage end,r.output_display_seq) as output_display_seq,
    least(r.output_display_native_seq,f.cut_seq) as output_display_native_seq,(r.output_seal_id is not null and r.output_sealed_at is null) as waiting_for_seal,r.created_at,false as frozen
    from flow_read_ranges(${tenant}::uuid,${session}::uuid) f
    cross join lateral(select * from runs r where r.tenant_id=${tenant}::uuid and r.session_id=f.physical and r.mailbox_position<=f.through_position and r.mailbox_position is not null
    and r.pruned_at is null and (r.state<>'queued' or r.started_at is not null or r.operation_kind='compact')
    and not(r.operation_kind='chat' and r.state='cancelled' and r.started_at is null)) r) root_turns`;

export function flowMessagePage(
  tenant: string,
  session: string,
  page: {
    direction: "before" | "after";
    position: string | undefined;
    cutoff: string | undefined;
    completedOnly: boolean;
    limit: number;
  },
) {
  const { direction, position, cutoff, completedOnly, limit } = page;
  const before = direction === "before";
  const selector = before ? sql`flow_page_turns_desc` : sql`flow_page_turns_asc`;
  const last = "9223372036854775807";
  return sql<HistoryTurnRow>`select ${tenant}::uuid as tenant_id,${session}::uuid as session_id,${session}::uuid as origin_session_id,
    p.run_id as turn_id,p.*,false as frozen
    from ${selector}(${tenant}::uuid,${session}::uuid,${before ? "1" : (position ?? "1")}::bigint,
      least(${before ? (position ?? last) : last}::bigint,${cutoff ?? last}::bigint),${completedOnly}::boolean,${limit}::int) p
    order by p.mailbox_position ${before ? sql`desc` : sql`asc`}`;
}

// Resolve the public child Session and Turn before looking at the shared
// physical stream. The parent Run supplies the original pending Tool references.
const childMessageRuns = sql`(select e.root_run_id as id,e.tenant_id,e.child_session_id as session_id,
  e.child_turn_id as turn_id,e.created_at,e.output_display_seq,p.pruned_at,p.projection_tools,
  null::text as root_human_inputs,'{}'::jsonb as root_input_receipts
  from subagent_conversations e join runs p on p.tenant_id=e.tenant_id and p.id=e.root_run_id and p.session_id=e.root_session_id)`;

export const childMessageTurns = (
  state: string,
) => sql<HistoryTurnRow>`(select e.tenant_id,e.child_session_id as session_id,e.child_session_id as origin_session_id,
  e.child_turn_id as turn_id,e.root_run_id as run_id,s.pi_session_id,1::bigint as mailbox_position,'chat'::text as operation_kind,
  case ${state}::text when 'preparing' then 'queued' when 'unknown' then 'failed' else ${state}::text end as state,
  null::text as stop_reason,e.output_display_seq,e.output_display_native_seq,false as waiting_for_seal,e.created_at,false as frozen
  from subagent_conversations e join sessions s on s.tenant_id=e.tenant_id and s.id=e.child_session_id and s.pruned_at is null
  join runs p on p.tenant_id=e.tenant_id and p.id=e.root_run_id and p.pruned_at is null) child_turns`;

const placedChildInput = sql`m.tenant_id=r.tenant_id and m.pi_session_id=s.pi_session_id::uuid
  and m.conversation_id=s.pi_conversation_id and m.source_input_id=r.id::text and m.run_id=r.id and m.role='user'`;

async function childMessageHeaders(
  db: Kysely<Database>,
  tenant: string,
  session: string,
  ids: readonly string[],
) {
  const result = await sql<{
    turn_id: string;
    content: string | null;
    coverage: string;
  }>`select r.turn_id,
    (select m.content from messages m where ${placedChildInput}) as content,r.output_display_seq as coverage
    from ${childMessageRuns} r join sessions s on s.id=r.session_id and s.tenant_id=r.tenant_id
    where r.tenant_id=${tenant}::uuid and r.session_id=${session}::uuid and r.turn_id=any(${[...ids]}::uuid[])
      and r.pruned_at is null and s.pruned_at is null`.execute(db);
  return new Map<string, HistoryHeader>(
    result.rows.map((row) => {
      const content = row.content === null ? "" : JSON.parse(row.content).model[0].content;
      const text =
        typeof content === "string"
          ? content
          : content
              .filter((b: { type: string }) => b.type === "text")
              .map((b: { text: string }) => b.text)
              .join("");
      return [
        row.turn_id,
        {
          input_text: text,
          input_source: null,
          input_uploads: [],
          scheduled_origin: null,
          output_display_seq: row.coverage,
        },
      ];
    }),
  );
}

export async function rootMessageHeaders(
  db: Kysely<Database>,
  tenant: string,
  session: string,
  ids: readonly string[],
  child = false,
  physicalFlows?: readonly string[],
) {
  if (!ids.length) return new Map<string, HistoryHeader>();
  if (child) return childMessageHeaders(db, tenant, session, ids);
  const rows = await sql<
    HistoryHeader & { turn_id: string }
  >`select ${physicalFlows ? sql`r.id` : sql`r.turn_id`} as turn_id,input_text,null::text as input_source,input_uploads,scheduled_origin,output_display_seq
    from runs r ${physicalFlows ? sql`join unnest(${[...ids]}::uuid[],${[...physicalFlows]}::uuid[]) p(id,flow) on r.id=p.id and r.session_id=p.flow` : sql``}
    where tenant_id=${tenant}::uuid ${physicalFlows ? sql`` : sql`and session_id=${session}::uuid and turn_id=any(${[...ids]}::uuid[])`} and pruned_at is null`.execute(
    db,
  );
  return new Map(rows.rows.map((r) => [r.turn_id, r]));
}

export async function rootMessagePosition(
  db: Kysely<Database>,
  tenant: string,
  turn: HistoryTurnRow,
  kind: "entry" | "task",
  id: string,
  child = false,
) {
  const lane = child
    ? sql`(select s.pi_conversation_id from sessions s join subagent_conversations e
    on e.tenant_id=s.tenant_id and e.child_session_id=s.id and e.child_turn_id=${turn.turn_id}::uuid and e.root_run_id=${turn.run_id}::uuid
    where s.tenant_id=${tenant}::uuid and s.id=${turn.origin_session_id}::uuid and s.pi_session_id=${turn.pi_session_id} and s.pruned_at is null)`
    : sql`${turn.flow_lane ?? "1"}::bigint`;
  return (
    await sql<{ seq: string; ordinal: number }>`select * from (
    select commit_seq as seq,ordinal from messages where tenant_id=${tenant}::uuid and run_id=${turn.run_id}::uuid and pi_session_id=${turn.pi_session_id} and conversation_id=${lane}
      ${turn.flow_seq != null ? sql`and commit_seq<=${turn.flow_seq}::bigint and entry_id between ${turn.flow_lo}::bigint and ${turn.flow_hi}::bigint` : sql``}
      and ${kind === "entry" ? sql`entry_id` : sql`tool_task_id`}=${id}::bigint
    ${
      kind === "task" && turn.flow_seq == null
        ? sql`union all select (v->>'seq')::bigint,(v->>'ordinal')::int from runs r cross join lateral jsonb_each(r.projection_tools) t(k,v)
      where r.id=${turn.run_id}::uuid and r.tenant_id=${tenant}::uuid and k=${id} and v->>'conversationId'=(${lane})::text`
        : sql``
    }
  ) refs order by seq desc,ordinal desc limit 1`.execute(db)
  ).rows[0];
}

export async function readRootMessageWindow(
  db: Kysely<Database>,
  input: HistoryWindowInput,
  child = false,
  flows = false,
  // Child Turns can use the Flow point reader with their own lane addressing.
  flowPoints = flows,
  placedInputHistory?: boolean,
) {
  if (!input.turns.length)
    return {
      transcripts: new Map(),
      positions: [],
      headers: new Set<string>(),
      more: false,
      bytes: 0,
      lastContinues: false,
    };
  const clipped = flows && input.turns.some((t) => t.flowRange !== undefined);
  const runSource = child ? childMessageRuns : sql`runs`,
    lane = child ? sql`s.pi_conversation_id` : clipped ? sql`t.source_lane` : sql`1`;
  const headerBytes = child
    ? sql`coalesce((select octet_length(m.content) from messages m where ${placedChildInput}),0)`
    : sql`octet_length(r.input_text)+octet_length(r.input_uploads::text)`;
  const order = sql.raw(input.direction === "before" ? "desc" : "asc"),
    compare = sql.raw(
      input.direction === "before" ? (input.inclusive ? "<=" : "<") : input.inclusive ? ">=" : ">",
    ),
    // Bind a per-Turn cursor. A disjunction on the outer Turn ID makes PG
    // start at the end of a long Run and discard every row before the cursor.
    cursor = input.position,
    seek = cursor
      ? sql`,${input.turns.map((t) => (t.turnId === cursor.turnId ? String(cursor.sequence) : input.direction === "before" ? "9223372036854775807" : "-1"))}::bigint[],
      ${input.turns.map((t) => (t.turnId === cursor.turnId ? cursor.ordinal : 0))}::int[]`
      : sql``,
    // For a Flow page the enclosing repeatable-read transaction has already
    // established these exact turn/stream pairs against the public window.
    turnId = flows ? sql`r.id` : sql`r.turn_id`,
    sessionJoin = flows
      ? sql`cross join lateral (select r.session_id::text as pi_session_id) s`
      : sql`join sessions s on s.tenant_id=r.tenant_id and s.id=r.session_id and s.pruned_at is null`,
    rangeArrays = clipped
      ? sql`,${input.turns.map((t) => t.flowRange?.lo ?? "1")}::bigint[],${input.turns.map((t) => t.flowRange?.hi ?? "9007199254740991")}::bigint[],${input.turns.map((t) => t.flowRange?.lane ?? "1")}::bigint[],${input.turns.map((t) => !!t.flowRange)}::boolean[],${input.turns.map((t) => t.flowRange?.partialCoverage ?? null)}::bigint[]`
      : sql``,
    rangeNames = clipped ? sql` ,lo,hi,source_lane,inherited,partial_coverage` : sql``,
    scope = sql`unnest(${input.turns.map((t) => t.turnId)}::uuid[],${input.turns.map((t) => t.through?.sequence ?? Number.MAX_SAFE_INTEGER)}::bigint[],${input.turns.map((t) => t.through?.ordinal ?? 2147483647)}::int[]${flows ? sql`,${input.turns.map((t) => t.piSessionId)}::uuid[]` : sql``}${rangeArrays}${seek}) with ordinality t(turn_id,through_seq,through_ordinal${flows ? sql`,physical` : sql``}${rangeNames}${cursor ? sql`,seek_seq,seek_ordinal` : sql``},turn_index)
      join ${runSource} r on r.tenant_id=${input.tenantId}::uuid and r.session_id=${flows ? sql`t.physical` : sql`${input.sessionId}::uuid`} and ${turnId}=t.turn_id and r.pruned_at is null
      ${sessionJoin}`,
    bound = (
      seq: ReturnType<typeof sql.raw>,
      ord: ReturnType<typeof sql.raw>,
    ) => sql`and (${seq},${ord}) <= (t.through_seq,t.through_ordinal)
        ${cursor ? sql`and (${seq},${ord}) ${compare} (t.seek_seq,t.seek_ordinal)` : sql``}`,
    limit = (input.maxRecords ?? HISTORY_PAGE_RECORDS) + 1;
  // A native Entry is unique within a physical stream. Locate it before testing
  // Run membership so a long Run cannot turn each dependency into a range scan.
  const assistantAt = sql`left join lateral(select a.entry_id,a.run_id,a.content from messages a
    where p.assistant_id is not null and a.tenant_id=r.tenant_id and a.pi_session_id=s.pi_session_id::uuid
      and a.conversation_id=${lane} and a.entry_id=p.assistant_id::bigint
      ${clipped ? sql`and a.entry_id between t.lo and t.hi and a.commit_seq<=t.through_seq` : sql``} limit 1) a on a.run_id=r.id`;
  const meta = await sql<
    HistoryWindowPoint & { pending: boolean; run_id: string }
  >`select p.*,r.id as run_id,${turnId} as turn_id,s.pi_session_id,${flows ? sql`${input.sessionId}::uuid` : sql`r.session_id`} as origin_session_id,t.turn_index,
      coalesce(octet_length(a.content),0) as assistant_bytes
    from ${scope} cross join lateral (
      (${
        flowPoints
          ? sql`select m.id::text as id,m.commit_seq as seq,m.ordinal,jsonb_build_object('role',m.role) as fields,
        m.body_bytes,m.assistant_entry_id::text as assistant_id,case when m.tool_task_id is not null then '"pi.tool"' end as task_kind,false as header,m.pending
        from flow_transcript_points m where ${clipped ? sql`m.id between t.lo and t.hi and (not t.inherited or not m.pending) and` : sql``} m.tenant_id=r.tenant_id and m.run_id=r.id and m.pi_session_id=s.pi_session_id::uuid and m.conversation_id=${lane}`
          : sql`select m.entry_id::text as id,m.commit_seq as seq,m.ordinal,jsonb_build_object('role',m.role) as fields,
        octet_length(m.content) as body_bytes,m.assistant_entry_id::text as assistant_id,
        case when m.tool_task_id is not null then '"pi.tool"' end as task_kind,false as header,false as pending
        from messages m where m.tenant_id=r.tenant_id and m.run_id=r.id and m.pi_session_id=s.pi_session_id::uuid and m.conversation_id=${lane} and m.entry_id is not null and m.role not in ('system','pi.system')`
      }
          ${bound(sql.raw("m.commit_seq"), sql.raw("m.ordinal"))}
        order by m.commit_seq ${order},m.ordinal ${order} limit ${limit})
      union all select k,(v->>'seq')::bigint,(v->>'ordinal')::int,'{}'::jsonb,octet_length(v::text),v->>'assistantEntryId','"pi.tool"',false,true
        from jsonb_each(r.projection_tools) q(k,v) where ${clipped ? sql`not t.inherited and` : sql``} v->>'conversationId'=(${lane})::text
        ${bound(sql.raw("(v->>'seq')::bigint"), sql.raw("(v->>'ordinal')::int"))}
      union all select '0',0,0,'{}'::jsonb,${headerBytes},null,null,true,false
        where true ${bound(sql.raw("0"), sql.raw("0"))}
    ) p ${assistantAt}
    order by t.turn_index,p.seq ${order},p.ordinal ${order} limit ${limit}`.execute(db);
  const { selected, bytes } = selectHistoryPoints(meta.rows, input);
  const picked = meta.rows.slice(0, selected.length).filter((p) => !p.header);
  type Body = NativeHistoryRow & { ordinal: number };
  const rows: Body[] = [],
    dependencies: Body[] = [],
    humans = new Map<
      string,
      Map<string, { inputId: string; text: string; uploads: UserUpload[] }>
    >();
  if (picked.length) {
    const bodies = await sql<{
      id: string;
      seq: string;
      ordinal: number;
      pi_session_id: string;
      content: string | null;
      source_input_id: string | null;
      human: string | null;
      native_input_id: string | null;
      mail: { text?: string; uploads?: UserUpload[]; initial?: boolean } | null;
      run_id: string;
      created_at: Date;
      pending: {
        arguments: Record<string, unknown>;
        commandFromProposal?: boolean;
        startedAt: string;
        reply: { toolCallId: string };
      } | null;
      assistant_id: string | null;
      proposal: string | null;
    }>`
      select p.id::text,p.seq::text,p.ordinal,s.pi_session_id,m.content,m.source_input_id,p.run_id,${placedInputHistory ? sql`m.input_display` : sql`null`} as mail,r.root_input_receipts->m.source_input_id->>'id' as native_input_id,case when starts_with(m.source_input_id,'input-') and strpos(r.root_human_inputs,to_json(m.source_input_id)::text||':')>0 then
        substring(r.root_human_inputs from strpos(r.root_human_inputs,to_json(m.source_input_id)::text||':')+length(to_json(m.source_input_id)::text)+1 for p.human_limit) end as human,coalesce(m.created_at,r.created_at) as created_at,
        case when p.pending then ${flowPoints ? sql`coalesce(r.projection_tools->p.id::text,flow_pending_tool(r.tenant_id,s.pi_session_id::uuid,r.id,${lane},p.id))` : sql`r.projection_tools->p.id::text`} end as pending,a.entry_id::text as assistant_id,a.content as proposal
      from unnest(${picked.map((p) => p.run_id)}::uuid[],${picked.map((p) => p.id)}::bigint[],${picked.map((p) => p.seq)}::bigint[],${picked.map((p) => p.ordinal)}::int[],${picked.map((p) => p.pending)}::boolean[],${picked.map((p) => p.assistant_id)}::bigint[],${picked.map((p) => (p.fields.role === "user" ? humanPresentationSliceCharacters(p.body_bytes) : 0))}::int[]) p(run_id,id,seq,ordinal,pending,assistant_id,human_limit)
      join ${runSource} r on r.id=p.run_id and r.tenant_id=${input.tenantId}::uuid ${flows ? sql`` : sql`and r.session_id=${input.sessionId}::uuid`}
      ${sessionJoin}
      ${clipped ? sql`join unnest(${input.turns.map((t) => t.turnId)}::uuid[]${rangeArrays},${input.turns.map((t) => t.through?.sequence ?? Number.MAX_SAFE_INTEGER)}::bigint[]) t(turn_id ,lo,hi,source_lane,inherited,partial_coverage,through_seq) on t.turn_id=p.run_id` : sql``}
      left join lateral(select m.run_id,m.content,m.created_at,m.source_input_id${placedInputHistory ? sql`,m.input_display` : sql``} from messages m
        where not p.pending and m.tenant_id=r.tenant_id and m.pi_session_id=s.pi_session_id::uuid
          and m.entry_id=p.id and m.conversation_id=${lane} limit 1) m on m.run_id=r.id
      ${assistantAt}`.execute(db);
    const headers = new Set(selected.filter((p) => p.header).map((p) => p.turn_id));
    for (const r of bodies.rows) {
      let pendingArgs = r.pending?.arguments;
      if (r.pending?.commandFromProposal) {
        const blocks = r.proposal && JSON.parse(r.proposal).model?.[0]?.content;
        const call = Array.isArray(blocks)
          ? blocks.find(
              (b) =>
                b.type === "toolCall" && b.name === "bash" && b.id === r.pending!.reply.toolCallId,
            )
          : undefined;
        if (typeof call?.arguments?.command !== "string")
          throw Error("Pending Bash proposal is missing");
        pendingArgs = { ...pendingArgs, command: call.arguments.command };
      }
      const entry = r.pending
        ? {
            kind: "pi.tool",
            input: { callId: r.pending.reply.toolCallId },
            state: {
              status: "running",
              checkpoint: { phase: "execute", arguments: pendingArgs },
            },
          }
        : JSON.parse(r.content!);
      const row = {
        id: r.id,
        seq: r.seq,
        ordinal: r.ordinal,
        fields: {},
        pi_session_id: r.pi_session_id,
        body: JSON.stringify({ value: entry }),
        committed_at: r.pending ? new Date(r.pending.startedAt) : r.created_at,
      };
      rows.push(row);
      if (r.assistant_id && r.proposal)
        dependencies.push({ ...row, id: r.assistant_id, body: '{"value":' + r.proposal + "}" });
      if (r.mail && r.source_input_id && !(r.mail.initial && headers.has(r.run_id))) {
        const collection = humans.get(r.pi_session_id) ?? new Map();
        collection.set(r.id, {
          inputId: r.source_input_id,
          text: r.mail.text ?? entry.model[0].content,
          uploads: parseUserUploads(r.mail.uploads ?? []),
        });
        humans.set(r.pi_session_id, collection);
      } else if (!child && r.source_input_id?.startsWith("input-")) {
        const human = r.human && (firstJsonObject(r.human) as StoredHumanInput | undefined);
        if (
          !human ||
          typeof human.text !== "string" ||
          (human.nativeId !== null && String(human.nativeId) !== r.native_input_id)
        )
          throw Error("Consumed root input metadata is missing");
        const collection = humans.get(r.pi_session_id) ?? new Map();
        collection.set(r.id, {
          inputId: r.source_input_id,
          text: human.text,
          uploads: parseUserUploads(human.uploads),
        });
        humans.set(r.pi_session_id, collection);
      }
    }
  }
  const metadata = await sql<{
    turn_id: string;
    coverage: string;
    created_at: Date;
    seq: string | null;
    type: string;
    payload: Record<string, unknown>;
    occurred_at: Date;
  }>`select ${turnId} as turn_id,${clipped ? sql`coalesce(t.partial_coverage,r.output_display_seq)` : sql`r.output_display_seq`} as coverage,r.created_at,e.seq,e.type,e.payload,e.occurred_at from ${scope}
      left join lateral(select seq,type,payload,occurred_at from session_terminal_events where ${clipped ? sql`t.partial_coverage is null and` : sql``} tenant_id=r.tenant_id and session_id=r.session_id and turn_id=${turnId} order by seq desc limit 1)e on true`.execute(
    db,
  );
  const byTurn = new Map(metadata.rows.map((r) => [r.turn_id, r]));
  return renderHistoryPoints(
    input,
    meta.rows,
    selected,
    bytes,
    rows,
    dependencies,
    humans,
    (id) => {
      const m = byTurn.get(id)!;
      return m.seq !== null
        ? terminalMetadata({ ...m, seq: m.seq })
        : {
            throughSequence: Number(m.coverage),
            terminalSequence: null,
            stopReason: null,
            failure: null,
            cancellation: null,
            occurredAt: m.created_at.toISOString(),
          };
    },
  );
}

export async function rootMessageNavigation(
  db: Kysely<Database>,
  input: {
    tenantId: string;
    sessionId: string;
    conversationId: number;
    limit: number;
    from?: number;
    through?: number;
    includeIds?: readonly number[];
    windowId?: string;
  },
) {
  const eligible = sql`m.tenant_id=${input.tenantId}::uuid and m.pi_session_id=${input.sessionId}::uuid and m.conversation_id=${input.conversationId}
    and m.navigation_caption is not null`;
  const rangeQuery = input.windowId
    ? sql`with bounds as materialized (
      select * from flow_read_ranges(${input.tenantId}::uuid,${input.windowId}::uuid)
    ), candidates as (
      select p.* from bounds f cross join lateral (
        (select m.entry_id,false as forced,m.commit_seq,m.role,m.created_at,m.run_id,m.navigation_caption
          from messages m where m.tenant_id=${input.tenantId}::uuid and m.pi_session_id=f.physical and m.conversation_id=f.source_lane
            and m.navigation_caption is not null and m.entry_id between greatest(f.entry_lo,${input.from ?? 0}::bigint) and least(f.entry_hi,${input.through ?? Number.MAX_SAFE_INTEGER}::bigint)
            and (f.cut_seq is null or m.commit_seq<=f.cut_seq) order by m.entry_id desc limit ${input.limit + 1})
        union all (select m.entry_id,true,m.commit_seq,m.role,m.created_at,m.run_id,m.navigation_caption from messages m
          where m.tenant_id=${input.tenantId}::uuid and m.pi_session_id=f.physical and m.conversation_id=f.source_lane
            and m.navigation_caption is not null and m.entry_id=any(${[...(input.includeIds ?? [])]}::bigint[])
            and m.entry_id between coalesce(f.entry_lo,1) and coalesce(f.entry_hi,9223372036854775807)
            and (f.cut_seq is null or m.commit_seq<=f.cut_seq))
      ) p
    ) select c.entry_id as id,c.commit_seq as seq,c.role,c.created_at,c.run_id as turn_id,c.forced,c.navigation_caption as preview
      from candidates c order by c.entry_id desc`
    : sql`with bounded as(
    (select entry_id,false as forced from messages m where ${eligible} and entry_id between ${input.from ?? 0}::bigint and ${input.through ?? Number.MAX_SAFE_INTEGER}::bigint order by entry_id desc limit ${input.limit + 1})
    union all (select entry_id,true from messages m where ${eligible} and entry_id=any(${[...(input.includeIds ?? [])]}::bigint[])))
    select m.entry_id as id,m.commit_seq as seq,m.role,m.created_at,r.turn_id,b.forced,
      m.navigation_caption as preview
      from bounded b join messages m on m.tenant_id=${input.tenantId}::uuid and m.pi_session_id=${input.sessionId}::uuid and m.entry_id=b.entry_id
      join runs r on r.id=m.run_id and r.tenant_id=m.tenant_id and r.pruned_at is null order by m.entry_id desc`;
  const { rows } = await sql<{
    id: string;
    seq: string;
    turn_id: string;
    role: "assistant" | "user";
    preview: string;
    created_at: Date;
    forced: boolean;
  }>`${rangeQuery}`.execute(db);
  const directional = rows.filter((r) => !r.forced),
    page = directional.slice(0, input.limit);
  const entries = [
    ...new Map(
      [...page, ...rows.filter((r) => r.forced)].map((r) => [
        r.id,
        {
          id: Number(r.id),
          seq: Number(r.seq),
          kind: "pi." + r.role,
          role: r.role,
          finalAssistant: r.role === "assistant",
          text: r.preview ?? "",
          turnId: r.turn_id,
          createdAt: r.created_at.toISOString(),
        },
      ]),
    ).values(),
  ].sort((a, b) => a.id - b.id);
  return { entries, hasMore: directional.length > input.limit, beforeId: page.at(-1)?.id ?? null };
}
