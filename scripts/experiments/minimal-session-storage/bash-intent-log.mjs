import { createHash } from "node:crypto";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { entryWithCoverage, entryWithCheckpoint } from "./projection-statements.mjs";

export async function installBashIntentLog(db) {
  await db.query(await readFile(new URL("./bash-intent-log.sql", import.meta.url), "utf8"));
  await db.query(`create function log_covered_entry(uuid,uuid,uuid,bigint,text,uuid,int,bigint) returns table(accepted boolean,writes int)
    language plpgsql volatile as $body$ begin return query ${entryWithCoverage}; end $body$`);
  await db.query(`create function log_checkpoint_entry(uuid,uuid,uuid,bigint,text,uuid,int,bigint,int,bigint,jsonb) returns table(accepted boolean,writes int)
    language plpgsql volatile as $body$ begin return query ${entryWithCheckpoint}; end $body$`);
}

const indexes = [
  [
    "bash_intent_task",
    "unique index bash_intent_task on messages(run_id,tool_task_id) where role='pma.bash-intent'",
  ],
  [
    "bash_result_task",
    "index bash_result_task on messages(run_id,tool_task_id) where role='toolResult' and tool_task_id is not null",
  ],
  [
    "message_display_cover",
    "index message_display_cover on messages(run_id,display_through desc,commit_seq desc) where display_through is not null",
  ],
];
export async function setBashLogIndexes(db, mode) {
  for (const [name, definition] of indexes) {
    if (mode && (mode === "coverage" || mode === "folded" || name !== "message_display_cover"))
      await db.query(
        "create " + definition.replace("index " + name, "index if not exists " + name),
      );
    else await db.query("drop index if exists " + name);
  }
}
// Only for the isolated public read pool. Its authenticated Session reader and
// flow-page functions keep their normal repeatable-read snapshot and cursor SQL.
export async function setBashLogReader(db, mode, bounded = true) {
  const { rows: columns } =
    await db.query(`select attname,quote_ident(attname) col from pg_attribute
    where attrelid='flow_study.runs'::regclass and attnum>0 and not attisdropped order by attnum`);
  const select = columns.map(({ attname, col }) => {
    if (mode && !bounded && attname === "projection_tools")
      return `case when r.state='completed' and r.output_sealed_at is not null and r.history_head is not null then r.projection_tools
      else r.projection_tools || bash_wait_refs(r.tenant_id,r.session_id,r.id,null,case when r.projection_state->>'writer'=r.lease_id::text then coalesce((r.projection_state->>'seq')::bigint,0) else 0 end) end as projection_tools`;
    if ((mode === "coverage" || mode === "folded") && attname === "output_display_seq")
      return "greatest(r.output_display_seq,cover.display_through) as output_display_seq";
    if ((mode === "coverage" || mode === "folded") && attname === "output_display_native_seq")
      return "greatest(r.output_display_native_seq,(select m.commit_seq from messages m where m.run_id=r.id and m.tenant_id=r.tenant_id and m.pi_session_id=r.session_id and m.display_through is not null order by m.commit_seq desc limit 1)) as output_display_native_seq";
    return "r." + col;
  });
  if (mode && bounded) {
    // One ordered scan over entries and unresolved intents. Apply the page's
    // seek and LIMIT before asking for any pending Tool body.
    await db.query(`create or replace view flow_web.flow_transcript_points as
      select m.tenant_id,m.pi_session_id,m.run_id,m.conversation_id,coalesce(m.entry_id,m.tool_task_id) as id,m.commit_seq,m.ordinal,m.role,
        case when m.entry_id is null then m.bash_presentation_bytes else octet_length(m.content) end as body_bytes,
        m.assistant_entry_id,m.tool_task_id,m.entry_id is null as pending from messages m
      where (m.entry_id is not null and m.role not in ('system','pi.system')) or (m.role='pma.bash-intent' and not exists(
        select from messages o where o.role='toolResult' and o.run_id=m.run_id and o.tool_task_id=m.tool_task_id
        and o.tenant_id=m.tenant_id and o.pi_session_id=m.pi_session_id and o.conversation_id=m.conversation_id and o.assistant_entry_id=m.assistant_entry_id
        and (o.commit_seq,o.ordinal)>(m.commit_seq,m.ordinal)))`);
    await db.query(`create or replace function flow_web.flow_pending_tool(uuid,uuid,uuid,bigint,bigint) returns jsonb language sql stable as $$
      select bash_wait_refs($1,$2,$3,$5,0,$4)->($5::text) $$`);
  } else {
    await db.query(
      "create or replace view flow_web.flow_transcript_points as select * from flow_study.flow_transcript_points",
    );
    await db.query(
      "create or replace function flow_web.flow_pending_tool(uuid,uuid,uuid,bigint,bigint) returns jsonb language sql stable as $$ select null::jsonb $$",
    );
  }
  await db.query(`create or replace view flow_web.runs as select ${select.join(",")} from flow_study.runs r
    ${mode === "coverage" || mode === "folded" ? `left join lateral (select m.display_through,m.commit_seq from messages m where m.run_id=r.id and m.tenant_id=r.tenant_id and m.pi_session_id=r.session_id and m.display_through is not null order by m.display_through desc,m.commit_seq desc limit 1) cover on true` : ""}`);
}

// A bounded, optional compression hint. The verified args are stored in full
// on a cache miss, including after a cold replay. No native JSON is parsed by PG.
const fingerprint = (text) => createHash("sha256").update(text, "utf16le").digest("hex");
export function foldBashProposals(view, f, frame) {
  for (const w of f.commit.writes) {
    if (w.type !== "entry" || w.value.kind !== "pi.assistant") continue;
    const blocks = w.value.model?.[0]?.content;
    const commands = new Map();
    if (Array.isArray(blocks))
      for (const b of blocks) {
        if (commands.size >= 32) break;
        if (b.type === "toolCall" && b.name === "bash" && typeof b.arguments?.command === "string")
          commands.set(b.id, fingerprint(b.arguments.command));
      }
    view.bashProposal = commands.size
      ? { entry: w.value.id, lane: w.value.conversationId, commands }
      : undefined;
  }
  for (const op of frame.ops) {
    if (op.kind !== "tool") continue;
    const ref = op.args[5],
      proposal = view.bashProposal;
    if (
      ref.reply.toolName === "bash" &&
      proposal?.entry === ref.assistantEntryId &&
      proposal?.lane === ref.conversationId &&
      proposal.commands.get(ref.reply.toolCallId) === fingerprint(ref.arguments.command)
    )
      op.sameCommand = true;
  }
}
// Translate already-admitted operations after grouping and async policy selection.
export function bashLogFrames(frames, mode) {
  return frames.map((frame) => {
    const display = frame.rootDisplay;
    let covered = false,
      folded = false;
    const ops = frame.ops.map((op) => {
      const a = op.args;
      const matches =
        mode !== "intents" &&
        !covered &&
        !frame.anchor &&
        display &&
        display.tenant === a[0] &&
        display.session === a[1] &&
        display.run === a[2];
      if ((op.kind === "tool" || op.kind === "tool-settled") && a[5].reply.toolName === "bash") {
        const intent = op.kind === "tool";
        const attach =
          intent &&
          matches &&
          display.writer === a[3] &&
          display.seq === a[5].seq &&
          a[5].conversationId === 1;
        if (attach) covered = true;
        return {
          ...op,
          kind: intent ? "bash-intent" : "bash-settlement",
          ...(attach ? { through: display.through } : {}),
        };
      }
      if (op.kind === "message" && matches && display.writer === a[5] && display.seq === a[3]) {
        const entry = JSON.parse(a[4]).findLast((m) => m.id != null && m.conversation === 1);
        if (entry) {
          covered = true;
          folded =
            mode === "folded" && frame.snapshot?.run === a[2] && frame.snapshot.writer === a[5];
          return {
            ...op,
            kind: "logged-entry",
            displayOrdinal: entry.ordinal,
            through: display.through,
            ...(folded ? { snapshot: frame.snapshot.state } : {}),
          };
        }
      }
      if (op.kind === "cancel") return { ...op, kind: "log-cancel" };
      if (op.kind === "finish") return { ...op, kind: "log-finish" };
      return op;
    });
    if (!covered) return { ...frame, ops };
    const { rootDisplay, ...rest } = frame;
    assert(rootDisplay);
    if (folded) delete rest.snapshot;
    return { ...rest, ops };
  });
}
