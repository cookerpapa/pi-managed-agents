const messageSql = (
  guarded,
  coverage = false,
) => `insert into messages(tenant_id,pi_session_id,entry_id,conversation_id,run_id,role,content,source_input_id,commit_seq,ordinal,assistant_entry_id,tool_task_id,navigation_caption,document_id,attachment_refs${coverage ? ",display_through" : ""})
 select $1,$2,v.id,v.conversation,$3,v.role,v.content,v.input,$4,v.ordinal,v.assistant,v.task,v.caption,v.document,v.refs${coverage ? ",case when v.ordinal=$7 then $8::bigint end" : ""} from
 jsonb_to_recordset($5::jsonb) v(id bigint,conversation bigint,role text,content text,input text,ordinal int,assistant bigint,task bigint,caption text,document bigint,refs jsonb) ${guarded ? "cross join owner" : ""}
 on conflict(tenant_id,pi_session_id,commit_seq,ordinal) do update set ordinal=excluded.ordinal
 where (messages.entry_id,messages.document_id,messages.run_id,messages.conversation_id,messages.commit_seq,messages.ordinal,messages.role,messages.content,messages.source_input_id,messages.assistant_entry_id,messages.tool_task_id,messages.navigation_caption)
 is not distinct from (excluded.entry_id,excluded.document_id,excluded.run_id,excluded.conversation_id,excluded.commit_seq,excluded.ordinal,excluded.role,excluded.content,excluded.source_input_id,excluded.assistant_entry_id,excluded.tool_task_id,excluded.navigation_caption) ${coverage ? "and messages.display_through is not distinct from excluded.display_through" : ""} returning entry_id`;
const inboxSql = (
  guarded,
) => `insert into inboxes select $1,$2,$3,$4,$5,$6,$7::jsonb,$5 ${guarded ? "from owner" : ""}
 on conflict(tenant_id,pi_session_id,conversation_id) do update set commit_seq=excluded.commit_seq,ordinal=excluded.ordinal,items=excluded.items
 where inboxes.doc_id=excluded.doc_id and (inboxes.commit_seq,inboxes.ordinal)<(excluded.commit_seq,excluded.ordinal) returning doc_id`;
const insertMessages = messageSql(false),
  upsertInbox = inboxSql(false);
const ownerWhere = (run, lease) =>
  `where tenant_id=$1 and session_id=$2 and id=${run} and lease_id=${lease} and state in ('running','cancel_requested')`;
function guardedMutation(owner, mutation) {
  return `with owner as materialized (${owner}), changed as (${mutation}) select exists(select 1 from owner) as accepted, (select count(*)::int from changed) as writes`;
}
const oneMessage = guardedMutation(
  `select id from runs ${ownerWhere("$3", "$6")} for share`,
  messageSql(true),
);
const oneInbox = guardedMutation(
  `select id from runs ${ownerWhere("$8", "$9")} for share`,
  inboxSql(true),
);
// This frame already needs to advance its Run's display coverage. The admitted
// UPDATE locks the writer and supplies the input row for the immutable INSERT.
const presentedMessage = guardedMutation(
  `update runs set output_first_topic=coalesce(output_first_topic,$7),
  output_first_partition=coalesce(output_first_partition,$8),output_first_offset=coalesce(output_first_offset,$9),
  output_display_seq=greatest(output_display_seq,$10),output_display_native_seq=greatest(output_display_native_seq,$4)
  ${ownerWhere("$3", "$6")} returning id`,
  messageSql(true),
);
// Recount a compacted root inside the same transaction that retires the Run.
const settledHead = `case when $5::jsonb ? 'recount' then ($5::jsonb - 'recount') ||
  (select jsonb_build_object('count',count(*),'bytes',coalesce(sum(octet_length(m.content)),0))
   from messages m where m.tenant_id=$1 and m.pi_session_id=$2 and m.conversation_id=1
     and m.entry_id>=($5::jsonb->>'floor')::bigint and m.entry_id<($5::jsonb->>'nextId')::bigint
     and m.commit_seq<=($5::jsonb->>'seq')::bigint)
  else $5::jsonb end`;
export const finishStatement = (
  head,
  releaseRecovery = false,
) => `update runs set state='completed',settled_at=clock_timestamp()${head ? ",history_head=" + settledHead : ""}${releaseRecovery ? ",native_work_handoff=null,projection_state=null,projection_partition=null,projection_first=null,projection_pin=null,cancel_positions='{}'::jsonb" : ""}
     where tenant_id=$1 and session_id=$2 and id=$3 and lease_id=$4 and state='running'${releaseRecovery ? " and projection_tools='{}'::jsonb" : ""} returning id`;

export const projectionTemplates = {
  message: oneMessage,
  presentedMessage,
  inbox: oneInbox,
  finish: finishStatement(true),
  finishAndRelease: finishStatement(true, true),
  plainMessage: insertMessages,
  plainInbox: upsertInbox,
};
export { oneMessage, oneInbox, insertMessages, upsertInbox };

export const entryWithCoverage = guardedMutation(
  `select id from runs ${ownerWhere("$3", "$6")} for share`,
  messageSql(true, true),
);

// Replace a separate writer lock with the recovery-coordinate UPDATE already
// required in this frame. Its returned Run is the message admission proof.
export const entryWithCheckpoint = guardedMutation(
  `update runs set projection_first=$10+1,projection_partition=$9,projection_pin=null,projection_state=$11::jsonb ${ownerWhere("$3", "$6")} returning id`,
  messageSql(true, true),
);
