const messageSql = (
  guarded,
) => `insert into messages(tenant_id,pi_session_id,entry_id,conversation_id,run_id,role,content,source_input_id,commit_seq,ordinal)
 select $1,$2,v.id,v.conversation,$3,v.role,v.content,v.input,$4,v.ordinal from
 jsonb_to_recordset($5::jsonb) v(id bigint,conversation bigint,role text,content text,input text,ordinal int) ${guarded ? "cross join owner" : ""}
 on conflict(tenant_id,pi_session_id,entry_id) do update set entry_id=excluded.entry_id
 where (messages.run_id,messages.conversation_id,messages.commit_seq,messages.ordinal,messages.role,messages.content,messages.source_input_id)
 is not distinct from (excluded.run_id,excluded.conversation_id,excluded.commit_seq,excluded.ordinal,excluded.role,excluded.content,excluded.source_input_id) returning entry_id`;
const inboxSql = (
  guarded,
) => `insert into inboxes select $1,$2,$3,$4,$5,$6,$7::jsonb,$5 ${guarded ? "from owner" : ""}
 on conflict(tenant_id,pi_session_id,conversation_id) do update set commit_seq=excluded.commit_seq,ordinal=excluded.ordinal,items=excluded.items
 where inboxes.doc_id=excluded.doc_id and (inboxes.commit_seq,inboxes.ordinal)<(excluded.commit_seq,excluded.ordinal) returning doc_id`;
const insertMessages = messageSql(false),
  upsertInbox = inboxSql(false);
function singleSql(mutation, run, lease) {
  return `with owner as materialized (select id from runs where tenant_id=$1 and session_id=$2 and id=${run} and lease_id=${lease} and state in ('running','cancel_requested') for share), changed as (${mutation}) select exists(select 1 from owner) as accepted, (select count(*)::int from changed) as writes`;
}
const oneMessage = singleSql(messageSql(true), "$3", "$6"),
  oneInbox = singleSql(inboxSql(true), "$8", "$9");
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
) => `update runs set state='completed',settled_at=clock_timestamp()${head ? ",history_head=" + settledHead : ""}${releaseRecovery ? ",projection_state=null,projection_partition=null,projection_first=null,projection_pin=null" : ""}
     where tenant_id=$1 and session_id=$2 and id=$3 and lease_id=$4 and state='running'${releaseRecovery ? " and projection_tools='{}'::jsonb" : ""} returning id`;

export const projectionTemplates = {
  message: oneMessage,
  inbox: oneInbox,
  finish: finishStatement(true),
  finishAndRelease: finishStatement(true, true),
  plainMessage: insertMessages,
  plainInbox: upsertInbox,
};
export { oneMessage, oneInbox, insertMessages, upsertInbox };
