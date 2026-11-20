const messageSql = (
  guarded,
) => `insert into messages(tenant_id,pi_session_id,entry_id,conversation_id,run_id,role,content,source_input_id,commit_seq,ordinal)
 select $1,$2,v.id,v.conversation,$3,v.role,v.content,v.input,$4,v.ordinal from
 jsonb_to_recordset($5::jsonb) v(id bigint,conversation bigint,role text,content text,input uuid,ordinal int) ${guarded ? "cross join owner" : ""}
 on conflict(tenant_id,pi_session_id,entry_id) do update set entry_id=excluded.entry_id
 where (messages.run_id,messages.commit_seq,messages.ordinal,messages.role,messages.content,messages.source_input_id)
 is not distinct from (excluded.run_id,excluded.commit_seq,excluded.ordinal,excluded.role,excluded.content,excluded.source_input_id) returning entry_id`;
const inboxSql = (
  guarded,
) => `insert into inboxes select $1,$2,1,$3,$4,$5,$6::jsonb ${guarded ? "from owner" : ""}
 on conflict(tenant_id,pi_session_id,conversation_id) do update set commit_seq=excluded.commit_seq,ordinal=excluded.ordinal,items=excluded.items
 where inboxes.doc_id=excluded.doc_id and (inboxes.commit_seq,inboxes.ordinal)<(excluded.commit_seq,excluded.ordinal) returning doc_id`;
const insertMessages = messageSql(false),
  upsertInbox = inboxSql(false);
function singleSql(mutation, run, lease) {
  return `with owner as materialized (select id from runs where tenant_id=$1 and session_id=$2 and id=${run} and lease_id=${lease} and state='running' for share), changed as (${mutation}) select exists(select 1 from owner) as accepted, (select count(*)::int from changed) as writes`;
}
const oneMessage = singleSql(messageSql(true), "$3", "$6"),
  oneInbox = singleSql(inboxSql(true), "$7", "$8");
export const finishStatement = (
  head,
  releaseRecovery = false,
) => `update runs set state='completed',settled_at=clock_timestamp()${head ? ",history_head=$5::jsonb" : ""}${releaseRecovery ? ",projection_state=null,projection_partition=null,projection_first=null,projection_pin=null" : ""}
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
