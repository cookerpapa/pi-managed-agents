-- Flow page reader port. Both layouts expose positions in the same physical
-- order. The ordinary layout obtains pending task bodies from its Run row.
create view flow_transcript_points as
 select m.tenant_id,m.pi_session_id,m.run_id,m.conversation_id,m.entry_id as id,m.commit_seq,m.ordinal,m.role,
   octet_length(m.content) as body_bytes,m.assistant_entry_id,m.tool_task_id,false as pending
 from messages m where m.entry_id is not null and m.role not in ('system','pi.system');
create function flow_pending_tool(uuid,uuid,uuid,bigint,bigint) returns jsonb language sql stable as $$ select null::jsonb $$;
