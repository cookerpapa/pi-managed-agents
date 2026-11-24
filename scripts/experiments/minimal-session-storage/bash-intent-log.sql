-- A native ToolTask uses this coordinate in the same commit as any sibling
-- writes. Entry/document IDs remain reserved for the corresponding Pi objects.
alter table messages
  add column display_through bigint,
  add column bash_presentation_bytes int,
  add column bash_operation uuid,
  add column bash_dispatch_offset bigint,
  add column bash_span_start bigint,
  add column bash_reply_topic text,
  add column bash_reply_floor bigint,
  drop constraint message_record_shape,
  add constraint message_record_shape check (
    (entry_id is not null and document_id is null and role <> 'pma.bash-intent')
    or (entry_id is null and document_id is not null and role = 'pi.agent'
        and source_input_id is null and navigation_caption is null
        and assistant_entry_id is null and tool_task_id is null)
    or (entry_id is null and document_id is null and role = 'pma.bash-intent'
        and bash_presentation_bytes is not null and bash_operation is not null
        and bash_dispatch_offset is not null and bash_dispatch_offset >= 0
        and bash_reply_topic is not null and bash_reply_floor is not null and bash_reply_floor >= 0
        and tool_task_id is not null and assistant_entry_id is not null
        and source_input_id is null and navigation_caption is null));

create view pending_bash_intents as
 select i.* from messages i where i.role='pma.bash-intent' and not exists (
   select from messages o where o.run_id=i.run_id and o.tool_task_id=i.tool_task_id and o.role='toolResult'
     and o.tenant_id=i.tenant_id and o.pi_session_id=i.pi_session_id and o.conversation_id=i.conversation_id
     and o.assistant_entry_id=i.assistant_entry_id and (o.commit_seq,o.ordinal)>(i.commit_seq,i.ordinal));
create function bash_wait_refs(t uuid,s uuid,r uuid,task bigint default null, settled_through bigint default 0, lane bigint default null) returns jsonb language sql stable as $$
 select coalesce(jsonb_object_agg(i.tool_task_id::text, jsonb_build_object(
   'taskId',i.tool_task_id,'assistantEntryId',i.assistant_entry_id,'seq',i.commit_seq,'ordinal',i.ordinal,'conversationId',i.conversation_id,
    'operationId',i.bash_operation,'offset',i.bash_dispatch_offset::text,'startedAt',to_char(i.created_at at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
   'arguments',v->'args','commandFromProposal',not ((v->'args')?'command'),
   'reply',jsonb_build_object('toolName','bash','toolCallId',v->>'callId','partition',0,'topic',i.bash_reply_topic,'offset',i.bash_reply_floor::text)
 ) || case when i.bash_span_start is not null then jsonb_build_object('allocationSpanStart',i.bash_span_start::text) else '{}'::jsonb end),'{}'::jsonb)
 from pending_bash_intents i cross join lateral(select i.content::jsonb as v) j
 where i.tenant_id=t and i.pi_session_id=s and i.run_id=r and i.commit_seq>settled_through and (task is null or i.tool_task_id=task) and (lane is null or i.conversation_id=lane)
$$;
-- Cold presentation and the Worker recovery matcher use different views of
-- the same records. The sealed Worker does not load reply routing coordinates.
create function project_bash_log_op(topic text,part int,op jsonb,at_offset text)
 returns table(admitted boolean,affected int,blocked boolean) language plpgsql as $$
declare a jsonb:=op->'args'; ref jsonb:=a->5; t uuid:=(a->>0)::uuid; s uuid:=(a->>1)::uuid; r uuid:=(a->>2)::uuid;
 writer uuid:=(a->>3)::uuid; task bigint; lane bigint; assistant bigint; current_state text; args jsonb; pos bigint; original_op uuid; original_span bigint;
begin
 select state into current_state from runs where tenant_id=t and session_id=s and id=r and lease_id=writer and state in ('running','cancel_requested') for share;
 if not found then return query select false,0,false;return;end if;
 if op->>'kind'='log-cancel' then
   task:=(a->>5)::bigint;
   select coalesce(x.projection_tools->task::text,(
     select jsonb_build_object('operationId',i.bash_operation,'conversationId',i.conversation_id,'offset',i.bash_dispatch_offset::text)
     from messages i where i.run_id=r and i.tenant_id=t and i.pi_session_id=s and i.tool_task_id=task and i.role='pma.bash-intent'
       and not exists(select from messages o where o.run_id=i.run_id and o.tenant_id=t and o.pi_session_id=s and o.conversation_id=i.conversation_id
         and o.tool_task_id=task and o.role='toolResult' and o.assistant_entry_id=i.assistant_entry_id
         and (o.commit_seq,o.ordinal)>(i.commit_seq,i.ordinal) and o.commit_seq<(op->>'nativeSeq')::bigint)
   )) into ref from runs x where x.id=r;
   update runs set cancel_positions=case when ref->>'operationId'=a->>4 then jsonb_set(cancel_positions,array[a->>4],
     jsonb_build_object('taskId',task,'operationId',a->4,'conversationId',ref->'conversationId','offset',ref->'offset')) else cancel_positions end
     where id=r and (not cancel_positions?(a->>4) or cancel_positions->(a->>4)->>'taskId'=a->>5);
   return query select found,1,false;return;
 end if;
 task:=(a->>4)::bigint;lane:=(ref->>'conversationId')::bigint;assistant:=(ref->>'assistantEntryId')::bigint;
 if op->>'kind'='bash-intent' then
   args:=ref->'arguments';
   if ref->>'offset' is distinct from at_offset
      or not exists(select from messages m where m.tenant_id=t and m.pi_session_id=s
                     and m.run_id=r and m.conversation_id=lane and m.entry_id=assistant and m.role='assistant')
      or jsonb_typeof(args->'command') is distinct from 'string' or (args-'command'-'timeout')<>'{}'::jsonb
     then raise exception 'Unmatched Bash proposal or unknown arguments' using errcode='PZ002';end if;
   if op->'sameCommand'='true'::jsonb then args:=args-'command';end if;
   insert into messages(tenant_id,pi_session_id,run_id,conversation_id,role,tool_task_id,assistant_entry_id,commit_seq,ordinal,content,display_through,
     created_at,bash_presentation_bytes,bash_operation,bash_dispatch_offset,bash_span_start,bash_reply_topic,bash_reply_floor)
   values(t,s,r,lane,'pma.bash-intent',task,assistant,(ref->>'seq')::bigint,(ref->>'ordinal')::int,jsonb_build_object('callId',ref#>'{reply,toolCallId}','args',args)::text,
     (op->>'through')::bigint,(ref->>'startedAt')::timestamptz,octet_length(ref::text),(ref->>'operationId')::uuid,at_offset::bigint,(ref->>'allocationSpanStart')::bigint,
     ref#>>'{reply,topic}',(ref#>>'{reply,offset}')::bigint);
   return query select true,1,current_state='cancel_requested';return;
 end if;
 select i.bash_operation,i.bash_dispatch_offset,i.bash_span_start into strict original_op,pos,original_span from messages i join messages o on o.run_id=i.run_id and o.tool_task_id=i.tool_task_id
   and o.tenant_id=i.tenant_id and o.pi_session_id=i.pi_session_id and o.conversation_id=i.conversation_id
   and o.assistant_entry_id=i.assistant_entry_id and (o.commit_seq,o.ordinal)>(i.commit_seq,i.ordinal)
   where i.tenant_id=t and i.pi_session_id=s and i.run_id=r and i.conversation_id=lane and i.tool_task_id=task and i.assistant_entry_id=assistant
     and i.role='pma.bash-intent' and o.role='toolResult';
 if original_op is distinct from (ref->>'operationId')::uuid then raise exception 'Bash settlement changed operation' using errcode='PZ002';end if;
 if op->'confirmUse'='true'::jsonb then
   insert into conversation_worlds as w(tenant_id,pi_session_id,conversation_id,seen_offset,state)
     select t,s,lane,pos,jsonb_build_object('sandbox',jsonb_build_object('status','active','continuityId',span.allocation_id::text))
     from workspace_route_spans span where span.tenant_id=t and span.pi_session_id=s::text and span.run_id=r and span.conversation_id=lane
       and span.topic=project_bash_log_op.topic and span.partition=part and span.first_offset=original_span and span.allocation_id is not null
     on conflict(tenant_id,pi_session_id,conversation_id) do update set state=excluded.state,seen_offset=excluded.seen_offset
       where (w.seen_offset is null or w.seen_offset<=excluded.seen_offset) and w.state is distinct from excluded.state;
 end if;
 if a->6 is null then
   update runs set cancel_positions=cancel_positions-original_op::text where id=r and cancel_positions?original_op::text;
 end if;
 return query select true,1,false;
end $$;
