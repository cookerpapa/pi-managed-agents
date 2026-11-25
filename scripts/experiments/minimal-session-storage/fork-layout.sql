-- The candidate's immutable message store uses the common flow_segments directory.
alter table runs add column if not exists workspace_grant jsonb,add column if not exists output_display_seq bigint default 0,add column if not exists output_seal_id uuid,add column if not exists output_sealed_at timestamptz;
alter table runs drop constraint runs_state_check,
  add check(state in ('queued','running','settling','cancel_requested','completed','cancelled','failed','timed_out'));
alter table flows add column native_id_floor bigint not null default 2 check(native_id_floor>0),add column agent_base_stream uuid, add column agent_base_seq bigint, add column agent_base_ordinal int,
  add check(num_nonnulls(agent_base_stream,agent_base_seq,agent_base_ordinal) in (0,3)),
  add foreign key(tenant_id,agent_base_stream,agent_base_seq,agent_base_ordinal) references messages(tenant_id,pi_session_id,commit_seq,ordinal);
create index fork_stream_agent_base on flows(tenant_id,agent_base_stream,agent_base_seq,agent_base_ordinal) where agent_base_stream is not null;
create table sessions (
  tenant_id uuid not null,id uuid not null,flow_id uuid not null,
  family_id uuid not null, parent_session_id uuid,agent_name text not null,creation_hash text,title text not null default '',
  primary key(tenant_id,id),unique(tenant_id,flow_id),unique(tenant_id,family_id,id),
  foreign key(tenant_id,flow_id) references flows,
  foreign key(tenant_id,family_id,parent_session_id) references sessions(tenant_id,family_id,id),
  check((parent_session_id is null and family_id=id and agent_name='root') or
    (parent_session_id is not null and id<>parent_session_id and id<>family_id and agent_name<>'root'))
);
create unique index fork_child_name on sessions(tenant_id,parent_session_id,agent_name) where parent_session_id is not null;
-- Persist the request fingerprint independently of destination deletion. A retry
-- can identify a deleted destination, but must never resurrect it.
create table fork_requests (
  tenant_id uuid not null,source_id uuid not null,request_key text not null,target_id uuid not null,
  fingerprint text not null, primary key(tenant_id,source_id,request_key)
);
create table fork_gc (tenant_id uuid not null,flow_id uuid not null,after_seq bigint not null default 0,after_ordinal int not null default -1,primary key(tenant_id,flow_id),
  foreign key(tenant_id,flow_id) references flows on delete cascade);

-- The original file on a human input can be absent from the model-facing Entry.
-- A header lives with its Turn. A follow-up becomes readable at the coordinates
-- of its own canonical Entry, including when its presentation metadata arrives first.
create table flow_input_assets (
  tenant_id uuid not null,source_flow uuid not null,run_id uuid not null references runs(id) on delete cascade,
  request_key text not null,attachment_id uuid not null,
  primary key(tenant_id,run_id,request_key,attachment_id)
);
create index flow_input_asset_object on flow_input_assets(tenant_id,attachment_id);


create function fork_agent_at(t uuid,s uuid,lane bigint,through_seq bigint) returns setof messages language sql stable as $$
  with local as materialized (select * from agent_config_at(t,s,lane,through_seq))
  select * from local union all
  select m.* from flows b join messages m on m.tenant_id=b.tenant_id
    and (m.pi_session_id,m.commit_seq,m.ordinal)=(b.agent_base_stream,b.agent_base_seq,b.agent_base_ordinal)
    where b.tenant_id=t and b.flow_id=s and lane=1 and not exists(select from local)
$$;
create or replace function message_root_ranges(t uuid, public_id uuid, physical uuid, needs_base boolean)
returns table(stream uuid,lane bigint,lo bigint,hi bigint,seq bigint) language sql stable set search_path from current as $$
  with own as (select flow_id from sessions where tenant_id=t and id=public_id)
  select flow_id,1::bigint,1::bigint,9007199254740991::bigint,9007199254740991::bigint from own
  union all select g.source_flow,g.conversation_id,g.first_entry,g.last_entry,g.through_seq
    from own join flow_segments g on g.tenant_id=t and g.destination_flow=own.flow_id
$$;

-- Pin exactly one visible path in the caller's transaction. Web admission
-- requires the sealed Turn's final answer. Request scopes may end on a Tool Result.
create function pin_message_fork(t uuid,src uuid,dest uuid,target_run uuid,lane bigint,anchor bigint,kind text, request_ceiling bigint default null)
returns boolean language plpgsql as $$
declare target record; config record; ceiling bigint; last_id bigint;
begin
  if kind not in ('web','entry','request') then raise exception 'Invalid Fork policy'; end if;
  if target_run is null and kind='request' then
    select m.run_id into strict target_run from (
      select src as physical,1::bigint as lo,9223372036854775807::bigint as hi,9223372036854775807::bigint as visible
      union all select g.source_flow,g.first_entry,g.last_entry,g.through_seq from flow_segments g
        where g.tenant_id=t and g.destination_flow=src and g.conversation_id=lane
    ) path join message_entries m on m.tenant_id=t and m.pi_session_id=path.physical and m.conversation_id=lane
      and m.entry_id=anchor and m.entry_id between path.lo and path.hi and m.commit_seq<=path.visible;
  end if;
  select m.pi_session_id,m.entry_id,m.commit_seq,r.mailbox_position,r.state,r.history_head,m.role,
    g.segment_no,g.through_seq,g.last_entry,g.tail_coverage,g.through_position,r.output_display_seq into target
    from runs r join messages m on m.tenant_id=r.tenant_id and m.pi_session_id=r.session_id and m.run_id=r.id
    left join flow_segments g on g.tenant_id=t and g.destination_flow=src and g.source_flow=m.pi_session_id
      and g.conversation_id=lane and m.entry_id between g.first_entry and g.last_entry and m.commit_seq<=g.through_seq
    where r.tenant_id=t and r.id=target_run and m.entry_id=anchor and m.conversation_id=lane
      and (m.pi_session_id=src or g.segment_no is not null)
      and (kind='request' or (r.state='completed' and (r.output_seal_id is null or r.output_sealed_at is not null) and r.history_head is not null and m.role='assistant'
        and m.content::json#>>'{model,0,stopReason}' in ('stop','length')));
  if not found or (request_ceiling is not null and request_ceiling<target.commit_seq) then return false; end if;
  last_id:=target.entry_id;
  ceiling:=target.through_seq;
  if kind='web' then
    if target.tail_coverage is not null and target.mailbox_position=target.through_position then return false; end if;
    -- The sealed Turn bounds both coordinates. A newer Turn can already have
    -- published a previously reserved lower ID by the time somebody clicks Fork.
    ceiling:=least(ceiling,coalesce((target.history_head->>'seq')::bigint,
      (select m.commit_seq from messages m where m.tenant_id=t and m.run_id=target_run
        and m.pi_session_id=target.pi_session_id order by m.commit_seq desc,m.ordinal desc limit 1)));
    last_id:=(target.history_head->>'nextId')::bigint-1;
    if last_id is null or last_id<anchor then
      select max(m.entry_id) into last_id from messages m where m.tenant_id=t and m.pi_session_id=target.pi_session_id
        and m.run_id=target_run and m.conversation_id=lane and m.commit_seq<=ceiling;
    end if;
    last_id:=least(last_id,target.last_entry);
    if exists(select from messages m where m.tenant_id=t and m.run_id=target_run and m.pi_session_id=target.pi_session_id
      and m.conversation_id=lane and m.role='assistant' and m.entry_id>anchor and m.entry_id<=last_id
      and m.commit_seq<=ceiling and m.content::json#>>'{model,0,stopReason}' in ('stop','length')) then return false; end if;
  elsif kind='request' then ceiling:=least(ceiling,coalesce(request_ceiling,target.commit_seq));
  elsif ceiling is null then
    select m.commit_seq into strict ceiling from messages m where m.tenant_id=t and m.pi_session_id=target.pi_session_id
      order by m.commit_seq desc,m.ordinal desc limit 1;
  end if;
  select * into config from fork_agent_at(t,target.pi_session_id,lane,target.commit_seq);
  insert into flows(tenant_id,flow_id,native_id_floor,agent_base_stream,agent_base_seq,agent_base_ordinal)
    select t,dest,greatest(native_id_floor,last_id+1,(target.history_head->>'nextId')::bigint),config.pi_session_id,config.commit_seq,config.ordinal
    from flows where tenant_id=t and flow_id=target.pi_session_id;
  perform freeze_flow_segments(t,src,dest,target.pi_session_id,lane,last_id,ceiling,coalesce(target.mailbox_position,1),
    case when kind='request' then greatest(1,target.output_display_seq) end);
  return true;
end $$;

-- Session API authentication and public Session creation participate in the same
-- PG transaction. Bind the function to the candidate schema selected at install.
create function fork_web_window(t uuid,window_id uuid,target_run uuid,entry bigint,dest uuid,request_key_text text,request_hash text)
returns boolean language plpgsql set search_path from current as $$
declare src uuid; prior fork_requests%rowtype;
begin
  perform pg_advisory_xact_lock_shared(hashtextextended(t::text,17));
  perform pg_advisory_xact_lock(hashtextextended(t::text || window_id::text || request_key_text,23));
  select * into prior from fork_requests where tenant_id=t and source_id=window_id and request_key=request_key_text;
  if found then
    if prior.fingerprint<>request_hash or not exists(select from sessions where tenant_id=t and id=prior.target_id) then
      raise exception 'Fork request changed or its destination was deleted' using errcode='PZ005';
    end if;
    -- Let the public API look up the concurrent winner's committed receipt.
    raise exception 'Retry concurrent Fork receipt' using errcode='40001';
  end if;
  select flow_id into src from sessions where tenant_id=t and id=window_id for share;
  if not found or not pin_message_fork(t,src,dest,target_run,1,entry,'web') then return false; end if;
  insert into sessions(tenant_id,id,flow_id,family_id,agent_name) values(t,dest,dest,dest,'root');
  insert into conversation_worlds(tenant_id,pi_session_id,conversation_id,state)
    select t,dest,1,state from conversation_worlds where tenant_id=t and pi_session_id=src and conversation_id=1;
  insert into fork_requests values(t,window_id,request_key_text,dest,request_hash);
  return true;
end $$;

create function flow_read_ranges(t uuid,window_id uuid)
returns table(physical uuid,through_position bigint,prior_positions bigint,entry_lo bigint,entry_hi bigint,cut_seq bigint,source_lane bigint,tail_coverage bigint)
language sql stable set search_path from current as $$
  with own as (select flow_id from sessions where tenant_id=t and id=window_id),
  path as (select segment_no,source_flow,through_position,first_entry,last_entry,through_seq,conversation_id,tail_coverage
    from flow_segments g join own on own.flow_id=g.destination_flow where tenant_id=t
    union all select 2147483647,flow_id,coalesce((select r.mailbox_position from runs r
       where r.tenant_id=t and r.session_id=own.flow_id and r.mailbox_position is not null order by r.mailbox_position desc limit 1),0),
       null,null,null,1,null from own)
  select source_flow,through_position,coalesce(sum(through_position) over(order by segment_no rows between unbounded preceding and 1 preceding),0)::bigint,
    first_entry,last_entry,through_seq,conversation_id,tail_coverage from path where through_position>0
$$;

create view flow_session_tips as
  select s.*,coalesce(
    (select r.id from runs r where r.tenant_id=s.tenant_id and r.session_id=s.flow_id
      and r.mailbox_position is not null order by r.mailbox_position desc limit 1),
    (select r.id from flow_segments g join runs r on r.tenant_id=g.tenant_id and r.session_id=g.source_flow
      and r.mailbox_position=g.through_position where g.tenant_id=s.tenant_id and g.destination_flow=s.flow_id
      order by g.segment_no desc limit 1)
  ) as tip_run_id from sessions s;

-- Caller and directory mutation share one transaction. Stop/drain active writers
-- before deletion. Never replace a live source with another Session's target.
create function detach_message_window(t uuid, window_id uuid, dest uuid, target_run uuid, lane bigint, anchor bigint, policy text)
returns table(members uuid[],removed_turns bigint) language plpgsql set search_path from current as $$
declare own sessions%rowtype; streams uuid[]; total bigint;
begin
  perform pg_advisory_xact_lock(hashtextextended(t::text,17));
  select * into strict own from sessions where tenant_id=t and id=window_id for update;
  if dest is null then
    with recursive tree(id,flow_id) as (
      select own.id,own.flow_id union all
      select c.id,c.flow_id from sessions c join tree p on c.parent_session_id=p.id
      where c.tenant_id=t and c.family_id=own.family_id)
      select array_agg(id),array_agg(flow_id) into members,streams from tree;
  else members:=array[window_id]; streams:=array[own.flow_id]; end if;
  perform id from sessions where tenant_id=t and id=any(members) order by id for update;
  if dest is null then
    update runs set state='cancelled',settled_at=now() where tenant_id=t and session_id=any(streams)
      and state='queued' and started_at is null and lease_id is null;
  end if;
  if exists(select from runs where tenant_id=t and session_id=any(streams) and
    (state not in ('completed','failed','cancelled','timed_out') or (lease_id is not null and history_head is null) or (output_seal_id is not null and output_sealed_at is null))) then
    raise exception 'Source still running' using errcode='PZ004';
  end if;
  removed_turns:=0;
  if dest is not null then
    select coalesce(sum(through_position),0) into total from flow_read_ranges(t,window_id);
    if not pin_message_fork(t,own.flow_id,dest,target_run,lane,anchor,policy) then
      raise exception 'Invisible or unfinished prune anchor' using errcode='PZ005';
    end if;
    insert into conversation_worlds(tenant_id,pi_session_id,conversation_id,state)
      select t,dest,1,state from conversation_worlds where tenant_id=t and pi_session_id=own.flow_id and conversation_id=lane;
    update sessions set flow_id=dest where tenant_id=t and id=window_id;
    select total-coalesce(sum(through_position),0) into removed_turns from flow_read_ranges(t,window_id);
  else delete from sessions where tenant_id=t and id=any(members); end if;
  insert into fork_gc(tenant_id,flow_id) select t,unnest(streams)
    union select tenant_id,source_flow from flow_segments where tenant_id=t and destination_flow=any(streams)
    union select tenant_id,agent_base_stream from flows where tenant_id=t and flow_id=any(streams) and agent_base_stream is not null
    on conflict(tenant_id,flow_id) do update set after_seq=0,after_ordinal=-1;
  delete from flow_segments where tenant_id=t and destination_flow=any(streams);
  delete from inboxes where tenant_id=t and pi_session_id=any(streams);
  delete from conversation_worlds where tenant_id=t and pi_session_id=any(streams);
  return next;
end $$;

create function guard_new_flow_run() returns trigger language plpgsql as $$
begin
  perform 1 from sessions where tenant_id=new.tenant_id and flow_id=new.session_id for key share;
  if not found then raise exception 'No live Session owns this flow' using errcode='23503'; end if;
  return new;
end $$;


-- Only messages with retained media have index entries. All branches share the source row.
create index fork_attachment_refs on messages using gin(attachment_refs) where attachment_refs is not null;
create function visible_message_attachment(t uuid,win uuid,asset uuid) returns boolean
language sql stable set search_path from current as $$
 select exists(select from flow_read_ranges(t,win) f join messages m
   on m.tenant_id=t and m.pi_session_id=f.physical and m.conversation_id=f.source_lane
   and m.entry_id between coalesce(f.entry_lo,1) and coalesce(f.entry_hi,9223372036854775807)
   and (f.cut_seq is null or m.commit_seq<=f.cut_seq) where m.attachment_refs is not null and m.attachment_refs ? asset::text)
   or exists(select from flow_input_assets a join runs r on r.tenant_id=a.tenant_id and r.id=a.run_id
     join flow_read_ranges(t,win) f on f.physical=a.source_flow and r.mailbox_position<=f.through_position
     where a.tenant_id=t and a.attachment_id=asset and (a.request_key=r.id::text or exists(
       select from messages m where m.tenant_id=t and m.pi_session_id=f.physical and m.run_id=r.id
       and m.conversation_id=f.source_lane and m.source_input_id=a.request_key
       and m.entry_id between coalesce(f.entry_lo,1) and coalesce(f.entry_hi,9223372036854775807)
       and (f.cut_seq is null or m.commit_seq<=f.cut_seq))))
$$;

-- Inspect at most 512 message keys per transaction, including those kept by a
-- peer. A detach resets the cursor on each source whose reachability changed.
create function collect_flow_batch(t uuid) returns boolean language plpgsql set search_path from current as $$
declare work fork_gc%rowtype; last_seq bigint; last_ord int; chunk int; base uuid;
begin
  perform pg_advisory_xact_lock(hashtextextended(t::text,17));
  select * into work from fork_gc where tenant_id=t order by flow_id limit 1 for update;
  if not found then return false; end if;
  if exists(select from sessions where tenant_id=t and flow_id=work.flow_id) then
    delete from fork_gc where tenant_id=t and flow_id=work.flow_id;
    return true;
  end if;
  if not exists(select from flow_segments where tenant_id=t and source_flow=work.flow_id) then
    select agent_base_stream into base from flows where tenant_id=t and flow_id=work.flow_id;
    if base is not null then
      update flows set agent_base_stream=null,agent_base_seq=null,agent_base_ordinal=null
        where tenant_id=t and flow_id=work.flow_id;
      insert into fork_gc(tenant_id,flow_id) values(t,base)
        on conflict(tenant_id,flow_id) do update set after_seq=0,after_ordinal=-1;
    end if;
  end if;
  with page as materialized (
    select m.commit_seq,m.ordinal,m.entry_id,m.document_id,m.conversation_id
    from messages m where m.tenant_id=t and m.pi_session_id=work.flow_id
      and (m.commit_seq,m.ordinal)>(work.after_seq,work.after_ordinal) order by m.commit_seq,m.ordinal limit 512
  ), discarded as (
    delete from messages m using page p where m.tenant_id=t and m.pi_session_id=work.flow_id
      and (m.commit_seq,m.ordinal)=(p.commit_seq,p.ordinal)
      and not exists(select from flow_segments g where g.tenant_id=t and g.source_flow=work.flow_id
        and g.conversation_id=p.conversation_id and p.entry_id between g.first_entry and g.last_entry and p.commit_seq<=g.through_seq)
      and (p.document_id is null or (
        not exists(select from flows f where f.tenant_id=t and
          (f.agent_base_stream,f.agent_base_seq,f.agent_base_ordinal)=(work.flow_id,p.commit_seq,p.ordinal))
        and not exists(select from flow_segments g join message_entries e on e.tenant_id=g.tenant_id
          and e.pi_session_id=g.source_flow and e.conversation_id=g.conversation_id
          and e.entry_id between g.first_entry and g.last_entry and e.commit_seq<=g.through_seq
          where g.tenant_id=t and g.source_flow=work.flow_id and g.conversation_id=p.conversation_id
          and e.commit_seq>=p.commit_seq and e.commit_seq<coalesce((select n.commit_seq from messages n
            where n.tenant_id=t and n.pi_session_id=work.flow_id and n.conversation_id=p.conversation_id
            and n.document_id is not null and (n.commit_seq,n.ordinal)>(p.commit_seq,p.ordinal)
            order by n.commit_seq,n.ordinal limit 1),9223372036854775807)))))
  select commit_seq,ordinal into last_seq,last_ord from page order by commit_seq desc,ordinal desc limit 1;
  if found then
    update fork_gc set after_seq=last_seq,after_ordinal=last_ord where tenant_id=t and flow_id=work.flow_id;
    return true;
  end if;
  delete from flow_input_assets a where a.tenant_id=t and a.source_flow=work.flow_id and a.request_key<>a.run_id::text
    and not exists(select from messages m where m.tenant_id=t and m.pi_session_id=a.source_flow
      and m.run_id=a.run_id and m.source_input_id=a.request_key);
  delete from runs where id in (select r.id from runs r where r.tenant_id=t and r.session_id=work.flow_id
    and r.mailbox_position>coalesce((select max(g.through_position) from flow_segments g
      where g.tenant_id=t and g.source_flow=work.flow_id),0)
    and not exists(select from messages m where m.tenant_id=t and m.run_id=r.id)
    order by r.mailbox_position limit 512);
  get diagnostics chunk=row_count;
  if chunk=512 then return true; end if;
  delete from fork_gc where tenant_id=t and flow_id=work.flow_id;
  delete from flows f where f.tenant_id=t and f.flow_id=work.flow_id
    and not exists(select from flow_segments where tenant_id=t and source_flow=f.flow_id)
    and not exists(select from messages where tenant_id=t and pi_session_id=f.flow_id);
  return true;
end $$;
