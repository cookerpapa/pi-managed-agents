-- RUN_FLOW is the physical flow column in the matched Projector's runs relation.
alter table runs add column prev_run_id uuid;
alter table runs add constraint flow_run_identity unique(tenant_id,id);
alter table runs add constraint flow_predecessor foreign key(tenant_id,prev_run_id) references runs(tenant_id,id);
create index flow_successors on runs(tenant_id,prev_run_id) where prev_run_id is not null;
-- The destructive-operation guard must not scan a long, fully settled flow.
create index flow_unsettled on runs(tenant_id,RUN_FLOW) where ((mailbox_position is not null and (state not in('completed','cancelled','failed','timed_out') or (lease_id is not null and history_head is null))) or (mailbox_position is null and state in('running','preparing')));
create table if not exists sessions(id uuid primary key,tenant_id uuid not null,unique(tenant_id,id));
alter table sessions add column flow_id uuid not null,add column SESSION_ANCHOR uuid,
 add column title text not null default 'Conversation',add column workspace_id uuid not null default gen_random_uuid(),
 add column working_directory text not null default '/workspace',
 add unique(tenant_id,flow_id),add foreign key(tenant_id,SESSION_ANCHOR) references runs(tenant_id,id);
create index flow_anchors on sessions(tenant_id,SESSION_ANCHOR) where SESSION_ANCHOR is not null;
-- A scratch equivalent of a durable cleanup job. Only Sessions and Runs own history.
create table flow_gc_outbox(id uuid primary key default gen_random_uuid(),tenant_id uuid not null,tip uuid not null);

FLOW_MODE_FUNCTIONS

-- Every writable flow has one linear local prefix. Crossing into an inherited
-- prefix takes a single lookup of the first local Run's predecessor.
create function flow_visible(t uuid,tip uuid,target uuid) returns boolean language plpgsql stable as $$
declare wanted runs%rowtype; cursor_flow uuid; cursor_position bigint;
begin
 select * into wanted from runs where tenant_id=t and id=target;
 if not found or wanted.mailbox_position is null or wanted.state<>'completed' or wanted.history_head is null then return false; end if;
 while tip is not null loop
   select RUN_FLOW,mailbox_position into strict cursor_flow,cursor_position from runs where tenant_id=t and id=tip;
   if cursor_flow=wanted.RUN_FLOW then return wanted.mailbox_position<=cursor_position; end if;
   select prev_run_id into tip from runs where tenant_id=t and RUN_FLOW=cursor_flow and mailbox_position is not null order by mailbox_position limit 1;
 end loop;
 return false;
end $$;

create function flow_fork(t uuid,s uuid,target uuid,destination uuid,new_flow uuid) returns void language plpgsql as $$
declare own sessions%rowtype;
begin
 select * into strict own from sessions where tenant_id=t and id=s for share;
 -- A retained flow always has its first root, pinned through predecessor FKs.
 if exists(select from runs where tenant_id=t and RUN_FLOW=new_flow and mailbox_position=1) then raise exception 'Flow already contains history';end if;
 if not flow_visible(t,(select v.tip_run_id from flow_session_tips v where v.tenant_id=t and v.id=s),target) then raise exception 'Invisible or unfinished Fork anchor';end if;
 insert into sessions(id,tenant_id,flow_id,SESSION_ANCHOR,title,workspace_id,working_directory)
 select destination,t,new_flow,target,title,workspace_id,working_directory from sessions where id=s and tenant_id=t;
end $$;

create function flow_detach(t uuid,s uuid,expected_flow uuid,target uuid) returns uuid language plpgsql as $$
declare own sessions%rowtype; job uuid; old_tip uuid;
begin
 select * into strict own from sessions where tenant_id=t and id=s for update;
 if own.flow_id<>expected_flow then raise exception 'Flow changed';end if;
 if exists(select from runs where tenant_id=t and RUN_FLOW=own.flow_id and ((mailbox_position is not null and (state not in('completed','cancelled','failed','timed_out') or (lease_id is not null and history_head is null))) or (mailbox_position is null and state in('running','preparing')))) then
   raise exception 'Flow still has work';
 end if;
 select v.tip_run_id into strict old_tip from flow_session_tips v where v.tenant_id=t and v.id=s;
 if target is null then delete from sessions where id=s;
 else
   if not flow_visible(t,old_tip,target) then raise exception 'Invisible or unfinished prune anchor';end if;
   update sessions set flow_id=gen_random_uuid(),SESSION_ANCHOR=target where id=s;
 end if;
 if old_tip is not null then insert into flow_gc_outbox(tenant_id,tip) values(t,old_tip) returning id into job;end if;
 return job;
end $$;

create function flow_collect(job uuid,batch_size int) returns int language plpgsql as $$
declare t uuid; victim uuid; parent uuid; victim_flow uuid; removed int:=0;
begin
 if batch_size<1 then raise exception 'Empty GC batch';end if;
 select tenant_id,tip into t,victim from flow_gc_outbox where id=job for update;
 if not found then return 0;end if;
 while victim is not null and removed<batch_size loop
   select prev_run_id,RUN_FLOW into parent,victim_flow from runs where tenant_id=t and id=victim for update;
   if not found LIVE_FLOW_GUARD
     or exists(select from sessions where tenant_id=t and SESSION_ANCHOR=victim)
     or exists(select from runs where tenant_id=t and prev_run_id=victim) then victim:=null;exit;end if;
   delete from messages where tenant_id=t and run_id=victim;
   delete from runs where tenant_id=t and (id=victim GC_FAMILY);
   removed:=removed+1; victim:=parent;
 end loop;
 if victim is null then delete from flow_gc_outbox where id=job;
 else update flow_gc_outbox set tip=victim where id=job;end if;
 return removed;
end $$;

-- The queue editor may move two unstarted inputs. Their provisional edges
-- follow dispatch order; a claimed Run's history predecessor is immutable.
create function flow_swap_pending(t uuid,s uuid,l uuid,r uuid) returns void language plpgsql as $$
declare own sessions%rowtype; left_run runs%rowtype; right_run runs%rowtype; next_id uuid; spare bigint;
begin
 select * into strict own from sessions where tenant_id=t and id=s for update;
 select * into strict left_run from runs where tenant_id=t and RUN_FLOW=own.flow_id and id=l for update;
 select * into strict right_run from runs where tenant_id=t and RUN_FLOW=own.flow_id and id=r for update;
 if left_run.state<>'queued' or right_run.state<>'queued' or left_run.started_at is not null or right_run.started_at is not null
    or right_run.prev_run_id is distinct from l or left_run.mailbox_position>=right_run.mailbox_position then raise exception 'Queue changed';end if;
 select id into next_id from runs where tenant_id=t and RUN_FLOW=own.flow_id and prev_run_id=r for update;
 if next_id is not null and exists(select from runs where id=next_id and started_at is not null) then raise exception 'Successor already started';end if;
 select max(mailbox_position)+1 into spare from runs where tenant_id=t and RUN_FLOW=own.flow_id;
 update runs set mailbox_position=spare where id=l;
 update runs set mailbox_position=left_run.mailbox_position,prev_run_id=left_run.prev_run_id where id=r;
 update runs set mailbox_position=right_run.mailbox_position,prev_run_id=r where id=l;
 update runs set prev_run_id=l where id=next_id;
 ON_QUEUE_TIP_SWAP
end $$;
