-- Install with drained old writers in one transaction. The Turn is both the
-- durable queue entry and the execution anchor. Keep Session ownership stable.
alter table turns add column ready_at timestamptz;
create index turns_ready_fifo on turns(ready_at,id) where state='queued' and ready_at is not null;

create function turn_can_head(t uuid,f uuid,pos bigint) returns boolean language sql stable set search_path from current as $$
  select not exists(select from turns p where p.tenant_id=t and p.session_id=f and p.mailbox_position<pos
    and (p.state not in ('completed','failed','timed_out','cancelled')
      or (p.output_seal_id is not null and p.output_sealed_at is null)))
$$;
create function set_turn_ready() returns trigger language plpgsql set search_path from current as $$
begin
  new.ready_at:=null;
  if new.state='queued' and new.lease_id is null and new.output_seal_id is null
    and turn_can_head(new.tenant_id,new.session_id,new.mailbox_position) then
    new.ready_at:=new.created_at;
  end if;
  return new;
end $$;
create trigger turn_ready_before before insert or update of state on turns for each row execute function set_turn_ready();

create function promote_ready_turn() returns trigger language plpgsql set search_path from current as $$
begin
  perform id from sessions where tenant_id=new.tenant_id and flow_id=new.session_id for no key update;
  update turns r set ready_at=r.created_at where r.id=(
    select id from turns where tenant_id=new.tenant_id and session_id=new.session_id and state='queued'
    order by mailbox_position limit 1)
    and r.ready_at is null and r.lease_id is null and r.output_seal_id is null
    and turn_can_head(r.tenant_id,r.session_id,r.mailbox_position);
  return null;
end $$;
create constraint trigger turn_promote_successor after update of state,output_sealed_at on turns
  deferrable initially deferred for each row when(new.state in ('completed','failed','timed_out','cancelled')
    and (new.output_seal_id is null or new.output_sealed_at is not null)
    and (old.state is distinct from new.state or old.output_sealed_at is distinct from new.output_sealed_at))
  execute function promote_ready_turn();

create function signal_ready_turn() returns trigger language plpgsql set search_path from current as $$
declare target uuid; src uuid; version text; payload text;
begin
  select s.id into target from sessions s join turns r on r.tenant_id=s.tenant_id and r.session_id=s.flow_id
    where r.id=new.id and r.ready_at is not null and r.state='queued';
  if target is null then return null; end if;
  if new.started_at is null then
    src:=new.session_id;
    if new.mailbox_position=1 then
      select source_flow into src from flow_segments where tenant_id=new.tenant_id and destination_flow=new.session_id
        order by segment_no desc limit 1;
      select history_head->>'version' into version from turns where tenant_id=new.tenant_id and session_id=src
        and state='completed' and history_head is not null order by mailbox_position desc limit 1;
    else
      select history_head->>'version' into version from turns where tenant_id=new.tenant_id and session_id=src
        and mailbox_position=new.mailbox_position-1 and state='completed';
    end if;
  end if;
  payload:=case when version is null then target::text else json_build_array(new.tenant_id,target,new.id,version,new.session_id,src)::text end;
  perform pg_notify('pma_segment_ready_'||md5(tg_table_schema),payload);
  return null;
end $$;
create constraint trigger turn_signal_insert after insert on turns deferrable initially deferred for each row
  when(new.ready_at is not null) execute function signal_ready_turn();
create constraint trigger turn_signal_update after update on turns deferrable initially deferred for each row
  when(new.ready_at is not null and old.ready_at is distinct from new.ready_at) execute function signal_ready_turn();
update turns r set ready_at=r.created_at from sessions s where r.tenant_id=s.tenant_id and r.session_id=s.flow_id
  and r.state='queued' and r.lease_id is null and r.output_seal_id is null and turn_can_head(r.tenant_id,r.session_id,r.mailbox_position);
create or replace function finish_session_mail() returns trigger language plpgsql set search_path from current as $$
declare new_successor boolean := false; next_turn uuid; first_input user_or_agent_messages%rowtype;
begin
  perform id from sessions where tenant_id=new.tenant_id and flow_id=new.session_id for no key update;
  if new.state='cancelled' or old.state='cancel_requested' then
    select id into next_turn from turns where tenant_id=new.tenant_id and session_id=new.session_id
      and mailbox_position>new.mailbox_position and state='queued' order by mailbox_position limit 1;
    update user_or_agent_messages set state='withdrawn',withdrawing=false,return_requested=true
      where turn_id=new.id and source='user' and state in ('pending','sending');
    update user_or_agent_messages set turn_id=next_turn,state=case when next_turn is null then 'held' else 'pending' end,
      starts_turn=false,boundary_task=null,steer_notice=null,steer_seen_epoch=null
      where turn_id=new.id and source='agent' and state in ('pending','sending');
    return null;
  end if;
  update user_or_agent_messages set state='withdrawn',withdrawing=false where turn_id=new.id and withdrawing and state in ('pending','sending');
  select * into first_input from user_or_agent_messages where turn_id=new.id and state in ('pending','sending') order by position limit 1;
  if found then
    select id into next_turn from turns where tenant_id=new.tenant_id and session_id=new.session_id and state='queued' order by mailbox_position limit 1;
    if next_turn is null then
      new_successor:=true; next_turn:=gen_random_uuid();
      insert into turns(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,input_uploads,state)
        select next_turn,new.tenant_id,new.session_id,max(mailbox_position)+1,gen_random_uuid(),first_input.body,first_input.uploads,'queued'
        from turns where tenant_id=new.tenant_id and session_id=new.session_id;
    end if;
    update user_or_agent_messages set turn_id=next_turn,state='pending',boundary_task=null,starts_turn=(new_successor and id=first_input.id),steer_notice=null,steer_seen_epoch=null where turn_id=new.id and state in ('pending','sending');
  end if;
  return null;
end $$;

create trigger finish_agent_session_inputs after update of state on turns for each row
 when(old.state is distinct from new.state and new.state in ('completed','cancelled','failed','timed_out')) execute function finish_session_mail();
