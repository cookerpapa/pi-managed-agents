-- Extension of the isolated minimal schema. Only root Runs enter the claim queue.
create table sessions (id uuid primary key, tenant_id uuid not null, unique(tenant_id,id));
alter table runs
  add foreign key(tenant_id,session_id) references sessions(tenant_id,id),
  alter mailbox_position drop not null,
  drop constraint runs_state_check,
  add root_run_id uuid references runs(id),
  add caller_run_id uuid references runs(id),
  add agent_id uuid references runs(id),
  add previous_run_id uuid references runs(id),
  add source_task_id bigint,
  add context_mode text,
  add context_at bigint,
  add native_lane bigint,
  add native_created_seq bigint,
  add native_owner bigint,
  add native_answer_id bigint,
  add native_outcome_seq bigint not null default 0,
  add constraint root_or_child check (
    (root_run_id is null and caller_run_id is null and agent_id is null and mailbox_position is not null
     and source_task_id is null and previous_run_id is null and native_lane is null and context_mode is null
     and state in ('queued','running','cancel_requested','completed','failed','cancelled'))
    or (root_run_id is not null and caller_run_id is not null and agent_id is not null and root_run_id<>id
     and mailbox_position is null and lease_id is null and source_task_id is not null and source_task_id>0
     and context_mode is not null and context_mode in ('fresh','branch','continue')
     and (context_mode='fresh')=(context_at is null) and (context_at is null or context_at>0)
     and (context_mode='continue')=(previous_run_id is not null) and (previous_run_id is null)=(agent_id=id)
     and (native_lane is null or native_lane>1)
     and state in ('preparing','running','completed','failed','cancelled','unknown')));
drop index runs_one_active;
create unique index runs_one_active on runs(tenant_id,session_id)
  where mailbox_position is not null and state in ('running','cancel_requested');
create unique index delegated_live_agent on runs(agent_id) where agent_id is not null and state in ('preparing','running');
create unique index delegated_successor on runs(previous_run_id) where previous_run_id is not null;
create unique index delegated_lane on runs(tenant_id,session_id,native_lane) where native_lane is not null;
create index delegated_root on runs(root_run_id,id) where root_run_id is not null;

create function check_delegated_closure() returns trigger language plpgsql as $$
begin
  if new.state='completed' and exists(select 1 from runs where root_run_id=new.id and state in ('preparing','running')) then
    raise exception 'Root still owns open child work';
  elsif new.state in ('failed','cancelled') then
    update runs set state=case when new.state='cancelled' then 'cancelled' else 'unknown' end,
      settled_at=new.settled_at where root_run_id=new.id and state in ('preparing','running');
  end if;
  return new;
end $$;
create trigger delegated_closure before update of state on runs for each row
  when (old.root_run_id is null and old.state is distinct from new.state and new.state in ('completed','failed','cancelled'))
  execute function check_delegated_closure();

-- Called inside project_ready, after the shared root writer has been admitted.
create function project_delegated_outcome(scope jsonb, change jsonb, seq bigint, at_time timestamptz)
returns boolean language plpgsql as $$
begin
  update runs r set
    native_lane=coalesce(r.native_lane,(change->>'lane')::bigint),
    native_created_seq=case when change?'lane' then coalesce(r.native_created_seq,seq) else r.native_created_seq end,
    native_owner=coalesce(r.native_owner,(change->>'owner')::bigint),
    native_answer_id=coalesce(r.native_answer_id,(change->>'answer')::bigint),
    native_outcome_seq=seq,
    state=case when change?'activity' then change->>'activity'
      when r.state in ('preparing','unknown') and (change?'stopped' or change->>'caller'='aborted') then 'cancelled'
      when r.state='preparing' and change->>'caller'='closed' then 'unknown' else r.state end,
    settled_at=case when change->>'activity'='running' then null
      when change->>'activity' in ('completed','failed','cancelled') or
        (r.state in ('preparing','unknown') and (change?'stopped' or change?'caller')) then at_time else r.settled_at end
    where r.id=(change->>'id')::uuid and r.root_run_id=(scope->>2)::uuid
      and r.tenant_id=(scope->>0)::uuid and r.session_id=(scope->>1)::uuid and r.native_outcome_seq<seq
      and (not(change?'lane') or r.native_lane is null or r.native_lane=(change->>'lane')::bigint)
      and (not(change?'answer') or r.native_answer_id is null or r.native_answer_id=(change->>'answer')::bigint);
  return found;
end $$;
