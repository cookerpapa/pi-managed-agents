-- The installer holds its installation lock and has drained old writers.
-- All these changes and the installation version advance commit together.
set local search_path=pma_segments,public;
set local synchronous_commit=on;
lock table public.runs,turns,sessions,public.session_leases,public.supervisor_connections in share row exclusive mode;
do $$ begin
 if exists(select from public.session_leases where released_at is null)
   or exists(select from public.supervisor_connections where state='active' and expires_at>clock_timestamp()) then
   raise exception 'Drain Session writers before changing Turn references';
 end if;
end $$;

-- The legacy relations still have shared readers and dependent views. A validated
-- empty constraint also rejects any concurrent/accidental legacy admission.
alter table public.runs add constraint segmented_turn_authority check(false);
alter table public.run_transitions add check(false),drop constraint run_transitions_tenant_id_run_id_fkey;
alter table public.subagent_conversations add check(false),drop constraint subagent_conversations_tenant_id_root_run_id_fkey;
alter table public.turn_control_requests add check(false),drop constraint turn_control_requests_target_run_id_fkey,
 drop constraint turn_control_requests_tenant_id_session_id_turn_id_fkey;
alter table public.session_terminal_events add check(false),drop constraint session_terminal_events_run_fk;

alter table turns add unique(tenant_id,id);
alter table public.execution_target_bindings drop constraint execution_target_bindings_run_id_fkey,
 add foreign key(tenant_id,run_id) references turns(tenant_id,id);
alter table public.run_skill_catalogs drop constraint run_skill_catalogs_tenant_id_run_id_fkey,
 add foreign key(tenant_id,run_id) references turns(tenant_id,id) on delete cascade;
alter table public.environment_validations drop constraint environment_validations_run_fk,
 add foreign key(tenant_id,run_id) references turns(tenant_id,id) on delete cascade;
alter table public.source_control_issue_jobs drop constraint source_control_issue_jobs_run_id_fkey,
 add foreign key(tenant_id,run_id) references turns(tenant_id,id) on delete cascade;
alter table public.session_schedules drop constraint session_schedules_last_run_id_fkey,
 add foreign key(tenant_id,last_run_id) references turns(tenant_id,id) on delete set null (last_run_id);
alter table pma_segments.session_terminal_events add foreign key(tenant_id,run_id) references turns(tenant_id,id) on delete cascade;

-- Only upload_id was used by the platform's ticket expiry and deletion checks.
-- The retired in-flight native mutation pin writer cannot address this shape.
do $$ begin
 if exists(select from public.session_upload_pins) then raise exception 'Unsettled legacy upload pins'; end if;
end $$;
drop table public.session_upload_pins;
create table public.session_upload_pins(
 tenant_id uuid not null, session_id uuid not null, input_id uuid not null,
 upload_id uuid not null references public.session_uploads(id) deferrable initially deferred,
 primary key(tenant_id,session_id,input_id,upload_id),
 foreign key(tenant_id,session_id,input_id) references user_or_agent_messages(tenant_id,session_id,id) on delete cascade
);
create index session_upload_pins_upload on public.session_upload_pins(upload_id);

drop trigger pin_agent_session_input on user_or_agent_messages;
drop trigger fork_header_uploads on turns;
drop trigger fork_followup_uploads on turns;
create function pin_mail_uploads() returns trigger language plpgsql set search_path from current as $$
declare n int;
begin
 insert into public.session_upload_pins(tenant_id,session_id,input_id,upload_id)
 select new.tenant_id,new.session_id,new.id,u.id
 from jsonb_array_elements(new.uploads) file join public.session_uploads u on u.id=(file->>'id')::uuid and u.tenant_id=new.tenant_id
   and u.session_id=new.session_id and u.state='ready'
 order by u.id for update of u;
 get diagnostics n=row_count;
 if n<>jsonb_array_length(new.uploads) then raise exception 'Unadmitted input attachment'; end if;
 return null;
end $$;
create trigger pin_mail_uploads after insert on user_or_agent_messages for each row
 when(jsonb_array_length(new.uploads)>0) execute function pin_mail_uploads();

create function release_mail_uploads() returns trigger language plpgsql set search_path from current as $$
begin
 if new.state='placed' then
   perform pin_input_assets(new.tenant_id,(select session_id from turns where id=new.turn_id),new.turn_id,new.id::text,new.uploads::json);
   update public.session_uploads u set accepted_at=coalesce(u.accepted_at,clock_timestamp())
   from public.session_upload_pins p where (p.tenant_id,p.session_id,p.input_id)=(new.tenant_id,new.session_id,new.id) and u.id=p.upload_id;
 else
   update public.session_uploads u set expires_at=greatest(u.expires_at,clock_timestamp()+interval '24 hours')
   from public.session_upload_pins p where (p.tenant_id,p.session_id,p.input_id)=(new.tenant_id,new.session_id,new.id) and u.id=p.upload_id;
 end if;
 delete from public.session_upload_pins where (tenant_id,session_id,input_id)=(new.tenant_id,new.session_id,new.id);
 return null;
end $$;
create trigger release_mail_uploads after update of state,return_ack on user_or_agent_messages for each row
 when(jsonb_array_length(new.uploads)>0 and (new.state='placed' or (new.state='withdrawn' and (not new.return_requested or new.return_ack)))
      and (new.state,new.return_ack) is distinct from (old.state,old.return_ack)) execute function release_mail_uploads();
-- v2's public input endpoint accepted text only. Refuse a private fixture with
-- outstanding attachments rather than guessing whether its originals exist.
do $$ begin
 if exists(select from user_or_agent_messages where uploads<>'[]'::jsonb and
   (state in('sending','pending','held') or (state='withdrawn' and return_requested and not return_ack))) then
  raise exception 'Resolve pre-cutover input attachments before installing mailbox references';
 end if;
end $$;

insert into public.run_skill_catalogs(tenant_id,run_id,versions)
 select tenant_id,id,'[]'::jsonb from turns on conflict(run_id) do nothing;
create function capture_turn_skills() returns trigger language plpgsql set search_path from current as $$
declare owner public.sessions%rowtype;
begin
 select p.* into owner from sessions w join public.sessions p on p.tenant_id=w.tenant_id and p.id=w.id
 where w.tenant_id=new.tenant_id and w.flow_id=new.session_id;
 insert into public.run_skill_catalogs(tenant_id,run_id,versions)
 select new.tenant_id,new.id,coalesce(jsonb_agg(chosen.version_id order by chosen.package_id) filter(where chosen.enabled),'[]'::jsonb)
 from (select distinct on(i.package_id) i.package_id,i.version_id,i.enabled from public.skill_installations i
   where owner.id is not null and i.tenant_id=new.tenant_id and ((i.scope_kind='personal' and i.scope_id=owner.created_by_user_id)
    or (i.scope_kind='tenant' and i.scope_id=new.tenant_id) or (i.scope_kind='project' and i.scope_id=owner.project_id))
   order by i.package_id,case i.scope_kind when 'project' then 0 when 'personal' then 1 else 2 end) chosen;
 return null;
end $$;
create trigger capture_turn_skills after insert on turns for each row execute function capture_turn_skills();

alter table turns add column execution_target_snapshot jsonb not null default '[]'::jsonb;
create function freeze_turn_targets() returns trigger language plpgsql set search_path from current as $$
begin
 if tg_op='UPDATE' then
   if new.execution_target_snapshot is distinct from old.execution_target_snapshot then
     raise exception 'Admitted target grants are immutable';
   end if;
 else
   select coalesce(jsonb_agg(jsonb_build_object('targetId',g.id,'revision',g.revision::text,'kind',g.kind,
     'name',g.name,'capabilities',g.capabilities) order by g.id),'[]'::jsonb) into new.execution_target_snapshot
   from (select e.* from sessions s join public.execution_targets e on e.tenant_id=s.tenant_id and e.bound_session_id=s.id
     where s.tenant_id=new.tenant_id and s.flow_id=new.session_id and s.parent_session_id is null and e.enabled
     order by e.id for share of e) g;
 end if;
 return new;
end $$;
create trigger freeze_turn_targets before insert or update of execution_target_snapshot on turns
 for each row execute function freeze_turn_targets();

create view pma_session_reads.turn_tool_contexts as
 select t.id,t.tenant_id,s.id as session_id,s.parent_session_id,t.execution_target_snapshot,
 coalesce(t.workspace_grant->'tools','[]'::jsonb) as tools
 from turns t join sessions s on s.tenant_id=t.tenant_id and s.flow_id=t.session_id;
create function guard_turn_target_binding() returns trigger language plpgsql set search_path from current as $$
begin
 if not exists(select from pma_session_reads.turn_tool_contexts c join turns t on t.id=c.id
   join public.session_leases l on l.tenant_id=c.tenant_id and l.pi_session_id=c.session_id::text and l.lease_id=t.lease_id
   where c.id=new.run_id and c.tenant_id=new.tenant_id and c.session_id=new.session_id and l.pi_session_id=new.pi_session_id
   and t.lease_id=new.writer_id and t.fencing_token=new.fencing_token and l.fencing_token=t.fencing_token
   and l.released_at is null and l.writer_failed_at is null and l.writer_sealed_at is null and l.valid_until>clock_timestamp()
   and t.state='running' and t.output_seal_id is null and new.conversation_id=1
   and exists(select from jsonb_array_elements(c.execution_target_snapshot) g
     where (g->>'targetId')::uuid=new.target_id and (g->>'revision')::bigint=new.target_revision)
   for share of t,l) then raise exception 'Execution binding does not match admitted Turn'; end if;
 return new;
end $$;
create trigger guard_turn_target_binding before insert on public.execution_target_bindings
 for each row execute function guard_turn_target_binding();

create function close_segment_target_bindings() returns trigger language plpgsql set search_path from current as $$
begin
 update public.execution_target_bindings set closed=true where tenant_id=new.tenant_id and
   pi_session_id=new.pi_session_id and writer_id=new.lease_id and not closed;
 return null;
end $$;
create trigger close_segment_target_bindings after update of writer_failed_at,writer_sealed_at,released_at on public.session_leases
 for each row when (old.writer_failed_at is null and old.writer_sealed_at is null and old.released_at is null
 and (new.writer_failed_at is not null or new.writer_sealed_at is not null or new.released_at is not null))
 execute function close_segment_target_bindings();
