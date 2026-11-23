create view flow_session_tips as
 select s.*, coalesce((
   select r.id from runs r
   where r.tenant_id=s.tenant_id and r.RUN_FLOW=s.flow_id and r.mailbox_position is not null
   order by r.mailbox_position desc limit 1
 ),s.base_run_id) as tip_run_id from sessions s;

create function flow_admit(t uuid,s uuid,expected_flow uuid,new_run uuid,prompt text) returns uuid language plpgsql as $$
declare own sessions%rowtype; existing runs%rowtype; prior_id uuid; prior_pos bigint;
begin
 select * into strict own from sessions where tenant_id=t and id=s for update;
 if own.flow_id<>expected_flow then raise exception 'Flow changed'; end if;
 select * into existing from runs where tenant_id=t and id=new_run;
 if found then
   if existing.RUN_FLOW<>own.flow_id or existing.initial_input<>prompt then raise exception 'Admission identity changed'; end if;
   return existing.id;
 end if;

 -- Separate statement after acquiring the row lock so a waiting admission sees its predecessor.
 select id,mailbox_position into prior_id,prior_pos from runs
 where tenant_id=t and RUN_FLOW=own.flow_id and mailbox_position is not null
 order by mailbox_position desc limit 1;
 insert into runs(id,tenant_id,RUN_FLOW,prev_run_id,mailbox_position,idempotency_key,initial_input,model_ref,state,environment_snapshot)
 values(new_run,t,own.flow_id,coalesce(prior_id,own.base_run_id),coalesce(prior_pos,0)+1,
        new_run,prompt,'fixture','queued',jsonb_build_object('workspaceId',own.workspace_id,'cwd',own.working_directory));
 return new_run;
end $$;
