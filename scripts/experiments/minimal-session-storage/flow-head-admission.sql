create view flow_session_tips as
 select s.*, s.head_run_id as tip_run_id from sessions s;

create function flow_admit(t uuid,s uuid,expected_flow uuid,new_run uuid,prompt text) returns uuid language plpgsql as $$
declare own sessions%rowtype; prior runs%rowtype; existing runs%rowtype;
begin
 select * into strict own from sessions where tenant_id=t and id=s for update;
 if own.flow_id<>expected_flow then raise exception 'Flow changed'; end if;
 select * into existing from runs where tenant_id=t and id=new_run;
 if found then
   if existing.RUN_FLOW<>own.flow_id or existing.initial_input<>prompt then raise exception 'Admission identity changed';end if;
   return existing.id;
 end if;
 select * into prior from runs where tenant_id=t and id=own.head_run_id;
 insert into runs(id,tenant_id,RUN_FLOW,prev_run_id,mailbox_position,idempotency_key,initial_input,model_ref,state,environment_snapshot)
 values(new_run,t,own.flow_id,own.head_run_id,case when prior.RUN_FLOW=own.flow_id then prior.mailbox_position+1 else 1 end,new_run,prompt,'fixture','queued',jsonb_build_object('workspaceId',own.workspace_id,'cwd',own.working_directory));
 update sessions set head_run_id=new_run where id=s;
 return new_run;
end $$;
