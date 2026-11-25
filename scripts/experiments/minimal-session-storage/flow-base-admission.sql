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

-- Visit one edge per inherited flow. Queue positions restart at one on each
-- branch, so add the lengths of earlier retained prefixes for presentation.
create function flow_read_ranges(t uuid, window_id uuid)
returns table(physical uuid, through_position bigint, prior_positions bigint, entry_lo bigint, entry_hi bigint, cut_seq bigint, source_lane bigint, tail_coverage bigint) language sql stable as $$
 with recursive ranges(physical, through_position, depth) as (
   select r.RUN_FLOW,r.mailbox_position,0 from flow_session_tips s
   join runs r on r.tenant_id=t and r.id=s.tip_run_id where s.tenant_id=t and s.id=window_id
   union all
   select parent.RUN_FLOW,parent.mailbox_position,p.depth+1 from ranges p
   join runs first on first.tenant_id=t and first.RUN_FLOW=p.physical and first.mailbox_position=1
   join runs parent on parent.tenant_id=t and parent.id=first.prev_run_id
 ) select physical,through_position,
 coalesce(sum(through_position) over(order by depth desc rows between unbounded preceding and 1 preceding),0)::bigint, null::bigint,null::bigint,null::bigint,1::bigint,null::bigint
 from ranges
$$;
