// Each SQL helper executes the original parameterized mutation. The ordered
// loop and partition checkpoint execute in one synchronous autocommit call.
import { projectionTemplatesFor } from "./projection-statements.mjs";

export async function installPrefixSql(db, templates) {
  await db.query(`create table accepted_fact_projection_offsets(topic text,partition int,next_offset bigint not null,primary key(topic,partition));
    alter table runs add column projection_first bigint,add column projection_partition int,add column projection_state jsonb,add column native_work_handoff jsonb,add column root_compacts jsonb,add column root_input_receipts jsonb,add column root_human_inputs text,add column root_input_seq bigint not null default 0,
      add column cancel_positions jsonb not null default '{}',add column projection_tools jsonb not null default '{}',add column projection_pin bigint;
    create index active_projection_start on runs(projection_partition,projection_first) where state in ('running','cancel_requested') and projection_first is not null;`);
  await installProjectionFunctions(db, templates, "runs");
}

export function installPhysicalTurnProjection(db) {
  return installProjectionFunctions(db, projectionTemplatesFor(true), "turns");
}

async function installProjectionFunctions(
  db,
  { message, presentedMessage, inbox, finishAndRelease },
  relation,
) {
  const replaceUsage = `on conflict(tenant_id,pi_session_id,conversation_id) do update
    set commit_seq=excluded.commit_seq,ordinal=excluded.ordinal,value=excluded.value
    where u.doc_id=excluded.doc_id and (u.commit_seq,u.ordinal)<(excluded.commit_seq,excluded.ordinal)`;
  const usageValues = (
    row,
  ) => `(${row}->>'tenant')::uuid,(${row}->>'session')::uuid,(${row}->>'conversation')::bigint,
    (${row}->>'document')::bigint,(${row}->>'seq')::bigint,(${row}->>'ordinal')::int,${row}->'value'`;
  const waitAssignment = `projection_tools=case when op->>'kind'='tool'
                then jsonb_set(projection_tools,array[a->>4],a->5) else projection_tools-(a->>4) end,
                cancel_positions=case when op->>'kind'='tool-settled' and a->6 is null then cancel_positions-(a#>>'{5,operationId}') else cancel_positions end`;
  const displayAssignment = `output_first_topic=coalesce(output_first_topic,p_topic),
    output_first_partition=coalesce(output_first_partition,p_partition),output_first_offset=coalesce(output_first_offset,(frame->>'offset')::bigint),
    output_display_seq=greatest(output_display_seq,(d->>'through')::bigint),output_display_native_seq=greatest(output_display_native_seq,(d->>'seq')::bigint)`;
  await db.query(`create or replace function prefix_message(uuid,uuid,uuid,bigint,text,uuid) returns table(accepted boolean,writes int)
      language sql volatile as $fn$ ${message} $fn$;
    create or replace function prefix_presented_message(uuid,uuid,uuid,bigint,text,uuid,text,int,bigint,bigint) returns table(accepted boolean,writes int)
      language plpgsql volatile as $fn$ begin return query ${presentedMessage}; end $fn$;
    create or replace function prefix_inbox(uuid,uuid,bigint,bigint,bigint,int,text,uuid,uuid) returns table(accepted boolean,writes int)
      language sql volatile as $fn$ ${inbox} $fn$;
    create or replace function prefix_finish(uuid,uuid,uuid,uuid,text) returns setof uuid language sql volatile as $fn$ ${finishAndRelease} $fn$;
    create or replace function project_ready(p_topic text,p_partition int,p_expected bigint,p_next bigint,p_frames jsonb,p_relaxed boolean default false,p_allow_finish boolean default false,p_bulk_usage boolean default false)
    returns text language plpgsql as $body$
    declare saved bigint; frame jsonb; op jsonb; a jsonb; ok boolean; n int; applied int; owner_state text; suppress_dispatch boolean := false; suppress_this boolean; snap jsonb; pending_usage jsonb[]; display_written boolean; combine_wait_display boolean; d jsonb;
    begin
      select next_offset into strict saved from accepted_fact_projection_offsets
       where topic=p_topic and partition=p_partition for update;
      if p_next<=saved then return 'covered'; end if;
      if saved<>p_expected then raise exception 'Partition owner or prefix changed' using errcode='40001'; end if;
      for frame in select value from jsonb_array_elements(p_frames) loop
        if p_relaxed and (coalesce((frame->>'strict')::boolean,false) or
          (jsonb_array_length(frame->'ops')>0 and (frame->>'history')::boolean is distinct from true)) then
          raise exception 'Control boundary in asynchronous transaction'; end if;
        if frame?'sessionMail' then
          perform id from sessions where tenant_id=(frame#>>'{sessionMail,0}')::uuid and flow_id=(frame#>>'{sessionMail,1}')::uuid for no key update;
          if not found then raise exception 'Missing admitted input Session'; end if;
        end if;
        applied:=0; display_written:=false; d:=frame->'rootDisplay';
        combine_wait_display:=d is not null and (frame->'ops' @> '[{"kind":"tool"}]' or frame->'ops' @> '[{"kind":"tool-settled"}]');
        for op in select value from jsonb_array_elements(frame->'ops') loop
          a:=op->'args';
          if p_relaxed and ((op->>'kind' not in ('message','tool','logged-entry','bash-intent','inbox','pin','world') and not (op->>'kind' in ('finish','log-finish') and p_allow_finish)) or
            (op->>'kind'='inbox' and (a->>6)::jsonb <> '[]'::jsonb)) then raise exception 'Control operation in asynchronous transaction'; end if;
          case op->>'kind'
          when 'child-outcomes','delegated-outcomes' then
            perform 1 from ${relation} where tenant_id=(a->>0)::uuid and session_id=(a->>1)::uuid and id=(a->>2)::uuid
              and lease_id=(a->>3)::uuid and state in ('running','cancel_requested') for share;
            ok:=found; n:=1;
            if ok then
              for snap in select value from jsonb_array_elements(op->'changes') loop
                if op->>'kind'='delegated-outcomes' then
                  if not project_delegated_outcome(a,snap,(op->>'seq')::bigint,(op->>'at')::timestamptz)
                    and (snap?'activity' or snap?'stopped' or snap?'lane') then
                    raise exception 'Missing delegated Run or conflicting outcome' using errcode='PZ002';
                  end if;
                  continue;
                end if;
                update subagent_conversations e set native_activity=case when snap?'activity' then snap->>'activity' else e.native_activity end,
                  native_stop_seen=e.native_stop_seen or (snap?'stopped'),
                  native_caller=case when e.native_caller='aborted' then 'aborted' else coalesce(snap->>'caller',e.native_caller) end,
                  native_answer_id=coalesce((snap->>'answer')::bigint,e.native_answer_id),
                  native_outcome_seq=(op->>'seq')::bigint,native_outcome_at=(op->>'at')::timestamptz
                where e.tenant_id=(a->>0)::uuid and e.root_run_id=(a->>2)::uuid and e.id=(snap->>'id')::uuid
                  and e.native_outcome_seq<(op->>'seq')::bigint
                  and (not (snap?'answer') or e.native_answer_id is null or e.native_answer_id=(snap->>'answer')::bigint);
                if not found and (snap?'activity' or snap?'stopped') then raise exception 'Missing child outcome target or changed order' using errcode='PZ002'; end if;
              end loop;
            end if;
          when 'world' then
            perform 1 from ${relation} where tenant_id=(a->>0)::uuid and session_id=(a->>1)::uuid
              and id=(a->>2)::uuid and lease_id=(a->>3)::uuid and state in ('running','cancel_requested') for share;
            ok:=found; n:=1;
            if ok then
              insert into conversation_worlds as w(tenant_id,pi_session_id,conversation_id,seen_offset,state)
              values ((a->>0)::uuid,(a->>1)::uuid,(a->>4)::bigint,(frame->>'offset')::bigint,op->'state')
              on conflict(tenant_id,pi_session_id,conversation_id) do update set state=excluded.state,seen_offset=excluded.seen_offset
                where (w.seen_offset is null or w.seen_offset<=excluded.seen_offset) and w.state is distinct from excluded.state;
            end if;
          when 'root-compact' then
            update ${relation} set root_compacts=jsonb_build_object('requests',coalesce(root_compacts->'requests','{}'::jsonb)||(op->'requests'),
              'tasks',coalesce(root_compacts->'tasks','{}'::jsonb)||(op->'tasks'),'submissions',coalesce(root_compacts->'submissions','{}'::jsonb)||(op->'submissions'),
              'latest',coalesce(op->>'latest',root_compacts->>'latest'),'seq',frame->'nativeSeq')
              where id=(a->>2)::uuid and tenant_id=(a->>0)::uuid and session_id=(a->>1)::uuid and lease_id=(a->>3)::uuid and state in ('running','cancel_requested');
            ok:=found;n:=1;
          when 'session-input' then
            perform segment_input_receipts((a->>0)::uuid,(a->>1)::uuid,op->'receipts');
            ok:=true; n:=1;
          when 'root-input' then
            update ${relation} set root_input_receipts=coalesce(root_input_receipts,'{}'::jsonb)||(op->'receipts'),
              root_human_inputs=coalesce(op->>'document',root_human_inputs),root_input_seq=(frame->>'nativeSeq')::bigint
              where id=(a->>2)::uuid and tenant_id=(a->>0)::uuid and session_id=(a->>1)::uuid and lease_id=(a->>3)::uuid and state in ('running','cancel_requested');
            ok:=found;n:=1;
          when 'logged-entry' then
            if op?'snapshot' then
              select accepted,writes into strict ok,n from log_checkpoint_entry((a->>0)::uuid,(a->>1)::uuid,(a->>2)::uuid,(a->>3)::bigint,a->>4,(a->>5)::uuid,(op->>'displayOrdinal')::int,(op->>'through')::bigint,p_partition,(frame->>'offset')::bigint,op->'snapshot');
            else
            select accepted,writes into strict ok,n from log_covered_entry((a->>0)::uuid,(a->>1)::uuid,(a->>2)::uuid,(a->>3)::bigint,a->>4,(a->>5)::uuid,(op->>'displayOrdinal')::int,(op->>'through')::bigint);
            end if;
          when 'bash-intent','bash-settlement','log-cancel' then
            select admitted,affected,blocked into strict ok,n,suppress_this from project_bash_log_op(p_topic,p_partition,op,frame->>'offset');
            if suppress_this then
              if frame?'intentPrefix' and jsonb_array_length(p_frames)>1 then raise exception 'Isolate cancelling tool owner' using errcode='PZ001'; end if;
              suppress_dispatch:=true;
            end if;
          when 'log-finish' then
            if exists(select from pending_bash_intents i join ${relation} r on r.id=i.run_id and r.lease_id=(a->>3)::uuid and r.state='running' where i.tenant_id=(a->>0)::uuid and i.pi_session_id=(a->>1)::uuid and i.run_id=(a->>2)::uuid)
              then raise exception 'Unsettled Bash at Run completion' using errcode='PZ002'; end if;
            select count(*)::int into n from prefix_finish((a->>0)::uuid,(a->>1)::uuid,(a->>2)::uuid,(a->>3)::uuid,a->>4);
            ok:=n=1;
          when 'message' then
            if d is not null and not display_written and not combine_wait_display and d->>'tenant'=a->>0 and d->>'session'=a->>1
              and d->>'run'=a->>2 and d->>'writer'=a->>5 and (d->>'seq')::bigint=(a->>3)::bigint then
              select accepted,writes into strict ok,n from prefix_presented_message(
                (a->>0)::uuid,(a->>1)::uuid,(a->>2)::uuid,(a->>3)::bigint,a->>4,(a->>5)::uuid,
                p_topic,p_partition,(frame->>'offset')::bigint,(d->>'through')::bigint);
              display_written:=true;
            else
              select accepted,writes into strict ok,n from prefix_message((a->>0)::uuid,(a->>1)::uuid,(a->>2)::uuid,(a->>3)::bigint,a->>4,(a->>5)::uuid);
            end if;
          when 'inbox' then
            select accepted,writes into strict ok,n from prefix_inbox((a->>0)::uuid,(a->>1)::uuid,(a->>2)::bigint,(a->>3)::bigint,(a->>4)::bigint,(a->>5)::int,a->>6,(a->>7)::uuid,(a->>8)::uuid);
          when 'finish' then
            select count(*)::int into n from prefix_finish((a->>0)::uuid,(a->>1)::uuid,(a->>2)::uuid,(a->>3)::uuid,a->>4);
            ok:=n=1;
            if not ok and exists(select 1 from ${relation} where id=(a->>2)::uuid and lease_id=(a->>3)::uuid
              and state in ('running','cancel_requested') and projection_tools<>'{}'::jsonb)
              then raise exception 'Incomplete native Tool at Run settlement' using errcode='PZ002'; end if;
          when 'tool','tool-settled' then
            select projection_tools->(a->>4),state into snap,owner_state from ${relation} where tenant_id=(a->>0)::uuid
              and session_id=(a->>1)::uuid and id=(a->>2)::uuid and lease_id=(a->>3)::uuid and state in ('running','cancel_requested') for no key update;
            ok:=found; n:=0;
            if ok then
              if op->>'kind'='tool' and owner_state='cancel_requested' then
                if frame?'intentPrefix' and jsonb_array_length(p_frames)>1 then
                  raise exception 'Isolate cancelling tool owner' using errcode='PZ001';
                end if;
                suppress_dispatch:=true;
              end if;
              if (snap is not null and snap is distinct from (a->5)) or (op->>'kind'='tool-settled' and snap is null)
                then raise exception 'Conflicting native task reference' using errcode='PZ002'; end if;
              if op->'confirmUse'='true'::jsonb and op->>'kind'='tool-settled' then
                insert into conversation_worlds as w (tenant_id,pi_session_id,conversation_id,seen_offset,state)
                select span.tenant_id,(a->>1)::uuid,span.conversation_id,(snap->>'offset')::bigint,
                  jsonb_build_object('sandbox',jsonb_build_object('status','active','continuityId',span.allocation_id::text))
                from workspace_route_spans span where span.run_id=(a->>2)::uuid and span.tenant_id=(a->>0)::uuid
                  and span.pi_session_id=(a->>1) and span.conversation_id=(snap->>'conversationId')::bigint
                  and span.topic=p_topic and span.partition=p_partition and span.first_offset=(snap->>'allocationSpanStart')::bigint
                  and span.allocation_id is not null
                on conflict(tenant_id,pi_session_id,conversation_id) do update set state=excluded.state,seen_offset=excluded.seen_offset
                  where (w.seen_offset is null or w.seen_offset<=excluded.seen_offset) and w.state#>>'{sandbox,continuityId}' is distinct from excluded.state#>>'{sandbox,continuityId}';
              end if;
              if combine_wait_display and not display_written and d->>'tenant'=a->>0 and d->>'session'=a->>1
                and d->>'run'=a->>2 and d->>'writer'=a->>3 then
                update ${relation} set ${waitAssignment}, ${displayAssignment} where id=(a->>2)::uuid;
                display_written:=true;
              else
                update ${relation} set ${waitAssignment} where id=(a->>2)::uuid;
              end if;
              n:=1;
            end if;
          when 'cancel' then
            update ${relation} set cancel_positions=case when projection_tools->(a->>5)->>'operationId'=a->>4 then
                jsonb_set(cancel_positions,array[a->>4],jsonb_build_object('taskId',a->5,'operationId',a->4,
                  'conversationId',projection_tools->(a->>5)->'conversationId','offset',projection_tools->(a->>5)->'offset'))
                else cancel_positions end
              where tenant_id=(a->>0)::uuid and session_id=(a->>1)::uuid and id=(a->>2)::uuid
                and lease_id=(a->>3)::uuid and state in ('running','cancel_requested')
                and (not (cancel_positions ? (a->>4)) or cancel_positions->(a->>4)->'taskId'=a->5);
            ok:=found; n:=1;
          when 'anchor' then
            update ${relation} set projection_first=(frame->>'offset')::bigint,projection_partition=p_partition
              where tenant_id=(a->>0)::uuid and session_id=(a->>1)::uuid and id=(a->>2)::uuid
                and lease_id=(a->>3)::uuid and state in ('running','cancel_requested') and projection_first is null;
            get diagnostics n = row_count;
            ok:=n=1;
          when 'pin' then
            update ${relation} set projection_pin=(frame->>'offset')::bigint
              where tenant_id=(a->>0)::uuid and session_id=(a->>1)::uuid and id=(a->>2)::uuid and lease_id=(a->>3)::uuid and state in ('running','cancel_requested');
            get diagnostics n = row_count;
            ok:=n=1;
          else raise exception 'Unknown projection operation';
          end case;
          if not ok then
            if jsonb_array_length(p_frames)>1 or applied>0 then raise exception 'Isolate obsolete candidate' using errcode='PZ001'; end if;
            update accepted_fact_projection_offsets set next_offset=p_next where topic=p_topic and partition=p_partition;
            return 'obsolete';
          end if;
          if n<>(op->>'count')::int then raise exception 'Conflicting projection' using errcode='PZ002'; end if;
          applied:=applied+1;
        end loop;
        if frame?'usage' then
          if applied=0 then raise exception 'Usage lacks admitted source'; end if;
          for op in select value from jsonb_array_elements(frame->'usage') loop
            if p_bulk_usage then
              pending_usage:=array_append(pending_usage,op);
            else
              insert into usage_latest as u values (${usageValues("op")}) ${replaceUsage};
              if not found then raise exception 'Conflicting accepted usage position' using errcode='PZ002'; end if;
            end if;
          end loop;
        end if;
        if frame?'namedDisplays' then
          if applied=0 then raise exception 'Child display lacks admitted source'; end if;
          for a in select value from jsonb_array_elements(frame->'namedDisplays') loop
            update ${relation} set output_display_seq=greatest(output_display_seq,(a->>'through')::bigint),
              output_display_native_seq=greatest(output_display_native_seq,(a->>'seq')::bigint)
              where tenant_id=(a->>'tenant')::uuid and root_run_id=(a->>'run')::uuid and agent_id=(a->>'session')::uuid and id=(a->>'turn')::uuid;
            if not found then raise exception 'Named child display scope changed' using errcode='40001'; end if;
          end loop;
        end if;
        if frame?'childDisplays' then
          if applied=0 then raise exception 'Child display lacks admitted source'; end if;
          for a in select value from jsonb_array_elements(frame->'childDisplays') loop
            update subagent_conversations set output_display_seq=greatest(output_display_seq,(a->>'through')::bigint),
              output_display_native_seq=greatest(output_display_native_seq,(a->>'seq')::bigint)
            where tenant_id=(a->>'tenant')::uuid and root_run_id=(a->>'run')::uuid and child_session_id=(a->>'session')::uuid and child_turn_id=(a->>'turn')::uuid;
            if not found then raise exception 'Child display scope changed' using errcode='40001'; end if;
          end loop;
        end if;
        if d is not null and not display_written then
          update ${relation} set ${displayAssignment}
            where id=(d->>'run')::uuid and tenant_id=(d->>'tenant')::uuid and session_id=(d->>'session')::uuid and lease_id=(d->>'writer')::uuid and state in ('running','cancel_requested');
          if not found then raise exception 'Root display owner changed' using errcode='40001'; end if;
        end if;
        if frame?'writerStart' then
          a:=frame->'writerStart';
          update ${relation} set output_first_topic=coalesce(output_first_topic,p_topic),output_first_partition=coalesce(output_first_partition,p_partition),
            output_first_offset=coalesce(output_first_offset,(frame->>'offset')::bigint)
            where tenant_id=(a->>0)::uuid and session_id=(a->>1)::uuid and id=(a->>2)::uuid and lease_id=(a->>3)::uuid;
          if not found then raise exception 'Independent writer changed' using errcode='40001'; end if;
        end if;
        if frame?'snapshot' then
          snap:=frame->'snapshot';
          update ${relation} set projection_first=(frame->>'offset')::bigint+1,projection_partition=p_partition,projection_pin=null,
            projection_state=snap->'state' where id=(snap->>'run')::uuid and lease_id=(snap->>'writer')::uuid and state in ('running','cancel_requested');
          if not found then raise exception 'Checkpoint owner changed' using errcode='40001'; end if;
        elsif frame?'anchor' and frame->'ops'->0->>'kind'<>'anchor' then
          a:=frame->'anchor';
          update ${relation} set projection_first=(frame->>'offset')::bigint,projection_partition=p_partition
            where id=(a->>2)::uuid and lease_id=(a->>3)::uuid and projection_first is null;
        end if;
        if frame?'pin' and frame->'ops'->0->>'kind'<>'pin' then
          a:=frame->'pin';
          update ${relation} set projection_pin=(frame->>'offset')::bigint
            where id=(a->>2)::uuid and lease_id=(a->>3)::uuid and state in ('running','cancel_requested');
          if not found then raise exception 'Pin owner changed' using errcode='40001'; end if;
        end if;
      end loop;
      if pending_usage is not null then
        insert into usage_latest as u select ${usageValues("item")}
          from unnest(pending_usage) as v(item) ${replaceUsage};
        get diagnostics n = row_count;
        if n<>cardinality(pending_usage) then raise exception 'Conflicting accepted usage position' using errcode='PZ002'; end if;
      end if;
      update accepted_fact_projection_offsets set next_offset=p_next where topic=p_topic and partition=p_partition;
      if p_relaxed then perform set_config('synchronous_commit','off',true); end if;
      return case when suppress_dispatch then 'recorded_without_dispatch' else 'applied' end;
    end $body$;`);
}
export const applyPrefixSql = "select project_ready($1,$2,$3,$4,$5::jsonb) as outcome";
// The function has no SET clause. The local setting survives function return
// through the implicit COMMIT, then reverts before the pooled client is reused.
const movingSql = (
  bulk,
) => `with result as materialized (select project_ready($1,$2,$3,$4,$5::jsonb,$6,$7,${bulk}) as outcome)
  select outcome,current_setting('synchronous_commit') as mode from result`;
export const applyMovingSql = movingSql(false);
export const applyBulkUsageSql = movingSql(true);
export async function projectionStarts(db, topic, bounds) {
  const { rows } = await db.query(
    `select h.partition,h.next_offset,min(r.projection_first) as first
    from accepted_fact_projection_offsets h left join runs r on r.projection_partition=h.partition
    and r.state in ('running','cancel_requested') and r.projection_first is not null
    where h.topic=$1 and h.partition=any($2::int[]) group by h.partition,h.next_offset`,
    [topic, bounds.map((b) => b.partition)],
  );
  const map = new Map(rows.map((r) => [r.partition, r]));
  return bounds.map((b) => {
    const row = map.get(b.partition),
      next = row ? BigInt(row.next_offset) : b.low;
    const first = row?.first == null ? next : BigInt(row.first);
    const start = first < next ? first : next;
    return [
      b.partition,
      next,
      !row || start < b.low || next > b.high
        ? new Error("Required projection prefix is unavailable")
        : start,
    ];
  });
}
