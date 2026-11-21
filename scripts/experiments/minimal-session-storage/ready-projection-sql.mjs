// Each SQL helper executes the original parameterized mutation. The ordered
// loop and partition checkpoint execute in one synchronous autocommit call.
export async function installPrefixSql(db, { message, inbox, finishAndRelease }) {
  const replaceUsage = `on conflict(tenant_id,pi_session_id,conversation_id) do update
    set commit_seq=excluded.commit_seq,ordinal=excluded.ordinal,value=excluded.value
    where u.doc_id=excluded.doc_id and (u.commit_seq,u.ordinal)<(excluded.commit_seq,excluded.ordinal)`;
  const usageValues = (
    row,
  ) => `(${row}->>'tenant')::uuid,(${row}->>'session')::uuid,(${row}->>'conversation')::bigint,
    (${row}->>'document')::bigint,(${row}->>'seq')::bigint,(${row}->>'ordinal')::int,${row}->'value'`;
  await db.query(`create table accepted_fact_projection_offsets(topic text,partition int,next_offset bigint not null,primary key(topic,partition));
    alter table runs add column projection_first bigint,add column projection_partition int,add column projection_state jsonb,
      add column cancel_positions jsonb not null default '{}',add column projection_tools jsonb not null default '{}',add column projection_pin bigint;
    create index active_projection_start on runs(projection_partition,projection_first) where state in ('running','cancel_requested') and projection_first is not null;
    create function prefix_message(uuid,uuid,uuid,bigint,text,uuid) returns table(accepted boolean,writes int)
      language sql volatile as $fn$ ${message} $fn$;
    create function prefix_inbox(uuid,uuid,bigint,bigint,bigint,int,text,uuid,uuid) returns table(accepted boolean,writes int)
      language sql volatile as $fn$ ${inbox} $fn$;
    create function prefix_finish(uuid,uuid,uuid,uuid,text) returns setof uuid language sql volatile as $fn$ ${finishAndRelease} $fn$;
    create function project_ready(p_topic text,p_partition int,p_expected bigint,p_next bigint,p_frames jsonb,p_relaxed boolean default false,p_allow_finish boolean default false,p_bulk_usage boolean default false)
    returns text language plpgsql as $body$
    declare saved bigint; frame jsonb; op jsonb; a jsonb; ok boolean; n int; applied int; owner_state text; suppress_dispatch boolean := false; snap jsonb; pending_usage jsonb[];
    begin
      select next_offset into strict saved from accepted_fact_projection_offsets
       where topic=p_topic and partition=p_partition for update;
      if p_next<=saved then return 'covered'; end if;
      if saved<>p_expected then raise exception 'Partition owner or prefix changed' using errcode='40001'; end if;
      for frame in select value from jsonb_array_elements(p_frames) loop
        if p_relaxed and (coalesce((frame->>'strict')::boolean,false) or
          (jsonb_array_length(frame->'ops')>0 and (frame->>'history')::boolean is distinct from true)) then
          raise exception 'Control boundary in asynchronous transaction'; end if;
        applied:=0;
        for op in select value from jsonb_array_elements(frame->'ops') loop
          a:=op->'args';
          if p_relaxed and ((op->>'kind' not in ('message','tool','inbox','pin') and not (op->>'kind'='finish' and p_allow_finish)) or
            (op->>'kind'='inbox' and (a->>6)::jsonb <> '[]'::jsonb)) then raise exception 'Control operation in asynchronous transaction'; end if;
          case op->>'kind'
          when 'message' then
            select accepted,writes into strict ok,n from prefix_message((a->>0)::uuid,(a->>1)::uuid,(a->>2)::uuid,(a->>3)::bigint,a->>4,(a->>5)::uuid);
          when 'inbox' then
            select accepted,writes into strict ok,n from prefix_inbox((a->>0)::uuid,(a->>1)::uuid,(a->>2)::bigint,(a->>3)::bigint,(a->>4)::bigint,(a->>5)::int,a->>6,(a->>7)::uuid,(a->>8)::uuid);
          when 'finish' then
            select count(*)::int into n from prefix_finish((a->>0)::uuid,(a->>1)::uuid,(a->>2)::uuid,(a->>3)::uuid,a->>4);
            ok:=n=1;
            if not ok and exists(select 1 from runs where id=(a->>2)::uuid and lease_id=(a->>3)::uuid
              and state in ('running','cancel_requested') and projection_tools<>'{}'::jsonb)
              then raise exception 'Incomplete native Tool at Run settlement' using errcode='PZ002'; end if;
          when 'tool','tool-settled' then
            select projection_tools->(a->>4),state into snap,owner_state from runs where tenant_id=(a->>0)::uuid
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
              update runs set projection_tools=case when op->>'kind'='tool'
                then jsonb_set(projection_tools,array[a->>4],a->5) else projection_tools-(a->>4) end,
                cancel_positions=case when op->>'kind'='tool-settled' and a->6 is null then cancel_positions-(a#>>'{5,operationId}') else cancel_positions end where id=(a->>2)::uuid;
              n:=1;
            end if;
          when 'cancel' then
            update runs set cancel_positions=case when projection_tools->(a->>5)->>'operationId'=a->>4 then
                jsonb_set(cancel_positions,array[a->>4],jsonb_build_object('taskId',a->5,'operationId',a->4,
                  'conversationId',projection_tools->(a->>5)->'conversationId','offset',projection_tools->(a->>5)->'offset'))
                else cancel_positions end
              where tenant_id=(a->>0)::uuid and session_id=(a->>1)::uuid and id=(a->>2)::uuid
                and lease_id=(a->>3)::uuid and state in ('running','cancel_requested')
                and (not (cancel_positions ? (a->>4)) or cancel_positions->(a->>4)->'taskId'=a->5);
            ok:=found; n:=1;
          when 'anchor' then
            update runs set projection_first=(frame->>'offset')::bigint,projection_partition=p_partition
              where tenant_id=(a->>0)::uuid and session_id=(a->>1)::uuid and id=(a->>2)::uuid
                and lease_id=(a->>3)::uuid and state in ('running','cancel_requested') and projection_first is null;
            get diagnostics n = row_count;
            ok:=n=1;
          when 'pin' then
            update runs set projection_pin=(frame->>'offset')::bigint
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
        if frame?'snapshot' then
          snap:=frame->'snapshot';
          update runs set projection_first=(frame->>'offset')::bigint+1,projection_partition=p_partition,projection_pin=null,
            projection_state=snap->'state' where id=(snap->>'run')::uuid and lease_id=(snap->>'writer')::uuid and state in ('running','cancel_requested');
          if not found then raise exception 'Checkpoint owner changed' using errcode='40001'; end if;
        elsif frame?'anchor' and frame->'ops'->0->>'kind'<>'anchor' then
          a:=frame->'anchor';
          update runs set projection_first=(frame->>'offset')::bigint,projection_partition=p_partition
            where id=(a->>2)::uuid and lease_id=(a->>3)::uuid and projection_first is null;
        end if;
        if frame?'pin' and frame->'ops'->0->>'kind'<>'pin' then
          a:=frame->'pin';
          update runs set projection_pin=(frame->>'offset')::bigint
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
