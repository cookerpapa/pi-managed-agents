// Each SQL helper executes the original parameterized mutation. The ordered
// loop and partition checkpoint execute in one synchronous autocommit call.
export async function installPrefixSql(db, { message, inbox, finish }) {
  await db.query(`create table accepted_fact_projection_offsets(topic text,partition int,next_offset bigint not null,primary key(topic,partition));
    alter table runs add column projection_first bigint,add column projection_partition int;
    create index active_projection_start on runs(projection_partition,projection_first) where state='running' and projection_first is not null;
    create function prefix_message(uuid,uuid,uuid,bigint,text,uuid) returns table(accepted boolean,writes int)
      language sql volatile as $fn$ ${message} $fn$;
    create function prefix_inbox(uuid,uuid,bigint,bigint,int,text,uuid,uuid) returns table(accepted boolean,writes int)
      language sql volatile as $fn$ ${inbox} $fn$;
    create function prefix_finish(uuid,uuid,uuid,uuid,text) returns setof uuid language sql volatile as $fn$ ${finish} $fn$;
    create function project_ready(p_topic text,p_partition int,p_expected bigint,p_next bigint,p_frames jsonb)
    returns text language plpgsql as $body$
    declare saved bigint; frame jsonb; op jsonb; a jsonb; ok boolean; n int; applied int;
    begin
      select next_offset into strict saved from accepted_fact_projection_offsets
       where topic=p_topic and partition=p_partition for update;
      if p_next<=saved then return 'covered'; end if;
      if saved<>p_expected then raise exception 'Partition owner or prefix changed' using errcode='40001'; end if;
      for frame in select value from jsonb_array_elements(p_frames) loop
        applied:=0;
        for op in select value from jsonb_array_elements(frame->'ops') loop
          a:=op->'args';
          case op->>'kind'
          when 'message' then
            select accepted,writes into strict ok,n from prefix_message((a->>0)::uuid,(a->>1)::uuid,(a->>2)::uuid,(a->>3)::bigint,a->>4,(a->>5)::uuid);
          when 'inbox' then
            select accepted,writes into strict ok,n from prefix_inbox((a->>0)::uuid,(a->>1)::uuid,(a->>2)::bigint,(a->>3)::bigint,(a->>4)::int,a->>5,(a->>6)::uuid,(a->>7)::uuid);
          when 'finish' then
            select count(*)::int into n from prefix_finish((a->>0)::uuid,(a->>1)::uuid,(a->>2)::uuid,(a->>3)::uuid,a->>4);
            ok:=n=1;
          when 'anchor' then
            update runs set projection_first=(frame->>'offset')::bigint,projection_partition=p_partition
              where tenant_id=(a->>0)::uuid and session_id=(a->>1)::uuid and id=(a->>2)::uuid
                and lease_id=(a->>3)::uuid and state='running' and projection_first is null;
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
        if frame?'anchor' and frame->'ops'->0->>'kind'<>'anchor' then
          a:=frame->'anchor';
          update runs set projection_first=(frame->>'offset')::bigint,projection_partition=p_partition
            where id=(a->>2)::uuid and lease_id=(a->>3)::uuid and projection_first is null;
        end if;
      end loop;
      update accepted_fact_projection_offsets set next_offset=p_next where topic=p_topic and partition=p_partition;
      return 'applied';
    end $body$;`);
}
export const applyPrefixSql = "select project_ready($1,$2,$3,$4,$5::jsonb) as outcome";
export async function projectionStarts(db, topic, bounds) {
  const { rows } = await db.query(
    `select h.partition,h.next_offset,min(r.projection_first) as first
    from accepted_fact_projection_offsets h left join runs r on r.projection_partition=h.partition
    and r.state='running' and r.projection_first is not null
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
