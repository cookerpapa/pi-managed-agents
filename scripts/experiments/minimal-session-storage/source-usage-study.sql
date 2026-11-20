-- Archived model DDL for reproducing the #49 writer experiment only.
-- Source migration SHA-256: 2c2dfbaf22e987f4b222bc58e59d621aac397d242bcc75f3d3a538c03b8393e2
create function pi_usage_canonical(v jsonb) returns text
language plpgsql immutable strict parallel safe as $$
declare result text;
begin
  case jsonb_typeof(v)
    when 'object' then
      select '{'||coalesce(string_agg(to_jsonb(key)::text||':'||pi_usage_canonical(value),',' order by key collate "C"),'')||'}'
      into result from jsonb_each(v);
    when 'array' then
      select '['||coalesce(string_agg(pi_usage_canonical(value),',' order by ordinal),'')||']'
      into result from jsonb_array_elements(v) with ordinality a(value,ordinal);
    when 'number' then
      if (v::text)::numeric<>trunc((v::text)::numeric) or abs((v::text)::numeric)>9007199254740991 then
        raise exception 'Usage receipt contains an invalid count' using errcode='22023';
      end if;
      result:=trunc((v::text)::numeric)::text;
    else result:=v::text;
  end case;
  return result;
end $$;
create function pi_usage_hash(v jsonb) returns text language sql immutable strict parallel safe
  return encode(sha256(convert_to(pi_usage_canonical(v),'UTF8')),'hex');

create function pi_record_model_usage(events jsonb) returns integer
language plpgsql as $$
declare event jsonb; c jsonb; receipt_id text; previous jsonb;
begin
  if jsonb_typeof(events)<>'array' or jsonb_array_length(events) not between 1 and 128 then
    raise exception 'Invalid model receipt batch' using errcode='22023';
  end if;
  for event in select value from jsonb_array_elements(events)
    order by value->'context'->>'invocationId',value->>'kind',value->>'executionId'
  loop
    c:=event->'context';
    if coalesce(c->>'version','')<>'1' or coalesce(event->>'kind','') not in ('started','attempt','usage','finished','interrupted')
      or coalesce(c->>'purpose','') not in ('agent','context_maintenance','image')
      or coalesce(c->>'provider','')='' or coalesce(c->>'model','')=''
      or (event->>'kind' in ('attempt','usage') and event->>'executionId' is null) then
      raise exception 'Invalid model source receipt' using errcode='22023';
    end if;
    insert into model_invocations(id,tenant_id,session_id,run_id,requested_at,provider,model,purpose,context,context_hash)
      values((c->>'invocationId')::uuid,(c->>'tenantId')::uuid,(c->>'sessionId')::uuid,(c->>'runId')::uuid,
        (c->>'requestedAt')::timestamptz,c->>'provider',c->>'model',c->>'purpose',c,pi_usage_hash(c))
      on conflict(id) do update set id=excluded.id where model_invocations.context=excluded.context;
    if not found then raise exception 'Model invocation identity changed' using errcode='23514'; end if;
    receipt_id:=(c->>'invocationId')||':'||(event->>'kind')||case when event ? 'executionId' then ':'||(event->>'executionId') else '' end;
    insert into model_metering_events(id,invocation_id,kind,execution_id,occurred_at,payload,payload_hash,valuation)
      values(receipt_id,(c->>'invocationId')::uuid,event->>'kind',(event->>'executionId')::uuid,
        (event->>'occurredAt')::timestamptz,event,pi_usage_hash(event),null)
      on conflict(id) do nothing;
    if not found then
      select payload into previous from model_metering_events where id=receipt_id;
      if previous is distinct from event then raise exception 'Model receipt identity changed' using errcode='23514'; end if;
    end if;
  end loop;
  return jsonb_array_length(events);
end $$;

create index model_usage_pending_valuation on model_metering_events(received_at,id) where kind='usage' and valuation is null;
