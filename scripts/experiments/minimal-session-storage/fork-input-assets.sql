create function pin_input_assets(t uuid,physical uuid,turn_id uuid,request text,uploads json) returns void
language plpgsql set search_path from current as $$
declare ref json; ready boolean;
begin
  for ref in select part from json_array_elements(uploads) u
    cross join lateral(values(u->'image'),(u->'original'->'attachment')) v(part)
    where part is not null order by part->>'id'
  loop
    select true into ready from public.session_attachment_objects o where o.tenant_id=t
      and o.id=(ref->>'id')::uuid and o.state='ready' and o.sha256=ref->>'sha256'
      and o.byte_length=(ref->>'byteLength')::bigint and o.mime_type=ref->>'mimeType' for share;
    if not found then raise exception 'Unavailable retained input attachment'; end if;
    insert into flow_input_assets values(t,physical,turn_id,request,(ref->>'id')::uuid) on conflict do nothing;
  end loop;
end $$;
create function pin_turn_header_uploads() returns trigger language plpgsql set search_path from current as $$
begin
  perform pin_input_assets(new.tenant_id,new.session_id,new.id,new.id::text,new.input_uploads::json);
  return new;
end $$;
create function pin_followup_uploads() returns trigger language plpgsql set search_path from current as $$
declare item record;
begin
  for item in select key,value->'uploads' as uploads from json_each(new.root_human_inputs::json->'inputs')
  loop
    if item.uploads is not null then
      perform pin_input_assets(new.tenant_id,new.session_id,new.id,item.key,item.uploads);
    end if;
  end loop;
  return new;
end $$;
create trigger fork_header_uploads after insert or update of input_uploads on runs for each row
  when(new.input_uploads<>'[]'::jsonb) execute function pin_turn_header_uploads();
create trigger fork_followup_uploads after update of root_human_inputs on runs for each row
  when(new.root_human_inputs is distinct from old.root_human_inputs and
    (strpos(new.root_human_inputs,'"original"')>0 or strpos(new.root_human_inputs,'"image"')>0))
  execute function pin_followup_uploads();
