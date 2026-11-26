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
