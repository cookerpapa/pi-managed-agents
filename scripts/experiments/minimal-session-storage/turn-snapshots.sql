-- The shared Pi history, tool executor and projection clients still address
-- "runs". Their view operates on the very same row as the Session scheduler.
alter table runs rename to turns;
alter table turns drop column model_ref, add model_snapshot jsonb not null;
create view runs as select t.*,
  t.model_snapshot->>'modelId' as model_ref,t.model_snapshot->>'modelId' as model_id,t.model_snapshot->>'provider' as provider,
  (t.model_snapshot->>'profileId')::uuid as model_profile_id,t.model_snapshot->>'thinkingLevel' as thinking_level,
  t.model_snapshot->>'serviceTier' as service_tier,nullif(t.model_snapshot->'runtime','null'::jsonb) as model_runtime_snapshot,
  (t.model_snapshot->>'credentialBindingId')::uuid as credential_binding_id,(t.model_snapshot->>'credentialBindingVersion')::bigint as credential_binding_version,
  (t.workspace_grant->>'workspaceId')::uuid as workspace_id,t.workspace_grant->>'cwd' as working_directory,
  (t.workspace_grant->>'computeSessionId')::uuid as compute_session_id,t.workspace_grant->>'executionMode' as execution_mode
from turns t;

create function freeze_turn_settings() returns trigger language plpgsql set search_path from current as $$
begin
  if tg_op='INSERT' then
    select desired_model,desired_workspace into strict new.model_snapshot,new.workspace_grant
      from sessions where tenant_id=new.tenant_id and flow_id=new.session_id for no key update;
    if new.model_snapshot is null then raise exception 'Session has no model selection'; end if;
  elsif (old.model_snapshot,old.workspace_grant) is distinct from (new.model_snapshot,new.workspace_grant) then
    raise exception 'Admitted Turn settings are immutable';
  end if;
  return new;
end $$;
create trigger freeze_turn_settings before insert or update of model_snapshot,workspace_grant on turns
 for each row execute function freeze_turn_settings();

create table session_workspace_changes (
 tenant_id uuid not null,session_id uuid not null,request_key text not null, fingerprint text not null,
 receipt jsonb not null,primary key(tenant_id,session_id,request_key),
 foreign key(tenant_id,session_id) references sessions(tenant_id,id) on delete cascade
);
