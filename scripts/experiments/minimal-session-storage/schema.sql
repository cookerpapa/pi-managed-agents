create table runs (
  id uuid primary key, tenant_id uuid not null, session_id uuid not null,
  mailbox_position bigint not null, idempotency_key uuid not null, initial_input text not null,
  model_ref text not null, state text not null check (state in ('queued','running','cancel_requested','completed')),
  lease_id uuid, created_at timestamptz not null default clock_timestamp(),
  started_at timestamptz, settled_at timestamptz, history_head jsonb, environment_snapshot jsonb,
  unique (tenant_id,session_id,mailbox_position), unique (tenant_id,session_id,idempotency_key)
);
create index runs_ready on runs (created_at,id) where state='queued';
create unique index runs_one_active on runs (tenant_id,session_id) where state in ('running','cancel_requested');
create table messages (
  tenant_id uuid not null, pi_session_id uuid not null, entry_id bigint, document_id bigint,
  conversation_id bigint not null, run_id uuid not null references runs(id),
  navigation_caption text, assistant_entry_id bigint, tool_task_id bigint, role text not null, content text not null, source_input_id text, commit_seq bigint not null,
  attachment_refs jsonb,
  ordinal integer not null, created_at timestamptz not null default clock_timestamp(),
  primary key (tenant_id,pi_session_id,commit_seq,ordinal),
  constraint message_record_shape check ((entry_id is not null and document_id is null) or
    (entry_id is null and document_id is not null and role='pi.agent' and source_input_id is null and navigation_caption is null and assistant_entry_id is null and tool_task_id is null))
);
create unique index messages_entry on messages(tenant_id,pi_session_id,entry_id) where entry_id is not null;
create index messages_conversation_entry on messages(tenant_id,pi_session_id,conversation_id,entry_id) where entry_id is not null;
create index messages_agent_at on messages(tenant_id,pi_session_id,conversation_id,commit_seq desc,ordinal desc) where document_id is not null;
create view message_entries as select * from messages where entry_id is not null;
create function agent_config_at(t uuid,s uuid,lane bigint,through_seq bigint) returns setof messages language sql stable as $$
  select m.* from messages m where m.tenant_id=t and m.pi_session_id=s and m.conversation_id=lane
    and m.document_id is not null and m.commit_seq<=through_seq order by m.commit_seq desc,m.ordinal desc limit 1
$$;
create index messages_run on messages(run_id,commit_seq,ordinal);
create unique index messages_input on messages(tenant_id,pi_session_id,conversation_id,source_input_id) where source_input_id is not null;
create table inboxes (
  tenant_id uuid not null, pi_session_id uuid not null, conversation_id bigint not null,
  doc_id bigint not null, commit_seq bigint not null, ordinal integer not null,
  items jsonb not null, created_seq bigint not null, primary key (tenant_id,pi_session_id,conversation_id)
);
-- Only the most recent confirmed resource observation for each live Conversation.
create table conversation_worlds (
  tenant_id uuid not null, pi_session_id uuid not null, conversation_id bigint not null,
  -- A Fork starts its own ordered stream and inherits only the observation.
  seen_offset bigint,
  state jsonb not null check (jsonb_typeof(state#>'{sandbox,continuityId}') is not distinct from 'string'
    and state#>>'{sandbox,status}'='active' and octet_length(state::text)<=4096),
  primary key(tenant_id,pi_session_id,conversation_id)
);

-- Read boundary shared by ordinary and Fork-enabled candidate layouts.
create function message_root_ranges(t uuid, public_id uuid, physical uuid, needs_base boolean)
returns table(stream uuid,lane bigint,lo bigint,hi bigint,seq bigint) language sql stable as $$
  select physical,1::bigint,1::bigint,9007199254740991::bigint,9007199254740991::bigint where not needs_base
$$;
