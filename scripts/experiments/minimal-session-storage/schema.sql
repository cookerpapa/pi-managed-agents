create table runs (
  id uuid primary key, tenant_id uuid not null, session_id uuid not null,
  mailbox_position bigint not null, idempotency_key uuid not null, initial_input text not null,
  model_ref text not null, state text not null check (state in ('queued','running','completed')),
  lease_id uuid, created_at timestamptz not null default clock_timestamp(),
  started_at timestamptz, settled_at timestamptz, history_head jsonb,
  unique (tenant_id,session_id,mailbox_position), unique (tenant_id,session_id,idempotency_key)
);
create index runs_ready on runs (created_at,id) where state='queued';
create unique index runs_one_active on runs (tenant_id,session_id) where state='running';
create table messages (
  tenant_id uuid not null, pi_session_id uuid not null, entry_id bigint not null,
  conversation_id bigint not null, run_id uuid not null references runs(id),
  role text not null, content text not null, source_input_id uuid, commit_seq bigint not null,
  ordinal integer not null, created_at timestamptz not null default clock_timestamp(),
  primary key (tenant_id,pi_session_id,entry_id),
  unique (tenant_id,pi_session_id,commit_seq,ordinal)
);
create index messages_run on messages(run_id,commit_seq,ordinal);
create unique index messages_input on messages(tenant_id,pi_session_id,source_input_id) where source_input_id is not null;
create table inboxes (
  tenant_id uuid not null, pi_session_id uuid not null, conversation_id bigint not null,
  doc_id bigint not null, commit_seq bigint not null, ordinal integer not null,
  items jsonb not null, primary key (tenant_id,pi_session_id,conversation_id)
);
