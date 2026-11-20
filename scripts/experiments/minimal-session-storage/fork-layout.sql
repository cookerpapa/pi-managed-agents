-- Private database only. The maintained directory adapter reads the same
-- immutable coordinate/metadata contract through pi_history_records.
create table pi_history_streams (
  tenant_id uuid not null, id text not null,
  stream_uuid uuid generated always as (id::uuid) stored,
  primary key (tenant_id,id), unique(tenant_id,stream_uuid)
);
create table pi_history_nodes (
  tenant_id uuid not null,id uuid not null,left_id uuid,right_id uuid,source_id text,
  conversation_id bigint,through_seq bigint,clear_task boolean,retained_tasks bigint[],
  min_id bigint not null,max_id bigint not null,item_count bigint not null,height int not null,
  digest text not null,node jsonb not null,primary key(tenant_id,id),
  foreign key(tenant_id,left_id) references pi_history_nodes,
  foreign key(tenant_id,right_id) references pi_history_nodes,
  foreign key(tenant_id,source_id) references pi_history_streams
);
create index fork_ranges on pi_history_nodes(tenant_id,source_id,conversation_id,min_id,max_id,through_seq);
create table fork_sessions (
  tenant_id uuid not null,id uuid not null,stream_id uuid not null,root_id uuid,
  primary key(tenant_id,id),unique(tenant_id,stream_id),
  foreign key(tenant_id,stream_id) references pi_history_streams(tenant_id,stream_uuid),
  foreign key(tenant_id,root_id) references pi_history_nodes
);
create table fork_completed_runs (
  tenant_id uuid not null,stream_id uuid not null,run_id uuid not null,
  primary key(tenant_id,stream_id,run_id),
  foreign key(tenant_id,stream_id) references pi_history_streams(tenant_id,stream_uuid)
);
create table fork_requests (
  tenant_id uuid not null,source_id uuid not null,request_key uuid not null,target_id uuid not null,
  conversation_id bigint not null,entry_id bigint not null,run_id uuid not null,
  primary key(tenant_id,source_id,request_key),
  foreign key(tenant_id,target_id) references fork_sessions(tenant_id,id) on delete cascade
);
create view pi_history_records as select tenant_id,pi_session_id as session_id,entry_id as record_id,
  commit_seq as seq,ordinal,'entry'::text as record_kind,conversation_id as entry_conversation_id,
  entry_id as entry_body_id,null::bigint as entry_task_id,role as fields,run_id as cloud_origin from messages;
create function fork_leaves(t uuid, root uuid) returns setof pi_history_nodes language sql stable as $$
  with recursive walk as (
    select n.* from pi_history_nodes n where n.tenant_id=t and n.id=root
    union all select c.* from walk p cross join lateral(values(p.left_id),(p.right_id)) e(id)
      join pi_history_nodes c on c.tenant_id=p.tenant_id and c.id=e.id
  ) select * from walk where source_id is not null
$$;
