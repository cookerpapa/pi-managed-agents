create table subagent_deliveries (
 operation_id uuid primary key, root_id uuid not null references runs(id) on delete cascade,
 writer uuid not null, caller_task bigint not null, caller_lane bigint not null,
 operation_hash text not null, destination text not null, call_id text not null, tool text not null,
 child_id uuid references runs(id) on delete cascade, kind text not null check(kind in ('watch','state','fixed','send','cancel','question','answered','finished')),
 value jsonb not null, due_at timestamptz, claim uuid, sent boolean not null default false,
 created_at timestamptz not null default now()
);
create index subagent_delivery_due on subagent_deliveries(due_at,operation_id) where not sent;
create index subagent_waiting_child on subagent_deliveries(child_id) where not sent;
create index subagent_delivery_root on subagent_deliveries(root_id);
