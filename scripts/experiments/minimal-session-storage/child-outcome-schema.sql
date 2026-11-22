-- Candidate composition only, with the candidate schema first in search_path.
alter table subagent_conversations
  add column native_activity text check(native_activity in ('running','completed','failed','cancelled')),
  add column native_stop_seen boolean not null default false,
  add column native_caller text check(native_caller in ('closed','aborted')),
  add column native_answer_id bigint,
  add column native_outcome_seq bigint not null default 0,
  add column native_outcome_at timestamptz;
create view subagent_executions as
select e.*,p.state,
  case p.state when 'failed' then 'native_task_failed' when 'unknown' then 'native_work_interrupted' end as failure_code,
  case p.state when 'failed' then 'Native conversation did not complete its input'
    when 'unknown' then 'Root work ended before native completion' end as failure_message,
  case when p.state in ('completed','failed','cancelled','unknown')
    then coalesce(case when e.native_activity='running' or p.state='unknown' then root.settled_at end,
      e.native_outcome_at,root.settled_at,e.updated_at) end as settled_at
from subagent_conversations e join runs root on root.tenant_id=e.tenant_id and root.id=e.root_run_id
cross join lateral(select case
  when e.native_activity='running' and root.state in ('completed','failed','timed_out','cancelled')
    then case when root.state='cancelled' then 'cancelled' else 'unknown' end
  when e.native_activity is not null then e.native_activity
  when e.native_stop_seen or e.native_caller='aborted' or root.state='cancelled' then 'cancelled'
  when e.native_caller='closed' or root.state in ('completed','failed','timed_out') then 'unknown'
  else 'preparing' end as state) p;
