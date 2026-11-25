-- Installed in the isolated segmented schema on a database with the regular
-- Worker registry, pi_sessions and session_leases in its search path.
alter table runs add column if not exists fencing_token bigint,
  add column if not exists stop_on_resume boolean not null default false,
  add column if not exists seal_offer_after timestamptz,
  add column if not exists recovery_requested_at timestamptz,
  add column if not exists output_seal_offset bigint;
create index segmented_expired_owner on runs(lease_id) where lease_id is not null and state in ('running','cancel_requested');
create function guard_segment_owner() returns trigger language plpgsql set search_path from current as $$
begin
  if new.state in ('running','cancel_requested') and (tg_op='INSERT' or old.lease_id is distinct from new.lease_id or old.state='queued' or old.fencing_token is distinct from new.fencing_token) then
    if not exists(select from sessions s join session_leases l on l.tenant_id=s.tenant_id and l.pi_session_id=s.id::text
      where s.tenant_id=new.tenant_id and s.flow_id=new.session_id and l.lease_id=new.lease_id
        and l.fencing_token=new.fencing_token and l.released_at is null and l.writer_sealed_at is null
        and l.writer_failed_at is null and l.valid_until>clock_timestamp()) then
      raise exception 'Turn requires the active lease of its own Session';
    end if;
  end if;
  if tg_op='UPDATE' and old.lease_id is not null and new.lease_id is distinct from old.lease_id and not exists(
    select from session_leases where lease_id=old.lease_id and writer_seal_offset is not null and released_at is not null) then
    raise exception 'Prior writer has no projected seal';
  end if;
  return new;
end $$;
create trigger independent_turn_owner before insert or update of state,lease_id,fencing_token on runs
  for each row execute function guard_segment_owner();

-- NOTIFY folds identical payloads within one transaction. Distinct Session
-- flows represent independently claimable work. Private schemas have separate
-- channels, including when the candidate shares PG with the serving layout.
create function notify_segment_work() returns trigger language plpgsql set search_path from current as $$
begin
  if new.state='queued' or exists(select from runs r where r.tenant_id=new.tenant_id and r.session_id=new.session_id
      and r.mailbox_position>new.mailbox_position and r.state='queued') then
    perform pg_notify('pma_segment_ready_'||md5(tg_table_schema),new.session_id::text);
  end if;
  return null;
end $$;
create trigger independent_turn_ready after insert or update of state on runs for each row
  when(new.state='queued') execute function notify_segment_work();
create trigger independent_successor_ready after update of state on runs for each row
  when(old.state is distinct from new.state and new.state in ('completed','failed','cancelled','timed_out'))
  execute function notify_segment_work();

-- Wake parked tool waiters only when their exact child Turn settles. Closing
-- a parent retires its callbacks and does not send a cancellation to children.
create function wake_segment_returns() returns trigger language plpgsql set search_path from current as $$
begin
  update subagent_deliveries set due_at=clock_timestamp(),claim=null where child_id=new.id and not sent;
  delete from subagent_deliveries where root_id=new.id;
  perform pg_notify('pma_named_children','');
  return null;
end $$;
create trigger independent_turn_return after update of state on runs for each row
  when(old.state is distinct from new.state and new.state in ('completed','cancelled','failed','timed_out'))
  execute function wake_segment_returns();

-- The retryable return dispatcher still needs the child's sealed answer and
-- Turn header if a user deletes the child's window before the Kafka ACK.
create or replace function flow_delivery_allows_gc(t uuid,physical uuid) returns boolean
language sql stable set search_path from current as $$
  select not exists(select from runs c join subagent_deliveries d on d.child_id=c.id
    where c.tenant_id=t and c.session_id=physical and not d.sent)
$$;
