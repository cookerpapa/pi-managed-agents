-- Candidate index for isolated plan and load measurements.
create index study_unfinished on turns(tenant_id,session_id,mailbox_position)
where state not in ('completed','failed','timed_out','cancelled')
   or (output_seal_id is not null and output_sealed_at is null);
