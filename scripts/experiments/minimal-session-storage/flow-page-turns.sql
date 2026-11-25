-- PAGE_ORDER is asc or desc. Each public page has at most 40 Turns and one lookahead.
create function flow_page_turns_PAGE_ORDER(t uuid, window_id uuid, low_pos bigint, high_pos bigint, completed_only boolean, page_size integer)
returns table(run_id uuid, pi_session_id text, mailbox_position bigint, operation_kind text, state text,
 stop_reason text, output_display_seq bigint, output_display_native_seq bigint, waiting_for_seal boolean, created_at timestamptz, flow_lo bigint, flow_hi bigint, flow_seq bigint, flow_lane bigint,partial_coverage bigint)
language plpgsql stable strict rows 41 set plan_cache_mode=force_generic_plan as $$
declare
  segment record;
  emitted integer;
  remaining integer := page_size;
begin
 if remaining <= 0 or low_pos > high_pos then return; end if;
 -- Mailbox ranges are disjoint in presentation order. Once a range fills
 -- the page, no later range can contribute a visible Turn to this request.
 for segment in
   select f.* from flow_read_ranges(t,window_id) f
   where f.prior_positions<high_pos and f.prior_positions+f.through_position>=low_pos
   order by f.prior_positions PAGE_ORDER
 loop
   return query
     select r.id,r.session_id::text,segment.prior_positions+r.mailbox_position,r.operation_kind::text,
       case when segment.tail_coverage is not null and r.mailbox_position=segment.through_position then 'running' else r.state::text end,
       case when segment.tail_coverage is null or r.mailbox_position<>segment.through_position then r.stop_reason::text end,
       coalesce(case when r.mailbox_position=segment.through_position then segment.tail_coverage end,r.output_display_seq),
       least(r.output_display_native_seq,segment.cut_seq),
       r.output_seal_id is not null and r.output_sealed_at is null,r.created_at,segment.entry_lo,segment.entry_hi,segment.cut_seq,segment.source_lane,case when r.mailbox_position=segment.through_position then segment.tail_coverage end
     from runs r
     where r.tenant_id=t and r.session_id=segment.physical and r.mailbox_position is not null
       and r.mailbox_position between greatest(1::bigint,low_pos-segment.prior_positions)
                                 and least(segment.through_position,high_pos-segment.prior_positions)
       and r.pruned_at is null and (r.state<>'queued' or r.started_at is not null or r.operation_kind='compact')
       and not(r.operation_kind='chat' and r.state='cancelled' and r.started_at is null)
       and (not completed_only or ((segment.tail_coverage is null or r.mailbox_position<>segment.through_position) and r.state in ('completed','failed','cancelled','timed_out')
                                and (r.output_seal_id is null or r.output_sealed_at is not null)))
     order by r.mailbox_position PAGE_ORDER limit remaining;
   get diagnostics emitted = row_count;
   remaining := remaining - emitted;
   exit when remaining=0;
 end loop;
end $$;
