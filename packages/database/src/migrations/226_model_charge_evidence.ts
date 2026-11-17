import { sql, type Kysely } from "kysely";

export async function up(db: Kysely<unknown>): Promise<void> {
  await sql`
    alter table model_metering_events drop constraint model_metering_events_kind_check;
    alter table model_metering_events add constraint model_metering_events_kind_check
      check(kind in ('started','attempt','usage','finished','interrupted','execution'));
    create table model_execution_writers (
      writer_id uuid primary key, tenant_id uuid not null, pi_session_id text not null,
      run_id uuid not null, fencing_token bigint not null check(fencing_token>0),
      topic text not null, partition integer not null check(partition>=0),
      close_offset bigint not null check(close_offset>=0), fingerprint text not null,
      closed_at timestamptz not null default clock_timestamp()
    );
    create index model_execution_writers_tenant on model_execution_writers(tenant_id,writer_id);
    create view model_billing_events as
    select q.*,
      case when q.kind<>'usage' then q.valuation else jsonb_set(q.valuation,'{customer}',
        case when q.customer_eligibility='billable' then q.valuation->'customer'
        when q.customer_eligibility='platform' and q.valuation->'customer' ? 'rateId'
          then (q.valuation->'customer') || '{"state":"waived","amountPico":"0"}'::jsonb
        when q.customer_eligibility='platform'
          then '{"state":"unpriced","reason":"platform_cost"}'::jsonb
        else jsonb_build_object('state','unconfirmed','reason',q.eligibility_reason) end
      ) end as billing_valuation
    from (
      select e.*, decision.eligibility_reason,
        case when decision.eligibility_reason='accepted_execution' then 'billable'
             when decision.eligibility_reason in ('failed_execution','superseded_request','writer_closed_without_settlement','ambiguous_provider_retry','invalid_execution_boundary') then 'platform'
             else 'pending' end as customer_eligibility
      from model_metering_events e join model_invocations i on i.id=e.invocation_id
      left join model_metering_events proof on proof.id=i.id::text||':execution' and proof.kind='execution'
      left join model_execution_writers w on w.tenant_id=i.tenant_id
        and w.writer_id=(i.context->'execution'->>'writerId')::uuid
        and w.fencing_token=(i.context->'execution'->>'fencingToken')::bigint
      left join lateral (
        select count(*) filter(where r.kind='attempt') attempts,
          bool_or(r.kind='attempt' and r.execution_id=e.execution_id) matched_attempt,
          bool_or(r.kind='finished') finished, bool_or(r.kind='interrupted') interrupted
        from model_metering_events r where r.invocation_id=i.id
      ) source on true
      cross join lateral (
        select case
          when e.kind<>'usage' then 'not_usage'
          when i.purpose='image' or (e.payload->'usage'->>'unsupportedDimensions')::boolean then 'unsupported_dimensions'
          when proof.id is not null and w.writer_id is not null and (
            proof.payload->'position'->>'topic'<>w.topic or
            (proof.payload->'position'->>'partition')::integer<>w.partition or
            (proof.payload->'position'->>'offset')::bigint>=w.close_offset
          ) then 'invalid_execution_boundary'
          when proof.payload->>'outcome'='superseded' then 'superseded_request'
          when proof.payload->>'outcome'='failed' then 'failed_execution'
          when proof.id is null and w.writer_id is not null then 'writer_closed_without_settlement'
          when proof.id is null then 'execution_not_confirmed'
          when source.attempts>1 then 'ambiguous_provider_retry'
          when not coalesce(source.finished,false) or coalesce(source.interrupted,false) or not coalesce(source.matched_attempt,false) then 'source_not_settled'
          when e.payload->'usage'->>'quality'<>'complete' or coalesce((e.payload->'usage'->'tokens'->>'total')::numeric,0)<=0 then 'missing_or_incomplete_usage'
          when proof.payload->>'outcome' in ('completed','cancelled') then 'accepted_execution'
          else 'execution_not_confirmed' end as eligibility_reason
      ) decision
    ) q;
  `.execute(db);
}

export async function down(): Promise<void> {
  throw new Error("Model charge evidence requires a drained matched release");
}
