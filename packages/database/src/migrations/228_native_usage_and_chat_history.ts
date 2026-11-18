import { sql, type Kysely, type QueryExecutorProvider } from "kysely";

export const migrationName = "228_native_usage_and_chat_history";

/** Old receipts authenticate the former journal and Fork roots describe frozen
 * frame ranges. Check before taking services offline and again at cutover. */
export async function assertProjectionHistoryCompatible(db: QueryExecutorProvider): Promise<void> {
  const {
    rows: [legacy],
  } = await sql<{ usage: boolean; frames: boolean }>`select exists (
    select 1 from pi_durable_owned_records r where r.kind='document'
      and r.fields->>'scope' like '["conversation",%'
      and r.fields->>'historyPolicy'='latest' and r.fields->>'nativeKind'='"pi.usage"'
      and (not exists (select 1 from pi_durable_current c where (c.tenant_id,c.session_id,c.id)=(r.tenant_id,r.session_id,r.id))
        or exists (select 1 from pi_durable_document_revisions d where (d.tenant_id,d.session_id,d.document_id)=(r.tenant_id,r.session_id,r.id)))
  ) as usage, exists (
    select 1 from pi_history_frames f join pi_history_records g
      on g.tenant_id=f.tenant_id and g.id=f.entry_record_id where g.fields->>'nativeKind'='"pi.system"'
  ) as frames`.execute(db);
  if (legacy?.usage || legacy?.frames) {
    const formats = [
      legacy.usage && "revision-backed or incomplete pi.usage",
      legacy.frames && "pi.system chat frames",
    ].filter(Boolean);
    throw new Error(
      "Retire obsolete development conversations through normal deletion and collect shared references before cutover: " +
        formats.join(", "),
    );
  }
}

export async function up<DB>(db: Kysely<DB>): Promise<void> {
  await sql`
    lock table runs, session_leases, pi_sessions in share row exclusive mode;
    do $$ begin
      if exists (select 1 from session_leases where released_at is null)
        or exists (select 1 from runs where state not in ('completed','cancelled','failed','timed_out')
          or recovery_requested_at is not null or (output_seal_id is not null and output_sealed_at is null))
      then raise exception 'Drain and seal native writers before the usage and chat history cutover'; end if;
    end $$`.execute(db);
  await assertProjectionHistoryCompatible(db);
}

export async function down(): Promise<void> {
  throw new Error("The previous native usage history is not retained");
}
