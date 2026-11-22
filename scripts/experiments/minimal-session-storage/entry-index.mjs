// Only installed in disposable comparison databases.
export const studyEntryIndexSql =
  "create index study_lane_entries on messages(tenant_id,pi_session_id,conversation_id,entry_id) where entry_id is not null";
