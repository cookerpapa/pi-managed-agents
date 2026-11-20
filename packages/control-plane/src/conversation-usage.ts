import { sql, type Kysely } from "kysely";
import type { Database } from "@pi-cloud/database";
import { addUsageTokens, type ConversationUsageResource } from "@pi-cloud/protocol";
import { readSessionModelUsage } from "@pi-cloud/accounting";
import { ControlPlaneStoreError } from "./control-plane-store-error.ts";

export async function readConversationUsage(
  database: Kysely<Database>,
  tenantId: string,
  sessionId: string,
): Promise<ConversationUsageResource> {
  if (!database.isTransaction)
    return database
      .transaction()
      .setIsolationLevel("repeatable read")
      .execute((tx) => readConversationUsage(tx, tenantId, sessionId));
  const family = await sql<{ id: string; title: string }>`with recursive family as (
    select id,title from sessions where tenant_id=${tenantId}::uuid and id=${sessionId}::uuid and pruned_at is null
    union
    select c.id,c.title from family f join subagent_conversations l on l.tenant_id=${tenantId}::uuid and l.parent_session_id=f.id
    join sessions c on c.id=l.child_session_id and c.tenant_id=l.tenant_id and c.pruned_at is null
    join runs r on r.id=l.root_run_id and r.tenant_id=l.tenant_id and r.pruned_at is null
  ) select id,title from family order by id`.execute(database);
  if (!family.rows.length)
    throw new ControlPlaneStoreError("not_found", "Conversation was not found");
  const totals = await readSessionModelUsage(
    database,
    tenantId,
    family.rows.map((r) => r.id),
  );
  const own = totals.get(sessionId)!;
  const children = family.rows
    .filter((r) => r.id !== sessionId)
    .map((r) => {
      const child = totals.get(r.id)!;
      return {
        sessionId: r.id,
        title: r.title,
        tokens: child.tokens,
        unconfirmedRequests: child.unconfirmedRequests,
      };
    });
  const includingChildren = { ...own.tokens };
  for (const child of children) addUsageTokens(includingChildren, child.tokens);
  return {
    sessionId,
    source: "provider_receipts",
    observedAt: new Date().toISOString(),
    tokens: own.tokens,
    unconfirmedRequests: own.unconfirmedRequests,
    models: own.models,
    children,
    includingChildren,
  };
}
