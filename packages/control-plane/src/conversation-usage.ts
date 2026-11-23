import { sql, type Kysely } from "kysely";
import type { Database } from "@pi-cloud/database";
import { addUsageTokens, type ConversationUsageResource } from "@pi-cloud/protocol";
import { readSessionModelUsage } from "@pi-cloud/accounting";
import { ControlPlaneStoreError } from "./control-plane-store-error.ts";

export async function readConversationUsage(
  database: Kysely<Database>,
  tenantId: string,
  sessionId: string,
  delegatedRuns = false,
): Promise<ConversationUsageResource> {
  if (!database.isTransaction)
    return database
      .transaction()
      .setIsolationLevel("repeatable read")
      .execute((tx) => readConversationUsage(tx, tenantId, sessionId, delegatedRuns));
  type Member = { id: string; title: string };
  const query = delegatedRuns
    ? sql<Member>`with recursive family(root,id,title) as (
        (select id,id,title from sessions where tenant_id=${tenantId}::uuid and id=${sessionId}::uuid and pruned_at is null
         union all
         select s.id,a.id,coalesce(a.agent_name,left(a.initial_input,160))
         from runs a join sessions s on s.id=a.session_id and s.tenant_id=a.tenant_id and s.pruned_at is null
         join runs r on r.id=a.root_run_id and r.tenant_id=a.tenant_id and r.pruned_at is null
         where a.tenant_id=${tenantId}::uuid and a.id=${sessionId}::uuid and a.id=a.agent_id and a.pruned_at is null)
        union
        select f.root,a.id,coalesce(a.agent_name,left(a.initial_input,160)) from family f
        join runs caller on caller.tenant_id=${tenantId}::uuid and caller.session_id=f.root and coalesce(caller.agent_id,caller.session_id)=f.id
        join runs a on a.caller_run_id=caller.id and a.session_id=f.root and a.tenant_id=caller.tenant_id and a.id=a.agent_id and a.pruned_at is null
        join runs r on r.id=a.root_run_id and r.tenant_id=a.tenant_id and r.pruned_at is null
      ) select id,title from family order by id`
    : sql<Member>`with recursive family as (
    select id,title from sessions where tenant_id=${tenantId}::uuid and id=${sessionId}::uuid and pruned_at is null
    union
    select c.id,c.title from family f join subagent_conversations l on l.tenant_id=${tenantId}::uuid and l.parent_session_id=f.id
    join sessions c on c.id=l.child_session_id and c.tenant_id=l.tenant_id and c.pruned_at is null
    join runs r on r.id=l.root_run_id and r.tenant_id=l.tenant_id and r.pruned_at is null
  ) select id,title from family order by id`;
  const family = await query.execute(database);
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
