import { sql, type Kysely } from "kysely";
import type { Database } from "@pi-cloud/database";
import {
  emptyUsageTokens,
  addUsageTokens,
  type ConversationUsageResource,
  type UsageTokens,
} from "@pi-cloud/protocol";
// Callers provide public Session IDs authorized for this tenant.
export async function readSessionModelUsage(
  database: Kysely<Database>,
  tenantId: string,
  sessionIds: string[],
) {
  // One row per actual execution. If its attempt and usage have both arrived,
  // prefer the usage. A request with only a start still contributes one gap.
  const { rows } = await sql<{
    session_id: string;
    provider: string | null;
    model: string | null;
    missing: string;
    input: string;
    output: string;
    cache_read: string;
    cache_write: string;
    total: string;
  }>`with attempts as (
    select distinct on(i.id,e.execution_id) i.id,i.session_id,e.payload->'usage' as u
    from model_invocations i left join model_metering_events e on e.invocation_id=i.id and e.kind in ('attempt','usage')
    where i.tenant_id=${tenantId}::uuid and i.session_id=any(${sessionIds}::uuid[])
    order by i.id,e.execution_id,(e.kind='usage') desc
  ), measured as (
    select id,session_id,case when u->>'quality'='complete' and (u->'tokens'->>'total')::numeric>0 then u end as receipt from attempts
  ), buckets as (
    select id,session_id,receipt->>'provider' as provider,receipt->>'model' as model,receipt->'tokens' as t from measured
  ) select session_id,provider,model,count(distinct id) filter(where t is null)::text as missing,
    sum((t->>'input')::numeric+(t->>'cacheRead')::numeric+(t->>'cacheWrite')::numeric)::text as input,
    sum((t->>'output')::numeric+(t->>'reasoning')::numeric)::text as output,
    sum((t->>'cacheRead')::numeric)::text as cache_read,sum((t->>'cacheWrite')::numeric)::text as cache_write,
    sum((t->>'total')::numeric)::text as total
    from buckets group by session_id,provider,model order by session_id,provider,model`.execute(
    database,
  );
  const totals = new Map(
    sessionIds.map((id) => [
      id,
      {
        tokens: emptyUsageTokens(),
        unconfirmedRequests: 0,
        models: [] as ConversationUsageResource["models"],
      },
    ]),
  );
  for (const r of rows) {
    const item = totals.get(r.session_id)!;
    if (r.provider === null || r.model === null) {
      item.unconfirmedRequests = Number(r.missing);
      continue;
    }
    const tokens: UsageTokens = {
      input: Number(r.input),
      output: Number(r.output),
      cacheRead: Number(r.cache_read),
      cacheWrite: Number(r.cache_write),
      total: Number(r.total),
    };
    addUsageTokens(item.tokens, tokens);
    item.models.push({ provider: r.provider, modelId: r.model, tokens });
  }
  return totals;
}
