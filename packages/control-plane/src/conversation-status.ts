import { receivingConversationRunId } from "@pi-cloud/runtime-core/run-readiness";
import {
  readProjectedContextEstimate,
  readSegmentContextEstimate,
  type ProjectedContextAddress,
} from "./projected-context.ts";
import { sql, type Kysely } from "kysely";
import type { Database } from "@pi-cloud/database";
import { type ConversationStatusResource, parseModelRuntimeSnapshot } from "@pi-cloud/protocol";
import { readNativeContextEstimate } from "@pi-cloud/pi-session-postgres/durable";
import { catalogModel } from "./runtime-model-catalog.ts";
import { ControlPlaneStoreError } from "./control-plane-store-error.ts";

export async function readConversationStatus(
  database: Kysely<Database>,
  tenantId: string,
  sessionId: string,
  history: { messages: boolean; delegated: boolean; independent?: boolean } = {
    messages: false,
    delegated: false,
  },
): Promise<ConversationStatusResource> {
  if (!database.isTransaction)
    return database
      .transaction()
      .setIsolationLevel("repeatable read")
      .setAccessMode("read only")
      .execute((tx) => readConversationStatus(tx, tenantId, sessionId, history));
  if (history.independent) {
    const {
      rows: [settings],
    } = await sql<{ model: { provider: string; modelId: string; runtime: unknown } }>`
      select coalesce(t.model_snapshot,s.desired_model) as model from flow_session_tips s
        join public.sessions p on p.id=s.id and p.tenant_id=s.tenant_id and p.pruned_at is null
        left join lateral (select model_snapshot from turns t where t.tenant_id=s.tenant_id and t.session_id=s.flow_id
          and t.state in ('queued','running','settling','cancel_requested') order by t.mailbox_position limit 1) t on true
        where s.tenant_id=${tenantId}::uuid and s.id=${sessionId}::uuid`.execute(database);
    if (!settings) throw new ControlPlaneStoreError("not_found", "Conversation was not found");
    const { provider, modelId } = settings.model,
      model = { provider, modelId };
    const runtime = settings.model.runtime
      ? parseModelRuntimeSnapshot(settings.model.runtime)
      : catalogModel(provider, modelId);
    const context = await readSegmentContextEstimate(database, tenantId, sessionId, model);
    return {
      sessionId,
      observedAt: new Date().toISOString(),
      model,
      context: {
        ...context,
        window: runtime?.contextWindow ?? null,
        compactThreshold: runtime?.autoCompactTokenLimit ?? null,
      },
    };
  }
  // A named child is addressed by its founding Run. Each continuation has its own native lane.
  const named = history.delegated
    ? (
        await sql<{ id: string; root_session: string; root_run: string }>`
    select c.id,c.session_id root_session,c.root_run_id root_run from runs c
    join runs r on r.id=c.root_run_id and r.tenant_id=c.tenant_id and r.pruned_at is null
    where c.tenant_id=${tenantId}::uuid and c.agent_id=${sessionId}::uuid and c.pruned_at is null
      and not exists(select from runs n where n.tenant_id=c.tenant_id and n.previous_run_id=c.id and n.pruned_at is null)`.execute(
          database,
        )
      ).rows[0]
    : undefined;
  const session = await database
    .selectFrom("sessions as s")
    .innerJoin("model_profiles as m", (join) =>
      join
        .onRef("m.tenant_id", "=", "s.tenant_id")
        .onRef("m.id", "=", "s.desired_model_profile_id"),
    )
    .select([
      "s.id",
      "s.pi_session_id",
      "s.pi_conversation_id",
      "s.session_kind",
      "s.conversation_fork_entry_id",
      "m.provider",
      "m.model_id",
    ])
    .where("s.tenant_id", "=", tenantId)
    .where("s.id", "=", named?.root_session ?? sessionId)
    .where("s.pruned_at", "is", null)
    .executeTakeFirst();
  if (!session) throw new ControlPlaneStoreError("not_found", "Conversation was not found");
  let address: ProjectedContextAddress = {
    tenant: tenantId,
    publicRoot: session.id,
    physical: session.pi_session_id,
    fork: session.conversation_fork_entry_id !== null,
  };
  let childRootRun = named?.root_run;
  if (named) address.invocation = named.id;
  else if (history.messages && session.session_kind === "subagent") {
    const edge = await database
      .selectFrom("subagent_conversations as e")
      .innerJoin("sessions as root", (join) =>
        join.onRef("root.id", "=", "e.root_session_id").onRef("root.tenant_id", "=", "e.tenant_id"),
      )
      .select(["e.root_run_id", "e.root_session_id", "root.conversation_fork_entry_id"])
      .where("e.tenant_id", "=", tenantId)
      .where("e.child_session_id", "=", sessionId)
      .where("root.pruned_at", "is", null)
      .executeTakeFirst();
    if (!edge) throw new ControlPlaneStoreError("not_found", "Conversation was not found");
    childRootRun = edge.root_run_id;
    address = {
      ...address,
      publicRoot: edge.root_session_id,
      fork: edge.conversation_fork_entry_id !== null,
      childSession: session.id,
    };
  }
  const active = history.messages
    ? await database
        .selectFrom("runs")
        .select(["provider", "model_id", "model_runtime_snapshot"])
        .where("tenant_id", "=", tenantId)
        .where("session_id", "=", address.publicRoot)
        .where("pruned_at", "is", null)
        .where("id", "=", childRootRun ?? receivingConversationRunId(tenantId, address.publicRoot))
        .$if(!childRootRun, (q) =>
          q.where("state", "in", ["queued", "running", "settling", "cancel_requested"]),
        )
        .executeTakeFirst()
    : await database
        .selectFrom("conversation_turns as c")
        .innerJoin("runs as r", (join) =>
          join.onRef("r.tenant_id", "=", "c.tenant_id").onRef("r.id", "=", "c.id"),
        )
        .select(["r.provider", "r.model_id", "r.model_runtime_snapshot"])
        .where("c.tenant_id", "=", tenantId)
        .where("c.session_id", "=", sessionId)
        .where("c.state", "in", ["queued", "running", "settling", "cancel_requested"])
        .$if(session.pi_conversation_id === null || Number(session.pi_conversation_id) === 1, (q) =>
          q.where("r.id", "=", receivingConversationRunId(tenantId, sessionId)),
        )
        .orderBy("c.mailbox_position", "desc")
        .limit(1)
        .executeTakeFirst();
  const model = {
    provider: active?.provider ?? session.provider,
    modelId: active?.model_id ?? session.model_id,
  };
  const runtime = active?.model_runtime_snapshot
    ? parseModelRuntimeSnapshot(active.model_runtime_snapshot)
    : catalogModel(model.provider, model.modelId);
  const scope = { tenantId, sessionId: session.pi_session_id };

  const context = history.messages
    ? await readProjectedContextEstimate(database, address, model)
    : await readNativeContextEstimate(database, {
        ...scope,
        conversationId: Number(session.pi_conversation_id ?? 1),
        ...model,
      });
  return {
    sessionId,
    observedAt: new Date().toISOString(),
    model,
    context: {
      ...context,
      window: runtime?.contextWindow ?? null,
      compactThreshold: runtime?.autoCompactTokenLimit ?? null,
    },
  };
}
