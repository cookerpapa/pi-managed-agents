import { receivingConversationRunId } from "@pi-cloud/runtime-core/run-readiness";
import { type Kysely } from "kysely";
import type { Database } from "@pi-cloud/database";
import { type ConversationStatusResource, parseModelRuntimeSnapshot } from "@pi-cloud/protocol";
import { readNativeContextEstimate } from "@pi-cloud/pi-session-postgres/durable";
import { catalogModel } from "./runtime-model-catalog.ts";
import { ControlPlaneStoreError } from "./control-plane-store-error.ts";

export async function readConversationStatus(
  database: Kysely<Database>,
  tenantId: string,
  sessionId: string,
): Promise<ConversationStatusResource> {
  if (!database.isTransaction)
    return database
      .transaction()
      .setIsolationLevel("repeatable read")
      .execute((tx) => readConversationStatus(tx, tenantId, sessionId));
  const session = await database
    .selectFrom("sessions as s")
    .innerJoin("model_profiles as m", (join) =>
      join
        .onRef("m.tenant_id", "=", "s.tenant_id")
        .onRef("m.id", "=", "s.desired_model_profile_id"),
    )
    .select(["s.id", "s.pi_session_id", "s.pi_conversation_id", "m.provider", "m.model_id"])
    .where("s.tenant_id", "=", tenantId)
    .where("s.id", "=", sessionId)
    .where("s.pruned_at", "is", null)
    .executeTakeFirst();
  if (!session) throw new ControlPlaneStoreError("not_found", "Conversation was not found");
  const active = await database
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

  const context = await readNativeContextEstimate(database, {
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
