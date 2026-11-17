import type { Kysely } from "kysely";
import type { Database } from "@pi-cloud/database";
import type { ModelMeteringContext } from "@pi-cloud/protocol";
import { receiptHash, MeteringConflict } from "./receipt-identity.ts";

export async function ensureModelInvocation(
  database: Kysely<Database>,
  context: ModelMeteringContext,
): Promise<void> {
  const fingerprint = receiptHash(context);
  const inserted = await database
    .insertInto("model_invocations")
    .values({
      id: context.invocationId,
      tenant_id: context.tenantId,
      session_id: context.sessionId,
      run_id: context.runId,
      requested_at: context.requestedAt,
      provider: context.provider,
      model: context.model,
      purpose: context.purpose,
      context,
      context_hash: fingerprint,
    })
    .onConflict((conflict) => conflict.column("id").doNothing())
    .returning("id")
    .executeTakeFirst();
  if (inserted) return;
  const existing = await database
    .selectFrom("model_invocations")
    .select("context_hash")
    .where("id", "=", context.invocationId)
    .executeTakeFirstOrThrow();
  if (existing.context_hash !== fingerprint)
    throw new MeteringConflict("Model invocation identity changed");
}
