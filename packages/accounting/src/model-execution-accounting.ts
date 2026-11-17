import type { Transaction } from "kysely";
import type { Database } from "@pi-cloud/database";
import { parseModelExecutionEvidence, type ModelExecutionEvidence } from "@pi-cloud/protocol";
import { ensureModelInvocation } from "./model-invocations.ts";
import { receiptHash, MeteringConflict } from "./receipt-identity.ts";

type Scope = {
  tenantId: string;
  piSessionId: string;
  runId: string;
  writerId: string;
  fencingToken: number;
};
type Position = { topic: string; partition: number; offset: bigint };

/** The execution role supplies committed facts, never prices or charge decisions. */
export class ModelExecutionAccounting {
  async settle(
    tx: Transaction<Database>,
    scope: Scope,
    evidence: readonly ModelExecutionEvidence[],
    position: Position,
  ): Promise<void> {
    for (const raw of [...evidence].sort((a, b) =>
      a.context.invocationId.localeCompare(b.context.invocationId),
    )) {
      const item = parseModelExecutionEvidence(raw),
        c = item.context;
      if (
        c.tenantId !== scope.tenantId ||
        c.runId !== scope.runId ||
        c.piSessionId !== scope.piSessionId ||
        c.execution?.writerId !== scope.writerId ||
        c.execution.fencingToken !== scope.fencingToken
      )
        throw new MeteringConflict("Model settlement execution identity changed");
      await ensureModelInvocation(tx, c);
      const payload = {
        kind: "execution",
        context: c,
        outcome: item.outcome,
        position: {
          topic: position.topic,
          partition: position.partition,
          offset: position.offset.toString(),
        },
      };
      const id = `${c.invocationId}:execution`,
        hash = receiptHash(payload);
      const inserted = await tx
        .insertInto("model_metering_events")
        .values({
          id,
          invocation_id: c.invocationId,
          kind: "execution",
          execution_id: null,
          occurred_at: c.requestedAt,
          payload,
          payload_hash: hash,
          valuation: null,
        })
        .onConflict((conflict) => conflict.column("id").doNothing())
        .returning("id")
        .executeTakeFirst();
      if (!inserted) {
        const existing = await tx
          .selectFrom("model_metering_events")
          .select("payload_hash")
          .where("id", "=", id)
          .executeTakeFirstOrThrow();
        if (existing.payload_hash !== hash)
          throw new MeteringConflict("Model settlement receipt changed");
      }
    }
  }

  async closeWriter(tx: Transaction<Database>, scope: Scope, position: Position): Promise<void> {
    const hash = receiptHash({
      tenantId: scope.tenantId,
      piSessionId: scope.piSessionId,
      runId: scope.runId,
      writerId: scope.writerId,
      fencingToken: scope.fencingToken,
      position: { ...position, offset: position.offset.toString() },
    });
    const inserted = await tx
      .insertInto("model_execution_writers")
      .values({
        writer_id: scope.writerId,
        tenant_id: scope.tenantId,
        pi_session_id: scope.piSessionId,
        run_id: scope.runId,
        fencing_token: String(scope.fencingToken),
        topic: position.topic,
        partition: position.partition,
        close_offset: position.offset.toString(),
        fingerprint: hash,
      })
      .onConflict((conflict) => conflict.column("writer_id").doNothing())
      .returning("writer_id")
      .executeTakeFirst();
    if (!inserted) {
      const existing = await tx
        .selectFrom("model_execution_writers")
        .select("fingerprint")
        .where("writer_id", "=", scope.writerId)
        .executeTakeFirstOrThrow();
      if (existing.fingerprint !== hash)
        throw new MeteringConflict("Model writer closure evidence changed");
    }
  }
}
