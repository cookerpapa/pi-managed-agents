import { sql, type Transaction } from "kysely";
import type { Database } from "@pi-cloud/database";
import type { MessageHistoryMutations } from "./conversation-tree-service.ts";
import { ControlPlaneStoreError } from "./control-plane-store-error.ts";

/** Matched PG schema. The public Session metadata and history pins commit together. */
export function segmentHistoryTransactions(schema: string): MessageHistoryMutations {
  const detach = async (
    tx: Transaction<Database>,
    tenant: string,
    source: string,
    dest: string | null,
    turn: string | null,
    entry: string | null,
  ) => {
    try {
      const {
        rows: [row],
      } = await sql<{
        members: string[];
        removed_turns: string;
      }>`select * from ${sql.id(schema, "detach_message_window")}(
        ${tenant}::uuid,${source}::uuid,${dest}::uuid,${turn}::uuid,1::bigint,${entry}::bigint,'web')`.execute(
        tx,
      );
      if (!row) throw Error("Missing history detach result");
      return row;
    } catch (error) {
      if (error && typeof error === "object" && "code" in error) {
        if (error.code === "PZ004")
          throw new ControlPlaneStoreError("conflict", "The conversation has unfinished work");
        if (error.code === "PZ005")
          throw new ControlPlaneStoreError("invalid_request", "The history anchor is unavailable");
      }
      throw error;
    }
  };
  return {
    schema,
    async fork(tx, tenant, source, target, request, key, fingerprint) {
      try {
        const {
          rows: [row],
        } = await sql<{ ok: boolean }>`select ${sql.id(schema, "fork_web_window")}(
          ${tenant}::uuid,${source}::uuid,${request.turnId}::uuid,${request.entryId}::bigint,${target}::uuid,${key},${fingerprint}) as ok`.execute(
          tx,
        );
        return row?.ok === true;
      } catch (error) {
        if (error && typeof error === "object" && "code" in error && error.code === "PZ005")
          throw new ControlPlaneStoreError(
            "idempotency_conflict",
            "The Fork request already addressed another or deleted conversation",
          );
        throw error;
      }
    },
    async prune(tx, t, s, r) {
      return Number(
        (await detach(tx, t, s, crypto.randomUUID(), r.turnId, r.entryId)).removed_turns,
      );
    },
    async retire(tx, t, s) {
      return (await detach(tx, t, s, null, null, null)).members;
    },
  };
}
