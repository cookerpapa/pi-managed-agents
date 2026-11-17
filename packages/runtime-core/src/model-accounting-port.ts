import type { Transaction } from "kysely";
import type { Database } from "@pi-cloud/database";
import type { ModelExecutionEvidence } from "@pi-cloud/protocol";
import type { AcceptedFactScope } from "./accepted-fact.ts";
import type { FactPosition } from "./accepted-fact-recovery.ts";

/** Bounded financial facts in the existing PG commit; no metering RPC or prices. */
export interface ModelAccountingPort {
  settle(
    tx: Transaction<Database>,
    scope: AcceptedFactScope,
    evidence: readonly ModelExecutionEvidence[],
    position: FactPosition,
  ): Promise<void>;
  closeWriter(
    tx: Transaction<Database>,
    scope: AcceptedFactScope,
    position: FactPosition,
  ): Promise<void>;
}
