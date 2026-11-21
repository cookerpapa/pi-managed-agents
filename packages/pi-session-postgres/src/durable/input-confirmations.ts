import { NATIVE_HUMAN_INPUT_DOCUMENT, type NativeInputConfirmation } from "@pi-cloud/protocol";
import type { CurrentStateRow } from "./current-state.ts";
import type { DurableScope } from "./types.ts";

type State = {
  conversationId: number;
  runId: string;
  inputs: Record<string, { nativeId: number | null; fingerprint: string }>;
  receipts: Record<string, { fingerprint: string }>;
};

function state(row: CurrentStateRow | undefined): State | undefined {
  if (!row || row.kind !== "document" || row.retired_seq !== null) return;
  const write = JSON.parse(row.body);
  if (write.record?.kind !== NATIVE_HUMAN_INPUT_DOCUMENT) return;
  if (write.type !== "document.create" || write.content?.kind !== "base")
    throw new Error("Input confirmation requires materialized native state");
  const value = write.content.value as State;
  if (value.runId !== write.record.key || !value.inputs || !value.receipts)
    throw new Error("Input confirmation scope changed");
  if (
    write.record.scope?.kind !== "conversation" ||
    !Number.isSafeInteger(write.record.scope.conversationId)
  )
    throw new Error("Input confirmation has no native Conversation");
  return { ...value, conversationId: write.record.scope.conversationId };
}

export type ProjectedInputConfirmation = Omit<NativeInputConfirmation, "sessionId"> & {
  conversationId: number;
};

/** Pure extraction inside the projection transaction; callers publish after COMMIT. */
export function collectInputConfirmations(
  scope: DurableScope,
  rows: readonly CurrentStateRow[],
  previous: readonly CurrentStateRow[],
): ProjectedInputConfirmation[] {
  const before = new Map(previous.map((row) => [row.id, row]));
  const result: ProjectedInputConfirmation[] = [];
  for (const row of rows) {
    const next = state(row);
    if (!next) continue;
    const old = state(before.get(row.id));
    result.push(...changedInputConfirmations(scope.tenantId, next, old));
  }
  return result;
}

export function changedInputConfirmations(
  tenantId: string,
  next: State,
  old?: State,
): ProjectedInputConfirmation[] {
  const result: ProjectedInputConfirmation[] = [];
  const base = {
    tenantId,
    conversationId: next.conversationId,
    runId: next.runId,
  };
  for (const [operationId, input] of Object.entries(next.inputs))
    if (input.nativeId !== null && old?.inputs[operationId]?.nativeId !== input.nativeId)
      result.push({ ...base, action: "submit", operationId, fingerprint: input.fingerprint });
  for (const [operationId, receipt] of Object.entries(next.receipts))
    if (!old?.receipts[operationId])
      result.push({ ...base, action: "mutate", operationId, fingerprint: receipt.fingerprint });
  return result;
}
