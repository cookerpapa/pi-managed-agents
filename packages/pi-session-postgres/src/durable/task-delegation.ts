import { BACKGROUND_CONTEXT as ctx } from "@earendil-works/chord/context";
import type {
  ConversationId,
  EntryId,
  EntryRecord,
  Storage,
  TaskId,
} from "@earendil-works/pi-durable";
import { durableToolState, type OriginalControlToolIntent } from "@pi-cloud/protocol";
import { delegatedRunId } from "@pi-cloud/protocol/delegation-identity";

/** Read only the addressed task, source assistant entry and frozen Run document
 * from the PG-hydrated native store. No transport ID is needed from the old boot. */
export async function restoredDelegationIntent(
  store: Pick<Storage, "task" | "entry" | "findDocument" | "document"> & {
    recordSequence(id: number): Promise<number | undefined>;
  },
  scope: { tenantId: string; piSessionId: string; runId: string },
  conversationId: number,
  taskId: number,
): Promise<OriginalControlToolIntent> {
  return nativeDelegationIntent(
    scope,
    conversationId,
    taskId,
    await store.task(taskId as TaskId, ctx),
    {
      sequence: () => store.recordSequence(taskId),
      assistant: async (id) =>
        (await store.entry(conversationId as ConversationId, id as EntryId, ctx))?.entry,
      frozenRun: async () => {
        const doc = await store.findDocument(
          {
            kind: "pi-cloud.run",
            scope: { kind: "conversation", conversationId: conversationId as ConversationId },
          },
          "current",
          ctx,
        );
        return doc ? (await store.document(doc.id, "current", ctx))?.value : undefined;
      },
    },
  );
}

/** Shared by the owning Worker and the Projector checking its reattachment. */
export async function nativeDelegationIntent(
  scope: { tenantId: string; piSessionId: string; runId: string },
  conversationId: number,
  taskId: number,
  record: Awaited<ReturnType<Storage["task"]>>,
  read: {
    sequence(): Promise<number | undefined>;
    assistant(id: number): Promise<EntryRecord | undefined>;
    frozenRun(): Promise<unknown>;
  },
): Promise<OriginalControlToolIntent> {
  const task = durableToolState(record);
  const checkpoint = record?.state.checkpoint as { replay?: string } | undefined;
  const assistant = (record?.input as { assistant?: number } | undefined)?.assistant;
  if (
    !task?.intent ||
    task.id !== taskId ||
    task.conversationId !== conversationId ||
    checkpoint?.replay !== "safe" ||
    !Number.isSafeInteger(assistant)
  )
    throw Error("No active replay-safe native delegation");
  const entry = await read.assistant(assistant!);
  const message = entry?.model?.[0];
  const callId = task.intent.callId;
  const call =
    message?.role === "assistant"
      ? message.content.find((block) => block.type === "toolCall" && block.id === callId)
      : undefined;
  if (
    entry?.kind !== "pi.assistant" ||
    entry.id !== assistant ||
    entry.conversationId !== conversationId ||
    call?.type !== "toolCall" ||
    call.name !== "subagent"
  )
    throw Error("Delegation does not match its native assistant call");
  const frozen = (await read.frozenRun()) as
    { runId?: string; baseEntry?: number | null } | undefined;
  if (!frozen || frozen.runId !== scope.runId)
    throw Error("Native delegation belongs to a different Run");
  let contextAnchor: number | null = null;
  if (task.intent.arguments.context === "branch") {
    const anchor = frozen.baseEntry;
    if (
      anchor !== null &&
      (typeof anchor !== "number" || !Number.isSafeInteger(anchor) || anchor < 1)
    )
      throw Error("Delegation has no matching frozen parent context");
    contextAnchor = anchor;
  }
  const sequence = await read.sequence();
  if (!sequence || !Number.isSafeInteger(sequence) || sequence < 1)
    throw Error("Missing native delegation checkpoint sequence");
  return {
    sequence,
    operationId: delegatedRunId(scope, taskId),
    taskId,
    conversationId,
    toolName: "subagent",
    nativeCallId: task.intent.callId,
    arguments: task.intent.arguments,
    contextAnchor,
  };
}
