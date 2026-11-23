import { CLOUD_TOOL_NAMES } from "@pi-cloud/protocol";
import type { Storage, EntryId, ConversationId } from "@earendil-works/pi-durable";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { PiTurnError } from "../pi-turn-runtime.ts";

export function directoryFailure(entry: unknown, lane: number): PiTurnError | undefined {
  const e = entry as
    | {
        kind?: string;
        conversationId?: number;
        model?: {
          role: string;
          toolName: string;
          isError?: boolean;
          details?: { code?: string; execution?: string; directory?: string };
        }[];
      }
    | undefined;
  if (e?.kind !== "pi.tool-result" || e.conversationId !== lane) return;
  const m = e.model?.[0],
    d = m?.details;
  if (
    m?.role !== "toolResult" ||
    !m.isError ||
    !CLOUD_TOOL_NAMES.some((n) => m.toolName === n) ||
    d?.code !== "workspace_directory_missing" ||
    d.execution !== "not_started" ||
    typeof d.directory !== "string"
  )
    return;
  return new PiTurnError(
    "workspace_directory_missing",
    `Working directory ${JSON.stringify(d.directory)} is unavailable. Choose a working directory or Workspace to continue.`,
    false,
  );
}

// A confirmed missing-directory result prevents a successor model request.
// On handoff only the current submission's last tool round can be outstanding.
export async function recoverDirectoryFailure(
  storage: Storage,
  lane: ConversationId,
  from: EntryId,
) {
  let cursor: import("@earendil-works/pi-durable").Cursor | undefined;
  do {
    const page = await storage.scanEntries(
      { conversationId: lane, minEntryId: from },
      32,
      cursor,
      BACKGROUND_CONTEXT,
    );
    for (const entry of page.items) {
      const error = directoryFailure(entry, lane);
      if (error) return error;
      if (
        entry.kind === "pi.assistant" &&
        entry.model?.some((m) => m.role === "assistant" && m.stopReason === "toolUse")
      )
        return;
    }
    cursor = page.next;
  } while (cursor);
}
