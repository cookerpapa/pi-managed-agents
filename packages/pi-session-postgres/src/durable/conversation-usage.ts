import { sql, type Kysely } from "kysely";
import type { Database } from "@pi-cloud/database";
import {
  emptyUsageTokens,
  addUsageTokens,
  type UsageTokens,
  type ImagePacked,
} from "@pi-cloud/protocol";
import type { ConversationId, EntryRecord, StorageWrite } from "@earendil-works/pi-durable";
import type { Message, Usage } from "@earendil-works/pi-ai";
import {
  estimateMessageTokens,
  calculateContextTokens,
} from "@earendil-works/pi-ai/utils/estimate";
import { readDurableEntries } from "./history.ts";
import { documentAddress, type DurableScope } from "./types.ts";
import { NATIVE_USAGE_DOCUMENT } from "./current-state.ts";

function tokens(value: unknown): UsageTokens {
  if (typeof value !== "object" || value === null) throw new Error("Native usage is invalid");
  const usage = value as Usage;
  const counts = [usage.input, usage.output, usage.cacheRead, usage.cacheWrite];
  if (!counts.every((n) => Number.isSafeInteger(n) && n >= 0))
    throw new Error("Native token count is invalid");
  const input = usage.input + usage.cacheRead + usage.cacheWrite;
  return {
    input,
    output: usage.output,
    cacheRead: usage.cacheRead,
    cacheWrite: usage.cacheWrite,
    total: input + usage.output,
  };
}
export function nativeUsageTotals(value: unknown) {
  const state = value as
    { models?: Record<string, unknown>; tools?: Record<string, unknown> } | undefined;
  const total = emptyUsageTokens();
  const models = Object.entries(state?.models ?? {}).map(([key, usage]) => {
    const at = key.indexOf("/");
    const item = { provider: key.slice(0, at), modelId: key.slice(at + 1), tokens: tokens(usage) };
    addUsageTokens(total, item.tokens);
    return item;
  });
  for (const usage of Object.values(state?.tools ?? {})) addUsageTokens(total, tokens(usage));
  return { tokens: total, models };
}

function isUsageBucket(value: unknown): boolean {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/** Batch the current, unkeyed UsageDoc of each requested native Conversation. */
export async function readNativeUsageBatch(
  database: Kysely<Database>,
  scope: DurableScope,
  conversations: readonly number[],
) {
  const result = new Map<number, ReturnType<typeof nativeUsageTotals>>();
  if (!conversations.length) return result;
  const addresses = conversations.map((conversationId) =>
    documentAddress({
      kind: NATIVE_USAGE_DOCUMENT,
      scope: { kind: "conversation", conversationId: conversationId as ConversationId },
    }),
  );
  const { rows } = await sql<{
    id: string;
    address: string;
    body: string | null;
  }>`select r.id::text,r.fields->>'address' as address,c.body
    from pi_durable_owned_records r left join pi_durable_current c
      on (c.tenant_id,c.session_id,c.id,c.seq)=(r.tenant_id,r.session_id,r.id,r.seq)
      and c.kind='document' and c.retired_seq is null
    where r.tenant_id=${scope.tenantId}::uuid and r.session_id=${scope.sessionId}
      and r.kind='document' and r.retired_seq is null and r.fields->>'address'=any(${addresses}::text[])`.execute(
    database,
  );
  for (const row of rows) {
    if (row.body === null) throw new Error("Native usage checkpoint is missing");
    const write = JSON.parse(row.body) as Extract<StorageWrite, { type: "document.create" }>;
    if (
      write.type !== "document.create" ||
      write.record?.scope?.kind !== "conversation" ||
      write.record.kind !== NATIVE_USAGE_DOCUMENT ||
      String(write.record.id) !== row.id ||
      documentAddress(write.record) !== row.address ||
      write.content?.kind !== "base" ||
      !isUsageBucket(write.content.value?.models) ||
      !isUsageBucket(write.content.value?.tools)
    ) {
      throw new Error("Invalid native usage checkpoint");
    }
    const id = write.record.scope.conversationId;
    if (result.has(id)) throw new Error("Duplicate native usage document");
    result.set(id, nativeUsageTotals(write.content.value));
  }
  return result;
}

/** Read-only estimate of the native active range. Opaque image blocks retain
 * Pi's fixed estimation weight; their data is never hydrated or read here. */
export function estimateStoredContext(
  entries: readonly ImagePacked<EntryRecord>[],
  model: { provider: string; modelId: string },
) {
  const ordered = [...entries].sort((a, b) => a.id - b.id);
  const head = ordered.findLast((entry) => entry.head !== undefined);
  const range = head ? ordered.filter((entry) => entry.id >= head.head!) : ordered;
  const edits = new Map(
    range.flatMap((entry) => (entry.edits ?? []).map((edit) => [edit.target, edit] as const)),
  );
  const active = head ? [head, ...range.filter((entry) => entry.head === undefined)] : range;
  const contributed = active.flatMap((entry) => {
    const edit = edits.get(entry.id);
    if (edit?.action === "omit") return [];
    const content = edit?.action === "replace" ? edit.messages : (entry.model ?? []);
    return content
      .filter(
        (message) =>
          message.role !== "assistant" ||
          !["aborted", "error", "deferred"].includes(message.stopReason),
      )
      .map((message) => ({ entryId: entry.id, message: message as unknown as Message }));
  });
  const messages: typeof contributed = [];
  for (let index = 0; index < contributed.length; index++) {
    const item = contributed[index]!,
      message = item.message;
    if (message.role === "toolResult") continue;
    messages.push(item);
    if (message.role !== "assistant") continue;
    const results = new Map<string, typeof item>();
    for (
      let next = index + 1;
      next < contributed.length && contributed[next]!.message.role !== "assistant";
      next++
    ) {
      const candidate = contributed[next]!;
      if (candidate.message.role === "toolResult" && !results.has(candidate.message.toolCallId))
        results.set(candidate.message.toolCallId, candidate);
    }
    for (const call of message.content.filter((part) => part.type === "toolCall"))
      messages.push(
        results.get(call.id) ?? {
          entryId: item.entryId,
          message: {
            role: "toolResult",
            toolCallId: call.id,
            toolName: call.name,
            content: [
              {
                type: "text",
                text: "Tool result unavailable: history ends before this call completed.",
              },
            ],
            isError: true,
            timestamp: message.timestamp,
          },
        },
      );
  }
  // Native compaction selects by Entry IDs. A response may have a timestamp
  // newer than a concurrently generated summary while still preceding its
  // placement; that old response must not restore the pre-compaction baseline.
  let baseline = -1,
    usageTokens = 0;
  for (let index = 0; index < messages.length; index++) {
    const item = messages[index]!,
      message = item.message;
    if (
      message.role === "assistant" &&
      (!head || item.entryId > head.id) &&
      message.provider === model.provider &&
      message.model === model.modelId
    ) {
      const tokens = calculateContextTokens(message.usage);
      if (tokens > 0) {
        baseline = index;
        usageTokens = tokens;
      }
    }
  }
  const trailing = messages
    .slice(baseline + 1)
    .reduce((sum, item) => sum + estimateMessageTokens(item.message), 0);
  return {
    estimatedTokens: usageTokens + trailing,
    method: baseline < 0 ? ("structural" as const) : ("latest_usage" as const),
  };
}
export async function readNativeContextEstimate(
  database: Kysely<Database>,
  input: DurableScope & { conversationId: number; provider: string; modelId: string },
) {
  const entries = await readDurableEntries(database, {
    ...input,
    activeOnly: true,
    limit: Number.MAX_SAFE_INTEGER,
  });
  return estimateStoredContext(
    entries.map((x) => x.entry),
    input,
  );
}
