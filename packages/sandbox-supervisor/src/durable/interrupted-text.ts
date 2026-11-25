import type { AssistantMessage, TextContent, ToolResultMessage } from "@earendil-works/pi-ai";
import type { Context } from "@earendil-works/chord";
import type { ContextView, Conversation, EntryRecord } from "@earendil-works/pi-durable";
import { isHostedToolBlock } from "../provider-hosted-transcript.ts";
import { providerTextPhase } from "./provider-response.ts";

export const INTERRUPTED_TEXT_ENTRY = "pi-cloud.interrupted-text-context";
export const INTERRUPTION_MARKER = "[Reply interrupted]";

/** A context-only representation, never a provider receipt or a new completion.
 * Retain ordinary text and its phase, with fresh deterministic message identities.
 * Provider reasoning, Tool carriers, annotations and replay signatures stay out. */
export function interruptedTextMessage(entry: EntryRecord): AssistantMessage | undefined {
  if (entry.kind !== "pi.assistant" || entry.model?.length !== 1) return;
  const source = entry.model[0];
  if (source?.role !== "assistant" || source.stopReason !== "aborted") return;
  const content: TextContent[] = [];
  for (const block of source.content) {
    if (block.type !== "text" || isHostedToolBlock(block) || !block.text) continue;
    const phase = providerTextPhase(block);
    content.push({
      type: "text",
      text: block.text,
      ...(phase
        ? {
            textSignature: JSON.stringify({
              v: 1,
              id: `msg_pc_interrupted_${entry.id}_${content.length}`,
              phase,
            }),
          }
        : {}),
    });
  }
  if (!content.some((block) => block.text.trim())) return;
  content[content.length - 1]!.text += `\n\n${INTERRUPTION_MARKER}`;
  return {
    role: "assistant",
    api: source.api,
    provider: source.provider,
    model: source.model,
    timestamp: source.timestamp,
    usage: structuredClone(source.usage),
    content,
    stopReason: "stop",
  };
}

const BASH_STOP_ENTRY = "pi-cloud.bash-stop-context";

function stoppedBashMessage(
  entry: EntryRecord,
  userCancelled: boolean,
): ToolResultMessage | undefined {
  const source = entry.model?.[0];
  if (
    entry.kind !== "pi.tool-result" ||
    source?.role !== "toolResult" ||
    source.toolName !== "bash" ||
    !source.isError
  )
    return;
  if (!entry.data || typeof entry.data !== "object" || Array.isArray(entry.data)) return;
  const diagnostics = entry.data.diagnostics;
  if (
    !Array.isArray(diagnostics) ||
    !diagnostics.some(
      (d) =>
        d &&
        typeof d === "object" &&
        !Array.isArray(d) &&
        d.code === "aborted" &&
        d.message === "Tool bash was aborted",
    )
  )
    return;
  const last = source.content.at(-1);
  if (last?.type !== "text") return;
  // Only change Pi's rendered diagnostic. stdout, attachments and other diagnostics stay intact.
  const nativeEnding = "[error] Tool bash was aborted\n</harness>";
  if (!last.text.endsWith(nativeEnding)) return;
  const reason = userCancelled
    ? "The user pressed Stop. Cancellation of this Bash command was requested."
    : "The agent stopped waiting for this Bash command.";
  const text =
    last.text.slice(0, -nativeEnding.length) +
    "[error] " +
    reason +
    " No final execution result was confirmed. If asked to continue, check the process, its effects or logs before running it again.\n</harness>";
  return { ...source, content: [...source.content.slice(0, -1), { ...last, text }] };
}

/** Apply once on the native transaction line after cancellation, or before a
 * cold Conversation can generate/compact. Inspect only its native active suffix.
 * A pre-existing edit wins, including omissions and edits inherited by a fork. */
export async function preserveInterruptedText(options: {
  conversation: Conversation;
  assertCurrent(): Promise<void>;
  context: Context;
  view?: ContextView;
  userCancelledBash?: ReadonlySet<number>;
}): Promise<ContextView> {
  const { conversation, context } = options;
  let view = options.view ?? (await conversation.context(context));
  for (;;) {
    const edited = new Set(
      view.entries.flatMap((entry) => entry.edits?.map((edit) => edit.target) ?? []),
    );
    const candidates: {
      entry: EntryRecord;
      message: AssistantMessage | ToolResultMessage;
      kind: string;
    }[] = [];
    const lastByTask = new Map<number, EntryRecord>();
    for (const entry of view.entries) {
      if (entry.kind === "pi.assistant" && entry.byTaskId !== undefined) {
        lastByTask.set(entry.byTaskId, entry);
      } else if (entry.kind === "pi.tool-result" && !edited.has(entry.id)) {
        const message = stoppedBashMessage(
          entry,
          entry.byTaskId !== undefined && options.userCancelledBash?.has(entry.byTaskId) === true,
        );
        if (message) candidates.push({ entry, message, kind: BASH_STOP_ENTRY });
      }
    }
    for (const entry of lastByTask.values()) {
      if (edited.has(entry.id)) continue;
      const message = interruptedTextMessage(entry);
      if (message) candidates.push({ entry, message, kind: INTERRUPTED_TEXT_ENTRY });
    }
    if (!candidates.length) return view;
    await options.assertCurrent();
    const tail = view.entries.reduce((id, entry) => Math.max(id, entry.id), 0);
    let changed = false;
    let stale = false;
    await conversation.commit(async (tx) => {
      const current = await tx.scanEntries({ conversationId: conversation.id }, 1);
      if ((current.items[0]?.id ?? 0) !== tail) {
        stale = true;
        return;
      }
      const eligible: typeof candidates = [];
      for (const candidate of candidates) {
        const { entry } = candidate;
        if (entry.byTaskId === undefined) continue;
        const task = await tx.task(entry.byTaskId);
        if (
          task?.conversationId !== entry.conversationId ||
          task.state.status !== "terminal" ||
          task.state.outcome.status !== "aborted" ||
          (candidate.kind === BASH_STOP_ENTRY
            ? task.kind !== "pi.tool"
            : task.kind !== "pi.generation")
        )
          continue;
        eligible.push(candidate);
      }
      for (const { entry, message, kind } of eligible) {
        await tx.appendEntry(conversation.id, {
          kind,
          edits: [{ target: entry.id, action: "replace", messages: [message] }],
        });
        changed = true;
      }
    }, context);
    if (!stale && !changed) return view;
    view = await conversation.context(context);
    if (!stale) return view;
  }
}
