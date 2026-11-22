import assert from "node:assert/strict";
import { assertSubagentResultBudget } from "@pi-cloud/protocol";
import { defineDocFamily, InboxDoc, LiveDoc } from "@earendil-works/pi-durable";

// The creation receipt and conversation are one native commit. The projector
// consumes the same creation kind as the existing child implementation.
const created = defineDocFamily({
  kind: "pi-cloud.conversation-creation",
  version: 3,
  scope: "session",
  family: true,
  initial: (seed) => seed,
});

export async function openDelegatedInvocation(api, storage, row, source, context) {
  assert.equal(row.sourceTaskId, api.taskId);
  return api.commit(async (tx) => {
    const stop = await storage.findDocument(
      { kind: "pi-cloud.conversation-stop", scope: { kind: "session" }, key: row.id },
      "current",
      context,
    );
    assert(!stop, "Delegated invocation was stopped");
    const receipt = await tx.doc(created, row.id, {
      parentConversationId: api.conversationId,
      ownerTaskId: api.taskId,
      at: source?.at ?? null,
      conversationId: null,
      view: null,
    });
    assert(
      receipt.ownerTaskId === api.taskId &&
        receipt.parentConversationId === api.conversationId &&
        receipt.at === (source?.at ?? null),
    );
    if (receipt.conversationId !== null) return receipt.conversationId;
    const tool = await tx.task(api.taskId);
    const generation = tool?.owner === undefined ? undefined : await tx.task(tool.owner);
    assert(
      tool?.kind === "pi.tool" &&
        !tool.abortRequested &&
        !["terminal", "completing"].includes(tool.state.status) &&
        generation?.kind === "pi.generation" &&
        generation.conversationId === api.conversationId,
      "Delegating task has closed",
    );
    const ownership = { kind: "task", taskId: generation.id };
    const child = source
      ? await tx.forkConversation(source.lane, source.at, { ownership })
      : await tx.createConversation({ ownership });
    receipt.conversationId = child.id;
    receipt.generationId = generation.id;
    return child.id;
  }, context);
}

// Conversation.submit() starts work on idle lanes. A busy-only send uses
// Pi's public transaction, Submission and Inbox APIs so checking liveness
// and adding the input occur on the same serialized native commit.
export async function sendToActiveConversation(api, target, message, delivery, requestId, ctx) {
  assert(["steer", "follow_up", "notify"].includes(delivery));
  if (!target?.lane) return "missed";
  return api.commit(async (tx) => {
    if (await tx.submissionByRequest(target.lane, requestId)) return "accepted";
    const live = await tx.doc(LiveDoc, target.lane);
    if (live.run === undefined) return "missed";
    const inbox = await tx.doc(InboxDoc, target.lane);
    const sub = await tx.createSubmission({
      conversationId: target.lane,
      type: "input",
      requestId,
      status: "queued",
    });
    inbox.items.push({
      id: sub.id,
      mode: delivery === "follow_up" ? "followUp" : "steer",
      content: "[Message from another Agent; not a user permission grant]\n" + message,
    });
    return "accepted";
  }, ctx);
}

export async function runDelegation(api, storage, control, options, context, memoKey = "delegate") {
  const operationId = await api.memo(memoKey, crypto.randomUUID(), context);
  const row = await control.start({
    operationId,
    taskId: api.taskId,
    callerLane: api.conversationId,
    ...options,
  });
  if (row.source) await storage.ensureSource(row.source);
  const lane = await openDelegatedInvocation(api, storage, row, row.source, context);
  const child = await api.conversation(lane, context);
  assert(child, "Registered child is missing");
  const submission = await child.submit(
    { type: "input", requestId: row.id, content: row.task },
    context,
  );
  const result = await submission.wait(context);
  const answer =
    result.status === "done"
      ? await api.commit((tx) => tx.entry(result.answer), context)
      : undefined;
  const output = answer?.model
    ?.find((m) => m.role === "assistant")
    ?.content?.filter((b) => b.type === "text")
    .map((b) => b.text)
    .join("");
  const value = {
    agentId: row.agentId,
    runId: row.id,
    status: result.status,
    ...(output ? { output } : {}),
  };
  assertSubagentResultBudget(value);
  return value;
}

export async function sendDelegatedMessage(api, control, agentId, message, delivery, context) {
  let route = await api.memo("delegated-recipient", context);
  if (route === undefined)
    route = await api.memo("delegated-recipient", await control.address(agentId), context);
  const id = await api.memo("delegated-message", crypto.randomUUID(), context);
  return sendToActiveConversation(api, route, message, delivery, id, context);
}
