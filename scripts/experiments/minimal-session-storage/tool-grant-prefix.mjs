import assert from "node:assert/strict";
import { isDeepStrictEqual } from "node:util";
import { resolveDurableDisplay } from "@pi-cloud/protocol";
import { foldLive } from "./moving-checkpoint.mjs";
import { operationsFor } from "./workspace-operations.mjs";

// A start description must refer to the exact argument write for this task.
export function matchingToolStart(fact, op) {
  const [group] = fact.display;
  if (
    fact.display.length !== 1 ||
    group.coveredThrough !== undefined ||
    group.events.length !== 1 ||
    !isDeepStrictEqual(group.scope, op.scope)
  )
    return;
  const [description] = group.events,
    { event, source } = description;
  if (
    event.type !== "tool.started" ||
    event.sessionId !== group.scope.sessionId ||
    event.turnId !== group.scope.turnId ||
    event.payload.toolName !== op.reply.toolName ||
    event.payload.toolCallId !== `task:${op.taskId}` ||
    source?.kind !== "write" ||
    !isDeepStrictEqual(source.path, ["value", "state", "checkpoint", "arguments"])
  )
    return;
  const write = fact.commit.writes[source.ordinal];
  if (write?.type === "task" && write.value.id === op.taskId) return description;
}

export function resolveToolStart(fact, description) {
  return resolveDurableDisplay(fact.commit, description, () =>
    assert.fail("Tool start accessed another document"),
  );
}

// Other native writes may carry a receipt, abort, policy change or order barrier.
export function plainExecution(fact, liveDocs, allowDisplay = false) {
  if (
    fact.kind !== "durable_commit" ||
    fact.tools.length ||
    fact.cancellations.length ||
    (fact.display.length && !allowDisplay) ||
    fact.modelEvidence?.length ||
    fact.inputQueueSessions?.length
  )
    return;
  const ops = operationsFor(fact),
    writes = fact.commit.writes;
  if (ops.length !== 1 || ops[0].action !== "execute" || writes.length < 1 || writes.length > 2)
    return;
  const op = ops[0],
    task = writes.find((w) => w.type === "task");
  if (!task || task.value.id !== op.taskId || task.value.abortRequested) return;
  if (fact.display.length && (writes.length !== 2 || !matchingToolStart(fact, op))) return;
  if (writes.length === 1) return op;
  // The native ToolTask can mark its own pi.live slot running in the intent
  // commit. Accept only that semantic change in its known Conversation document.
  const change = writes.find((w) => w.type === "document.change");
  const doc = change && liveDocs?.[change.id];
  if (!doc || doc.conversation !== op.conversationId || change.content.kind !== "delta") return;
  const slots = doc.value.tools;
  const slot = slots?.find(
    (s) =>
      s.taskId === op.taskId && s.callId === op.reply.toolCallId && s.name === op.reply.toolName,
  );
  if (slot?.status !== "pending") return;
  const expected = {
    ...doc.value,
    tools: slots.map((s) => (s === slot ? { ...slot, status: "running" } : s)),
  };
  if (isDeepStrictEqual(foldLive(liveDocs, [change])[change.id].value, expected)) return op;
}

// Project in order, optionally in a single PG transaction. Keep admission and transport capacity
// for the whole prefix before its single Kafka grant. Never wait for a second slot while
// already holding one: competing partitions must always be able to finish their prefixes.
export async function prepareToolGrants(records, current, grant, options) {
  const { limit, sender, router, targetFor, prepareSend } = options;
  const held = [],
    prefix = [],
    plans = [];
  try {
    for (const r of records.slice(0, limit)) {
      if (
        !current() ||
        !(router.canGroupOperation ? router.canGroupOperation(r) : plainExecution(r.fact))
      )
        break;
      const previous = prefix.at(-1);
      if (
        previous &&
        (r.partition !== previous.partition ||
          r.topic !== previous.topic ||
          r.offset !== previous.offset + 1n)
      )
        break;
      const target = targetFor(r);
      if (prefix.length && !sender.canReserve(target)) break;
      const slot = prefix.length ? sender.take(target) : await sender.reserve(target, current);
      if (!slot) break;
      prefix.push(r);
      held.push(slot);
    }
    if (!prefix.length || !current()) return 0;
    const grouped = options.batchProjection
      ? await router.projectOperations(prefix, current, options.markProjection)
      : undefined;
    for (let i = 0; i < (grouped?.length ?? prefix.length) && current(); i++) {
      const plan = grouped
        ? grouped[i]
        : await router.projectOperation(prefix[i], current, options.markProjection);
      if (!plan || !current()) break;
      assert(plan.commands.length === 1 && !plan.cancels.length);
      plans.push({ send: prepareSend(prefix[i], plan.commands[0], held[i]) });
    }
    if (!plans.length || !current()) return 0;
    const start = performance.now();
    const first = await grant(plans.length);
    assert(Number.isInteger(first) && first >= 0 && first <= plans.length);
    options.observeGrant?.(plans.length, performance.now() - start);
    for (let i = first; i < plans.length && current(); i++) {
      await plans[i].send();
    }
    return plans.length;
  } finally {
    for (const slot of held) if (!slot.op) sender.release(slot);
  }
}
