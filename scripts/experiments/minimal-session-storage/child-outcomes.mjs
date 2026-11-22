import assert from "node:assert/strict";
import { durableControlIntent } from "@pi-cloud/protocol";

// Only active native task/input IDs are needed to distinguish work still in
// progress. Historical terminal records stay out of the retained child summary.
function activity(lane) {
  if (Object.keys(lane.active).length) return "running";
  if (lane.aborted) return "cancelled";
  if (lane.failed) return "failed";
  if (lane.admitted) return "completed";
  return null;
}

export function projectChildOutcomes(
  view,
  fact,
  frame,
  dispatches = fact.tools,
  delegatedRuns = false,
) {
  const writes = fact.commit.writes;
  let lanes = view.childOutcomes,
    callers = view.childCallers;
  if (
    !lanes &&
    !callers &&
    !dispatches?.some((t) => t.target === "control") &&
    !writes.some(
      (w) =>
        w.record?.kind === "pi-cloud.conversation-creation" ||
        w.record?.kind === "pi-cloud.conversation-stop" ||
        w.record?.kind === "pi-cloud.tool-hostcall",
    )
  )
    return;
  const touched = new Map(),
    patches = new Map();
  function patch(id, change) {
    patches.set(id, { ...patches.get(id), ...change });
  }
  function trackCaller(task, id) {
    if (callers?.[task]?.includes(id)) return;
    if (callers === view.childCallers) callers = { ...callers };
    callers[task] = [...(callers[task] ?? []), id];
  }
  // An operation ID is known at dispatch, including before native child creation.
  for (const t of dispatches ?? [])
    if (
      t.target === "control" &&
      (t.toolName === "subagent" ||
        (t.toolName === "subagent_control" &&
          durableControlIntent(fact.commit, t).arguments.action === "run"))
    )
      trackCaller(t.taskId, t.recovery?.operationId ?? t.operationId);
  for (const w of writes) {
    if (w.type !== "document.create") continue;
    const { record, content } = w;
    if (
      record.kind === "pi-cloud.tool-hostcall" &&
      record.scope.kind === "task" &&
      content.kind === "base" &&
      (content.value.toolName === "subagent" ||
        (content.value.toolName === "subagent_control" &&
          content.value.arguments?.action === "run"))
    ) {
      trackCaller(record.scope.taskId, record.key);
    } else if (record.kind === "pi-cloud.conversation-stop") {
      assert(
        record.scope.kind === "session" &&
          content.kind === "base" &&
          content.value.stopped === true,
      );
      patch(record.key, { stopped: true });
    } else if (record.kind === "pi-cloud.conversation-creation") {
      const v = content.value,
        lane = v.conversationId;
      assert(
        record.scope.kind === "session" &&
          content.kind === "base" &&
          Number.isSafeInteger(lane) &&
          lane > 1,
      );
      if (lanes?.[lane]) {
        assert.equal(lanes[lane].id, record.key);
        continue;
      }
      if (lanes === view.childOutcomes) lanes = { ...lanes };
      lanes[lane] = { id: record.key, active: {} };
      trackCaller(v.ownerTaskId, record.key);
      if (delegatedRuns) patch(record.key, { lane, owner: v.generationId });
    }
  }
  if (!lanes && !callers && !patches.size) return;
  for (const w of writes) {
    if (w.type !== "task" && w.type !== "submission") continue;
    const t = w.value,
      lane = t.conversationId;
    if (
      w.type === "task" &&
      callers?.[t.id] &&
      (t.abortRequested || t.state.status === "terminal")
    ) {
      for (const id of callers[t.id])
        patch(id, { caller: t.abortRequested ? "aborted" : "closed" });
      if (t.state.status === "terminal") {
        if (callers === view.childCallers) callers = { ...callers };
        delete callers[t.id];
      }
    }
    if (!lanes?.[lane] || (w.type === "task" && t.background)) continue;
    const active =
      w.type === "task" ? t.state.status !== "terminal" : ["queued", "placed"].includes(t.status);
    const old = lanes[lane],
      original =
        w.type === "submission" && t.requestId === (delegatedRuns ? old.id : fact.scope.runId);
    const unanswered =
      w.type === "submission" && t.status === "unanswered" && (original || t.entry != null);
    const answer = original && t.status === "done" && t.type === "input" ? t.answer : undefined;
    if (original && t.type === "input" && t.status === "done")
      assert(Number.isSafeInteger(answer) && answer > 0);
    if (
      Boolean(old.active[t.id]) === active &&
      (!original || old.admitted) &&
      (!unanswered || (t.reason === "aborted" ? old.aborted : old.failed)) &&
      (answer === undefined || old.answer === answer)
    )
      continue;
    if (!touched.has(lane)) {
      touched.set(lane, { state: activity(old), answer: old.answer });
      if (lanes === view.childOutcomes) lanes = { ...lanes };
      lanes[lane] = { ...old, active: { ...old.active } };
    }
    const next = lanes[lane];
    if (active) next.active[t.id] = true;
    else delete next.active[t.id];
    if (original) next.admitted = true;
    if (unanswered) next[t.reason === "aborted" ? "aborted" : "failed"] = true;
    if (answer !== undefined) next.answer = answer;
  }
  for (const [lane, prior] of touched) {
    const next = lanes[lane],
      state = activity(next);
    if (state !== prior.state || prior.answer !== next.answer)
      patch(next.id, { activity: state, ...(next.answer ? { answer: next.answer } : {}) });
  }
  if (lanes !== view.childOutcomes) view.childOutcomes = lanes;
  if (callers !== view.childCallers)
    view.childCallers = Object.keys(callers).length ? callers : undefined;
  if (patches.size && frame)
    frame.ops.push({
      kind: delegatedRuns ? "delegated-outcomes" : "child-outcomes",
      count: 1,
      args: [fact.scope.tenantId, fact.scope.piSessionId, fact.scope.runId, fact.scope.writerId],
      seq: fact.commit.seq,
      at: fact.occurredAt,
      changes: [...patches].map(([id, v]) => ({ id, ...v })),
    });
}
