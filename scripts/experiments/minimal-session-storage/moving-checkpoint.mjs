import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { performance } from "node:perf_hooks";
import { applyImmutableBatches } from "@earendil-works/chord/delta";
import { LiveDoc } from "@earendil-works/pi-durable";
import { parseToolReplyRecovery } from "../../../packages/protocol/src/tool-recovery.ts";

import { emptyInboxRoutes, reopenEmptyInboxes } from "./family-inboxes.mjs";

const bounded = 16 * 1024;
export const batchHash = (batch) =>
  createHash("sha256").update(JSON.stringify(batch)).digest("hex");
const knownKeys = (object, keys) => Object.keys(object).every((k) => keys.includes(k));
export function assertFamilySettled(view) {
  assert(
    !Object.keys(view.pendingTools ?? {}).length &&
      Object.values(view.liveDocs ?? {}).every((doc) =>
        LiveDoc.definition.checkpointWhen(doc.value),
      ),
    "Run finished with an outstanding Tool or live conversation",
  );
}

// Pi's checkpoint predicate excludes in-flight text and running tools. The
// stricter bounded subset here also excludes retry details and tool outputs.
function compactLive(value) {
  return (
    value &&
    typeof value === "object" &&
    !Array.isArray(value) &&
    (!value.tools || Array.isArray(value.tools)) &&
    LiveDoc.definition.checkpointWhen(value) &&
    knownKeys(value, ["run", "tools"]) &&
    (!value.run ||
      (knownKeys(value.run, ["taskId", "inputs"]) &&
        Number.isSafeInteger(value.run.taskId) &&
        Array.isArray(value.run.inputs) &&
        value.run.inputs.every(Number.isSafeInteger))) &&
    (!value.tools ||
      value.tools.every(
        (slot) =>
          knownKeys(slot, ["callId", "name", "status", "entry", "taskId"]) &&
          ["pending", "done"].includes(slot.status),
      ))
  );
}

export function foldLive(documents = {}, writes) {
  let next = documents;
  const edit = () => {
    if (next === documents) next = { ...documents };
  };
  for (const w of writes) {
    if (w.type === "document.create" && w.record.kind === "pi.live") {
      assert(w.content.kind === "base" && w.record.scope.kind === "conversation");
      edit();
      next[w.record.id] = { conversation: w.record.scope.conversationId, value: w.content.value };
    } else if (w.type === "document.change" && next[w.id]) {
      const before = next[w.id];
      const value =
        w.content.kind === "base"
          ? w.content.value
          : applyImmutableBatches(before.value, [w.content.ops]);
      edit();
      next[w.id] = { ...before, value };
    } else if (w.type === "document.retire" && next[w.id]) {
      edit();
      delete next[w.id];
    }
  }
  return next;
}

export function checkpoint(view, record) {
  const inboxRoutes = emptyInboxRoutes(view.inboxes);
  if (
    view.rootInputDoc ||
    !inboxRoutes ||
    Object.keys(view.pendingTools ?? {}).length ||
    !Object.keys(view.liveDocs).length ||
    !Object.values(view.liveDocs).every((doc) => compactLive(doc.value))
  )
    return;
  const fields = {};
  for (const key of [
    "seq",
    "baseSeq",
    "nextId",
    "count",
    "bytes",
    "floor",
    "floorChanged",
    "markerId",
    "liveId",
    "agentRoutes",
    "worldDocs",
    "childOutcomes",
    "childCallers",
    "usageDocs",
    "lastFact",
  ])
    if (view[key] !== undefined) fields[key] = view[key];
  const state = {
    version: 1,
    writer: record.fact.scope.writerId,
    ...fields,
    inboxRoutes,
    liveDocs: view.liveDocs,
    answered: view.lastAnswerCommit !== undefined,
    offset: String(record.offset),
    hash: batchHash(record.fact.commit),
  };
  return Buffer.byteLength(JSON.stringify(state)) <= bounded ? state : undefined;
}

export function hydrate(state, writer, first) {
  assert(
    state.version === 1 &&
      state.writer === writer &&
      BigInt(state.offset) + 1n === BigInt(first) &&
      Number.isSafeInteger(state.seq) &&
      typeof state.lastFact === "string" &&
      /^[0-9a-f]{64}$/.test(state.hash) &&
      Object.values(state.liveDocs).every((doc) => compactLive(doc.value)),
    "Invalid active projection checkpoint",
  );
  const { version, offset, hash, answered, inboxRoutes, ...fields } = state;
  return {
    ...fields,
    writer,
    lastOffset: BigInt(offset),
    lastHash: hash,
    inboxes: reopenEmptyInboxes(inboxRoutes),
    ...(answered ? { lastAnswerCommit: performance.now() } : {}),
  };
}

// A conservative policy for the effects covered by this experiment. Durable
// Kafka publication and external dispatch have their own acknowledgements.
export function useSynchronousPolicy(fact, recoverableTools = false) {
  return (
    fact.kind !== "durable_commit" ||
    !!(
      (fact.tools?.length && !recoverableTools) ||
      fact.cancellations?.length ||
      fact.inputQueueSessions?.length ||
      fact.modelEvidence?.length ||
      fact.display?.length
    ) ||
    fact.commit.writes.some(
      (w) => w.type === "submission" && w.value.requestId && w.value.requestId !== fact.scope.runId,
    )
  );
}

// The canonical native Task supplies the identity. Its original reply
// descriptor is current recovery data. Kafka group progress fences execution.
export function foldToolReferences(
  view,
  previous,
  frame,
  fact,
  offset,
  workspaceBatch,
  rootPresentation = false,
) {
  const pending = { ...previous?.pendingTools };
  const writes = fact.commit.writes
    .filter((w) => w.type === "task" && w.value.kind === "pi.tool")
    .map((w) => w.value);
  const s = fact.scope,
    scope = [s.tenantId, s.piSessionId, s.runId, s.writerId];
  let registered = 0;
  for (const ref of fact.tools ?? []) {
    if (ref.target !== "cube" || ref.request?.operation !== "tool.execute" || !ref.replyRecovery)
      continue;
    const task = writes.find((t) => t.id === ref.taskId);
    const reply = parseToolReplyRecovery(ref.replyRecovery);
    assert(
      task?.state.status === "running" &&
        task.state.checkpoint?.phase === "execute" &&
        task.input.callId === reply.toolCallId &&
        ref.replyTopic === reply.topic &&
        ref.request.toolName === reply.toolName &&
        ref.scope.writerId === s.writerId &&
        ref.scope.runId === s.runId &&
        typeof ref.request.operationId === "string",
      "Unmatched native sandbox intent",
    );
    const value = {
      ...(ref.allocationSpanStart !== undefined
        ? { allocationSpanStart: ref.allocationSpanStart }
        : {}),
      operationId: ref.request.operationId,
      taskId: task.id,
      conversationId: task.conversationId,
      offset: String(offset),
      seq: fact.commit.seq,
      ...(rootPresentation
        ? {
            assistantEntryId: task.input.assistant,
            ordinal: fact.commit.writes.findIndex((w) => w.type === "task" && w.value === task),
            arguments: task.state.checkpoint.arguments,
            startedAt: fact.occurredAt,
          }
        : {}),
      reply,
    };
    assert(!pending[task.id], "Duplicate pending native Tool");
    pending[task.id] = value;
    frame.ops.push({ kind: "tool", args: [...scope, task.id, value], count: 1 });
    registered++;
  }
  // A native abort can precede the terminal task write. Keep a durable
  // boundary for the idempotent cancellation even when there is no new Entry.
  if (workspaceBatch)
    for (const cancel of fact.cancellations) {
      assert(writes.some((t) => t.id === cancel.taskId && t.abortRequested));
      frame.ops.push({
        kind: "cancel",
        args: [...scope, cancel.operationId, cancel.taskId],
        count: 1,
      });
      frame.strict = true;
    }
  for (const task of writes) {
    const original = pending[task.id];
    if (original && ["completing", "terminal"].includes(task.state.status)) {
      const confirmUse =
        original.allocationSpanStart !== undefined &&
        task.state.outcome?.status === "completed" &&
        BigInt(view.confirmedUseSpans?.[original.conversationId] ?? -1) <
          BigInt(original.allocationSpanStart);
      frame.ops.push({
        kind: "tool-settled",
        ...(confirmUse ? { confirmUse } : {}),
        args: [
          ...scope,
          task.id,
          original,
          ...(workspaceBatch &&
          fact.cancellations.some((c) => c.operationId === original.operationId)
            ? [true]
            : []),
        ],
        count: 1,
      });
      frame.strict = true;
      if (confirmUse)
        view.confirmedUseSpans = {
          ...view.confirmedUseSpans,
          [original.conversationId]: original.allocationSpanStart,
        };
      delete pending[task.id];
    }
  }
  view.pendingTools = pending;
  return registered > 0 && registered === fact.tools.length;
}

export function plainAnswer(frame, fact) {
  if (
    frame.strict ||
    frame.anchor ||
    frame.rejected ||
    !frame.ops.length ||
    useSynchronousPolicy(fact)
  )
    return false;
  if (!frame.ops.every((op) => op.kind === "message")) return false;
  const entries = fact.commit.writes.filter((w) => w.type === "entry");
  return (
    entries.length > 0 &&
    entries.every(
      ({ value }) =>
        value.model?.length === 1 &&
        value.model[0].role === "assistant" &&
        value.model[0].stopReason === "stop" &&
        value.model[0].content.every((block) => block.type === "text"),
    )
  );
}

// These project already-available sources. The separate PG transaction
// accepting the original user request remains synchronous.
export function replayableMetadata(frame, fact) {
  if (frame.strict || frame.rejected || !frame.ops.length || useSynchronousPolicy(fact))
    return false;
  return frame.ops.every((op) => {
    if (op.kind === "pin" || op.kind === "world") return true;
    if (op.kind === "inbox")
      return (
        op.args[6] === "[]" &&
        fact.commit.writes.some(
          (w) =>
            w.type === "document.create" &&
            w.record.kind === "pi.inbox" &&
            w.record.id === op.args[3],
        )
      );
    if (op.kind !== "message") return false;
    return JSON.parse(op.args[4]).every(
      (entry) =>
        entry.document !== undefined || (entry.role === "user" && entry.input === fact.scope.runId),
    );
  });
}

// The rows and partition head come from one MVCC snapshot. Multiple live
// families on a partition can have different moving floors.
export async function movingStarts(db, topic, bounds) {
  const { rows } = await db.query(
    `select h.partition,h.next_offset,r.id,r.lease_id,r.projection_first,r.projection_state,r.projection_pin
    from accepted_fact_projection_offsets h left join runs r on r.projection_partition=h.partition
      and r.state in ('running','cancel_requested') and r.projection_first is not null
    where h.topic=$1 and h.partition=any($2::int[])`,
    [topic, bounds.map((b) => b.partition)],
  );
  const groups = Map.groupBy(rows, (r) => r.partition);
  return bounds.map((b) => {
    const records = groups.get(b.partition) ?? [];
    const next = records.length ? BigInt(records[0].next_offset) : b.low;
    const start = records.reduce(
      (floor, r) =>
        r.projection_pin != null && BigInt(r.projection_pin) < floor
          ? BigInt(r.projection_pin)
          : floor,
      next,
    );
    if (
      !records.length ||
      start < b.low ||
      next > b.high ||
      records.some(
        (r) =>
          r.id &&
          (BigInt(r.projection_first) > next ||
            (!r.projection_state && r.projection_pin == null) ||
            (r.projection_pin != null &&
              (BigInt(r.projection_pin) < BigInt(r.projection_first) ||
                BigInt(r.projection_pin) >= next))),
      )
    )
      return [b.partition, next, new Error("Required projection suffix is unavailable"), []];
    try {
      return [
        b.partition,
        next,
        start,
        records
          .filter((r) => r.projection_state)
          .map((r) => [
            r.id,
            {
              ...hydrate(r.projection_state, r.lease_id, r.projection_first),
              ...(r.projection_pin != null ? { pinOffset: BigInt(r.projection_pin) } : {}),
            },
          ]),
      ];
    } catch (error) {
      return [b.partition, next, error, []];
    }
  });
}
