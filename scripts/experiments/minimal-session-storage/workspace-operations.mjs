// Candidate Worker wire. Native writes and the logical operation share one
// record. Neither the intent nor the Worker-side replay reader needs a route.
import assert from "node:assert/strict";
import { durableToolIntent, durableToolState, parseToolReplyRecovery } from "@pi-cloud/protocol";
import {
  decodeSessionLogEnvelope,
  encodeSessionLogFact,
} from "../../../packages/runtime-core/src/kafka-accepted-fact.ts";

const metadata = new WeakMap();
const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
function fields(object, keys) {
  assert.deepEqual(
    Object.keys(object).sort(),
    keys.toSorted(),
    "Unexpected Workspace operation fields",
  );
}
function checkShape(op) {
  if (op.action === "execute") {
    fields(op, ["action", "operationId", "taskId", "reply", "timeoutMs", "maximumOutputBytes"]);
    fields(op.reply, ["topic", "partition", "offset", "deadlineAt", "toolName", "toolCallId"]);
  } else if (op.action === "cancel") fields(op, ["action", "operationId", "taskId"]);
  else {
    assert.equal(op.action, "probe");
    fields(op, ["action", "operationId", "taskId", "conversationId"]);
  }
}
export function encodeWorkspaceFact(fact) {
  const { workspaceOperations, ...base } = fact;
  assert(
    !base.tools?.length &&
      !base.cancellations?.length &&
      !(base.kind === "path_probe" && base.execution),
    "Worker cannot publish a routed operation",
  );
  if (!workspaceOperations?.length) return encodeSessionLogFact(base);
  for (const op of workspaceOperations) checkShape(op);
  const wire = JSON.parse(encodeSessionLogFact(base));
  return JSON.stringify({ ...wire, workspaceOperations });
}
export function decodeWorkspaceFact(bytes) {
  const { workspaceOperations = [], ...wire } = JSON.parse(bytes.toString());
  assert(
    !wire.tools?.length &&
      !wire.cancellations?.length &&
      !(wire.kind === "path_probe" && wire.execution),
    "Worker sent a routed command",
  );
  const prepared = decodeSessionLogEnvelope(wire);
  assert(Array.isArray(workspaceOperations));
  if (!workspaceOperations.length) return prepared;
  const f = prepared.fact;
  const ids = new Set(),
    tasks = new Set();
  const operations = workspaceOperations.map((op) => {
    checkShape(op);
    assert(uuid.test(op.operationId) && Number.isSafeInteger(op.taskId) && op.taskId > 0);
    assert(
      !ids.has(op.operationId) && !tasks.has(op.taskId),
      "Duplicate operation in native batch",
    );
    ids.add(op.operationId);
    tasks.add(op.taskId);
    if (f.kind === "path_probe") {
      assert(Number.isSafeInteger(op.conversationId) && op.conversationId > 0);
      assert(op.action === "probe" && workspaceOperations.length === 1);
      return op;
    }
    assert.equal(f.kind, "durable_commit");
    const task = f.commit.writes.findLast(
      (w) => w.type === "task" && w.value.id === op.taskId,
    )?.value;
    const origin = f.origins.find((o) => o.recordId === op.taskId);
    assert(
      task?.kind === "pi.tool" &&
        origin?.runId === f.scope.runId &&
        uuid.test(origin.sessionId) &&
        uuid.test(origin.turnId),
      "Missing native operation origin",
    );
    const scope = { ...f.scope, sessionId: origin.sessionId, turnId: origin.turnId };
    if (op.action === "cancel") {
      assert(durableToolState(task)?.aborted, "Cancellation lacks native abort");
      return { ...op, scope, conversationId: task.conversationId };
    }
    assert.equal(op.action, "execute");
    const intent = durableToolIntent(f.commit, op.taskId);
    const reply = parseToolReplyRecovery(op.reply);
    assert.equal(reply.toolCallId, intent.callId);
    assert(
      Number.isSafeInteger(op.timeoutMs) &&
        op.timeoutMs > 0 &&
        Number.isSafeInteger(op.maximumOutputBytes) &&
        op.maximumOutputBytes > 0,
    );
    return { ...op, reply, scope, conversationId: task.conversationId, args: intent.arguments };
  });
  metadata.set(f, operations);
  return prepared;
}
export const operationsFor = (f) => metadata.get(f) ?? [];
