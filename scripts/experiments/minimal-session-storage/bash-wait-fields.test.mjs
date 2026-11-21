import assert from "node:assert/strict";
import test from "node:test";
import { singleBashRecords } from "./bash-wait-fields.mjs";
import { foldToolReferences } from "./moving-checkpoint.mjs";

const scope = { tenantId: "tenant", piSessionId: "session", runId: "run", writerId: "first" };
const call = {
  type: "toolCall",
  id: "provider-call",
  name: "bash",
  arguments: { command: "npm test", timeout: "90" },
};
const reply = {
  toolName: call.name,
  toolCallId: call.id,
  deadlineAt: 1900000000000,
  topic: "pi-cloud.tool-replies.v4.20000000-0000-4000-8000-000000000009",
  partition: 0,
  offset: "2",
};
const op = { taskId: 12, operationId: "operation", conversationId: 1, seq: 8, reply };
const entry = (id, byTaskId, seq, calls) => ({
  commit_seq: seq,
  content: JSON.stringify({
    id,
    byTaskId,
    conversationId: 1,
    kind: "pi.assistant",
    model: [{ role: "assistant", content: calls }],
  }),
});
const proposal = entry(10, 9, 7, [call]);

test("one stable assistant Entry identifies its owning generation and the validated fixed Bash arguments", () => {
  const [generation, tool] = singleBashRecords({ entries: [proposal], ops: [op] });
  assert.equal(generation.write.value.id, 9);
  assert.deepEqual(generation.write.value.state.on, [12]);
  assert.deepEqual(tool.write.value.input, { assistant: 10, callId: call.id });
  assert.equal(tool.write.value.owner, 9);
  assert.equal(tool.write.value.state.checkpoint.arguments.timeout, 90);
});

test("the single-call decoder refuses to guess an Entry or a multi-call round", () => {
  assert.throws(
    () => singleBashRecords({ entries: [entry(4, 3, 2, [call]), proposal], ops: [op] }),
    /ambiguous/,
  );
  assert.throws(
    () =>
      singleBashRecords({
        entries: [entry(10, 9, 7, [call, { ...call, id: "other" }])],
        ops: [op],
      }),
    /Multi-call/,
  );
});

test("the candidate pending reference does not encode validated arguments, original binding, replay policy or a later abort mark", () => {
  const [, leaf] = singleBashRecords({ entries: [proposal], ops: [op] });
  function observe(task, binding, previous) {
    const tools = previous
      ? []
      : [
          {
            target: "cube",
            taskId: op.taskId,
            scope,
            request: {
              operation: "tool.execute",
              operationId: op.operationId,
              activationId: binding,
              toolName: "bash",
            },
            replyRecovery: reply,
            replyTopic: reply.topic,
          },
        ];
    const v = {};
    foldToolReferences(
      v,
      previous,
      { ops: [] },
      { scope, commit: { seq: previous ? 9 : 8, writes: [{ type: "task", value: task }] }, tools },
      5n,
    );
    return v;
  }
  const task = leaf.write.value,
    base = observe(task, "binding-a");
  const withCheckpoint = (checkpoint) => ({
    ...task,
    state: { ...task.state, checkpoint: { ...task.state.checkpoint, ...checkpoint } },
  });
  assert.deepEqual(
    observe(
      withCheckpoint({ arguments: { command: "different value after beforeTool" } }),
      "binding-a",
    ),
    base,
  );
  assert.deepEqual(observe(withCheckpoint({ replay: "unsafe" }), "binding-a"), base);
  assert.deepEqual(observe(task, "binding-b"), base);
  assert.deepEqual(observe({ ...task, abortRequested: true }, "binding-a", base), base);
});
