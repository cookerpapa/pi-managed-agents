// The fixed Bash schema and the real reply mailbox. There is exactly one tool
// proposal per Run in this experiment; no execution location is supplied here.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { awaitWithContext } from "@earendil-works/chord/context";
import { durableToolState } from "@pi-cloud/protocol";
import { fixedRemoteToolDefinitions } from "../../../packages/sandbox-supervisor/src/fixed-remote-tool-definitions.ts";

export function continuationBash(mailbox, runId) {
  assert.match(runId, /^[a-f0-9-]{36}$/);
  let operation;
  const controller = new AbortController();
  const command = "printf '%s\\n' " + runId + " >> executions && pwd -P && cat marker";
  return {
    command,
    async intents(writes) {
      const intents = [];
      for (const write of writes) {
        const task = write.type === "task" && durableToolState(write.value);
        if (!task?.intent) continue;
        if (operation) {
          assert.equal(operation.taskId, task.id);
          assert.equal(operation.reply.toolCallId, task.intent.callId);
          continue;
        }
        assert.equal(task.intent.arguments.command, command);
        const id = randomUUID(),
          reply = {
            ...(await mailbox.replyPosition()),
            toolName: "bash",
            toolCallId: task.intent.callId,
            deadlineAt: Date.now() + 60000,
          };
        const wait = mailbox.wait(
          id,
          reply.toolCallId,
          "bash",
          AbortSignal.any([controller.signal, AbortSignal.timeout(60000)]),
        );
        operation = { taskId: task.id, reply, wait };
        intents.push({
          action: "execute",
          operationId: id,
          taskId: task.id,
          reply,
          timeoutMs: 10000,
          maximumOutputBytes: 51200,
        });
      }
      return intents;
    },
    tool: {
      ...fixedRemoteToolDefinitions().find((t) => t.name === "bash"),
      async execute(args, api, context) {
        assert.equal(args.command, command);
        assert.equal(operation?.taskId, api.taskId);
        assert.equal(operation.reply.toolCallId, api.callId);
        const response = await awaitWithContext(operation.wait.result, context);
        assert.equal(
          operation.wait.executionEvidence(),
          undefined,
          "Ordinary reply disclosed the physical execution target",
        );
        return { ...response.result, isError: response.isError };
      },
    },
    close() {
      controller.abort();
    },
  };
}
