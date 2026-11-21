import assert from "node:assert/strict";
import { kafkaProducerLane } from "../../../packages/runtime-core/src/kafka-accepted-fact.ts";
import { encodeWorkspaceFact } from "./workspace-operations.mjs";
export const targets = 4000,
  partitions =
    typeof __FANOUT_PARTITIONS__ === "number"
      ? __FANOUT_PARTITIONS__
      : Number(process.env.PI_CLOUD_FANOUT_PARTITIONS ?? 32),
  offered = 4600;
export function fanoutFixture(token) {
  assert.match(token, /^[a-f0-9]{16}$/);
  const id = (kind, n) =>
    token.slice(0, 8) +
    "-" +
    token.slice(8, 12) +
    "-4" +
    token.slice(12, 15) +
    "-8" +
    kind.toString(16).padStart(3, "0") +
    "-" +
    n.toString(16).padStart(12, "0");
  const bins = Array.from({ length: partitions }, () => []);
  for (let n = 0, filled = 0; filled < targets; n++) {
    const session = id(1, n),
      p = kafkaProducerLane(session, partitions);
    // 64 partitions receive 62 or 63 targets each.
    if (bins[p].length < Math.ceil((targets - p) / partitions)) {
      bins[p].push(session);
      filled++;
    }
  }
  const owners = Array.from({ length: targets }, (_, i) => {
    const partition = i % partitions,
      session = bins[partition][Math.floor(i / partitions)];
    return {
      partition,
      binding: id(2, i),
      scope: {
        tenantId: id(3, 0),
        sessionId: session,
        piSessionId: session,
        runId: id(4, i),
        turnId: id(4, i),
        writerId: id(5, i),
        fencingToken: 1,
      },
    };
  });
  const op = (arm, index) => id(6, arm * 1000000 + index);
  const replyTopic = "pi-cloud.tool-replies.v4." + id(7, 0);
  function frame(arm, index, seed) {
    const { scope, partition } = owners[index % targets],
      round = Math.floor(index / targets),
      taskId = 9 + round;
    const operationId = op(arm, index),
      call = "call-" + index;
    const writes = seed
      ? [{ type: "conversation", value: { id: 1 } }]
      : [
          {
            type: "task",
            value: {
              id: taskId,
              conversationId: 1,
              kind: "pi.tool",
              version: 1,
              input: { assistant: 8, callId: call },
              background: false,
              abortRequested: false,
              state: {
                status: "running",
                checkpoint: { phase: "execute", arguments: { command: "true" }, replay: "safe" },
              },
            },
          },
        ];
    const fact = {
      kind: "durable_commit",
      scope,
      factId: id(seed ? 8 : 9, arm * 1000000 + index),
      occurredAt: new Date().toISOString(),
      commit: { seq: seed ? 1 : 2 + round, nextId: taskId + 1, writes },
      origins: seed
        ? []
        : [
            {
              recordId: taskId,
              sessionId: scope.sessionId,
              turnId: scope.turnId,
              runId: scope.runId,
            },
          ],
      tools: [],
      cancellations: [],
      display: [],
      workspaceOperations: seed
        ? []
        : [
            {
              action: "execute",
              operationId,
              taskId,
              timeoutMs: 10000,
              maximumOutputBytes: 51200,
              reply: {
                topic: replyTopic,
                partition: 0,
                offset: "0",
                deadlineAt: Date.now() + 3600000,
                toolName: "bash",
                toolCallId: call,
              },
            },
          ],
    };
    return {
      partition,
      session: scope.sessionId,
      id: fact.factId,
      value: encodeWorkspaceFact(fact),
    };
  }
  return { owners, op, frame, replyTopic };
}
