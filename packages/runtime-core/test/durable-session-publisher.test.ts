import { expect, it, vi } from "vitest";
import { createHash } from "node:crypto";
import { PiCloudMetrics } from "@pi-cloud/observability";
import { ImageAttachmentCodec } from "@pi-cloud/session-attachments";
import { DurableSessionPublisher } from "../src/durable-session-publisher.ts";
import type { AcceptedDurableCommitFact, AcceptedFactWriter } from "../src/accepted-fact.ts";
import { toolInvocationFixture } from "./tool-invocation-fixture.ts";
import {
  MCP_TOOL_TIMEOUT_MS,
  TOOL_REPLY_DELIVERY_MARGIN_MS,
  type DurableCommitPublication,
} from "@pi-cloud/protocol";
import { validateControlToolReferences } from "../src/tool-invocation-fact.ts";

it("pins MCP original reply references in the same native intent without a second command body", async () => {
  const f = toolInvocationFixture(),
    facts: AcceptedDurableCommitFact[] = [];
  const topic = "pi-cloud.tool-replies.v4.10000000-0000-4000-8000-000000000009";
  const publisher = new DurableSessionPublisher({
    replyTopic: topic,
    replyPosition: async () => ({ topic, partition: 0, offset: "42" }),
    channels: {
      resolve: () => ({
        publication: { scope: f.scope },
        commitNative: async (fact) => {
          facts.push(fact);
        },
      }),
      checkHealth: async () => {},
    },
  });
  const publication: DurableCommitPublication = {
    ...f.publication,
    tools: [
      {
        taskId: 4,
        command: {
          target: "control",
          operationId: "20000000-0000-4000-8000-000000000008",
          executionReference: f.executionReference,
          toolCallId: "task:4",
          nativeCallId: "provider-1",
          conversationId: 1,
          toolName: "mcp_write_fixture",
          contextAnchor: null,
          arguments: { path: "large.txt", content: "hello" },
          occurredAt: f.command.occurredAt,
          implementation: {
            releaseId: "mcp",
            generation: 1,
            replay: "never",
            catalogScope: "mcp:10000000-0000-4000-8000-000000000001",
          },
        },
      },
    ],
  };
  await publisher.scoped(f.executionReference).publish(publication);
  const fact = facts[0]!;
  validateControlToolReferences(fact);
  expect(fact.tools[0]).toHaveProperty("replyRecovery", {
    topic,
    partition: 0,
    offset: "42",
    deadlineAt:
      Date.parse(f.command.occurredAt) + MCP_TOOL_TIMEOUT_MS + TOOL_REPLY_DELIVERY_MARGIN_MS,
    toolCallId: "provider-1",
    toolName: "mcp_write_fixture",
  });
  expect(fact.tools[0]).not.toHaveProperty("arguments");
  await publisher.close();
});

it("uploads images before Kafka timing, packs their references, and rejects publication after close", async () => {
  const { scope, executionReference } = toolInvocationFixture();
  let now = 0;
  const clock = vi.spyOn(performance, "now").mockImplementation(() => now);
  const metrics = new PiCloudMetrics("publisher-timing"),
    sample = vi.spyOn(metrics, "observeSessionMutation");
  const data = Buffer.from("image").toString("base64");
  const images = new ImageAttachmentCodec({
    put: async (_scope, bytes, mimeType) => {
      now += 100;
      return {
        id: crypto.randomUUID(),
        mimeType,
        byteLength: bytes.length,
        sha256: createHash("sha256").update(bytes).digest("hex"),
      };
    },
    get: async () => {
      throw new Error("unused");
    },
  });
  let accepted: AcceptedDurableCommitFact | undefined;
  const channel: AcceptedFactWriter = {
    publication: { scope },

    commitNative: async (fact) => {
      now += 7;
      accepted = fact;
    },
  };
  const publisher = new DurableSessionPublisher({
    images,
    metrics,
    replyTopic: "pi-cloud.tool-replies.v4.10000000-0000-4000-8000-000000000009",
    channels: { resolve: () => channel, checkHealth: async () => {} },
  });
  const publication = {
    commit: {
      seq: 1,
      nextId: 3,
      writes: [
        {
          type: "entry",
          value: {
            id: 2,
            conversationId: 1,
            kind: "pi.user",
            model: [
              {
                role: "user",
                content: [{ type: "image", mimeType: "image/png", data }],
                timestamp: 1,
              },
            ],
          },
        },
      ],
    },
    origins: [],
    inputQueueSessions: [scope.sessionId],
    tools: [],
    cancellations: [],
    display: [],
  };
  try {
    await publisher.scoped(executionReference).publish(publication);
    expect(sample.mock.calls).toEqual([
      ["attachment_prepare", 100],
      ["kafka_publish", 7],
    ]);
    expect(accepted?.inputQueueSessions).toEqual([scope.sessionId]);
    expect(JSON.stringify(accepted)).not.toContain(data);
    expect(JSON.stringify(accepted)).toContain("pi_cloud_image_v1");
    await publisher.close();
    await expect(publisher.scoped(executionReference).publish(publication)).rejects.toThrow(
      "closed",
    );
  } finally {
    clock.mockRestore();
    await publisher.close();
  }
});

it("needs no PG receipt or second payload ledger and leaves telemetry outside ACK semantics", async () => {
  const { scope, executionReference } = toolInvocationFixture();
  const publications: AcceptedDurableCommitFact[] = [];
  const channel: AcceptedFactWriter = {
    publication: { scope },

    commitNative: async (fact) => {
      publications.push(fact);
    },
  };
  const metrics = new PiCloudMetrics("publisher-observer");
  vi.spyOn(metrics, "observeSessionMutation").mockImplementation(() => {
    throw new Error("observer failed");
  });
  const publisher = new DurableSessionPublisher({
    metrics,
    replyTopic: "pi-cloud.tool-replies.v4.10000000-0000-4000-8000-000000000009",
    channels: { resolve: () => channel, checkHealth: async () => {} },
  });
  for (let seq = 1; seq <= 100; seq++)
    await publisher.scoped(executionReference).publish({
      commit: { seq, nextId: 2, writes: [] },
      origins: [],
      tools: [],
      cancellations: [],
      display: [],
    });
  expect(publications).toHaveLength(100);
  expect(new Set(publications.map((f) => f.factId)).size).toBe(100);
  await publisher.close();
});
