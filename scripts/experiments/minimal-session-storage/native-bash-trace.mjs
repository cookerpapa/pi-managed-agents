// Capture unmodified commits from the pinned Harness and use the real fixed Bash
// definition with an inert executor. No provider request or shell escapes the fixture.
import assert from "node:assert/strict";
import { createHash, randomUUID } from "node:crypto";
import { durableToolState } from "@pi-cloud/protocol";
import { setTimeout as delay } from "node:timers/promises";
import { BACKGROUND_CONTEXT as ctx } from "@earendil-works/chord/context";
import {
  Harness,
  MemoryStorage,
  createRegistry,
  defineExtension,
} from "@earendil-works/pi-durable";
import { createModels } from "@earendil-works/pi-ai/models";
import {
  fauxProvider,
  fauxToolCall,
  fauxAssistantMessage,
} from "@earendil-works/pi-ai/providers/faux";
import { fixedRemoteToolDefinitions } from "../../../packages/sandbox-supervisor/src/fixed-remote-tool-definitions.ts";
export async function nativeBashTrace(scope, onCommit, options = {}) {
  const callsPerRun = options.callsPerRun ?? 1;
  const frames = [],
    registry = createRegistry(),
    models = createModels(),
    fake = fauxProvider({
      tokensPerSecond: 0,
      ...(callsPerRun > 1
        ? { models: [{ id: "loop-model", contextWindow: 32768, maxTokens: 512 }] }
        : {}),
    });
  let issued = 0,
    executed = 0;
  class Observed extends MemoryStorage {
    #published = Promise.resolve();
    async commit(writes) {
      if (!options.waitForCommit) return this.observe(writes);
      this.#published = this.#published.then(() => this.observe(writes));
      return this.#published;
    }
    async observe(writes) {
      const p = this.prepareCommit(writes);
      const frame = { seq: p.seq, nextId: this.nextId, writes: structuredClone(p.writes) };
      if (options.onlyLatest) frames[0] = frame;
      else frames.push(frame);
      const sent = onCommit?.(frame);
      if (sent) await sent;
      return p.apply();
    }
  }
  models.setProvider(fake.provider);
  registry.install(
    defineExtension({
      name: "fixed-bash-study",
      tools: [
        {
          ...fixedRemoteToolDefinitions().find((t) => t.name === "bash"),
          execute: async (args) => {
            if (options.toolDelay) await delay(options.toolDelay);
            assert.equal(args.timeout, 10);
            const result =
              callsPerRun > 1
                ? createHash("shake256", { outputLength: 512 })
                    .update(options.callIdAt(executed))
                    .digest("hex")
                : "fixture";
            executed++;
            return { content: [{ type: "text", text: result }] };
          },
        },
      ],
    }),
  );
  if (callsPerRun > 1) {
    const respond = async (context) => {
      if (context.messages.at(-1)?.content?.[0]?.text?.startsWith("<conversation>"))
        return fauxAssistantMessage(
          "Earlier checks completed successfully. Continue the remaining checks.",
        );
      if (issued === callsPerRun) return fauxAssistantMessage("ok");
      const callId = options.callIdAt(issued++);
      if (options.modelDelay) await delay(options.modelDelay);
      return fauxAssistantMessage(
        fauxToolCall("bash", { command: "true", timeout: "10" }, { id: callId }),
        { stopReason: "toolUse" },
      );
    };
    // This profile expects one summary and one final response in addition to the tool requests.
    fake.setResponses(Array(callsPerRun + 2).fill(respond));
  } else {
    const first = fauxAssistantMessage(
      fauxToolCall("bash", { command: "true", timeout: "10" }, { id: options.callId }),
      { stopReason: "toolUse" },
    );
    fake.setResponses([
      async () => {
        if (options.modelDelay) await delay(options.modelDelay);
        return first;
      },
      fauxAssistantMessage("ok"),
    ]);
  }
  const h = await Harness.open(
    new Observed(),
    {
      models,
      registry,
      settings: {
        retry: { maxRetries: 0 },
        compaction:
          callsPerRun > 1
            ? { enabled: true, reserveTokens: 1024, keepRecentTokens: 512, backgroundTokens: 0 }
            : { enabled: false },
      },
    },
    ctx,
  );
  try {
    const m = fake.getModel(),
      root = await h.root(ctx, { agent: { model: { provider: m.provider, modelId: m.id } } });
    const sub = await root.submit(
      { type: "input", requestId: scope.runId, content: "Run the fixed test tool." },
      ctx,
    );
    await root.waitForIdle(ctx);
    assert.equal((await sub.wait(ctx)).status, "done");
    assert.equal(executed, callsPerRun);
  } finally {
    await h.close(ctx);
  }
  return frames;
}

export function nativeBashFact(scope, commit, display, operationId, replyTopic) {
  const task = commit.writes
    .map((w) => w.type === "task" && durableToolState(w.value))
    .find((t) => t?.intent);
  return {
    kind: "durable_commit",
    scope,
    factId: randomUUID(),
    occurredAt: new Date().toISOString(),
    commit,
    display,
    tools: [],
    cancellations: [],
    origins: commit.writes
      .filter((w) => ["task", "submission", "entry"].includes(w.type))
      .map((w) => ({
        recordId: w.value.id,
        sessionId: scope.sessionId,
        turnId: scope.turnId,
        runId: scope.runId,
      })),
    workspaceOperations: task
      ? [
          {
            action: "execute",
            taskId: task.id,
            operationId,
            maximumOutputBytes: 51200,
            timeoutMs: 10000,
            reply: {
              topic: replyTopic,
              partition: 0,
              offset: "0",
              deadlineAt: Date.now() + 3600000,
              toolName: "bash",
              toolCallId: task.intent.callId,
            },
          },
        ]
      : [],
  };
}
