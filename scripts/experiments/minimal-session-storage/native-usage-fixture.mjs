import assert from "node:assert/strict";
import { BACKGROUND_CONTEXT as ctx } from "@earendil-works/chord/context";
import { Type } from "@earendil-works/pi-ai";
import { createModels } from "@earendil-works/pi-ai/models";
import {
  fauxAssistantMessage,
  fauxProvider,
  fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import {
  Harness,
  MemoryStorage,
  createRegistry,
  defineExtension,
  defineTool,
  UsageDoc,
} from "@earendil-works/pi-durable";

export async function captureNativeUsage(requestId) {
  const commits = [];
  class ObservedStorage extends MemoryStorage {
    async commit(writes) {
      const prepared = this.prepareCommit(writes);
      commits.push({
        seq: prepared.seq,
        nextId: this.nextId,
        writes: structuredClone(prepared.writes),
      });
      return prepared.apply();
    }
  }
  const registry = createRegistry(),
    models = createModels();
  let executed = 0;
  registry.install(
    defineExtension({
      name: "study",
      tools: [
        defineTool({
          name: "inspect",
          description: "Read a fixed value",
          parameters: Type.Object({}),
          execute: async () => {
            executed++;
            return { content: [{ type: "text", text: "ok" }] };
          },
        }),
      ],
    }),
  );
  const fake = fauxProvider({ tokensPerSecond: 230, tokenSize: { min: 8, max: 8 } });
  fake.setResponses([
    fauxAssistantMessage(
      [fauxToolCall("inspect", {}, { id: "one" }), fauxToolCall("inspect", {}, { id: "two" })],
      { stopReason: "toolUse" },
    ),
    fauxAssistantMessage("streamed answer ".repeat(80)),
  ]);
  models.setProvider(fake.provider);
  const h = await Harness.open(
    new ObservedStorage(),
    { models, registry, settings: { retry: { maxRetries: 0 }, compaction: { enabled: false } } },
    ctx,
  );
  try {
    const m = fake.getModel();
    const root = await h.root(ctx, { agent: { model: { provider: m.provider, modelId: m.id } } });
    const sub = await root.submit(
      {
        type: "input",
        requestId,
        content: [{ type: "text", text: "Inspect twice, then answer." }],
      },
      ctx,
    );
    await root.waitForIdle(ctx);
    assert.equal((await sub.wait(ctx)).status, "done");
    assert.equal(executed, 2);
    assert.equal(fake.state.callCount, 2);
    return { commits, finalUsage: await h.snapshot(UsageDoc, root.id, ctx) };
  } finally {
    await h.close(ctx);
  }
}
