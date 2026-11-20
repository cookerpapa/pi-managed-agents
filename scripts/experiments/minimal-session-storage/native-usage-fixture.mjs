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

// Exercise the actual threshold decision with the pinned public Harness.
// All provider responses, including the summary, are local and deterministic.
export async function checkCompactionUsage() {
  class Trace extends MemoryStorage {
    summaries = 0;
    lastResponseTokens = 0;
    async commit(writes) {
      const seq = await super.commit(writes);
      for (const w of writes) {
        if (w.type !== "entry") continue;
        if (w.value.kind === "pi.compaction") this.summaries++;
        for (const m of w.value.model ?? [])
          if (m.role === "assistant") this.lastResponseTokens = m.usage.totalTokens;
      }
      return seq;
    }
  }
  const contextWindow = 4096,
    reserveTokens = 512,
    threshold = contextWindow - reserveTokens;
  const store = new Trace(),
    models = createModels();
  const fake = fauxProvider({
    models: [{ id: "compact-test", contextWindow, maxTokens: 256 }],
    tokensPerSecond: 0,
  });
  models.setProvider(fake.provider);
  fake.setResponses(Array.from({ length: 60 }, () => fauxAssistantMessage("ok")));
  const h = await Harness.open(
    store,
    {
      models,
      registry: createRegistry(),
      settings: {
        retry: { maxRetries: 0 },
        compaction: { enabled: true, reserveTokens, keepRecentTokens: 128, backgroundTokens: 0 },
      },
    },
    ctx,
  );
  try {
    const m = fake.getModel(),
      root = await h.root(ctx, { agent: { model: { provider: m.provider, modelId: m.id } } });
    async function submit(text) {
      const sub = await root.submit({ type: "input", content: [{ type: "text", text }] }, ctx);
      await root.waitForIdle(ctx);
      assert.equal((await sub.wait(ctx)).status, "done");
    }
    let cumulative = 0;
    for (let turn = 0; turn < 50 && cumulative <= threshold; turn++) {
      await submit("small repeated request ".repeat(3));
      const u = await h.snapshot(UsageDoc, root.id, ctx);
      cumulative = u.models[m.provider + "/" + m.id].totalTokens;
    }
    assert(cumulative > threshold);
    await submit("one more short input");
    assert.equal(store.summaries, 0);
    const lastShortTokens = store.lastResponseTokens;
    assert(lastShortTokens < threshold);
    const before = await h.snapshot(UsageDoc, root.id, ctx);
    await submit("large new input ".repeat(1400));
    assert.equal(store.summaries, 1);
    return {
      threshold,
      shortConversationTokens: before.models[m.provider + "/" + m.id].totalTokens,
      lastShortTokens,
      summariesAfterLargeInput: store.summaries,
    };
  } finally {
    await h.close(ctx);
  }
}
