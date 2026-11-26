// Each case runs the pinned native tool loop. The in-memory mailbox isolates Pi
// semantics; the PostgreSQL/RF3 suites exercise real reservation and fencing.
import assert from "node:assert/strict";
import { it } from "vitest";
import { Type } from "@earendil-works/pi-ai";
import { createModels } from "@earendil-works/pi-ai/models";
import {
  fauxProvider,
  fauxAssistantMessage,
  fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import {
  Harness,
  MemoryStorage,
  createRegistry,
  defineExtension,
  defineTool,
  hook,
  GenerationTask,
  InboxDoc,
} from "@earendil-works/pi-durable";
import { BACKGROUND_CONTEXT as ctx } from "@earendil-works/chord/context";
import { connectSessionInputs } from "./segment-input-worker.mjs";

it.each(["ordinary", "terminate", "handoff", "reset"])(
  "places external input at the %s native boundary",
  async (mode) => {
    const started = Promise.withResolvers(),
      release = Promise.withResolvers(),
      registry = createRegistry(),
      models = createModels(),
      fake = fauxProvider({ tokensPerSecond: 0 });
    const decisions = [],
      seen = [],
      store = new MemoryStorage();
    let boundary;
    registry.install(
      defineExtension({
        name: "test",
        hooks: [
          hook(GenerationTask, {
            afterTools: (_a, _r, api, c) => boundary.postTools(api.taskId, c),
          }),
        ],
        tools: [
          defineTool({
            name: "gate",
            description: "Finish on demand",
            parameters: Type.Object({}),
            execute: async () => {
              started.resolve();
              await release.promise;
              return {
                content: [{ type: "text", text: "result" }],
                ...(mode === "terminate"
                  ? { control: { terminate: true } }
                  : mode === "handoff"
                    ? { control: { handoff: "new context" } }
                    : {}),
              };
            },
          }),
        ],
      }),
    );
    models.setProvider(fake.provider);
    fake.setResponses([
      fauxAssistantMessage(fauxToolCall("gate", {}, { id: "gate-1" }), { stopReason: "toolUse" }),
      async (context) => {
        seen.push(context.messages);
        return fauxAssistantMessage("done");
      },
    ]);
    const h = await Harness.open(
      store,
      { registry, models, settings: { compaction: { enabled: false } } },
      ctx,
    );
    try {
      const m = fake.getModel(),
        root = await h.root(ctx, { agent: { model: { provider: m.provider, modelId: m.id } } });
      const mail = {
        state: "pending",
        id: "external",
        mode: "steer",
        source: "user",
        body: "steer-once",
        uploads: [],
      };
      boundary = connectSessionInputs({
        conversation: root,
        harness: h,
        storage: store,
        job: { scope: {}, authority: { assertActive() {} } },
        inputs: {
          async selectAt(_s, at) {
            if (mail.state !== "pending") return;
            mail.state = "sending";
            decisions.push(at);
            return mail;
          },
          async releaseBeforePlacement() {
            mail.state = "pending";
          },
        },
      });
      const first = await root.submit(
        { type: "input", requestId: "first", content: "use tool" },
        ctx,
      );
      await started.promise;
      if (mode === "reset")
        await root.submit(
          {
            type: "write",
            entry: {
              kind: "test.reset",
              head: "self",
              model: [{ role: "user", content: "new context", timestamp: 1 }],
            },
          },
          ctx,
        );
      release.resolve();
      await boundary.finish();
      assert.equal(seen.length, 1);
      assert.equal(JSON.stringify(seen[0]).split("steer-once").length - 1, 1);
      assert.deepEqual(decisions, mode === "ordinary" ? ["postTools"] : ["postTools", "idle"]);
      const result = await store.submissionByRequest(root.id, "external", ctx);
      assert.equal(result.status, "done");
      const initial = await first.wait(ctx);
      if (mode === "ordinary") assert.equal(result.answer, initial.answer);
      else assert.notEqual(result.answer, initial.answer);
      assert.deepEqual((await h.snapshot(InboxDoc, root.id, ctx)).items, []);
    } finally {
      release.resolve();
      await h.close(ctx);
    }
  },
);

it("leaves subsequent mail in PG when the original native input fails", async () => {
  const store = new MemoryStorage(),
    harness = await Harness.open(
      store,
      { registry: createRegistry(), models: createModels() },
      ctx,
    );
  try {
    const root = await harness.root(ctx),
      selections = [];
    const boundary = connectSessionInputs({
      job: { scope: {}, authority: { assertActive() {} } },
      storage: store,
      harness,
      conversation: root,
      inputs: {
        async selectAt(_s, at) {
          selections.push(at);
          assert.equal(at, "start");
          return { id: "no-model", source: "user", mode: "input", body: "hello", uploads: [] };
        },
      },
    });
    await boundary.start();
    assert.equal((await boundary.finish()).status, "unanswered");
    assert.deepEqual(selections, ["start"]);
  } finally {
    await harness.close(ctx);
  }
});

it("releases a reservation on definite attachment preparation failure", async () => {
  let released;
  const boundary = connectSessionInputs({
    job: { scope: {}, authority: { assertActive() {} } },
    inputs: {
      async selectAt() {
        return { id: "attachment", body: "image", uploads: [] };
      },
      async releaseBeforePlacement(_scope, id) {
        released = id;
      },
    },
    prepare: async () => {
      throw Error("attachment unavailable");
    },
  });
  await assert.rejects(boundary.start(), /attachment unavailable/);
  assert.equal(released, "attachment");
});
