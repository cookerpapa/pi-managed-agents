// Process fixture. The parent owns the Projector; the actors share no JS state.
import { once } from "node:events";
import { randomUUID } from "node:crypto";
import assert from "node:assert/strict";
import pg from "pg";
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
} from "@earendil-works/pi-durable";
import { BACKGROUND_CONTEXT as ctx } from "@earendil-works/chord/context";
import { createDatabase } from "@pi-cloud/database";
import { readSealedFamily, hydrateSealedFamily } from "@pi-cloud/pi-session-postgres/durable";
import { delegatedControl } from "./delegated-runs.mjs";
import { loadSettledDelegations } from "./delegated-cold.mjs";
import { runDelegation } from "./delegated-native.mjs";
const [{ scope, url, topic, recover, phase, target }] = await once(process, "message");
const pool = new pg.Pool({ connectionString: url }),
  control = delegatedControl(pool, scope);
class Store extends MemoryStorage {
  async commit(w) {
    const p = this.prepareCommit(w);
    const receipt = once(process, "message");
    process.send({
      kind: "commit",
      fact: {
        kind: "durable_commit",
        factId: randomUUID(),
        scope,
        occurredAt: new Date().toISOString(),
        commit: { seq: p.seq, nextId: this.nextId, writes: p.writes },
        display: [],
        tools: [],
        cancellations: [],
        origins: [],
      },
    });
    const [ack] = await receipt;
    assert.equal(ack.seq, p.seq);
    return p.apply();
  }
}
const store = new Store();
try {
  if (recover) {
    const db = createDatabase({ connectionString: url, maxConnections: 1 });
    try {
      await hydrateSealedFamily(
        await readSealedFamily(db, scope, topic, { delegatedRuns: true }),
        store,
      );
    } finally {
      await db.destroy();
    }
  }
  if (!recover && phase === "continuation") await loadSettledDelegations(pool, scope, store);
  const registry = createRegistry(),
    models = createModels(),
    fake = fauxProvider({ tokensPerSecond: 0 });
  models.setProvider(fake.provider);
  registry.install(
    defineExtension({
      name: "handoff",
      tools: [
        defineTool({
          name: "subagent",
          description: "Delegate",
          parameters: Type.Object({ task: Type.String() }),
          replay: "safe",
          async execute(args, api, c) {
            const got = await runDelegation(
              api,
              store,
              control,
              phase === "continuation"
                ? { task: args.task, mode: "continue", agentId: target }
                : { task: args.task, mode: "fresh" },
              c,
            );
            assert.equal(
              got.output,
              phase === "seed" ? "archived private answer" : "resumed child",
            );
            return { content: [{ type: "text", text: got.output }] };
          },
        }),
        defineTool({
          name: "hold_reply",
          description: "Controlled reply waiter",
          parameters: Type.Object({}),
          replay: "safe",
          async execute(_args, api, c) {
            const op = await api.memo("reply-reference", randomUUID(), c);
            process.send({ kind: "waiting", op });
            if (!recover) await new Promise(() => {});
            return { content: [{ type: "text", text: "reply " + op }] };
          },
        }),
      ],
    }),
  );
  fake.setResponses(
    Array.from({ length: 12 }, () => (transcript) => {
      const last = transcript.messages.at(-1);
      const child = transcript.messages.some(
        (m) => m.role === "user" && JSON.stringify(m.content).includes("child work"),
      );
      if (child && phase === "seed") return fauxAssistantMessage("archived private answer");
      if (child && phase === "continuation")
        assert(
          transcript.messages.some(
            (m) =>
              m.role === "assistant" &&
              JSON.stringify(m.content).includes("archived private answer"),
          ),
          "Cold continuation lost the child answer",
        );
      if (last.role === "toolResult")
        return fauxAssistantMessage(child ? "resumed child" : "resumed parent");
      return fauxAssistantMessage(
        child
          ? fauxToolCall("hold_reply", {}, "hold")
          : fauxToolCall(
              "subagent",
              { task: phase === "continuation" ? "continue child work" : "child work" },
              "delegate",
            ),
        { stopReason: "toolUse" },
      );
    }),
  );
  const h = await Harness.open(
    store,
    { models, registry, settings: { retry: { maxRetries: 0 }, compaction: { enabled: false } } },
    ctx,
  );
  const root = await h.root(ctx, {
    agent: { model: { provider: fake.getModel().provider, modelId: fake.getModel().id } },
  });
  if (!recover)
    await root.submit({ type: "input", requestId: scope.runId, content: "delegate the job" }, ctx);
  await root.waitForIdle(ctx);
  const context = await root.context(ctx);
  assert(JSON.stringify(context.messages.at(-1)).includes("resumed parent"));
  process.send({ kind: "done", seq: store.nextSeq - 1 });
  await h.close(ctx);
} catch (error) {
  process.send({ kind: "error", message: error.stack });
  process.exitCode = 1;
} finally {
  await pool.end();
  process.disconnect();
}
