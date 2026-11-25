import { createModels } from "@earendil-works/pi-ai/models";
import { Harness, createRegistry, type StorageWrite, type Seq } from "@earendil-works/pi-durable";
import { fixture } from "./fixtures/native-runner.ts";
import { createServer } from "node:http";
import { expect, it, vi } from "vitest";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { FAKE_MODEL_API_KEY } from "@pi-cloud/fake-model-server";
import { command } from "./fixtures/durable-command.ts";
import { INTERRUPTED_TEXT_ENTRY, INTERRUPTION_MARKER } from "../src/durable/interrupted-text.ts";

it("settles the native missing-directory result before ending the run without another model request", async () => {
  const f = await fixture("tool_hold", true);
  try {
    const running = f.run();
    await f.started.promise;
    f.missingDirectory();
    await expect(running).rejects.toMatchObject({
      code: "workspace_directory_missing",
      retryable: false,
    });
    const entries = (
      await f.storage.scanEntries(
        { conversationId: 1 as import("@earendil-works/pi-durable").ConversationId },
        100,
        undefined,
        BACKGROUND_CONTEXT,
      )
    ).items;
    expect(entries.filter((e) => e.kind === "pi.tool-result")).toHaveLength(1);
    expect(entries.find((e) => e.kind === "pi.tool-result")?.model?.[0]).toMatchObject({
      isError: true,
      details: { execution: "not_started" },
    });
    expect(f.server.observations).toHaveLength(1);
  } finally {
    await f.close();
  }
});
it("reattaches after the missing-directory result is durable but before the Run terminal, without sampling", async () => {
  const captured: import("@pi-cloud/protocol").DurableSessionCommit[] = [];
  let cut = false;
  const old = await fixture(
    "tool_hold",
    true,
    {},
    {
      command,
      bash: "true",
      publish: async (commit) => {
        if (cut) return;
        captured.push(structuredClone(commit));
        cut = commit.writes.some(
          (w) => w.type === "entry" && (w.value as { kind: string }).kind === "pi.tool-result",
        );
      },
    },
  );
  try {
    const work = old.run();
    await old.started.promise;
    old.missingDirectory();
    await expect(work).rejects.toMatchObject({ code: "workspace_directory_missing" });
  } finally {
    await old.close();
  }
  expect(cut).toBe(true);
  const next = await fixture("tool_hold", true);
  try {
    for (const c of captured)
      next.storage
        .prepareCommit(c.writes as unknown as readonly StorageWrite[], c.seq as Seq)
        .apply();
    await expect(next.run()).rejects.toMatchObject({ code: "workspace_directory_missing" });
    expect(next.server.observations).toHaveLength(0);
    expect(next.commands).toHaveLength(0);
  } finally {
    await next.close();
  }
});

it("executes one native post-validation handoff and releases the model permit during the tool wait", async () => {
  const f = await fixture("tool_hold", true);
  const running = f.run();
  try {
    await f.started.promise;
    expect(f.counts()).toEqual({ acquired: 1, returned: 1 });
    expect(f.commands).toHaveLength(1);
    f.release();
    expect((await running).stopReason).toBe("stop");
    const starts = f.events
      .map((value) => value.payload.event)
      .filter((event) => event.type === "tool.started");
    const ends = f.events
      .map((value) => value.payload.event)
      .filter((event) => event.type === "tool.completed");
    expect(starts).toHaveLength(1);
    expect(ends).toHaveLength(1);
    expect(starts[0]!.payload.toolCallId).toBe(f.commands[0]!.toolCallId);
    expect(ends[0]!.payload.toolCallId).toBe(starts[0]!.payload.toolCallId);
  } finally {
    f.release();
    await running.catch(() => {});
    await f.close();
  }
}, 15000);

it("finishes cancelled text through the real Runner without another display message", async () => {
  const model = createServer((request, response) => {
    request.resume();
    response.writeHead(200, { "content-type": "text/event-stream" });
    response.write(
      `data: ${JSON.stringify({
        id: "chatcmpl_partial",
        object: "chat.completion.chunk",
        created: 1700000000,
        model: command.payload.model.modelId,
        choices: [
          {
            index: 0,
            delta: { role: "assistant", content: "VISIBLE_INTERRUPTED_RUNNER_TEXT" },
            finish_reason: null,
          },
        ],
      })}\n\n`,
    );
    // Keep the response open until the Runner receives the user's Stop.
  });
  await new Promise<void>((resolve) => model.listen(0, "127.0.0.1", resolve));
  const address = model.address();
  if (!address || typeof address === "string") throw Error("Expected TCP fixture");
  const f = await fixture("text", false, {
    resolveModelRuntime: () => ({
      provider: command.payload.model.provider,
      modelId: command.payload.model.modelId,
      api: "openai-completions",
      apiKey: FAKE_MODEL_API_KEY,
      baseUrl: `http://127.0.0.1:${address.port}/v1`,
      inputModalities: ["text"],
      contextWindow: 131072,
      maxTokens: 1024,
    }),
  });
  const running = f.run().catch((error: unknown) => error);
  try {
    await vi.waitFor(() =>
      expect(f.events.some(({ payload }) => payload.event.type === "assistant.text.delta")).toBe(
        true,
      ),
    );
    f.stop();
    expect(await running).toMatchObject({ code: "turn_cancelled" });
    const entries = (
      await f.storage.scanEntries(
        { conversationId: 1 as import("@earendil-works/pi-durable").ConversationId },
        100,
        undefined,
        BACKGROUND_CONTEXT,
      )
    ).items;
    expect(entries.filter((entry) => entry.kind === INTERRUPTED_TEXT_ENTRY)).toHaveLength(1);
    expect(entries.find((entry) => entry.kind === "pi.assistant")?.model?.[0]).toMatchObject({
      stopReason: "aborted",
    });
    expect(
      JSON.stringify(entries.find((entry) => entry.kind === INTERRUPTED_TEXT_ENTRY)),
    ).toContain("VISIBLE_INTERRUPTED_RUNNER_TEXT");
    expect(JSON.stringify(f.events)).not.toContain(INTERRUPTION_MARKER);
    expect(f.commands).toHaveLength(0);
  } finally {
    f.stop();
    await running;
    await f.close();
    model.closeAllConnections();
    await new Promise<void>((resolve) => model.close(() => resolve()));
  }
}, 15000);

it.each(["steer", "follow_up"] as const)(
  "consumes durable %s input and keeps reused provider IDs in separate native Tool tasks",
  async (delivery) => {
    const f = await fixture("tool_hold", true);
    const running = f.run();
    try {
      await f.started.promise;
      const id = crypto.randomUUID();
      await f.runner.agentInput(id, "second instruction", delivery);
      f.release();
      await running;
      const submissions = await f.storage.scanSubmissions({}, 100, undefined, BACKGROUND_CONTEXT);
      expect(submissions.items.find((value) => value.requestId === id)?.status).toBe("done");
      const starts = f.events
        .map((value) => value.payload.event)
        .filter((event) => event.type === "tool.started");
      expect(new Set(starts.map((value) => value.payload.toolCallId)).size).toBe(starts.length);
      expect(f.counts().acquired).toBe(f.counts().returned);
    } finally {
      f.release();
      await running.catch(() => {});
      await f.close();
    }
  },
  15000,
);

it("co-commits native Stop with operation cancellation without waiting for an unconfirmed guest result", async () => {
  const f = await fixture("tool_hold", true);
  const running = f.run();
  void running.catch(() => {});
  try {
    await f.started.promise;
    f.stop();
    await expect(running).rejects.toMatchObject({ code: "turn_cancelled" });
    expect(f.cancellations).toEqual([f.commands[0]!.request.operationId]);
    const entries = (
      await f.storage.scanEntries(
        { conversationId: 1 as import("@earendil-works/pi-durable").ConversationId },
        100,
        undefined,
        BACKGROUND_CONTEXT,
      )
    ).items;
    const results = entries.filter((e) => e.kind === "pi.tool-result");
    expect(results).toHaveLength(1);
    const native = results[0]!.model?.[0];
    if (native?.role !== "toolResult") throw new Error("Missing native Bash result");
    expect(JSON.stringify(native.content)).toContain("Tool bash was aborted");
    const cold = await Harness.open(
      f.storage,
      { models: createModels(), registry: createRegistry() },
      BACKGROUND_CONTEXT,
    );
    try {
      const root = await cold.root(BACKGROUND_CONTEXT);
      const { messages } = await root.context(BACKGROUND_CONTEXT);
      const modelResults = messages.filter(
        (m) => m.role === "toolResult" && m.toolCallId === native.toolCallId,
      );
      expect(modelResults).toHaveLength(1);
      expect(JSON.stringify(modelResults)).toContain(
        "The user pressed Stop. Cancellation of this Bash command was requested.",
      );
      expect(JSON.stringify(modelResults)).toContain("No final execution result was confirmed.");
      expect(JSON.stringify(modelResults)).not.toContain("Tool bash was aborted");
    } finally {
      await cold.close(BACKGROUND_CONTEXT);
    }
    expect(f.server.observations).toHaveLength(1);
  } finally {
    f.release();
    await running.catch(() => {});
    await f.close();
  }
}, 15000);

it("stops provider IO when the current cloud lease is revoked", async () => {
  const f = await fixture("timeout"),
    running = f.run();
  void running.catch(() => {});
  try {
    await vi.waitFor(() => expect(f.server.observations).toHaveLength(1));
    f.revoke();
    await expect(running).rejects.toMatchObject({ code: "session_lease_revoked" });
  } finally {
    await running.catch(() => {});
    await f.close();
  }
}, 15000);

it.each([false, true])(
  "prepares native visual input before any model request (invalid=%s)",
  async (invalid) => {
    let prepared = false;
    const image = { type: "image" as const, mimeType: "image/png", data: "aW1hZ2U=" };
    const f = await fixture("text", false, {
      prepareInputUploads: async () => {
        if (invalid) throw new Error("invalid image");
        prepared = true;
        return { images: [image], hints: [] };
      },
      acquireModelPermit: async () => {
        expect(prepared).toBe(true);
        return () => {};
      },
    });
    const upload = {
      id: crypto.randomUUID(),
      sessionId: crypto.randomUUID(),
      workspaceId: crypto.randomUUID(),
      name: "shot.png",
      path: "/workspace/shot.png",
      byteLength: 5,
      mimeType: "image/png",
      sha256: "a".repeat(64),
      image: {
        id: crypto.randomUUID(),
        byteLength: 5,
        mimeType: "image/png" as const,
        sha256: "a".repeat(64),
      },
    };
    try {
      const running = f.run({
        ...command,
        payload: {
          ...command.payload,
          input: { kind: "prompt", text: "inspect", uploads: [upload] },
        },
      });
      if (invalid) {
        await expect(running).rejects.toThrow("invalid image");
        expect(f.server.observations).toHaveLength(0);
      } else {
        await running;
        expect(
          JSON.stringify(
            await f.storage.scanEntries(
              { conversationId: 1 as import("@earendil-works/pi-durable").ConversationId },
              100,
              undefined,
              BACKGROUND_CONTEXT,
            ),
          ),
        ).toContain(image.data);
      }
    } finally {
      await f.close();
    }
  },
  15000,
);

it("decides the project verification reminder from native progress and consumes it only once", async () => {
  const f = await fixture("tool_hold", false, {
    settlementPolicy: {
      command: "npm test",
      cwd: ".",
      workingDirectory: "/workspace",
      timeoutMs: 1000,
      maximumFollowUps: 1,
    },
  });
  try {
    await f.run();
    const entries = (
      await f.storage.scanEntries(
        { conversationId: 1 as import("@earendil-works/pi-durable").ConversationId },
        100,
        undefined,
        BACKGROUND_CONTEXT,
      )
    ).items;
    const reminders = entries.filter(
      (entry) =>
        entry.byTaskId !== undefined &&
        entry.model?.some(
          (message) =>
            message.role === "user" &&
            JSON.stringify(message.content).includes("project-defined verification step"),
        ),
    );
    expect(reminders).toHaveLength(1);
    expect(f.commands).toHaveLength(2); // Initial work, then the one reminder continuation.
    const { prepareNativeSettlementFollowUp } = await import("../src/pi-settlement-gate.ts");
    const answer = entries
      .flatMap((entry) => entry.model ?? [])
      .find((message) => message.role === "assistant")!;
    expect(
      await prepareNativeSettlementFollowUp({
        policy: {
          command: "npm test",
          cwd: ".",
          workingDirectory: "/workspace",
          timeoutMs: 1000,
          maximumFollowUps: 1,
        },
        storage: f.storage,
        conversationId: 1,
        answer: answer as import("@earendil-works/pi-ai").AssistantMessage,
      }),
    ).toBeUndefined();
  } finally {
    await f.close();
  }
}, 15000);

it("settles a Run after individual withdrawal and publishes a consumed human input once", async () => {
  const f = await fixture("tool_hold", true),
    running = f.run();
  const scope = command.payload;
  const submit = (suffix: string, text: string) =>
    f.runner.humanInput({
      action: "submit",
      tenantId: scope.tenantId,
      sessionId: scope.sessionId,
      runId: scope.runId,
      executionReference: scope.executionReference,
      input: {
        inputId: `input-${suffix.repeat(64)}`,
        idempotencyKey: suffix,
        fingerprint: suffix.repeat(64),
        text,
        uploads: [],
        mode: "follow_up",
        acceptedAt: new Date().toISOString(),
      },
    });
  try {
    await f.started.promise;
    const first = await submit("a", "withdraw this");
    await f.runner.humanInput({
      action: "mutate",
      tenantId: scope.tenantId,
      sessionId: scope.sessionId,
      runId: scope.runId,
      executionReference: scope.executionReference,
      mutation: {
        operationId: `edit-${"b".repeat(64)}`,
        fingerprint: "b".repeat(64),
        inputId: `input-${"a".repeat(64)}`,
        revision: first.queue.revision,
        change: { action: "cancel" },
      },
    });
    await submit("c", "human continuation");
    f.release();
    expect((await running).stopReason).toBe("stop");
    const humans = f.events.map((v) => v.payload.event).filter((e) => e.type === "user.input");
    expect(humans).toHaveLength(1);
    expect(humans[0]!.payload).toMatchObject({
      text: "human continuation",
      inputId: `input-${"c".repeat(64)}`,
    });
    expect(
      (await f.storage.scanSubmissions({}, 100, undefined, BACKGROUND_CONTEXT)).items.find(
        (s) => s.requestId === `input-${"a".repeat(64)}`,
      ),
    ).toMatchObject({ status: "unanswered", reason: "aborted" });
  } finally {
    f.release();
    await running.catch(() => {});
    await f.close();
  }
}, 15000);
