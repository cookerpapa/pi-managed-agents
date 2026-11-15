import { randomUUID } from "node:crypto";
import { afterEach, expect, it, vi } from "vitest";
import { Type, type Message } from "@earendil-works/pi-ai";
import { WebSocket } from "ws";
import { FakeResponsesServer, benchmarkPrompt } from "../src/responses-server.ts";
import { CloudModelRegistry } from "../../sandbox-supervisor/src/durable/model-registry.ts";
import { isRetryableAssistantError } from "@earendil-works/pi-ai/utils/retry";

const closers: (() => Promise<unknown>)[] = [];
afterEach(async () => {
  for (const close of closers.splice(0).reverse()) await close();
});
async function fixture(
  options: Partial<ConstructorParameters<typeof FakeResponsesServer>[0]> = {},
) {
  const key = `e30.${Buffer.from(JSON.stringify({ "https://api.openai.com/auth": { chatgpt_account_id: "owned-fixture" } })).toString("base64url")}.fixture`;
  const server = new FakeResponsesServer({
    model: "pma-benchmark-contract",
    apiKey: key,
    ...options,
  });
  await server.start();
  closers.push(() => server.stop());
  const post = (input: unknown, signal?: AbortSignal) =>
    fetch(server.origin + "/v1/responses", {
      method: "POST",
      headers: { authorization: `Bearer ${key}`, "content-type": "application/json" },
      body: JSON.stringify(input),
      ...(signal ? { signal } : {}),
    });
  return { server, key, post };
}

it("runs native Pi Responses text and fragmented tool round trips without a real provider", async () => {
  const f = await fixture(),
    registry = new CloudModelRegistry();
  const model = registry.configure({
    provider: "openai-codex",
    modelId: f.server.options.model,
    api: "openai-codex-responses",
    baseUrl: f.server.origin,
    apiKey: f.key,
    inputModalities: ["text", "image"],
    contextWindow: 1000000,
    maxTokens: 4096,
  });
  const messages: Message[] = [
    {
      role: "user",
      content: benchmarkPrompt({
        id: "native",
        kind: "mcp",
        rounds: 1,
        toolName: "fixture_echo",
        outputBytes: 512,
        chunkBytes: 32,
        toolResultBytes: 128,
      }),
      timestamp: 1,
    },
  ];
  const sample = () =>
    registry.models
      .streamSimple(
        model,
        {
          messages,
          tools: [
            {
              name: "fixture_echo",
              description: "Owned echo",
              parameters: Type.Object({ text: Type.String() }),
            },
          ],
        },
        {
          apiKey: f.key,
          transport: "sse",
          sessionId: randomUUID(),
          maxRetries: 0,
          timeoutMs: 5000,
        },
      )
      .result();
  const first = await sample();
  expect(first.stopReason, first.errorMessage).toBe("toolUse");
  const call = first.content.find((c) => c.type === "toolCall");
  expect(call).toBeDefined();
  if (!call || call.type !== "toolCall") throw new Error("No complete native Tool call");
  expect(call.name).toBe("fixture_echo");
  expect(call.arguments.text).toContain("PMA_TOOL native 0");
  messages.push(first, {
    role: "toolResult",
    toolCallId: call.id,
    toolName: call.name,
    content: [{ type: "text", text: "DONE" }],
    isError: false,
    timestamp: 2,
  });
  const final = await sample();
  expect(final.stopReason, final.errorMessage).toBe("stop");
  expect(final.content.some((c) => c.type === "text" && c.text.startsWith("PMA_DONE native"))).toBe(
    true,
  );
  expect(f.server.metrics().completed).toBe(2);
  expect(f.server.metrics().samples.every((s) => s.planId === "native")).toBe(true);
});

it("exposes a transient WebSocket error that the pinned native retry policy recognizes", async () => {
  const f = await fixture(),
    registry = new CloudModelRegistry();
  const model = registry.configure({
    provider: "openai-codex",
    modelId: f.server.options.model,
    api: "openai-codex-responses",
    baseUrl: f.server.origin,
    apiKey: f.key,
    inputModalities: ["text"],
    contextWindow: 1000000,
    maxTokens: 4096,
  });
  const result = await registry.models
    .streamSimple(
      model,
      {
        messages: [
          {
            role: "user",
            content: benchmarkPrompt({ id: "native-429", kind: "text", fault: "rate_limit_once" }),
            timestamp: 1,
          },
        ],
      },
      {
        apiKey: f.key,
        transport: "websocket",
        sessionId: randomUUID(),
        maxRetries: 0,
        timeoutMs: 5000,
      },
    )
    .result();
  expect(result.stopReason).toBe("error");
  expect(isRetryableAssistantError(result), result.errorMessage).toBe(true);
});

it("resumes a synthetic response on a new WebSocket using only its Tool result suffix", async () => {
  const f = await fixture();
  const open = async () => {
    const ws = new WebSocket(f.server.origin.replace("http:", "ws:") + "/responses", {
      headers: { authorization: `Bearer ${f.key}` },
    });
    await new Promise<void>((resolve, reject) => {
      ws.once("open", resolve);
      ws.once("error", reject);
    });
    closers.push(async () => {
      ws.terminate();
    });
    return ws;
  };
  const request = (ws: WebSocket, payload: unknown) =>
    new Promise<any>((resolve, reject) => {
      const receive = (raw: Buffer) => {
        const frame = JSON.parse(raw.toString());
        if (frame.type === "response.completed" || frame.type === "error") {
          ws.off("message", receive);
          resolve(frame);
        }
      };
      ws.on("message", receive);
      ws.send(JSON.stringify({ type: "response.create", ...(payload as object) }), (error) => {
        if (error) {
          ws.off("message", receive);
          reject(error);
        }
      });
    });
  const a = await open();
  const first = await request(a, {
    model: f.server.options.model,
    input: [
      {
        role: "user",
        content: benchmarkPrompt({ id: "ws", kind: "mcp", rounds: 1, toolName: "echo" }),
      },
    ],
    tools: [{ type: "function", name: "echo" }],
  });
  a.terminate();
  const b = await open(),
    call = first.response.output.find((v: any) => v.type === "function_call");
  const second = await request(b, {
    model: f.server.options.model,
    previous_response_id: first.response.id,
    input: [{ type: "function_call_output", call_id: call.call_id, output: "OK" }],
    tools: [{ type: "function", name: "echo" }],
  });
  expect(second.response.output[0].content[0].text).toContain("PMA_DONE ws");
  expect(f.server.metrics().samples[1]).toMatchObject({ incremental: true, inputItems: 1 });
  expect(
    await request(b, { model: f.server.options.model, previous_response_id: "expired", input: [] }),
  ).toHaveProperty("error.code", "previous_response_not_found");
});

it("enforces the owned model/plan, records bounded metadata, and handles once-only 429", async () => {
  const f = await fixture({ sampleEntries: 2, cacheEntries: 2 });
  const input = {
    model: f.server.options.model,
    stream: false,
    input: [
      {
        role: "user",
        content: benchmarkPrompt({
          id: "limit",
          kind: "text",
          fault: "rate_limit_once",
          outputBytes: 512,
        }),
      },
    ],
  };
  expect((await f.post(input)).status).toBe(429);
  const reply = await f.post(input);
  expect(reply.status).toBe(200);
  expect((await reply.json()).status).toBe("completed");
  expect((await f.post({ ...input, model: "gpt-real" })).status).toBe(400);
  expect(
    (await f.post({ ...input, input: [{ role: "user", content: "private arbitrary prompt" }] }))
      .status,
  ).toBe(400);
  const metrics = f.server.metrics();
  expect(metrics.samples).toHaveLength(2);
  expect(metrics.rejected).toBe(2);
  const retained = JSON.stringify(metrics);
  expect(retained).not.toContain(f.key);
  expect(retained).not.toContain("private arbitrary prompt");
});

it("cancels a delayed response and releases in-flight state", async () => {
  const f = await fixture(),
    abort = new AbortController();
  const pending = f.post(
    {
      model: f.server.options.model,
      stream: true,
      input: [
        {
          role: "user",
          content: benchmarkPrompt({ id: "cancel", kind: "text", firstByteMs: 10000 }),
        },
      ],
    },
    abort.signal,
  );
  const rejection = expect(pending).rejects.toThrow();
  await vi.waitFor(() => expect(f.server.metrics().inflight).toBe(1));
  abort.abort();
  await rejection;
  await vi.waitFor(() => expect(f.server.metrics()).toMatchObject({ inflight: 0, aborted: 1 }));
});

it("requires explicit opt-in for non-loopback fixture listening", () => {
  expect(
    () =>
      new FakeResponsesServer({
        model: "pma-benchmark-contract",
        apiKey: "synthetic-credential",
        host: "0.0.0.0",
      }),
  ).toThrow("explicit owned fixture");
});
