import { randomUUID } from "node:crypto";
import { expect, it, vi } from "vitest";
import { MODEL_METERING_HEADER, parseModelMeteringContext } from "@pi-cloud/protocol";
import { attributedModelFetch } from "../src/durable/model-accounting.ts";
import { command } from "./fixtures/durable-command.ts";

function fixture() {
  const input = structuredClone(command);
  input.payload.tenantId = randomUUID();
  input.payload.sessionId = randomUUID();
  const contexts: ReturnType<typeof parseModelMeteringContext>[] = [];
  const send = vi.fn<typeof fetch>(async (_url, init) => {
    contexts.push(
      parseModelMeteringContext(
        JSON.parse(
          Buffer.from(
            new Headers(init?.headers).get(MODEL_METERING_HEADER)!,
            "base64url",
          ).toString(),
        ),
      ),
    );
    return new Response("ok");
  });
  const request = (purpose: "agent" | "context_maintenance" = "agent") =>
    attributedModelFetch(input, {
      taskId: 10,
      provider: "openai-codex",
      model: "gpt-6-luna",
      purpose,
      fetch: send,
    });
  return { contexts, request };
}

it.each(["agent", "context_maintenance"] as const)(
  "attributes each actual %s HTTP request, including retries",
  async (purpose) => {
    const { contexts, request } = fixture();
    const fetch = request(purpose);
    await fetch("http://model.invalid");
    await fetch("http://model.invalid");
    expect(contexts).toHaveLength(2);
    expect(
      contexts.every(
        (c) =>
          c.purpose === purpose && c.execution?.taskId === 10 && c.execution.fencingToken === 7,
      ),
    ).toBe(true);
    expect(contexts[0]!.invocationId).not.toBe(contexts[1]!.invocationId);
  },
);
