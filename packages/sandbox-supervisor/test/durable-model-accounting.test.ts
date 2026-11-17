import { randomUUID } from "node:crypto";
import { expect, it, vi } from "vitest";
import {
  MODEL_METERING_HEADER,
  parseModelMeteringContext,
  type DurableSessionCommit,
} from "@pi-cloud/protocol";
import { DurableModelAccounting } from "../src/durable/model-accounting.ts";
import { command } from "./fixtures/durable-command.ts";

function fixture() {
  const input = structuredClone(command);
  input.payload.tenantId = randomUUID();
  input.payload.sessionId = randomUUID();
  const accounting = new DurableModelAccounting(input);
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
    accounting.request({
      taskId: 10,
      phase: purpose === "agent" ? "request" : "summarize",
      provider: "openai-codex",
      model: "gpt-6-luna",
      purpose,
      fetch: send,
    });
  const commit = (state: Record<string, unknown>): DurableSessionCommit => ({
    seq: 20,
    nextId: 30,
    writes: [{ type: "task", value: { id: 10, conversationId: 1, state } }],
  });
  return { accounting, contexts, request, commit };
}

it.each(["agent", "context_maintenance"] as const)(
  "joins %s HTTP identity to its native settlement without an extra commit",
  async (purpose) => {
    const f = fixture(),
      request = f.request(purpose);
    await request.fetch("http://model.invalid", { method: "POST", body: "{}" });
    expect(f.contexts[0]).toMatchObject({ purpose, execution: { taskId: 10, fencingToken: 7 } });
    request.finish("completed");
    expect(f.accounting.prepare({ seq: 19, nextId: 30, writes: [] }).evidence).toEqual([]);
    const commit = f.commit({ status: "terminal", outcome: { status: "completed" } });
    const prepared = f.accounting.prepare(commit);
    expect(prepared.evidence).toEqual([{ context: f.contexts[0], outcome: "completed" }]);
    expect(f.accounting.prepare(commit).evidence).toEqual(prepared.evidence);
    prepared.confirmed();
    expect(f.accounting.prepare(commit).evidence).toEqual([]);
  },
);

it("keeps partial progress separate and commits native cancellation evidence even before provider cleanup", async () => {
  const f = fixture(),
    request = f.request();
  await request.fetch("http://model.invalid");
  expect(
    f.accounting.prepare(f.commit({ status: "running", checkpoint: { phase: "request" } }))
      .evidence,
  ).toEqual([]);
  expect(
    f.accounting.prepare(f.commit({ status: "terminal", outcome: { status: "aborted" } })).evidence,
  ).toEqual([{ context: f.contexts[0], outcome: "cancelled" }]);
});

it("assigns actual HTTP retries distinct IDs and marks unused earlier requests superseded", async () => {
  const f = fixture(),
    request = f.request();
  await request.fetch("http://model.invalid");
  await request.fetch("http://model.invalid");
  request.finish("completed");
  expect(f.contexts[0]!.invocationId).not.toBe(f.contexts[1]!.invocationId);
  expect(
    f.accounting.prepare(f.commit({ status: "waiting", checkpoint: { phase: "tools" } })).evidence,
  ).toEqual([
    { context: f.contexts[0], outcome: "superseded" },
    { context: f.contexts[1], outcome: "completed" },
  ]);
});

it("uses the final native task state when a batch writes the same task more than once", async () => {
  const f = fixture(),
    request = f.request();
  await request.fetch("http://model.invalid");
  request.finish("completed");
  const completed = f.commit({ status: "terminal", outcome: { status: "completed" } });
  const cancelled = f.commit({ status: "terminal", outcome: { status: "aborted" } });
  const prepared = f.accounting.prepare({
    ...cancelled,
    writes: [...completed.writes, ...cancelled.writes],
  });
  expect(prepared.evidence).toEqual([{ context: f.contexts[0], outcome: "cancelled" }]);
});
