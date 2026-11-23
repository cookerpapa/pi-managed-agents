import { expect, it } from "vitest";
import { nativeSubagentFixture } from "./native-subagent-fixture.ts";
import { ExecutionStreamProjector } from "../../runtime-core/src/execution-stream-projection.ts";
import type { AcceptedDurableCommitFact } from "../../runtime-core/src/accepted-fact.ts";

it("reloads an evicted active identity while preserving the real native replay prefix", async () => {
  const f = await nativeSubagentFixture();
  const projector = new ExecutionStreamProjector(f.database, {
    maximumEntries: 1,
    maximumBytes: 4096,
  });
  const head = await f.projection.head();
  const fact = (n: number): AcceptedDurableCommitFact => ({
    kind: "durable_commit",
    factId: crypto.randomUUID(),
    scope: f.scope,
    occurredAt: new Date().toISOString(),
    commit: {
      seq: head.seq + n,
      nextId: head.nextId + n,
      writes: [
        {
          type: "task",
          value: {
            id: head.nextId + n - 1,
            conversationId: 1,
            kind: "fixture.memory",
            version: 1,
            input: {},
            background: false,
            abortRequested: false,
            state: { status: "terminal", outcome: { status: "completed", result: {} } },
          },
        },
      ],
    },
    origins: [],
    display: [],
    tools: [],
    cancellations: [],
  });
  const first = { topic: "memory", partition: 0, offset: 10n, fact: fact(1) },
    second = { topic: "memory", partition: 0, offset: 11n, fact: fact(2) };
  try {
    await projector.project(first);
    expect(
      await projector.accepts({
        ...first,
        fact: {
          ...first.fact,
          scope: { ...f.scope, writerId: crypto.randomUUID(), runId: crypto.randomUUID() },
        },
      }),
    ).toBe(false);
    expect(projector.statistics().identityCache.evictions).toBe(1);
    await projector.project(second);
    expect((await f.projection.head()).seq).toBe(head.seq + 2);
    expect(projector.statistics().identityCache.loads).toBe(3);
    projector.reset();
    await expect(projector.project(second)).rejects.toThrow("prefix is missing");
    await projector.project(first);
    await projector.project(second);
    expect((await f.projection.head()).seq).toBe(head.seq + 2);
  } finally {
    await f.close();
  }
}, 30000);
