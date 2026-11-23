import { sql } from "kysely";
import { expect, it } from "vitest";
import { randomUUID } from "node:crypto";
import { readSessionModelUsage, ModelMeteringService } from "@pi-cloud/accounting";
import type { ModelMeteringContext, ModelMeteringEvent, ModelUsageFact } from "@pi-cloud/protocol";
import { nativeSubagentFixture } from "./native-subagent-fixture.ts";
import { ExecutionStreamProjector } from "../../runtime-core/src/execution-stream-projection.ts";
import {
  encodeSessionLogFact,
  decodeSessionLogFact,
} from "../../runtime-core/src/kafka-accepted-fact.ts";
import type {
  AcceptedDurableCommitFact,
  AcceptedFact,
} from "../../runtime-core/src/accepted-fact.ts";

it("keeps provider usage stable across projection, writer closure, stale output and duplicate delivery", async () => {
  const f = await nativeSubagentFixture();
  const projector = new ExecutionStreamProjector(f.database);
  const metering = new ModelMeteringService(f.database);
  const head = await f.projection.head();
  const context = (taskId: number): ModelMeteringContext => ({
    version: 1,
    invocationId: randomUUID(),
    requestedAt: new Date().toISOString(),
    tenantId: f.scope.tenantId,
    sessionId: f.scope.sessionId,
    runId: f.scope.runId,
    piSessionId: f.scope.piSessionId,
    conversationId: 1,
    provider: "openai-codex",
    model: "gpt-6-luna",
    purpose: "agent",
    execution: { writerId: f.scope.writerId, fencingToken: f.scope.fencingToken, taskId },
  });
  const valid = context(head.nextId),
    late = context(head.nextId + 1);
  const fact = (c: ModelMeteringContext, seq: number): AcceptedDurableCommitFact => ({
    kind: "durable_commit",
    factId: randomUUID(),
    scope: f.scope,
    occurredAt: c.requestedAt,
    commit: {
      seq,
      nextId: c.execution!.taskId! + 1,
      writes: [
        {
          type: "task",
          value: {
            id: c.execution!.taskId!,
            conversationId: 1,
            kind: "fixture.model",
            version: 1,
            input: {},
            background: false,
            abortRequested: false,
            state: { status: "terminal", outcome: { status: "completed", result: {} } },
          },
        },
      ],
    },
    origins: [
      {
        recordId: c.execution!.taskId!,
        sessionId: c.sessionId,
        turnId: f.scope.turnId,
        runId: c.runId,
      },
    ],
    tools: [],
    cancellations: [],
    display: [],
  });
  const project = (value: AcceptedFact, offset: bigint) =>
    projector.project({
      fact: decodeSessionLogFact(Buffer.from(encodeSessionLogFact(value))).fact as AcceptedFact,
      topic: "financial-projection",
      partition: 0,
      offset,
    });
  const usage: ModelUsageFact = {
    provider: "codex",
    model: "gpt-6-luna",
    serviceTier: "default",
    failed: false,
    quality: "complete",
    unsupportedDimensions: false,
    tokens: {
      input: 100,
      cacheRead: 0,
      cacheWrite: 0,
      output: 20,
      reasoning: 0,
      unclassified: 0,
      total: 120,
    },
  };
  const receipts = (c: ModelMeteringContext): ModelMeteringEvent[] => {
    const executionId = randomUUID(),
      occurredAt = c.requestedAt;
    return [
      { context: c, kind: "started", occurredAt },
      { context: c, kind: "attempt", occurredAt, executionId },
      { context: c, kind: "usage", occurredAt, executionId, usage },
      { context: c, kind: "finished", occurredAt, cancelled: false, statusCode: 200 },
    ];
  };
  try {
    const events = [...receipts(late), ...receipts(valid)];
    await metering.ingest({ events });
    const usage = async () =>
      (await readSessionModelUsage(f.database, f.scope.tenantId, [f.scope.sessionId])).get(
        f.scope.sessionId,
      )!.tokens.total;
    expect(await usage()).toBe(240);
    const accepted = fact(valid, head.seq + 1);
    const retained = {
      ...JSON.parse(encodeSessionLogFact(accepted)),
      modelEvidence: [{ context: valid, outcome: "completed" }],
    };
    const decoded = decodeSessionLogFact(JSON.stringify(retained)).fact;
    expect(JSON.parse(encodeSessionLogFact(decoded))).not.toHaveProperty("modelEvidence");
    if (decoded.kind !== "durable_commit") throw new Error("Expected retained model commit");
    const position = { topic: "financial-projection", partition: 0, offset: 10n };
    await projector.project({ fact: decoded, ...position });
    // Old native commit fingerprints include the opaque proof. Rewind can verify it
    // without creating a financial event, even after a fresh Projector starts.
    const {
      rows: [stored],
    } = await sql<{
      metadata: unknown;
    }>`select cloud_metadata::jsonb metadata from pi_durable_commits where tenant_id=${f.scope.tenantId}::uuid and session_id=${f.scope.piSessionId} and seq=${decoded.commit.seq}`.execute(
      f.database,
    );
    expect(stored?.metadata).toEqual({ modelEvidence: retained.modelEvidence });
    await new ExecutionStreamProjector(f.database).project({ fact: decoded, ...position });
    expect((await f.projection.head()).seq).toBe(head.seq + 1);
    expect(
      await f.database
        .selectFrom("model_metering_events")
        .select("id")
        .where("id", "=", `${valid.invocationId}:execution`)
        .executeTakeFirst(),
    ).toBeUndefined();
    await project(
      {
        kind: "work_finished",
        factId: randomUUID(),
        scope: f.scope,
        agentId: "root",
        baseSequence: 0,
        nativeSequence: head.seq + 1,
        terminal: { type: "turn.completed", payload: { stopReason: "stop" } },
        occurredAt: new Date().toISOString(),
      },
      11n,
    );
    await project(fact(late, head.seq + 2), 12n);
    expect(projector.statistics().nativeLive).toEqual({
      views: 0,
      pendingDocuments: 0,
      deferredReceipts: 0,
      estimatedBytes: 0,
    });
    expect(projector.statistics().prefixes).toBe(0);
    expect((await f.projection.head()).seq).toBe(head.seq + 1);
    expect(
      await f.database
        .selectFrom("model_metering_events")
        .select("id")
        .where("id", "=", `${late.invocationId}:execution`)
        .executeTakeFirst(),
    ).toBeUndefined();
    await metering.ingest({ events });
    expect(await usage()).toBe(240);
    expect(
      await f.database
        .selectFrom("model_execution_writers")
        .select("writer_id")
        .where("writer_id", "=", f.scope.writerId)
        .execute(),
    ).toHaveLength(0);
    const page = await metering.list(f.scope.tenantId, {
      from: "2020-01-01T00:00:00.000Z",
      to: "2099-01-01T00:00:00.000Z",
      limit: 10,
    });
    expect(
      page.items.find((i) => i.context.invocationId === valid.invocationId)!.usage[0]!.eligibility
        .state,
    ).toBe("pending");
    expect(
      page.items.find((i) => i.context.invocationId === late.invocationId)!.usage[0]!.eligibility
        .state,
    ).toBe("pending");
  } finally {
    await f.close();
  }
});
