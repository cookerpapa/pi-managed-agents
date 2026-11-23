import { randomUUID } from "node:crypto";
import { afterAll, beforeAll, expect, it } from "vitest";
import { createDatabase, runMigrations } from "@pi-cloud/database";
import {
  parseModelBillingPage,
  type ModelMeteringContext,
  type ModelMeteringEvent,
  type ModelUsageFact,
} from "@pi-cloud/protocol";
import { ModelMeteringService } from "../src/model-metering-service.ts";
import {
  ModelExecutionAccounting,
  type ModelExecutionEvidence,
} from "./fixtures/historical-model-execution.ts";

const db = createDatabase({
  connectionString: process.env.PI_CLOUD_POSTGRES_INTEGRATION_URL!,
  maxConnections: 4,
});
beforeAll(() => runMigrations(db, "up"));
afterAll(() => db.destroy());
const sourceUsage: ModelUsageFact = {
  provider: "codex",
  model: "gpt-6-luna",
  serviceTier: "default",
  failed: false,
  quality: "complete",
  unsupportedDimensions: false,
  tokens: {
    input: 100,
    cacheRead: 50,
    cacheWrite: 0,
    output: 20,
    reasoning: 10,
    unclassified: 0,
    total: 180,
  },
};
const query = { from: "2020-01-01T00:00:00.000Z", to: "2099-01-01T00:00:00.000Z", limit: 50 };
async function fixture() {
  const tenantId = randomUUID(),
    runId = randomUUID(),
    sessionId = randomUUID();
  const at = Date.now() + 60000;
  const service = new ModelMeteringService(db),
    accounting = new ModelExecutionAccounting();
  await service.createRate(tenantId, {
    kind: "customer",
    provider: "openai-codex",
    model: "gpt-6-luna",
    serviceTier: "default",
    effectiveAt: new Date(at).toISOString(),
    currency: "USD",
    perMillion: { input: "1", cacheRead: "1", cacheWrite: "1", output: "1", reasoning: "1" },
  });
  const context = (
    writerId: string = randomUUID(),
    fencingToken = 1,
    taskId = 10,
  ): ModelMeteringContext => ({
    version: 1,
    invocationId: randomUUID(),
    tenantId,
    runId,
    sessionId,
    piSessionId: sessionId,
    conversationId: 1,
    provider: "openai-codex",
    model: "gpt-6-luna",
    purpose: "agent",
    requestedAt: new Date(at + 1000).toISOString(),
    execution: { writerId, fencingToken, taskId },
  });
  const scope = (c: ModelMeteringContext) => ({
    tenantId,
    piSessionId: sessionId,
    runId,
    writerId: c.execution!.writerId,
    fencingToken: c.execution!.fencingToken,
  });
  const position = (offset = 1n) => ({ topic: "accounting-test", partition: 0, offset });
  const proof = (
    c: ModelMeteringContext,
    outcome: ModelExecutionEvidence["outcome"],
    offset = 1n,
  ) =>
    db
      .transaction()
      .execute((tx) =>
        accounting.settle(tx, scope(c), [{ context: c, outcome }], position(offset)),
      );
  const close = (c: ModelMeteringContext, offset = 2n) =>
    db.transaction().execute((tx) => accounting.closeWriter(tx, scope(c), position(offset)));
  const events = (
    c: ModelMeteringContext,
    usage = sourceUsage,
    cancelled = false,
  ): ModelMeteringEvent[] => {
    const executionId = randomUUID(),
      occurredAt = c.requestedAt;
    return [
      { context: c, kind: "started", occurredAt },
      { context: c, kind: "attempt", occurredAt, executionId },
      { context: c, kind: "usage", occurredAt, executionId, usage },
      { context: c, kind: "finished", occurredAt, statusCode: 200, cancelled },
    ];
  };
  const receipt = async (c: ModelMeteringContext) => {
    const page = parseModelBillingPage(await service.list(tenantId, query));
    return page.items.find((i) => i.context.invocationId === c.invocationId)!.usage[0]!;
  };
  return { tenantId, service, accounting, context, scope, position, proof, close, events, receipt };
}

it.each(["source_first", "projection_first"])(
  "settles complete usage exactly once with %s delivery",
  async (order) => {
    const f = await fixture(),
      c = f.context(),
      events = f.events(c);
    if (order === "source_first") {
      await f.service.ingest({ events });
      expect((await f.receipt(c)).valuation.customer).toEqual({
        state: "unconfirmed",
        reason: "execution_not_confirmed",
      });
    }
    await f.proof(c, "completed");
    await f.service.ingest({ events });
    await f.service.ingest({ events });
    expect((await f.receipt(c)).eligibility.state).toBe("billable");
    expect((await f.receipt(c)).valuation.customer).toMatchObject({
      state: "priced",
      amountPico: "180000000",
    });
    await f.close(c);
    await db.deleteFrom("sessions").where("id", "=", c.sessionId).execute();
    expect((await f.receipt(c)).eligibility.state).toBe("billable");
    expect(
      (await f.service.summary(f.tenantId, query)).categories.find((p) => p.kind === "customer"),
    ).toMatchObject({ state: "priced", amountPico: "180000000", count: "1" });
  },
);

it("charges valid user Stop with complete usage even when the provider labels cancellation failed", async () => {
  const f = await fixture(),
    c = f.context();
  await f.proof(c, "cancelled");
  await f.close(c);
  await f.service.ingest({ events: f.events(c, { ...sourceUsage, failed: true }, true) });
  expect((await f.receipt(c)).eligibility.state).toBe("billable");
  expect((await f.receipt(c)).valuation.customer).toMatchObject({
    state: "priced",
    amountPico: "180000000",
  });
});

it("distinguishes a valid late old receipt, obsolete work and its replacement on the same Run", async () => {
  const f = await fixture(),
    valid = f.context(),
    stale = f.context(valid.execution!.writerId, 1, 11),
    next = f.context(randomUUID(), 2, 12);
  await f.proof(valid, "completed");
  await f.close(valid);
  await f.proof(next, "completed", 4n);
  await f.service.ingest({ events: [...f.events(stale), ...f.events(next), ...f.events(valid)] });
  expect((await f.receipt(valid)).eligibility.state).toBe("billable");
  expect((await f.receipt(next)).eligibility.state).toBe("billable");
  expect((await f.receipt(stale)).eligibility).toEqual({
    state: "platform",
    reason: "writer_closed_without_settlement",
  });
  expect((await f.receipt(stale)).usage.tokens.total).toBe(180);
  expect((await f.receipt(stale)).valuation.customer).toMatchObject({
    state: "waived",
    amountPico: "0",
  });
  const summary = await f.service.summary(f.tenantId, query);
  expect(
    summary.categories.find((p) => p.kind === "customer" && p.state === "priced"),
  ).toMatchObject({ count: "2", amountPico: "360000000" });
});

it("missing usage remains unconfirmed and ambiguous provider retries do not become duplicate customer charges", async () => {
  const f = await fixture(),
    incomplete = f.context(),
    retry = f.context();
  await f.proof(incomplete, "cancelled");
  await f.service.ingest({
    events: f.events(incomplete, { ...sourceUsage, quality: "unclassified" }, true),
  });
  expect((await f.receipt(incomplete)).valuation.customer.state).toBe("unconfirmed");
  await f.proof(retry, "completed");
  const receipts = f.events(retry),
    extra = f.events(retry).filter((e) => e.kind === "attempt" || e.kind === "usage");
  await f.service.ingest({ events: [...receipts, ...extra] });
  const page = parseModelBillingPage(await f.service.list(f.tenantId, query));
  const item = page.items.find((i) => i.context.invocationId === retry.invocationId)!;
  expect(item.usage).toHaveLength(2);
  expect(
    item.usage.every(
      (u) =>
        u.eligibility.reason === "ambiguous_provider_retry" &&
        u.valuation.customer.state === "waived",
    ),
  ).toBe(true);
});

it("rejects conflicting identity evidence and cannot charge settlement beyond the writer cutoff", async () => {
  const f = await fixture(),
    c = f.context();
  await expect(
    db
      .transaction()
      .execute((tx) =>
        f.accounting.settle(
          tx,
          { ...f.scope(c), fencingToken: 9 },
          [{ context: c, outcome: "completed" }],
          f.position(),
        ),
      ),
  ).rejects.toThrow("identity changed");
  await f.proof(c, "completed", 3n);
  await f.close(c, 2n);
  await f.service.ingest({ events: f.events(c) });
  expect((await f.receipt(c)).eligibility).toEqual({
    state: "platform",
    reason: "invalid_execution_boundary",
  });
  await expect(f.close(c, 4n)).rejects.toThrow("closure evidence changed");
});
