// Real Worker receipt preparation and accounting tables with a deterministic provider receipt.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { createDatabase } from "@pi-cloud/database";
import { ModelMeteringService } from "@pi-cloud/accounting";
import { createExecutionReference, MODEL_METERING_HEADER } from "@pi-cloud/protocol";
import { DurableModelAccounting } from "../../../packages/sandbox-supervisor/src/durable/model-accounting.ts";
import { up as installReceipts } from "../../../packages/database/src/migrations/210_model_metering.ts";
import { up as installEligibility } from "../../../packages/database/src/migrations/226_model_charge_evidence.ts";
import { readConversationUsage } from "../../../packages/control-plane/src/conversation-usage.ts";
import { createControlPlaneApplication } from "../../../packages/control-plane/src/application.ts";
import { emptyEventRuntime } from "../../../packages/control-plane/test/fixtures/event-runtime.ts";

export function namedModelFixture(url, scope) {
  const database = createDatabase({ connectionString: url, maxConnections: 2 });
  const metering = new ModelMeteringService(database);
  const lanes = new Map(),
    measured = new Map();
  async function provider(_url, init) {
    const c = JSON.parse(
      Buffer.from(init.headers.get(MODEL_METERING_HEADER), "base64url").toString(),
    );
    const base = { context: c, occurredAt: c.requestedAt },
      executionId = randomUUID();
    const usage = {
      provider: c.provider,
      model: c.model,
      serviceTier: "default",
      failed: false,
      unsupportedDimensions: false,
      quality: "complete",
      tokens: {
        input: 7,
        output: 3,
        cacheRead: 0,
        cacheWrite: 0,
        reasoning: 0,
        unclassified: 0,
        total: 10,
      },
    };
    await metering.ingest({
      events: [
        { ...base, kind: "started" },
        { ...base, kind: "attempt", executionId },
        { ...base, kind: "usage", executionId, usage },
        { ...base, kind: "finished", cancelled: false, statusCode: 200 },
      ],
    });
    measured.set(c.sessionId, (measured.get(c.sessionId) ?? 0) + 10);
    return new Response(null, { status: 204 });
  }
  return {
    async install() {
      await installReceipts(database);
      await installEligibility(database);
    },
    bind(conversationId, sessionId) {
      const accounting = new DurableModelAccounting({
        payload: {
          ...scope,
          sessionId,
          executionReference: createExecutionReference(
            scope.writerId,
            scope.runId,
            scope.fencingToken,
          ),
          piSession: { id: scope.piSessionId, conversationId },
        },
      });
      lanes.set(conversationId, accounting);
      return async (request, native, task, model) => {
        const meter = accounting.request({
          taskId: native.taskId,
          phase: task.state.checkpoint.phase,
          model: model.id,
          provider: model.provider,
          purpose: native.purpose,
          fetch: provider,
        });
        // The faux model has no HTTP transport. Exercise the Worker fetch wrapper once per model invocation.
        await meter.fetch("https://provider.invalid/test");
        return {
          model,
          options: request.options,
          release() {},
          onOutcome: (sample) => meter.finish(sample.outcome),
        };
      };
    },
    prepare(commit) {
      const prepared = Array.from(lanes.values(), (a) => a.prepare(commit));
      const evidence = prepared.flatMap((p) => p.evidence);
      const origins = Array.from(
        new Map(
          evidence.map(({ context: c }) => [
            c.execution.taskId,
            {
              recordId: c.execution.taskId,
              sessionId: c.sessionId,
              turnId: c.conversationId === 1 ? scope.turnId : c.sessionId,
              runId: scope.runId,
            },
          ]),
        ).values(),
      );
      return {
        evidence,
        origins,
        confirmed() {
          for (const p of prepared) p.confirmed();
        },
      };
    },
    async verifyUsage(childId) {
      const app = await createControlPlaneApplication({
        database,
        tenantId: scope.tenantId,
        defaultModelProfileId: randomUUID(),
        delegatedRunHistory: true,
        eventRuntime: emptyEventRuntime(),
      });
      try {
        await app.listen(0, "127.0.0.1");
        const origin = await app.getUrl();
        async function usage(id) {
          const response = await fetch(origin + "/v1/conversations/" + id + "/usage");
          assert.equal(response.status, 200);
          return response.json();
        }
        for (const id of [scope.sessionId, childId]) {
          const u = await usage(id);
          assert(measured.get(id) > 0);
          assert.equal(u.tokens.total, measured.get(id));
          assert.equal(u.unconfirmedRequests, 0);
          assert.equal(
            u.includingChildren.total,
            id === childId ? measured.get(id) : [...measured.values()].reduce((a, b) => a + b, 0),
          );
        }
        await assert.rejects(
          readConversationUsage(database, randomUUID(), childId, true),
          /Conversation was not found/,
        );
        await assert.rejects(
          readConversationUsage(database, scope.tenantId, randomUUID(), true),
          /Conversation was not found/,
        );
        const result = await database
          .selectFrom("model_billing_events")
          .select(["customer_eligibility"])
          .where("kind", "=", "usage")
          .execute();
        assert.equal(
          result.length * 10,
          [...measured.values()].reduce((a, b) => a + b, 0),
        );
        assert(result.every((r) => r.customer_eligibility === "billable"));
      } finally {
        await app.close();
      }
    },
    close: () => database.destroy(),
  };
}
