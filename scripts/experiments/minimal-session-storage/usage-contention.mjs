// Compare the genuine receipt DML and projection on one isolated PG instance.
// --direct uses the archived model DDL; a migration path uses its source.
import assert from "node:assert/strict";
import { createHash, randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import pg from "pg";
import { Kysely, PostgresDialect, CompiledQuery } from "kysely";
import { testPostgres } from "../../lib/test-postgres.mjs";
import { applyMovingSql } from "./ready-projection-sql.mjs";
import { ModelExecutionAccounting } from "../../../packages/accounting/test/fixtures/historical-model-execution.ts";
import { readSessionModelUsage } from "../../../packages/accounting/src/session-model-usage.ts";
import { ModelMeteringService } from "../../../packages/accounting/src/model-metering-service.ts";
import * as model from "../../../packages/database/src/migrations/210_model_metering.ts";
import * as resource from "../../../packages/database/src/migrations/211_resource_metering.ts";
import * as evidence from "../../../packages/database/src/migrations/226_model_charge_evidence.ts";
import {
  sessions,
  tenant,
  installProjection,
  prefixes,
  resetProjection,
  project,
  verifyProjection,
} from "./fixed-usage-projection.mjs";

const measuredSql = process.argv[2] === "--direct";
const migration = process.argv[2] && !measuredSql ? resolve(process.argv[2]) : undefined;
const direct = migration ? await import(pathToFileURL(migration).href) : null;
const fixture = await testPostgres({}),
  projection = new pg.Client(fixture.url);
const meters = [];
const withSettlement = process.argv.includes("--settle");
const accountingDb = new Kysely({
  dialect: new PostgresDialect({ pool: new pg.Pool({ connectionString: fixture.url, max: 1 }) }),
});
const settlement = new ModelExecutionAccounting();
try {
  await projection.connect();
  await installProjection(projection);
  await projection.query("create database receipt_study");
  for (const name of ["postgres", "receipt_study"]) {
    const u = new URL(fixture.url);
    u.pathname = "/" + name;
    const pool = new pg.Pool({ connectionString: u.href, max: 1 });
    const db = new Kysely({ dialect: new PostgresDialect({ pool }) });
    const receiver = new ModelMeteringService(db);
    meters.push({ db, pool, receiver });
    await model.up(db);
    await resource.up(db);
    await evidence.up(db);
    if (direct) await direct.up(db);
    else if (measuredSql)
      await pool.query(
        await readFile(new URL("./source-usage-study.sql", import.meta.url), "utf8"),
      );
  }
  const batches = [],
    stamp = new Date().toISOString();
  for (let n = 0; n < 64; n++) {
    const events = [];
    for (let i = 0; i < 8; i++) {
      const { s, r, w } = sessions[i * 2 + ((n + 1) % 2)],
        executionId = randomUUID();
      const context = {
        version: 1,
        invocationId: randomUUID(),
        tenantId: tenant,
        sessionId: s,
        runId: r,
        piSessionId: s,
        conversationId: 1,
        requestedAt: stamp,
        provider: "mock",
        model: "model",
        purpose: "agent",
        execution: { writerId: w, fencingToken: 1, taskId: n + 1 },
      };
      const usage = {
        provider: "mock",
        model: "model",
        serviceTier: "default",
        quality: "complete",
        failed: false,
        unsupportedDimensions: false,
        tokens: {
          input: 80,
          cacheRead: 40,
          cacheWrite: 0,
          output: 6,
          reasoning: 2,
          total: 128,
          unclassified: 0,
        },
      };
      events.push(
        ...[
          { kind: "started" },
          { kind: "attempt", executionId },
          { kind: "usage", executionId, usage },
          { kind: "finished", statusCode: 200, cancelled: false },
        ].map((e) => ({ ...e, context, occurredAt: stamp })),
      );
    }
    batches.push(events);
  }
  async function projectWithSettlement() {
    const start = performance.now();
    for (const [n, args] of prefixes.entries()) {
      const contexts = batches[n].filter((e) => e.kind === "started").map((e) => e.context);
      const offsets = new Map(
        contexts.map((c, i) => [c.invocationId, BigInt(n * 16 + i * 2 + ((n + 1) % 2))]),
      );
      await accountingDb.transaction().execute(async (tx) => {
        const result = await tx.executeQuery(
          CompiledQuery.raw(applyMovingSql, [...args.slice(0, 5), false, true]),
        );
        assert.deepEqual(result.rows, [{ outcome: "applied", mode: "on" }]);
        for (const c of contexts.sort((a, b) => a.invocationId.localeCompare(b.invocationId)))
          await settlement.settle(
            tx,
            {
              tenantId: tenant,
              piSessionId: c.piSessionId,
              runId: c.runId,
              writerId: c.execution.writerId,
              fencingToken: 1,
            },
            [{ context: c, outcome: "completed" }],
            { topic: "bench", partition: 0, offset: offsets.get(c.invocationId) },
          );
      });
    }
    return performance.now() - start;
  }
  async function trial(layout) {
    await resetProjection(projection);
    const meter = layout === "alone" ? null : meters[layout === "same" ? 0 : 1];
    if (withSettlement) await projection.query("truncate model_metering_events,model_invocations");
    if (meter && !(withSettlement && layout === "same"))
      await meter.pool.query("truncate model_metering_events,model_invocations");
    async function collect() {
      if (!meter) return null;
      const at = performance.now();
      for (const events of batches) {
        if (direct || measuredSql) {
          const {
            rows: [row],
          } = await meter.pool.query("select pi_record_model_usage($1::jsonb) as n", [
            JSON.stringify(events),
          ]);
          assert.equal(row.n, events.length);
        } else assert.equal((await meter.receiver.ingest({ events })).accepted, events.length);
      }
      return performance.now() - at;
    }
    const [projectionMs, receiptMs] = await Promise.all([
      withSettlement ? projectWithSettlement() : project(projection),
      collect(),
    ]);
    await verifyProjection(projection);
    if (withSettlement)
      assert.equal(
        (
          await projection.query(
            "select count(*)::int as n from model_metering_events where kind='execution'",
          )
        ).rows[0].n,
        512,
      );
    if (meter) {
      const reported = await readSessionModelUsage(
        meter.db,
        tenant,
        sessions.map((owner) => owner.s),
      );
      for (const item of reported.values()) {
        assert.equal(item.unconfirmedRequests, 0);
        assert.equal(item.tokens.total, 32 * 128);
      }
      assert.equal(
        (
          await meter.pool.query(
            "select count(*)::int as n from model_metering_events where kind<>'execution'",
          )
        ).rows[0].n,
        2048,
      );
    }
    return { layout, projectionMs, receiptMs };
  }
  for (const layout of ["alone", "same", "split"]) await trial(layout);
  const trials = [];
  for (const order of [
    ["alone", "same", "split"],
    ["same", "split", "alone"],
    ["split", "alone", "same"],
    ["split", "same", "alone"],
    ["alone", "split", "same"],
    ["same", "alone", "split"],
  ])
    for (const layout of order) trials.push(await trial(layout));
  console.log(
    JSON.stringify(
      {
        source: direct || measuredSql ? "direct-sql" : "metering-receiver",
        ...(migration
          ? {
              migrationSha256: createHash("sha256")
                .update(await readFile(migration))
                .digest("hex"),
            }
          : {}),
        postgres: (await projection.query("show server_version")).rows[0].server_version,
        withSettlement,
        cpus: 2,
        memoryMiB: 768,
        projectionConnections: 1,
        receiptConnections: 1,
        messages: 1024,
        requests: 512,
        receipts: 2048,
        trials,
      },
      null,
      2,
    ),
  );
} finally {
  try {
    await accountingDb.destroy();
    for (const m of meters) await m.db.destroy();
    await projection.end();
  } finally {
    await fixture.close();
  }
}
