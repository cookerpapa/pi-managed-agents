/** Logged PG feature comparison. The serving database is never a benchmark target. */
import assert from "node:assert/strict";
import { randomUUID, createHash } from "node:crypto";
import { readFile, writeFile } from "node:fs/promises";
import { performance } from "node:perf_hooks";
import pg from "pg";
import { sql } from "kysely";
import { createDatabase, runMigrations } from "@pi-cloud/database";
import { Harness, MemoryStorage, createRegistry } from "@earendil-works/pi-durable";
import { BACKGROUND_CONTEXT as ctx } from "@earendil-works/chord/context";
import { createModels } from "@earendil-works/pi-ai/models";
import { fauxAssistantMessage, fauxProvider } from "@earendil-works/pi-ai/providers/faux";
import { DurablePostgresProjection } from "@pi-cloud/pi-session-postgres/durable";
import { readNativeUsageBatch } from "../../../packages/pi-session-postgres/src/durable/conversation-usage.ts";
import { ModelExecutionAccounting } from "@pi-cloud/accounting";
import { createPrivateTenant } from "../../../packages/control-plane/src/tenant-administration.ts";
import { ControlPlaneStore } from "../../../packages/control-plane/src/control-plane-store.ts";
import {
  captureNativePresentation,
  mayCaptureNativePresentation,
} from "../../../packages/runtime-core/src/frozen-history.ts";
import { ExecutionStreamProjector } from "../../../packages/runtime-core/src/execution-stream-projection.ts";
import { execIn, runtimeDirectory, serviceAddress } from "../../lib/production-kubernetes.mjs";
import { distribution } from "../../lib/runtime-load-scheduler.mjs";

assert.equal(process.env.PI_CLOUD_PROJECTOR_SQL_DIAGNOSTIC, "1");
const [output, mode] = process.argv.slice(2);
assert(
  output && (mode === undefined || mode === "smoke" || mode === "transport" || mode === "paging"),
);
const count = mode ? 4 : 96,
  lanes = Math.min(12, count);
const marker = "62c1b314-0e91-4142-a359-5aa676aa8659";
const stages = [
  "raw",
  "raw-key",
  "native-required",
  "native-history-indexes",
  "native",
  "chat",
  "accounting",
  "accepted",
  "finished",
];
const nativeTables = [
  "pi_durable_owned_records",
  "pi_durable_current",
  "pi_durable_write_refs",
  "pi_history_records",
  "pi_history_bodies",
  "pi_durable_document_revisions",
  "pi_durable_commits",
];
const historyTables = ["pi_history_records", "pi_history_bodies", "pi_durable_write_refs"];
const endpoint = new URL(
  (await readFile(runtimeDirectory + "/secrets/database-url", "utf8")).trim(),
);
endpoint.hostname = serviceAddress("postgres");
const admin = createDatabase({ connectionString: endpoint.toString(), maxConnections: 1 });
const prefix = "pma_cost_" + randomUUID().replaceAll("-", "").slice(0, 14);
const template = prefix + "_seed",
  allocated = new Set();
const open = (name) => {
  const u = new URL(endpoint);
  u.pathname = "/" + name;
  return createDatabase({ connectionString: u.toString(), maxConnections: lanes });
};
async function create(name, parent) {
  assert(name.startsWith(prefix + "_") && (!parent || allocated.has(parent)));
  await sql`create database ${sql.id(name)} template ${sql.id(parent ?? "template0")}`.execute(
    admin,
  );
  allocated.add(name);
}
async function drop(name) {
  assert(allocated.has(name));
  await sql`drop database ${sql.id(name)}`.execute(admin);
  allocated.delete(name);
}
async function parallel(items, operation, concurrency = lanes) {
  const results = await Promise.allSettled(
    Array.from({ length: concurrency }, async (_, lane) => {
      for (let i = lane; i < items.length; i += concurrency) await operation(items[i], i, lane);
    }),
  );
  const failed = results.find((x) => x.status === "rejected");
  if (failed) throw failed.reason;
}
async function nativeSample() {
  const batches = [];
  class FixtureStorage extends MemoryStorage {
    async commit(writes) {
      const p = this.prepareCommit(writes);
      const seq = p.apply();
      batches.push({ seq, nextId: this.nextId, writes: structuredClone(p.writes) });
      return seq;
    }
  }
  const models = createModels(),
    provider = fauxProvider({
      tokensPerSecond: 1_000_000,
      tokenSize: { min: 4096, max: 4096 },
      models: [{ id: "lab" }],
    });
  models.setProvider(provider.provider);
  const text = (
    marker +
    Array.from({ length: 64 }, (_, i) => createHash("sha256").update(String(i)).digest("hex")).join(
      "",
    )
  ).slice(0, 4096);
  const answer = fauxAssistantMessage(text);
  answer.usage = { ...answer.usage, input: 17, output: 8, totalTokens: 25 };
  provider.setResponses([answer]);
  const harness = await Harness.open(
    new FixtureStorage(),
    {
      models,
      registry: createRegistry(),
      settings: { retry: { maxRetries: 0 }, compaction: { enabled: false } },
    },
    ctx,
  );
  try {
    const model = provider.getModel();
    const root = await harness.root(ctx, {
      agent: { model: { provider: model.provider, modelId: model.id } },
    });
    const request = await root.submit(
      {
        type: "input",
        content: [{ type: "text", text: "fixture" }],
        requestId: "request-" + marker,
      },
      ctx,
    );
    assert.equal((await request.wait(ctx)).status, "done");
    await root.waitForIdle(ctx);
  } finally {
    await harness.close(ctx);
  }
  const end = batches.findIndex((b) =>
    b.writes.some((w) => w.type === "entry" && w.value.kind === "pi.assistant"),
  );
  assert(
    end > 0 &&
      batches[end].writes.some((w) => w.type === "submission") &&
      batches[end].writes.some((w) => w.type === "task"),
  );
  return batches.slice(0, end + 1);
}
function origins(scope, batch) {
  return batch.writes.flatMap((w) =>
    w.type === "entry" || w.type === "task" || w.type === "submission"
      ? [
          {
            recordId: w.value.id,
            sessionId: scope.sessionId,
            turnId: scope.turnId,
            runId: scope.runId,
          },
        ]
      : [],
  );
}
async function seed(db, batches) {
  await runMigrations(db, "up");
  await sql`create table assay_append(tenant_id uuid not null, session_id text not null, seq bigint not null, payload text not null)`.execute(
    db,
  );
  const tenant = await createPrivateTenant(db, {
    slug: "projection-cost",
    ownerDisplayName: "Synthetic fixture",
    initialModel: { provider: "openai-codex", modelId: "gpt-6-luna" },
  });
  const store = new ControlPlaneStore({ database: db, ...tenant });
  const project = await store.createProject({
    name: "Projection assay",
    source: { kind: "empty" },
  });
  const worker = randomUUID();
  await db
    .insertInto("workers")
    .values({
      id: worker,
      supervisor_id: worker,
      boot_id: randomUUID(),
      state: "ready",
      max_concurrent_sessions: count,
    })
    .execute();
  const cases = new Array(count);
  await parallel(
    cases,
    async (_, i) => {
      const { sessionId } = await store.createSession(
        project.projectId,
        project.workspaceId,
        "fixture " + i,
        "elastic",
      );
      const { runId, turnId } = await store.acceptTurn(sessionId, randomUUID(), {
        prompt: "fixture",
      });
      const scope = {
        tenantId: tenant.tenantId,
        sessionId,
        piSessionId: sessionId,
        runId,
        turnId,
        writerId: randomUUID(),
        fencingToken: 1,
      };
      await db.transaction().execute(async (tx) => {
        await tx
          .updateTable("runs")
          .set({
            state: "running",
            worker_id: worker,
            lease_id: scope.writerId,
            fencing_token: 1,
            started_at: new Date(),
            output_publication: {
              scope: { ...scope, leaseId: scope.writerId, piConversationId: 1 },
            },
          })
          .where("id", "=", runId)
          .execute();
        await tx
          .insertInto("session_leases")
          .values({
            tenant_id: scope.tenantId,
            pi_session_id: sessionId,
            lease_id: scope.writerId,
            worker_id: worker,
            fencing_token: 1,
            valid_until: new Date(Date.now() + 86400000),
          })
          .execute();
        await tx
          .updateTable("pi_sessions")
          .set({ lease_epoch: 1 })
          .where("tenant_id", "=", scope.tenantId)
          .where("id", "=", sessionId)
          .execute();
      });
      const stream = JSON.parse(JSON.stringify(batches).replaceAll(marker, sessionId));
      const projection = new DurablePostgresProjection(db, scope, undefined, "references");
      for (const batch of stream.slice(0, -1)) {
        const state = projection.liveState.fork();
        await db.transaction().execute(async (tx) => {
          await projection.applyIn(tx, batch, origins(scope, batch), {
            state,
            position: { topic: "projection-cost", partition: i % lanes, offset: BigInt(batch.seq) },
          });
          if (mayCaptureNativePresentation(batch.writes))
            await captureNativePresentation(tx, scope, batch.seq);
        });
        projection.liveState.adopt(state);
      }
      // Clone a supported materialized predecessor. Every variant receives the
      // same current live document and needs no external replay stream.
      await db
        .transaction()
        .execute((tx) => projection.checkpointLiveIn(tx, projection.liveState, stream.at(-2).seq));
      const commit = stream.at(-1),
        task = commit.writes.find((w) => w.type === "task").value.id;
      const context = {
        version: 1,
        invocationId: randomUUID(),
        requestedAt: new Date().toISOString(),
        tenantId: scope.tenantId,
        sessionId,
        runId,
        piSessionId: sessionId,
        conversationId: 1,
        provider: "openai-codex",
        model: "gpt-6-luna",
        purpose: "agent",
        execution: { writerId: scope.writerId, fencingToken: 1, taskId: task },
      };
      const fact = {
        kind: "durable_commit",
        factId: randomUUID(),
        occurredAt: context.requestedAt,
        scope,
        commit,
        origins: origins(scope, commit),
        tools: [],
        cancellations: [],
        display: [{ scope, events: [], coveredThrough: 2 }],
        modelEvidence: [{ context, outcome: "completed" }],
      };
      cases[i] = {
        fact,
        raw: JSON.stringify(fact),
        end: {
          kind: "work_finished",
          scope,
          agentId: "root",
          factId: randomUUID(),
          baseSequence: 2,
          nativeSequence: commit.seq,
          terminal: { type: "turn.completed", payload: { stopReason: "stop" } },
          occurredAt: context.requestedAt,
        },
      };
    },
    Math.min(lanes, 4),
  );
  await sql`analyze`.execute(db);
  return cases;
}
async function dropSecondary(db, keepHistory) {
  const rows = (
    await sql`select i.indexrelid::regclass::text as name,t.relname from pg_index i join pg_class t on t.oid=i.indrelid
    where t.relnamespace='public'::regnamespace and t.relname=any(${nativeTables}::text[]) and not i.indisunique and not i.indisexclusion
    and not exists(select 1 from pg_constraint c where c.conindid=i.indexrelid) order by 1`.execute(
      db,
    )
  ).rows;
  for (const r of rows)
    if (!keepHistory || !historyTables.includes(r.relname))
      await sql`drop index ${sql.id("public", r.name)}`.execute(db);
  return rows.filter((r) => !keepHistory || !historyTables.includes(r.relname)).map((r) => r.name);
}
async function tableStats(db) {
  const rows = (
    await sql`select relname,n_tup_ins::float8 ins,n_tup_upd::float8 upd,n_tup_del::float8 del,n_tup_hot_upd::float8 hot,
    pg_indexes_size(relid)::float8 indexes,pg_table_size(relid)::float8 heap from pg_stat_user_tables`.execute(
      db,
    )
  ).rows;
  return new Map(rows.map((r) => [r.relname, r]));
}
async function measureReads(db, cases) {
  const entry = [],
    tasks = [],
    usage = [];
  for (const { fact } of cases.slice(0, Math.min(count, 24))) {
    const native = new DurablePostgresProjection(db, fact.scope);
    const expected = fact.commit.writes.find((w) => w.type === "entry").value;
    let start = performance.now();
    const read = await native.get(expected.id);
    entry.push(performance.now() - start);
    assert.deepEqual(read.value, expected);
    start = performance.now();
    const terminal = await native.scan({ kind: "task", fields: { status: "terminal" } }, 8);
    tasks.push(performance.now() - start);
    assert(terminal.some((r) => r.value.kind === "pi.generation"));
    start = performance.now();
    const values = await readNativeUsageBatch(db, fact.scope, [1]);
    usage.push(performance.now() - start);
    assert(values.has(1));
  }
  return {
    entry: distribution(entry),
    terminalTasks: distribution(tasks),
    usage: distribution(usage),
  };
}
async function compare(name, stage, round, cases) {
  await create(name, template);
  const db = open(name),
    observer = open(name);
  try {
    assert.equal((await sql`select current_database() as n`.execute(db)).rows[0].n, name);
    let excluded = [];
    if (stage === "raw-key")
      await sql`create unique index assay_key on assay_append(tenant_id,session_id,seq)`.execute(
        db,
      );
    if (stage === "native-required" || stage === "native-history-indexes")
      excluded = await dropSecondary(db, stage === "native-history-indexes");
    await Promise.all(Array.from({ length: lanes }, () => sql`select 1`.execute(db)));
    const before = await tableStats(observer);
    const {
      rows: [wal],
    } = await sql`select pg_current_wal_insert_lsn()::text as start`.execute(observer);
    const raw = stage.startsWith("raw"),
      accepted = stage === "accepted" || stage === "finished";
    const chat = ["chat", "accounting"].includes(stage);
    const accounting = new ModelExecutionAccounting(),
      projector = new ExecutionStreamProjector(db, accounting);
    const oldQuery = pg.Client.prototype.query;
    let calls = 0;
    const latency = [];
    pg.Client.prototype.query = function (...args) {
      calls++;
      return Reflect.apply(oldQuery, this, args);
    };
    let elapsed;
    try {
      const start = performance.now();
      await parallel(cases, async ({ fact, raw: payload, end }, i, lane) => {
        const begin = performance.now(),
          scope = fact.scope;
        const position = {
          topic: "projection-cost",
          partition: lane,
          offset: BigInt(100 + Math.floor(i / lanes) * 2),
        };
        if (raw)
          await db
            .transaction()
            .execute((tx) =>
              sql`insert into assay_append values(${scope.tenantId}::uuid,${scope.sessionId},${fact.commit.seq},${payload})`.execute(
                tx,
              ),
            );
        else if (accepted) {
          const applied = await projector.project({ ...position, fact });
          assert.equal(applied.admitted, true);
          if (stage === "finished") {
            const result = await projector.project({
              ...position,
              offset: position.offset + 1n,
              fact: end,
            });
            assert.equal(result.terminal.type, "turn.completed");
          }
        } else {
          const native = new DurablePostgresProjection(db, scope, undefined, "references");
          await db.transaction().execute(async (tx) => {
            assert(await native.applyIn(tx, fact.commit, fact.origins));
            if (chat)
              await captureNativePresentation(tx, scope, fact.commit.seq, undefined, {
                sessionId: scope.sessionId,
                turnId: scope.turnId,
                mailboxPosition: "1",
                pruned: false,
                sharedEntries: false,
              });
            if (stage === "accounting")
              await accounting.settle(tx, scope, fact.modelEvidence, position);
          });
        }
        latency.push(performance.now() - begin);
      });
      elapsed = performance.now() - start;
    } finally {
      pg.Client.prototype.query = oldQuery;
    }
    const {
      rows: [walEnd],
    } =
      await sql`select pg_wal_lsn_diff(pg_current_wal_insert_lsn(),${wal.start}::pg_lsn)::float8 as bytes`.execute(
        observer,
      );
    await db.destroy(); // Pool shutdown flushes backend-local table statistics.
    const after = await tableStats(observer),
      changes = {};
    for (const [table, r] of after) {
      const b = before.get(table);
      if (!b) continue;
      if (r.ins !== b.ins || r.upd !== b.upd || r.del !== b.del)
        changes[table] = {
          inserts: r.ins - b.ins,
          updates: r.upd - b.upd,
          hotUpdates: r.hot - b.hot,
          deletes: r.del - b.del,
          indexGrowthBytes: r.indexes - b.indexes,
          tableGrowthBytes: r.heap - b.heap,
        };
    }
    const seq = cases[0].fact.commit.seq;
    const {
      rows: [check],
    } = await sql`select
      (select count(*)::int from assay_append) as raw,
      (select count(*)::int from pi_durable_commits where seq=${seq}) as commits,
      (select count(*)::int from pi_history_frames where seq=${seq}) as frames,
      (select count(*)::int from model_metering_events where kind='execution') as accounting,
      (select count(*)::int from runs where state='completed') as finished`.execute(observer);
    assert.deepEqual(check, {
      raw: raw ? count : 0,
      commits: raw ? 0 : count,
      frames: chat || accepted ? count : 0,
      accounting: stage === "accounting" || accepted ? count : 0,
      finished: stage === "finished" ? count : 0,
    });
    if (raw) {
      const {
        rows: [saved],
      } =
        await sql`select payload from assay_append where tenant_id=${cases[0].fact.scope.tenantId}::uuid and session_id=${cases[0].fact.scope.sessionId} and seq=${seq}`.execute(
          observer,
        );
      assert.equal(saved.payload, cases[0].raw);
    }
    const reads = raw ? undefined : await measureReads(observer, cases);
    const timing = distribution(latency);
    return {
      stage,
      round,
      count,
      lanes,
      elapsedMs: Math.round(elapsed),
      answersPerSecond: Math.round((count / elapsed) * 100000) / 100,
      p50Ms: Math.round(timing.p50 * 100) / 100,
      p95Ms: Math.round(timing.p95 * 100) / 100,
      callsPerAnswer: calls / count,
      clusterWalIntervalBytes: walEnd.bytes,
      reads,
      excluded,
      changes,
      verified: check,
    };
  } finally {
    try {
      await db.destroy();
      await observer.destroy();
    } finally {
      await drop(name);
    }
  }
}
const results = [];
try {
  const batches = await nativeSample();
  await create(template);
  const db = open(template);
  let cases;
  try {
    cases = await seed(db, batches);
  } finally {
    await db.destroy();
  }
  let transport, paging;
  if (mode === "paging") {
    const { comparePages } = await import("./projection-page-cost.mjs");
    const pages = open(template);
    try {
      paging = await comparePages(pages, cases);
    } finally {
      await pages.destroy();
    }
  } else if (mode === "transport") {
    const code = await readFile(new URL("./kafka-pg-append.mjs", import.meta.url), "utf8");
    const log = execIn(
      "session-runtime",
      ["node", "--import", "tsx", "--input-type=module", "-e", code, template],
      { input: cases[0].raw },
    );
    const line = log.split("\n").find((s) => s.startsWith('{"rawKafkaPg":true,'));
    assert(line, "Missing raw Kafka sink result");
    transport = JSON.parse(line);
  } else {
    const orders =
      mode === "smoke"
        ? [stages]
        : [stages, stages.slice().reverse(), [...stages.slice(4), ...stages.slice(0, 4)]];
    for (const [round, order] of orders.entries())
      for (const s of order) {
        const value = await compare(prefix + "_case", s, round + 1, cases);
        results.push(value);
        console.log(s, round + 1, value.answersPerSecond, value.callsPerAnswer);
      }
  }
  await drop(template);
  assert.equal(allocated.size, 0);
  await writeFile(
    output,
    JSON.stringify(
      {
        cleaned: true,
        provider: "local Pi faux",
        transport,
        paging,
        inputStages: batches.map((b) => b.writes.map((w) => w.value?.kind ?? w.type)),
        mixedWrites: batches
          .at(-1)
          .writes.map((w) => [w.type, w.value?.kind].filter(Boolean).join(":")),
        finalPayloadBytes: Buffer.byteLength(cases[0].raw),
        results,
      },
      null,
      2,
    ) + "\n",
  );
} finally {
  try {
    for (const name of [...allocated].reverse()) await drop(name);
  } finally {
    await admin.destroy();
  }
}
