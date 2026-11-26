// Replay fixed Pi commits directly into the serving candidate. Only the PG projection leg is timed.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import pg from "pg";
import { Kysely, PostgresDialect } from "kysely";
import { kafkaProducerLane } from "@pi-cloud/runtime-core/kafka-accepted-fact";
import { testPostgres } from "../../lib/test-postgres.mjs";
import { receiptHash } from "../../../packages/accounting/src/receipt-identity.ts";
import { up as receipts } from "../../../packages/database/src/migrations/210_model_metering.ts";
import { up as eligibility } from "../../../packages/database/src/migrations/226_model_charge_evidence.ts";
import { visibleOutput } from "./visible-output.mjs";
import { installFlowHistory, seedFlowHeads } from "./flow-history.mjs";
import { SteerHint } from "./steer-hint.mjs";
import { connectSessionInputs } from "./segment-input-worker.mjs";
import { SegmentInputs } from "./segment-inputs.mjs";
import { ForkStorage, installForkTables } from "./fork-storage.mjs";
import { captureNativeUsage } from "./native-usage-fixture.mjs";
import { normalRootProjection } from "./normal-root-projection.mjs";

const partitions = 32,
  runsPerPartition = Number(process.argv[2] ?? 8);
assert(Number.isSafeInteger(runsPerPartition) && runsPerPartition >= 1 && runsPerPartition <= 64);
const seed = randomUUID(),
  tenantId = randomUUID(),
  when = new Date().toISOString();
const { commits } = await captureNativeUsage(seed, 20);
const visiblePerRun = commits.filter((c) =>
  Object.values(visibleOutput(c.writes)).some(Boolean),
).length;
assert.equal(visiblePerRun, 41);
const shape = commits.map((c) => {
  const task =
    c.writes.some((w) => w.type === "entry" && w.value.kind === "pi.assistant") &&
    c.writes.find(
      (w) =>
        w.type === "task" &&
        w.value.kind === "pi.generation" &&
        ["waiting", "terminal"].includes(w.value.state.status),
    );
  return { json: JSON.stringify(c), task: task?.value?.id };
});
assert.equal(shape.filter((c) => c.task).length, 21);
const lanes = Array.from({ length: partitions }, () => []);
let remaining = partitions * runsPerPartition;
while (remaining) {
  const sessionId = randomUUID(),
    p = kafkaProducerLane(sessionId, partitions);
  if (lanes[p].length === runsPerPartition) continue;
  remaining--;
  const runId = randomUUID(),
    writerId = randomUUID();
  const scope = {
    tenantId,
    piSessionId: sessionId,
    sessionId,
    runId,
    turnId: runId,
    writerId,
    fencingToken: 1,
  };
  lanes[p].push({
    scope,
    calls: shape.map(
      ({ task }) =>
        task && {
          version: 1,
          invocationId: randomUUID(),
          tenantId,
          sessionId,
          piSessionId: sessionId,
          conversationId: 1,
          runId,
          execution: { writerId, fencingToken: 1, taskId: task },
          provider: "fixture",
          model: "fixture",
          purpose: "agent",
          requestedAt: when,
        },
    ),
  });
}
const groups = lanes.map((runs, partition) => {
  const records = [];
  for (const [i, c] of shape.entries())
    for (const { scope, calls } of runs) {
      const context = calls[i],
        fact = {
          kind: "durable_commit",
          factId: randomUUID(),
          occurredAt: when,
          scope,
          commit: JSON.parse(c.json.replaceAll(seed, scope.runId)),
          tools: [],
          cancellations: [],
          display: [],
          origins: context
            ? [
                {
                  recordId: c.task,
                  sessionId: scope.sessionId,
                  turnId: scope.turnId,
                  runId: scope.runId,
                },
              ]
            : [],
        };
      records.push({
        position: { partition, topic: "comparison", offset: BigInt(records.length) },
        fact,
      });
    }
  return records;
});
const readSplit =
  process.argv[3] === "flow-page-cost" && ["standby", "standby-control"].includes(process.argv[4]);
const own = await testPostgres({}, { standby: readSplit });
try {
  const waitStudy = process.argv[3] === "flow-page-locks";
  const pageCost = process.argv[3] === "flow-page-cost" && (process.argv[4] ?? "main");
  if (pageCost)
    assert(
      [
        "main",
        "fixed32",
        "selector",
        "standby",
        "standby-control",
        "scroll",
        "scroll-tools",
      ].includes(pageCost),
    );
  if (pageCost === "selector") assert(process.argv[5], "Pass the reference checkout");
  const comparePages = pageCost || waitStudy || process.argv[3] === "flow-pages";
  if (comparePages) {
    const { studyFlowPaging } = await import("./flow-paging-load.mjs");
    await studyFlowPaging(
      own.url,
      tenantId,
      groups,
      lanes.flat().map((r) => r.scope),
      visiblePerRun,
      waitStudy ? own.setCpuLimit : undefined,
      pageCost,
      pageCost === "selector" ? process.argv[5] : undefined,
      own.standbyUrl,
    );
  }
  const compareFlow = process.argv[3] === "flow",
    compareSql = process.argv[3] === "flow-sql";
  const tape = Array.from({ length: partitions }, () => []);
  const compareMail = process.argv[3] === "session-inputs";
  const pairs = compareMail && process.argv[4] === "pairs";
  const warmMail = compareMail && (pairs || process.argv[4] === "warm");
  const checkBoundaries = compareMail && process.argv[4] === "boundaries";
  const layouts = checkBoundaries
    ? [true]
    : pairs
      ? [false, true, ...Array.from({ length: 6 }, () => [true, false, false, true]).flat()]
      : compareMail
        ? warmMail
          ? [false, true, true, false, false, true, false, true, true, false]
          : [false, true, true, false, true, false, false, true]
        : comparePages
          ? []
          : compareFlow
            ? [false, true, true, false, true, false, false, true]
            : compareSql
              ? [false]
              : [false, false];
  for (const [index, layout] of layouts.entries()) {
    const flowHistory = !compareMail && layout,
      segmentInputs = compareMail && layout;
    const sample = index + 1;
    const schema = "rate_" + randomUUID().replaceAll("-", "");
    const pool = new pg.Pool({
      connectionString: own.url,
      options: "-c search_path=" + schema,
      max: 12,
    });
    let queries,
      timed = false;
    pool.on("connect", (client) => {
      const query = client.query.bind(client);
      client.query = (...args) => {
        if (timed) {
          const text = typeof args[0] === "string" ? args[0] : (args[0].text ?? "");
          const kind = text.startsWith("with result as")
            ? "prefix"
            : text.includes('"model_invocations"')
              ? "invocation"
              : text.includes('"model_metering_events"')
                ? "evidence"
                : text.split(/\s+/, 1)[0].toLowerCase();
          if (compareSql && kind === "prefix") tape[args[1][1]].push({ text, params: args[1] });
          queries[kind] = (queries[kind] ?? 0) + 1;
        }
        return query(...args);
      };
    });
    const db = new Kysely({ dialect: new PostgresDialect({ pool }) });
    try {
      await pool.query("create schema " + schema);
      await pool.query(await readFile(new URL("./schema.sql", import.meta.url), "utf8"));
      await pool.query(
        "alter table runs add output_first_topic text,add output_first_partition int,add output_first_offset bigint,add output_display_seq bigint default 0,add output_display_native_seq bigint default 0",
      );
      await receipts(db);
      await eligibility(db);
      const p = normalRootProjection(
        pool,
        "comparison",
        () => assert.fail(),
        () => assert.fail(),
        undefined,
        { partitions, independentSessions: compareMail },
      );
      if (compareMail) p.view.sessionInputs = segmentInputs;
      await p.view.install();
      if (segmentInputs) {
        await pool.query(
          "alter table runs add input_uploads jsonb default '[]',add workspace_grant jsonb",
        );
        await installForkTables(pool);
        // This tape includes data-plane commits only. The managed scheduler
        // and API are exercised separately with their real Worker registry.
        const ddl = await readFile(new URL("./session-inputs.sql", import.meta.url), "utf8");
        await pool.query(ddl);
      }
      if (flowHistory) await installFlowHistory(pool, true);
      await p.view.reset(new Map(lanes.map((_, i) => [i, 0n])), 16, "recorded-history");
      const runs = lanes.flat(),
        calls = runs.flatMap((r) => r.calls.filter(Boolean));
      await pool.query(
        `insert into runs(id,tenant_id,session_id,lease_id,initial_input,idempotency_key,model_ref,state,mailbox_position)
      select "runId","tenantId","sessionId","writerId",'Inspect',"runId",'fixture','running',1 from jsonb_to_recordset($1::jsonb) t("runId" uuid,"tenantId" uuid,"sessionId" uuid,"writerId" uuid)`,
        [JSON.stringify(runs.map((r) => r.scope))],
      );
      if (segmentInputs) {
        await new ForkStorage(pool).register(
          tenantId,
          runs.map((r) => r.scope.sessionId),
        );
        await pool.query(
          "insert into user_or_agent_messages(tenant_id,session_id,id,sender_id,source,mode,body,fingerprint,turn_id,state,request_key) select tenant_id,session_id,id,tenant_id,'user','input',initial_input,'fixture',id,'sending',id::text from runs",
        );
      }
      if (checkBoundaries) {
        // Exercise the normal indexed Worker check with many pending follow-ups
        // and no eligible steer. Each lane models a different Session.
        await pool.query(`insert into user_or_agent_messages(tenant_id,session_id,id,sender_id,source,mode,body,fingerprint,turn_id,request_key)
          select r.tenant_id,r.session_id,gen_random_uuid(),r.session_id,'agent','follow_up','later','test',r.id,n::text
          from runs r cross join generate_series(1,256) n`);
        await pool.query("analyze user_or_agent_messages");
        const mailbox = new SegmentInputs(db),
          scope = runs.slice(0, 32).map((r) => r.scope),
          samples = [];
        const probe = async (s, n) => {
          const at = performance.now();
          assert.equal(await mailbox.selectAt(s, "postTools", n), undefined);
          return performance.now() - at;
        };
        await Promise.all(scope.map((s) => probe(s, 1)));
        const start = performance.now();
        await Promise.all(
          scope.map(async (s) => {
            for (let n = 0; n < 100; n++) samples.push(await probe(s, n + 2));
          }),
        );
        const ms = performance.now() - start;
        samples.sort((a, b) => a - b);
        console.log(
          JSON.stringify({
            workload: "empty postTools lookups",
            sessions: scope.length,
            pendingFollowUpsPerSession: 256,
            checks: samples.length,
            ms,
            checksPerSecond: Math.round((samples.length * 1000) / ms),
            p50: samples[Math.ceil(samples.length * 0.5) - 1],
            p95: samples[Math.ceil(samples.length * 0.95) - 1],
            p99: samples[Math.ceil(samples.length * 0.99) - 1],
          }),
        );
        let queries = 0;
        const workers = scope.map((s) => {
          const hint = new SteerHint();
          hint.clear(hint.ticket); // bootstrap found only follow-ups above
          return connectSessionInputs({
            job: { scope: s, steerHint: hint, authority: { assertActive() {} } },
            inputs: {
              selectAt(...args) {
                queries++;
                return mailbox.selectAt(...args);
              },
            },
          });
        });
        const localStart = performance.now();
        await Promise.all(
          workers.map(async (worker) => {
            for (let i = 0; i < 100; i++) await worker.postTools(i + 2);
          }),
        );
        assert.equal(queries, 0);
        console.log(
          JSON.stringify({
            workload: "clean Worker tool boundary",
            sessions: workers.length,
            checks: workers.length * 100,
            postToolsQueries: queries,
            ms: performance.now() - localStart,
          }),
        );
        continue;
      }
      if (flowHistory) await seedFlowHeads(pool, tenantId);
      // A model request normally reaches the gateway before the Worker publishes its settlement.
      await pool.query(
        `insert into model_invocations(id,tenant_id,session_id,run_id,requested_at,provider,model,purpose,context,context_hash)
        select (c->>'invocationId')::uuid,(c->>'tenantId')::uuid,(c->>'sessionId')::uuid,(c->>'runId')::uuid,(c->>'requestedAt')::timestamptz,'fixture','fixture','agent',c,h from jsonb_to_recordset($1::jsonb) t(c jsonb,h text)`,
        [JSON.stringify(calls.map((c) => ({ c, h: receiptHash(c) })))],
      );
      queries = {};
      timed = true;
      const begin = performance.now();
      await Promise.all(
        groups.map(async (group) => {
          for (const r of group)
            assert(
              (await p.nativeStore.projection.project(r.fact, r.position, () => true)).admitted,
            );
        }),
      );
      const ms = performance.now() - begin;
      timed = false;
      if (segmentInputs)
        assert.equal(
          (
            await pool.query(
              "select count(*)::int n from user_or_agent_messages where state='placed'",
            )
          ).rows[0].n,
          runs.length,
        );
      const {
        rows: [totals],
      } = await pool.query(
        "select (select count(*)::int from model_metering_events where kind='execution') receipts,(select count(*)::int from messages) messages,(select count(*)::int from message_entries where role='toolResult') tools",
      );
      assert.equal(totals.receipts, 0);
      assert.deepEqual(queries, { prefix: p.view.stats.commits });
      assert.equal(totals.tools, 20 * runs.length);
      console.log(
        JSON.stringify({
          sample,
          ...(compareMail ? { segmentInputs, warmup: warmMail && index < 2 } : {}),
          ...(compareFlow ? { flowHistory } : {}),
          runs: runs.length,
          commits: groups.flat().length,
          ...totals,
          ms: Math.round(ms),
          visibleRecordsPerSecond: Math.round((visiblePerRun * runs.length * 1000) / ms),
          recordsPerSecond: Math.round((groups.flat().length * 1000) / ms),
          pgTransactions: p.view.stats.commits,
          sync: p.view.stats.syncTransactions,
          poolWaitMsPerTransaction: p.view.stats.poolMs / p.view.stats.commits,
          heldMsPerTransaction: p.view.stats.holdMs / p.view.stats.commits,
          queries,
        }),
      );
    } finally {
      try {
        await pool.query("drop schema if exists " + schema + " cascade");
      } finally {
        await db.destroy();
      }
    }
  }
  if (compareSql)
    await (
      await import("./flow-sql-replay.mjs")
    ).compareFlowSql(
      own.url,
      tape,
      lanes.flat().map((r) => r.scope),
      visiblePerRun * lanes.flat().length,
    );
} finally {
  await own.close();
}
