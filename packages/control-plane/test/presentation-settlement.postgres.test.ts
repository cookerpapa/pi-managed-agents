import { afterEach, expect, it } from "vitest";
import { sql, type CompiledQuery } from "kysely";
import { PostgresSubagentConversations } from "@pi-cloud/subagent-control/subagent-conversations";
import {
  settleChildHistory,
  settleHistoryTurn,
} from "../../runtime-core/src/presentation-settlement.ts";
import { nativeSubagentFixture } from "./native-subagent-fixture.ts";

const fixtures: Array<Awaited<ReturnType<typeof nativeSubagentFixture>>> = [];
afterEach(async () => {
  for (const f of fixtures.splice(0)) await f.close();
});
async function fixture() {
  const f = await nativeSubagentFixture();
  fixtures.push(f);
  return f;
}
async function finishRoot(f: Awaited<ReturnType<typeof fixture>>) {
  await f.database
    .updateTable("runs")
    .set({ state: "completed", settled_at: new Date() })
    .where("id", "=", f.scope.runId)
    .execute();
}
async function headers(f: Awaited<ReturnType<typeof fixture>>) {
  return (
    await sql<{
      session_id: string;
      fields: Record<string, unknown>;
      payload: Record<string, unknown>;
    }>`
    select v.session_id,b.fields,h.payload_text::jsonb as payload
    from pi_history_views v join pi_history_blocks b on b.tenant_id=v.tenant_id and b.source_session_id=v.session_id
    join pi_history_bodies h on h.tenant_id=b.tenant_id and h.id=b.header_body_id
    where v.tenant_id=${f.scope.tenantId}::uuid order by v.session_id`.execute(f.database)
  ).rows;
}

it("settles a root without planning native child state, and retains its complete header", async () => {
  const f = await fixture();
  await finishRoot(f);
  const captured: CompiledQuery[] = [];
  const observed = f.database.withPlugin({
    transformQuery(args) {
      captured.push(f.database.getExecutor().compileQuery(args.node, args.queryId));
      return args.node;
    },
    async transformResult(args) {
      return args.result;
    },
  });
  await observed.transaction().execute(async (tx) => {
    await settleHistoryTurn(tx, f.scope.tenantId, f.scope.sessionId, f.scope.turnId);
    await settleChildHistory(tx, f.scope.tenantId, f.scope.runId);
  });
  expect(await headers(f)).toMatchObject([
    {
      session_id: f.scope.sessionId,
      fields: { runId: f.scope.runId, runState: "completed", coverage: "0", nativeCoverage: "0" },
      payload: {
        input_text: "Delegate",
        input_source: null,
        input_uploads: [],
        scheduled_origin: null,
        output_display_seq: "0",
        terminal: null,
      },
    },
  ]);
  const selectingHeader = captured.find((q) => q.sql.includes("jsonb_build_object('input_text'"))!;
  const plan = await f.database.executeQuery<{ "QUERY PLAN": unknown }>({
    ...selectingHeader,
    sql: `explain (analyze,buffers,format json) ${selectingHeader.sql}`,
  });
  // Even a never-executed child branch previously expanded the planner's join
  // search. A root header must depend only on its own control/terminal records.
  const relations = JSON.stringify(plan.rows).matchAll(/"Relation Name":"([^"]+)"/g);
  expect([...new Set([...relations].map((m) => m[1]))].sort()).toEqual([
    "runs",
    "session_terminal_events",
    "sessions",
  ]);
  expect(captured.some((q) => q.sql.includes("pi_history_live_turns"))).toBe(false);
  const before = await headers(f);
  await f.database.transaction().execute(async (tx) => {
    await settleHistoryTurn(tx, f.scope.tenantId, f.scope.sessionId, f.scope.turnId);
    await settleChildHistory(tx, f.scope.tenantId, f.scope.runId);
  });
  expect(await headers(f)).toEqual(before);
});

it("settles only dirty terminal children of the specified Run using native state", async () => {
  const f = await fixture();
  const jobs = new PostgresSubagentConversations({ database: f.database, native: f.native });
  const children = [];
  for (let i = 0; i < 2; i++) {
    const call = await f.command({ task: `Child ${i}` });
    children.push(
      await jobs.start({
        id: call.operationId,
        tenantId: f.scope.tenantId,
        rootRunId: f.scope.runId,
        parentSessionId: f.scope.sessionId,
        parentTurnId: f.scope.turnId,
        executionReference: f.reference,
        parentToolCallId: `task:${call.taskId}`,
        source: { sequence: call.nativeSequence, taskId: call.taskId },
        prompt: `Child ${i}`,
        contextMode: "fresh",
        contextAnchor: null,
        sandboxMode: "none",
      }),
    );
  }
  const [done, active] = children;
  await f.settle(done!.executionId, "completed", "Retained child answer");
  await f.database
    .transaction()
    .execute((tx) => settleChildHistory(tx, f.scope.tenantId, f.scope.runId));
  expect((await headers(f)).map((h) => h.session_id)).toEqual([done!.childSessionId]);
  const directoryRoot = async () =>
    (
      await sql<{ root_id: string }>`select root_id from pi_history_views
      where tenant_id=${f.scope.tenantId}::uuid and session_id=${done!.childSessionId}::uuid`.execute(
        f.database,
      )
    ).rows[0]!.root_id;
  const initialRoot = await directoryRoot();
  await f.database
    .transaction()
    .execute((tx) => settleChildHistory(tx, f.scope.tenantId, crypto.randomUUID()));
  await f.database
    .transaction()
    .execute((tx) => settleChildHistory(tx, crypto.randomUUID(), f.scope.runId));
  expect((await headers(f)).map((h) => h.session_id)).toEqual([done!.childSessionId]);
  await finishRoot(f);
  await f.database.transaction().execute(async (tx) => {
    await settleHistoryTurn(tx, f.scope.tenantId, f.scope.sessionId, f.scope.turnId);
    await settleChildHistory(tx, f.scope.tenantId, f.scope.runId);
  });
  const result = await headers(f);
  expect(result).toHaveLength(3);
  expect(result.find((h) => h.session_id === active!.childSessionId)?.fields.runState).toBe(
    "failed",
  );
  expect(
    result.find((h) => h.session_id === done!.childSessionId)?.payload.input_source,
  ).toBeTypeOf("string");
  expect(await directoryRoot()).toBe(initialRoot);
  expect(
    (
      await sql`select 1 from pi_history_live_turns where tenant_id=${f.scope.tenantId}::uuid
    and run_id=${f.scope.runId}::uuid`.execute(f.database)
    ).rows,
  ).toEqual([]);
});

it("does not freeze a pruned or never-started cancelled chat, but permits empty compaction input", async () => {
  const f = await fixture();
  await f.database
    .updateTable("runs")
    .set({ state: "cancelled", settled_at: new Date(), started_at: null })
    .where("id", "=", f.scope.runId)
    .execute();
  const settle = () =>
    f.database
      .transaction()
      .execute((tx) => settleHistoryTurn(tx, f.scope.tenantId, f.scope.sessionId, f.scope.turnId));
  await settle();
  expect(await headers(f)).toEqual([]);
  await f.database
    .updateTable("runs")
    .set({ operation_kind: "compact", input_text: "", pruned_at: new Date() })
    .where("id", "=", f.scope.runId)
    .execute();
  await settle();
  expect(await headers(f)).toEqual([]);
  await f.database
    .updateTable("runs")
    .set({ pruned_at: null })
    .where("id", "=", f.scope.runId)
    .execute();
  await settle();
  expect(await headers(f)).toMatchObject([
    { fields: { operationKind: "compact" }, payload: { input_text: "" } },
  ]);
});
