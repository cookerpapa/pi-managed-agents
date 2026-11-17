import { ExecutionStreamBoundary } from "../../runtime-core/src/execution-stream-boundary.ts";
import { beforeAll, afterAll, expect, it } from "vitest";
import { sql, type Kysely } from "kysely";
import { createDatabase, runMigrations, type Database } from "@pi-cloud/database";
import { ControlPlaneStore, createPrivateTenant } from "../src/index.ts";
import { createTestWorker } from "./admit-test-execution.ts";
import { TurnExecutionBackendError } from "../../runtime-core/src/run-executor.ts";
import { ExecutionStreamProjector } from "../../runtime-core/src/execution-stream-projection.ts";
import { parseKafkaAcceptedFact } from "../../runtime-core/src/kafka-accepted-fact.ts";
import type {
  AcceptedDurableCommitFact,
  AcceptedExecutionSealFact,
} from "../../runtime-core/src/accepted-fact.ts";
import { PostgresDurableCommitProjector } from "../../runtime-core/src/postgres-durable-commit-projector.ts";
import { confirmAgentExit } from "../../runtime-core/src/quarantined-session-recovery.ts";
import { captureNativePresentation } from "../../runtime-core/src/frozen-history.ts";

let db: Kysely<Database>, store: ControlPlaneStore, tenantId: string;
beforeAll(async () => {
  db = createDatabase({
    connectionString: process.env.PI_CLOUD_POSTGRES_INTEGRATION_URL!,
    maxConnections: 3,
  });
  await runMigrations(db, "up");
  const tenant = await createPrivateTenant(db, {
    slug: `durable-${crypto.randomUUID()}`,
    ownerDisplayName: "Durable test",
  });
  tenantId = tenant.tenantId;
  store = new ControlPlaneStore({
    database: db,
    tenantId,
    defaultModelProfileId: tenant.defaultModelProfileId,
  });
}, 60000);
afterAll(async () => {
  await db?.destroy();
});

it("persists non-display Tasks without presentation queries and preserves retries and terminal failures", async () => {
  const project = await store.createProject({ name: "Presentation", source: { kind: "empty" } });
  await db
    .updateTable("environment_versions")
    .set({ state: "validated", validated_at: new Date() })
    .where("id", "=", project.environment.environmentVersionId)
    .execute();
  const session = await store.createSession(
    project.projectId,
    project.workspaceId,
    "Presentation",
    "elastic",
  );
  const turn = await store.acceptTurn(session.sessionId, crypto.randomUUID(), {
    prompt: "capture",
  });
  const worker = await createTestWorker(db);
  const queries: string[] = [];
  const observed = db.withPlugin({
    transformQuery(args) {
      queries.push(db.getExecutor().compileQuery(args.node, args.queryId).sql);
      return args.node;
    },
    async transformResult(args) {
      return args.result;
    },
  });
  const projector = new PostgresDurableCommitProjector(observed);
  const executor = worker.executor({
    async execute(_request, _lifecycle, admission) {
      const scope = admission.publication.scope;
      let seq = 0;
      const publish = async (
        writes: AcceptedDurableCommitFact["commit"]["writes"],
        capture: boolean,
      ) => {
        const fact: AcceptedDurableCommitFact = {
          kind: "durable_commit",
          factId: crypto.randomUUID(),
          scope,
          occurredAt: new Date().toISOString(),
          commit: { seq: ++seq, nextId: 10, writes },
          origins: writes.map((w) => ({
            recordId: (w.value as { id: number }).id,
            runId: turn.runId,
            turnId: turn.turnId,
            sessionId: session.sessionId,
          })),
          tools: [],
          cancellations: [],
          display: [],
        };
        queries.length = 0;
        await projector.project(fact, {
          topic: "presentation-" + turn.runId,
          partition: 0,
          offset: BigInt(seq),
        });
        expect(queries.some((q) => q.includes("select g.id as retained_id"))).toBe(capture);
      };
      const task = (id: number, kind: string, state: Record<string, unknown>) => ({
        type: "task",
        value: {
          id,
          conversationId: 1,
          kind,
          version: 1,
          input: {},
          background: false,
          abortRequested: false,
          state,
        },
      });
      await publish([{ type: "conversation", value: { id: 1 } }], false);
      await publish(
        [task(2, "pi.generation", { status: "running", checkpoint: { phase: "request" } })],
        false,
      );
      await publish(
        [
          task(2, "pi.generation", {
            status: "running",
            checkpoint: { phase: "retry", attempt: 1, until: Date.now() },
          }),
        ],
        true,
      );
      await publish(
        [
          task(2, "pi.generation", {
            status: "terminal",
            outcome: { status: "completed", result: {} },
          }),
        ],
        false,
      );
      await publish(
        [task(3, "pi.tool", { status: "running", checkpoint: { phase: "execute" } })],
        false,
      );
      await publish(
        [task(3, "pi.tool", { status: "terminal", outcome: { status: "aborted" } })],
        true,
      );
      await publish(
        [task(4, "pi.compaction", { status: "running", checkpoint: { phase: "summarize" } })],
        false,
      );
      await publish(
        [
          task(4, "pi.compaction", {
            status: "terminal",
            outcome: { status: "failed", error: "fixture failure" },
          }),
        ],
        true,
      );
      await publish(
        [
          {
            type: "entry",
            value: {
              id: 5,
              conversationId: 1,
              kind: "pi.assistant",
              model: [
                {
                  role: "assistant",
                  stopReason: "stop",
                  content: [{ type: "text", text: "retained" }],
                },
              ],
            },
          },
        ],
        true,
      );
      const frames = () =>
        sql<{ seq: string; ordinal: number }>`select f.seq::text,f.ordinal
        from pi_history_frames f join pi_history_blocks b on b.tenant_id=f.tenant_id and b.id=f.block_id
        where b.tenant_id=${tenantId}::uuid and b.source_pi_session_id=${scope.piSessionId} order by f.seq,f.ordinal`.execute(
          db,
        );
      expect((await frames()).rows.map((r) => Number(r.seq))).toEqual([3, 6, 8, 9]);
      const retained = await sql<{ count: string }>`select count(*) from pi_durable_write_refs
        where tenant_id=${tenantId}::uuid and session_id=${scope.piSessionId}`.execute(db);
      expect(Number(retained.rows[0]!.count)).toBe(seq);
      // Ungated cold reconstruction is an independent oracle: it must not add
      // any display record that the live publication gate omitted.
      const before = (await frames()).rows;
      for (let n = 1; n <= seq; n++)
        await db
          .transaction()
          .execute((tx) =>
            captureNativePresentation(tx, { tenantId, sessionId: scope.piSessionId }, n),
          );
      expect((await frames()).rows).toEqual(before);
      return { stopReason: "stop", lastEventSeq: 0 };
    },
  });
  expect((await executor.dispatchRun(turn.runId)).status).toBe("completed");
});

it.each(
  (["none", "before_seal", "after_seal"] as const).flatMap((stop) =>
    (["worker_shutdown", "native_storage_unavailable"] as const).map(
      (failure) => [stop, failure] as const,
    ),
  ),
)(
  "re-admits the same Run after closure and preserves Stop (%s, %s)",
  async (stop, failure) => {
    const project = await store.createProject({
      name: `Recovery-${stop}-${failure}`,
      source: { kind: "empty" },
    });
    await db
      .updateTable("environment_versions")
      .set({ state: "validated", validated_at: new Date() })
      .where("id", "=", project.environment.environmentVersionId)
      .execute();
    const session = await store.createSession(
      project.projectId,
      project.workspaceId,
      "Recovery",
      "elastic",
    );
    const turn = await store.acceptTurn(session.sessionId, "recover-once", {
      prompt: "preserve this logical input",
    });
    const old = await createTestWorker(db),
      replacement = await createTestWorker(db);
    const first = old.executor({
      execute: async () => {
        throw new TurnExecutionBackendError(failure, "owner stopped", true);
      },
    });
    expect((await first.dispatchRun(turn.runId)).status).toBe("recovering");
    const outbox = await db
      .selectFrom("outbox")
      .select("payload")
      .where(
        sql<boolean>`payload->>'kind'='execution_seal' and payload#>>'{scope,runId}'=${turn.runId}`,
      )
      .executeTakeFirstOrThrow();
    const seal = parseKafkaAcceptedFact(
      JSON.stringify(outbox.payload),
    ) as AcceptedExecutionSealFact;
    expect(seal.recover).toBe(true);
    expect(seal.terminal).toBeUndefined();
    const requestStop = () =>
      store.acceptTurnCancellation(session.sessionId, turn.turnId, "stop-recovery", {
        gracePeriodMs: 1000,
      });
    if (stop === "before_seal") await requestStop();
    const started = Promise.withResolvers<void>(),
      finish = Promise.withResolvers<void>();
    const next = replacement.executor({
      execute: async (request) => {
        expect(request.runId).toBe(turn.runId);
        expect(request.turnId).toBe(turn.turnId);
        expect(request.stopOnResume).toBe(stop === "none" ? undefined : true);
        started.resolve();
        await finish.promise;
        if (stop !== "none")
          throw new TurnExecutionBackendError("worker_shutdown", "second owner stopped", true);
        return { stopReason: "stop", lastEventSeq: Number(request.nextEventSeq) - 1 };
      },
    });
    expect(await next.dispatchRun(turn.runId)).toEqual({ status: "idle" });
    const native: AcceptedDurableCommitFact = {
      kind: "durable_commit",
      factId: crypto.randomUUID(),
      scope: seal.scope,
      occurredAt: seal.occurredAt,
      commit: { seq: 1, nextId: 2, writes: [{ type: "conversation", value: { id: 1 } }] },
      origins: [],
      tools: [],
      cancellations: [],
      display: [],
    };
    const prefix = { fact: native, topic: "recovery-test", partition: 0, offset: 0n };
    const displaced = new ExecutionStreamProjector(db);
    await displaced.project(prefix);
    const projector = new ExecutionStreamProjector(db);
    await projector.project(prefix);
    const record = { fact: seal, topic: "recovery-test", partition: 0, offset: 2n };
    await projector.project(record);
    const queued = await db
      .selectFrom("runs")
      .selectAll()
      .where("id", "=", turn.runId)
      .executeTakeFirstOrThrow();
    expect(queued.state).toBe("queued");
    expect(queued.lease_id).toBeNull();
    expect(queued.ready_at).not.toBeNull();
    expect(
      (
        await db
          .selectFrom("pi_sessions")
          .select(
            sql<string>`(select count(*) from session_leases l where l.tenant_id=pi_sessions.tenant_id and l.pi_session_id=pi_sessions.id and l.released_at is null)`.as(
              "active_writers",
            ),
          )
          .where("id", "=", seal.scope.piSessionId)
          .executeTakeFirstOrThrow()
      ).active_writers,
    ).toBe("0");
    if (stop === "after_seal") await requestStop();
    if (stop !== "none") {
      const control = await db
        .selectFrom("turn_control_requests")
        .select("payload")
        .where("target_run_id", "=", turn.runId)
        .where("kind", "=", "cancel")
        .executeTakeFirstOrThrow();
      expect(control.payload).toMatchObject({ inputReturnPending: true });
    }
    if (failure === "native_storage_unavailable") {
      expect(queued.available_at.getTime()).toBeGreaterThan(Date.parse(seal.occurredAt));
      expect(await next.dispatchRun(turn.runId)).toEqual({ status: "idle" });
      // Advance this owned queue fixture after proving closure retained its delay.
      await db
        .updateTable("runs")
        .set({ available_at: new Date(0) })
        .where("id", "=", turn.runId)
        .execute();
    }
    const running = next.dispatchRun(turn.runId);
    try {
      await started.promise;
      const current = await db
        .selectFrom("runs")
        .select(["lease_id", "agent_exited_at"])
        .where("id", "=", turn.runId)
        .executeTakeFirstOrThrow();
      expect(current.lease_id).not.toBe(seal.scope.writerId);
      // This handler cached OPEN before replacement and resumes after the new
      // grant. Both native and already-covered display writes must recheck PG.
      const late = { ...native, commit: { seq: 2, nextId: 2, writes: [] } };
      await new PostgresDurableCommitProjector(db).project(late, { ...prefix, offset: 1n });
      expect(
        (
          await sql`select seq from pi_durable_commits where tenant_id=${tenantId}::uuid
        and session_id=${seal.scope.piSessionId} order by seq`.execute(db)
        ).rows,
      ).toEqual([{ seq: "1" }]);
      await db
        .updateTable("runs")
        .set({ output_display_seq: "100", output_display_native_seq: "1" })
        .where("id", "=", turn.runId)
        .execute();
      await expect(
        displaced.project({
          ...prefix,
          offset: 1n,
          fact: {
            kind: "agent_event",
            factId: crypto.randomUUID(),
            scope: seal.scope,
            occurredAt: seal.occurredAt,
            nativeSequence: 1,
            event: {
              schemaVersion: 1,
              eventId: crypto.randomUUID(),
              sessionId: session.sessionId,
              turnId: turn.turnId,
              agentId: "root",
              seq: 1,
              occurredAt: seal.occurredAt,
              type: "assistant.text.delta",
              payload: { text: "stale coverage" },
            },
          },
        }),
      ).rejects.toThrow("Display checkpoint preceded its native commit");
      await projector.project(record);
      await confirmAgentExit(db, {
        tenantId,
        sessionId: session.sessionId,
        runId: turn.runId,
        leaseId: seal.scope.writerId,
      });
      const unchanged = await db
        .selectFrom("runs")
        .select(["lease_id", "agent_exited_at", "state"])
        .where("id", "=", turn.runId)
        .executeTakeFirstOrThrow();
      expect(unchanged).toEqual({ ...current, state: "running" });
      const boundary = new ExecutionStreamBoundary(db);
      expect(await boundary.isOpen(record, true)).toBe(false);
      const count = await db
        .selectFrom("runs")
        .select(({ fn }) => fn.countAll<string>().as("count"))
        .where("session_id", "=", session.sessionId)
        .executeTakeFirstOrThrow();
      expect(count.count).toBe("1");
    } finally {
      finish.resolve();
      await running;
    }
  },
  30000,
);
