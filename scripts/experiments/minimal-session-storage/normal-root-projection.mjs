// Matched source for root Runs and explicitly validated child displays. No legacy
// native projection. One SessionProjector owns admission, live events and grant.
import { worldRoutes } from "./conversation-world.mjs";
import { rootOpeningHead } from "./session-prefix.mjs";
import { reopenRootProjection } from "./root-projection-handoff.mjs";
import assert from "node:assert/strict";
import { changedInputConfirmations } from "@pi-cloud/pi-session-postgres/durable";
import { sql } from "kysely";
import { resolveDurableDisplay } from "@pi-cloud/protocol";
import { ReadyProjection } from "./ready-projection.mjs";
import { projectRecord } from "./project-record.mjs";
import { projectionTemplates } from "./projection-statements.mjs";
import { WorkspaceOperationRouter } from "./workspace-operation-router.mjs";
import { assertFamilySettled } from "./moving-checkpoint.mjs";
import { emptyInboxRoutes } from "./family-inboxes.mjs";

function presentationResult(fact, session, before, admitted) {
  const events = [],
    covers = [];
  if (admitted) {
    for (const d of fact.display) {
      for (const e of d.events)
        events.push(resolveDurableDisplay(fact.commit, e, (id) => session.liveDocs[id]?.value));
      if (d.coveredThrough !== undefined)
        covers.push({ sessionId: d.scope.sessionId, through: d.coveredThrough });
    }
  }
  const after = session?.rootInputDoc;
  const inputConfirmations =
    admitted && fact.inputQueueSessions?.length && after && before !== after
      ? changedInputConfirmations(
          fact.scope.tenantId,
          { ...after.value, conversationId: 1 },
          before ? { ...before.value, conversationId: 1 } : undefined,
        ).map(({ conversationId, ...receipt }) => ({
          ...receipt,
          sessionId: fact.scope.sessionId,
        }))
      : [];
  return { admitted, events, covers, inputConfirmations };
}

export function normalRootProjection(
  pool,
  topic,
  select,
  resolve,
  validateChild,
  {
    delegatedRuns = false,
    independentSessions = false,
    partitions = 1,
    bashLog = false,
    namedExecutor = Boolean(process.env.PI_CLOUD_SUBAGENT_EXECUTOR_URL),
  } = {},
) {
  assert(
    [false, "intents", "coverage", "folded"].includes(bashLog),
    "Unknown candidate Bash projection layout",
  );
  if (namedExecutor && !validateChild)
    validateChild = async (scope, child) => {
      const result = await pool.query(
        "select 1 from runs where tenant_id=$1 and session_id=$2 and root_run_id=$3 and agent_id=$4 and id=$5",
        [scope.tenantId, scope.piSessionId, scope.runId, child.sessionId, child.turnId],
      );
      assert.equal(result.rowCount, 1, "Unregistered named child display");
    };
  const view = new ReadyProjection(pool, topic, new Map(), projectionTemplates);
  view.delegatedRuns = delegatedRuns;
  view.independentSessions = independentSessions;
  view.sessionInputs = independentSessions;
  view.bashLog = bashLog;
  view.namedExecutor = namedExecutor;
  view.configure(16, "recorded-history");
  const env = {
    pool,
    sessions: view.sessions,
    partitions,
    stage: {
      integrated: true,
      sink: "single",
      scene: "plain",
      rootPresentation: true,
      independentSessions,
    },
    nextOffsets: new Map(),
    metrics: {
      byPartition: Array(partitions).fill(0),
      queries: 0,
      skipped: 0,
      peakWaiting: 0,
      inboxSnapshots: 0,
      inboxBytes: 0,
      duplicate: 0,
      stale: 0,
    },
    account() {},
    send() {},
  };
  const router = new WorkspaceOperationRouter({ pool, view, env, select, resolve });
  const deliveries = new WeakMap(),
    childDisplays = new Map();
  const nativeStore = {
    controlProjected(pos) {
      const next = pos.offset + 1n;
      if (next > view.next.get(pos.partition)) view.next.set(pos.partition, next);
    },
    async replayPlan(_db, t, bounds) {
      assert.equal(t, topic);
      return { starts: await view.recover(bounds), projectedNext: new Map(view.next) };
    },
    projection: {
      reset() {
        view.revoke();
        router.spans.reset();
        childDisplays.clear();
      },
      statistics() {
        let pendingDocuments = 0,
          estimatedBytes = 0;
        for (const s of view.sessions.values()) {
          for (const doc of Object.values(s.liveDocs ?? {})) {
            pendingDocuments++;
            estimatedBytes += Buffer.byteLength(JSON.stringify(doc));
          }
        }
        return { views: view.sessions.size, deferredReceipts: 0, pendingDocuments, estimatedBytes };
      },
      async replay() {
        /* Rejected writer; no candidate state or physical effect. */
      },
      async project(
        fact,
        position,
        current,
        physicalOwner,
        openingIndependentWriter = false,
        decodedOperations,
      ) {
        assert.equal(fact.scope.sessionId, fact.scope.piSessionId);
        assert(
          (fact.inputQueueSessions ?? []).every((id) => id === fact.scope.sessionId),
          "Unsupported cross-session input",
        );
        for (const { scope: child } of fact.display) {
          if (child.sessionId === fact.scope.sessionId) continue;
          assert(
            validateChild &&
              child.tenantId === fact.scope.tenantId &&
              child.piSessionId === fact.scope.piSessionId &&
              child.runId === fact.scope.runId &&
              child.writerId === fact.scope.writerId &&
              child.fencingToken === fact.scope.fencingToken,
            "Unmatched child display scope",
          );
          let known = childDisplays.get(fact.scope.writerId);
          if (known?.get(child.sessionId) === child.turnId) continue;
          await validateChild(fact.scope, child);
          if (!current()) return { admitted: false, events: [], covers: [] };
          if (!known) childDisplays.set(fact.scope.writerId, (known = new Map()));
          known.set(child.sessionId, child.turnId);
        }
        const { scope } = fact;
        let previous = view.sessions.get(scope.runId),
          openingRow;
        if (!previous && fact.commit.seq > 1) {
          openingRow =
            (await pool.query(rootOpeningHead, [scope.runId, scope.tenantId, scope.piSessionId]))
              .rows[0] ?? null;
          if (openingRow?.has_handoff)
            previous = await reopenRootProjection(
              pool,
              scope,
              position,
              fact.commit.seq,
              !!validateChild,
              delegatedRuns,
              bashLog,
              physicalOwner,
            );
          if (!current()) return { admitted: false, events: [], covers: [] };
          if (previous) view.sessions.set(scope.runId, previous);
        }
        const before = previous?.rootInputDoc;
        const r = {
            ...position,
            fact,
            openingRow,
            decodedOperations,
            openingIndependentWriter,
            partitionKey: physicalOwner,
          },
          plan = await router.projectOperation(r, current);
        const session = view.sessions.get(fact.scope.runId);
        const admitted =
          current() &&
          session?.writer === fact.scope.writerId &&
          session.seq === fact.commit.seq &&
          session.lastFact === fact.factId;
        if (admitted && plan) deliveries.set(fact, plan);
        return presentationResult(fact, session, before, admitted);
      },
      async prepareCoverage() {
        throw Error("Standalone native display is outside the ordinary root composition");
      },
      closedWriterCleanup() {
        return () => {};
      },
      async checkpointLiveIn(_tx, scope, current, sequence) {
        assert(current());
        const s = view.sessions.get(scope.runId);
        assert(
          s && s.writer === scope.writerId && (sequence === undefined || sequence === s.seq),
          "Native root has not reached its completion",
        );
        if (sequence === undefined) return () => {}; // Forced seal records active work via the recovery checkpoint.
        assertFamilySettled(s);
        assert(emptyInboxRoutes(s.inboxes));
        return () => {};
      },
      releaseSettled(scope) {
        childDisplays.delete(scope.writerId);
        if (view.sessions.get(scope.runId)?.writer === scope.writerId) {
          view.sessions.delete(scope.runId);
          router.spans.forget(scope);
        }
      },
    },
    async settle(tx, f, pos, publicState) {
      assert(
        f.kind === "work_finished" &&
          (["turn.completed", "turn.cancelled"].includes(f.terminal.type) ||
            (f.terminal.type === "turn.failed" &&
              (independentSessions || f.terminal.payload.code === "workspace_directory_missing"))),
      );
      const state =
        f.terminal.type === "turn.completed"
          ? "completed"
          : f.terminal.type === "turn.failed"
            ? "failed"
            : "cancelled";
      const s = view.sessions.get(f.scope.runId);
      assert(
        s &&
          (state !== "completed" || s.lastAnswerCommit !== undefined) &&
          s.seq === f.nativeSequence &&
          s.writer === f.scope.writerId,
      );
      const h = {
        version: f.factId,
        ...worldRoutes(s),
        ...(s.agentRoutes ? { agentRoutes: s.agentRoutes } : {}),
        seq: s.seq,
        nextId: s.nextId,
        floor: s.floor,
        count: s.count,
        bytes: s.bytes,
      };
      if (s.floorChanged) {
        const {
          rows: [row],
        } =
          await sql`select count(*)::int count,coalesce(sum(octet_length(content)),0)::bigint bytes from messages where tenant_id=${f.scope.tenantId}::uuid and pi_session_id=${f.scope.piSessionId} and conversation_id=1 and entry_id>=${s.floor} and entry_id<${s.nextId} and commit_seq<=${s.seq}`.execute(
            tx,
          );
        h.count = row.count;
        h.bytes = Number(row.bytes);
      }
      const checked =
        await sql`with bound as (update runs set history_head=${JSON.stringify(h)}::jsonb,projection_first=null,projection_partition=null,projection_pin=null,projection_state=null,native_work_handoff=null,cancel_positions='{}'::jsonb
        where id=${f.scope.runId}::uuid and tenant_id=${f.scope.tenantId}::uuid and lease_id=${s.writer}::uuid and state=${publicState ?? state} and output_seal_offset=${String(pos.offset)}::bigint and projection_tools='{}'::jsonb ${bashLog ? sql`and not exists(select from pending_bash_intents where run_id=${f.scope.runId}::uuid)` : sql``} returning id),
        retired as (delete from workspace_route_spans where run_id in(select id from bound))select id from bound`.execute(
          tx,
        );
      assert.equal(checked.rows.length, 1);
    },
  };
  return {
    view,
    async prepareNativeHistory(records, current) {
      // The caller has admitted a warm writer and ruled out every external operation.
      return view.apply(records, current, (r, draft) => projectRecord(r, draft, env));
    },
    presentationResult,
    prepareHistory: (records, current) => router.prepareHistory(records, current),
    nativeStore,
    async dispatch(r, current, grant) {
      if (r.fact.kind === "work_finished" || r.fact.kind === "execution_seal") {
        nativeStore.controlProjected(r);
        return;
      }
      const plan = deliveries.get(r.fact);
      deliveries.delete(r.fact);
      if (plan) await router.dispatch(plan, r, current, grant);
    },
  };
}
