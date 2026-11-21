// Matched source for ordinary text/tool root Sessions; never opens a legacy
// native projection. One SessionProjector owns admission, live events and grant.
import assert from "node:assert/strict";
import { sql } from "kysely";
import { resolveDurableDisplay } from "@pi-cloud/protocol";
import { ReadyProjection } from "./ready-projection.mjs";
import { projectionTemplates } from "./projection-statements.mjs";
import { WorkspaceOperationRouter } from "./workspace-operation-router.mjs";
import { assertFamilySettled } from "./moving-checkpoint.mjs";
import { emptyInboxRoutes } from "./family-inboxes.mjs";

export function normalRootProjection(pool, topic, select, resolve) {
  const view = new ReadyProjection(pool, topic, new Map(), projectionTemplates);
  view.configure(16, "recorded-history");
  const env = {
    pool,
    sessions: view.sessions,
    partitions: 1,
    stage: { integrated: true, sink: "single", scene: "plain", rootPresentation: true },
    nextOffsets: new Map(),
    metrics: {
      byPartition: [0],
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
  const deliveries = new WeakMap();
  const nativeStore = {
    brokerTimedRetention: {
      replyFloors(database, topics) {
        return sql`select p.v#>>'{reply,topic}' as topic,min((p.v#>>'{reply,offset}')::bigint)::text as floor
        from runs r cross join lateral jsonb_each(r.projection_tools) p(k,v) where p.v#>>'{reply,topic}'=any(${topics}::text[]) group by p.v#>>'{reply,topic}'`.execute(
          database,
        );
      },
    },
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
      async project(fact, position, current) {
        assert.equal(fact.scope.sessionId, fact.scope.piSessionId);
        assert(
          !fact.modelEvidence?.length && !fact.inputQueueSessions?.length,
          "Unexpected accounting or inbox confirmation in ordinary root profile",
        );
        const r = { ...position, fact },
          plan = await router.projectOperation(r, current);
        const session = view.sessions.get(fact.scope.runId);
        const admitted =
          current() &&
          session?.writer === fact.scope.writerId &&
          session.seq === fact.commit.seq &&
          session.lastFact === fact.factId;
        const events = [],
          covers = [];
        if (admitted) {
          if (plan) deliveries.set(fact, plan);
          for (const d of fact.display) {
            assert.equal(d.scope.sessionId, fact.scope.sessionId);
            for (const e of d.events)
              events.push(
                resolveDurableDisplay(fact.commit, e, (id) => session.liveDocs[id]?.value),
              );
            if (d.coveredThrough !== undefined)
              covers.push({ sessionId: d.scope.sessionId, through: d.coveredThrough });
          }
        }
        return { admitted, events, covers };
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
          s && s.writer === scope.writerId && sequence === s.seq,
          "Native root has not reached its completion",
        );
        assertFamilySettled(s);
        assert(emptyInboxRoutes(s.inboxes));
        return () => {};
      },
      releaseSettled(scope) {
        if (view.sessions.get(scope.runId)?.writer === scope.writerId) {
          view.sessions.delete(scope.runId);
          router.spans.forget(scope);
        }
      },
    },
    async settle(tx, f, pos) {
      assert(f.kind === "work_finished" && f.terminal.type === "turn.completed");
      const s = view.sessions.get(f.scope.runId);
      assert(
        s &&
          s.lastAnswerCommit !== undefined &&
          s.seq === f.nativeSequence &&
          s.writer === f.scope.writerId,
      );
      const h = {
        version: f.factId,
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
        await sql`with bound as (update runs set history_head=${JSON.stringify(h)}::jsonb,projection_first=null,projection_partition=null,projection_pin=null,projection_state=null,native_work_handoff=null
        where id=${f.scope.runId}::uuid and tenant_id=${f.scope.tenantId}::uuid and lease_id=${s.writer}::uuid and state='completed' and output_seal_offset=${String(pos.offset)}::bigint and projection_tools='{}'::jsonb returning id),
        retired as (delete from workspace_route_spans where run_id in(select id from bound))select id from bound`.execute(
          tx,
        );
      assert.equal(checked.rows.length, 1);
    },
  };
  return {
    view,
    nativeStore,
    async dispatch(r, current, grant) {
      if (r.fact.kind === "work_finished") {
        nativeStore.controlProjected(r);
        return;
      }
      const plan = deliveries.get(r.fact);
      deliveries.delete(r.fact);
      if (plan) await router.dispatch(plan, r, current, grant);
    },
  };
}
