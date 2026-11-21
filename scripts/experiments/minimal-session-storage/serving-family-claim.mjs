// The actor owns the renewal loop. SIGKILL stops renewal without changing PG.
import assert from "node:assert/strict";
import { until } from "./family-process.mjs";
import { randomUUID } from "node:crypto";
import { createDatabase } from "@pi-cloud/database";
import {
  RunExecutor,
  TurnExecutionCancelledError,
} from "../../../packages/runtime-core/src/run-executor.ts";
import { SessionLeaseCoordinator } from "../../../packages/runtime-core/src/session-lease-coordinator.ts";
import { RunCancellationExecutor } from "../../../packages/runtime-core/src/run-cancellation-executor.ts";
import { registerTestWorker } from "../../../packages/control-plane/test/fixtures/registered-worker.ts";
import { admitTestExecution } from "../../../packages/control-plane/test/admit-test-execution.ts";

export async function claimRun(connectionString, runId) {
  const database = createDatabase({ connectionString, maxConnections: 3 });
  const workerId = randomUUID();
  await database
    .insertInto("workers")
    .values({
      id: workerId,
      boot_id: randomUUID(),
      supervisor_id: workerId,
      state: "ready",
      max_concurrent_sessions: 1,
    })
    .execute();
  await registerTestWorker(database, workerId);
  const owner = new SessionLeaseCoordinator({ database, workerId, leaseDurationMs: 12000 });
  const entered = Promise.withResolvers(),
    completed = Promise.withResolvers();
  const executor = new RunExecutor({
    database,
    workerId,
    executionAuthority: owner,
    backend: {
      admit: (tx, r, _mark, facts) => admitTestExecution(owner, tx, r, facts),
      execute: async (request, lifecycle, admission) => {
        entered.resolve({ request, lifecycle, admission });
        return completed.promise;
      },
    },
  });
  const running = executor.dispatchNext();
  running.then(() => entered.reject(Error("No claim")), entered.reject);
  const { request, lifecycle, admission } = await entered.promise;
  assert.equal(request.runId, runId);
  const { leaseId, piConversationId, ...scope } = admission.publication.scope;
  assert.equal(piConversationId, 1);
  const identity = await owner.heartbeatIdentity();
  let timer,
    renewalError,
    closed = false;
  async function renew() {
    const response = await owner.renewFromHeartbeat({
      protocolVersion: 1,
      type: "supervisor.heartbeat",
      messageId: randomUUID(),
      sentAt: new Date().toISOString(),
      payload: {
        ...identity,
        acceptingAssignments: true,
        maxConcurrentSessions: 1,
        families: [
          {
            tenantId: scope.tenantId,
            piSessionId: scope.piSessionId,
            leaseId,
            writerId: scope.writerId,
            fencingToken: scope.fencingToken,
          },
        ],
      },
    });
    assert(response.payload.familyLeaseRenewals?.length === 1, "Lease renewal was not accepted");
    if (!closed)
      timer = setTimeout(
        () =>
          renew().catch((e) => {
            renewalError = e;
          }),
        3000,
      );
  }
  await renew();
  return {
    scope,
    bootId: identity.bootId,
    request,
    async cancel(abort) {
      const canceller = new RunCancellationExecutor({
        database,
        executionAuthority: owner,
        backend: {
          cancel: async (command, control) => {
            assert.equal(command.target.runId, runId);
            await control.started(admission);
            await abort();
            return { reason: command.reason, forced: false };
          },
        },
      });
      // The serving Worker checks pending cancellations again after an idle
      // SKIP LOCKED claim. A renewal or projection may hold the Run row.
      const result = await until(
        async () => {
          const attempt = await canceller.dispatchTargetRun(runId);
          return attempt.status === "idle" ? undefined : attempt;
        },
        "claim recovering Run cancellation",
        10000,
      );
      assert.equal(result.status, "cancelled");
      return result.controlRequestId;
    },
    async finish(write, stopped = request.stopOnResume) {
      if (renewalError) throw renewalError;
      lifecycle.executionExited();
      lifecycle.publicationReady({ close: async () => {}, finish: write });
      if (stopped) completed.reject(new TurnExecutionCancelledError("user_request", false));
      else completed.resolve({ stopReason: "stop" });
      await running;
    },
    async close() {
      closed = true;
      clearTimeout(timer);
      await database.destroy();
    },
  };
}
