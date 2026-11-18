import { recordEnvironmentFailure } from "./environment-failure.ts";
import { sql, type Transaction } from "kysely";
import type { Database } from "@pi-cloud/database";
import { isTerminalRunState } from "@pi-cloud/domain";
import { parsePiCloudEvent, type PiCloudEvent } from "@pi-cloud/protocol";
import type { AcceptedWorkFinishedFact } from "./accepted-fact.ts";
import type { FactPosition } from "./accepted-fact-recovery.ts";
import { projectionOffsetWrite } from "./accepted-fact-recovery.ts";
import { transitionLockedRun, type RunTransitionState } from "./run-state.ts";
import { advanceLaneReadiness } from "./run-readiness.ts";
import { settleHistoryTurn, settleChildHistory } from "./presentation-settlement.ts";

/** Called with an unsealed Root Run under its row and Session writer locks.
 * Pi has settled the family; this materializes cloud work and presentation. */
export async function finishWorkIn(
  tx: Transaction<Database>,
  run: RunTransitionState,
  fact: AcceptedWorkFinishedFact,
  position: FactPosition,
  highWaterMark: number,
  retireWriter: boolean,
): Promise<PiCloudEvent | undefined> {
  const now = new Date();
  // A control-plane terminal decision already in progress remains authoritative.
  // Its pending writer seal will finish the presentation. Recovery requests,
  // which keep the Run live, may be overtaken by a definitive native completion.
  if (isTerminalRunState(run.state)) {
    // A different terminal decision wins, but this ordered native completion
    // still proves the local family has exited. Keep that evidence for the
    // pending forced closure; it is not proof that Cube processes stopped.
    const released = tx
      .updateTable("runs")
      .set({ agent_exited_at: now, execution_released_at: now })
      .where("tenant_id", "=", fact.scope.tenantId)
      .where("id", "=", fact.scope.runId);
    await sql`with released as (${released}) ${projectionOffsetWrite(position)}`.execute(tx);
    return;
  }
  const terminal = fact.terminal;
  if (terminal.type === "turn.failed")
    await recordEnvironmentFailure(tx, { ...fact.scope, code: terminal.payload.code, now });
  const state =
    terminal.type === "turn.completed"
      ? "completed"
      : terminal.type === "turn.cancelled"
        ? "cancelled"
        : /(?:^|_)timeout$/.test(terminal.payload.code) ||
            terminal.payload.code === "pi_timeout" ||
            terminal.payload.code === "turn_timed_out"
          ? "timed_out"
          : "failed";
  if (state === "cancelled" && run.state !== "cancel_requested")
    run = await transitionLockedRun(tx, fact.scope, run, {
      runState: "cancel_requested",
      reason: "native_abort",
      now,
    });
  await transitionLockedRun(tx, fact.scope, run, {
    runState: state,
    reason: "native_work_finished",
    now,
    ...(terminal.type === "turn.failed"
      ? { failure: terminal.payload }
      : {
          stopReason:
            terminal.type === "turn.completed" ? terminal.payload.stopReason : "cancelled",
        }),
  });
  const event = parsePiCloudEvent({
    schemaVersion: 1,
    eventId: fact.factId,
    sessionId: fact.scope.sessionId,
    turnId: fact.scope.turnId,
    agentId: fact.agentId,
    seq: Math.max(fact.baseSequence, highWaterMark) + 1,
    occurredAt: fact.occurredAt,
    ...terminal,
  });
  const terminalEvent = tx.insertInto("session_terminal_events").values({
    event_id: event.eventId,
    tenant_id: fact.scope.tenantId,
    session_id: fact.scope.sessionId,
    turn_id: fact.scope.turnId,
    agent_id: event.agentId,
    run_id: fact.scope.runId,
    seq: event.seq,
    schema_version: event.schemaVersion,
    type: terminal.type,
    payload: terminal.payload,
    occurred_at: new Date(fact.occurredAt),
    persisted_at: now,
    interrupted_prefix: null,
  });
  const sealed = tx
    .updateTable("runs")
    .set({
      output_sealed_at: now,
      output_seal_offset: position.offset.toString(),
      last_event_seq: event.seq,
      execution_released_at: now,
      agent_exited_at: now,
      recovery_requested_at: null,
      output_first_topic: sql`coalesce(output_first_topic,${position.topic})`,
      output_first_partition: sql`coalesce(output_first_partition,${position.partition})`,
      output_first_offset: sql`coalesce(output_first_offset,${position.offset.toString()}::bigint)`,
    })
    .where("tenant_id", "=", fact.scope.tenantId)
    .where("id", "=", fact.scope.runId);
  const cancellations = tx
    .updateTable("turn_control_requests")
    .set({ state: "completed", completed_at: now, failure_code: null })
    .where("tenant_id", "=", fact.scope.tenantId)
    .where("target_run_id", "=", fact.scope.runId)
    .where("kind", "=", "cancel")
    .where("state", "in", ["pending", "dispatched", "acknowledged"]);
  const session = tx
    .updateTable("sessions")
    .set({
      state: "idle",
      next_event_seq: event.seq + 1,
      row_version: sql`row_version+1`,
      updated_at: now,
      last_active_at: now,
    })
    .where("tenant_id", "=", fact.scope.tenantId)
    .where("id", "=", fact.scope.sessionId);
  const writer = tx
    .updateTable("session_leases")
    .set({
      writer_sealed_at: now,
      writer_seal_offset: position.offset.toString(),
      released_at: now,
    })
    .where("tenant_id", "=", fact.scope.tenantId)
    .where("lease_id", "=", fact.scope.writerId);
  // These mutations target distinct tables under the admitted Run/writer locks.
  // Subsequent history assembly reads their effects in a new statement.
  await sql`with finished as (${terminalEvent}), sealed as (${sealed}),
    cancellations as (${cancellations}), session_state as (${session})
    ${retireWriter ? sql`, retired as (${writer})` : sql``}
    ${projectionOffsetWrite(position)}`.execute(tx);
  await settleHistoryTurn(tx, fact.scope.tenantId, fact.scope.sessionId, fact.scope.turnId);
  await settleChildHistory(tx, fact.scope.tenantId, fact.scope.runId);
  await advanceLaneReadiness(tx, fact.scope.tenantId, fact.scope.sessionId);
  return event;
}
