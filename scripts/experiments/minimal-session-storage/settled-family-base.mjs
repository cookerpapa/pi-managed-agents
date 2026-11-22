// Start the later Run's Projector fold from exactly the root shape restored by
// the candidate Worker. Entries remain in PG and are selected only on cold read.
import assert from "node:assert/strict";
import { AcceptedFamilyCheckpoint } from "@pi-cloud/pi-session-postgres/durable";
import { rootOpeningHead, priorState, settledRootWrites } from "./session-prefix.mjs";

export async function settledFamilyBase(pool, scope, sequence) {
  const {
    rows: [row],
  } = await pool.query(rootOpeningHead + " and r.turn_id=$4 and r.fencing_token=$5", [
    scope.runId,
    scope.tenantId,
    scope.piSessionId,
    scope.turnId,
    scope.fencingToken,
  ]);
  if (!row || row.has_handoff || Number(row.mailbox_position) === 1) return;
  assert(
    ["running", "cancel_requested"].includes(row.state) && row.lease_id === scope.writerId,
    "Unowned later Run",
  );
  const h = priorState({ ...row, inbox_items: [] });
  assert.equal(h.seq + 1, sequence, "New Run missed its first native commit");
  assert.deepEqual(
    Object.values(row.inbox_routes),
    [1],
    "Settled predecessor needs a root-only cold base",
  );
  const records = settledRootWrites({ ...row, inbox_items: [] }, h).filter(
    ({ write }) => write.record?.kind !== "pi.inbox",
  );
  return AcceptedFamilyCheckpoint.resume(
    scope.writerId,
    { seq: h.seq, nextId: h.nextId, records, heads: [], sequences: [] },
    [Number(row.inbox_id)],
  );
}
