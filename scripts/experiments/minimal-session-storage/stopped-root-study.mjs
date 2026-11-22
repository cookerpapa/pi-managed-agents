import assert from "node:assert/strict";
import { browserNote, browserMessage } from "./browser-signals.mjs";
import { until } from "./family-process.mjs";

export async function verifyStoppedRoot({
  actor,
  receipt,
  first,
  originalPrompt,
  second,
  third,
  edited,
  id,
  api,
  pool,
  a,
  work,
  browserRun,
  endpoint,
  counts,
}) {
  const { another } = await browserMessage("/study", "stopClicked");
  assert.equal(another.delivery, "steer");
  assert.equal(another.runId, receipt.runId);
  const ids = [receipt.inputId, another.inputId],
    runId = receipt.runId;
  const stop = await until(
    async () =>
      (
        await pool.query(
          "select id from turn_control_requests where target_run_id=$1 and kind='cancel'",
          [runId],
        )
      ).rows[0],
    "public Stop recorded",
  );
  const page = () => api.getReturnedInputs(id, undefined, { requestId: stop.id, afterPosition: 0 });
  assert((await page()).pending);
  actor.send({ stop: true });
  const stopped = await a;
  assert.equal(stopped.modelCalls, 1);
  await until(
    async () => (await api.getRun(runId)).state === "cancelled",
    "cancelled root settlement",
  );
  const returned = await page();
  assert(!returned.pending);
  assert.deepEqual(
    returned.items.map((i) => [i.inputId, i.text]),
    [
      [ids[0], second],
      [ids[1], third],
    ],
  );
  const {
    rows: [r],
  } = await pool.query(
    `select root_input_receipts,history_head,(select count(*)::int from messages where run_id=r.id and source_input_id=any($2)) placements,
    (select count(*)::int from runs where session_id=r.session_id) runs from runs r where id=$1`,
    [runId, ids],
  );
  assert.equal(r.runs, 1);
  assert.equal(r.placements, 0);
  assert(r.history_head);
  for (const key of ids)
    assert.deepEqual(
      [
        r.root_input_receipts[key].status,
        r.root_input_receipts[key].reason,
        r.root_input_receipts[key].entry,
      ],
      ["unanswered", "aborted", undefined],
    );
  assert.deepEqual(counts(), { dispatches: 1, cancels: 1 });
  await browserMessage("/study", "editedReload");
  assert.equal((await api.getReturnedInputs(id)).requestId, null);
  assert.equal(
    (await pool.query("select count(*)::int n from runs where session_id=$1", [id])).rows[0].n,
    1,
  );
  await browserNote("/study", "sendEdited");
  const next = await browserRun(2);
  const resumed = await work(next.runId);
  assert.deepEqual(resumed.restoredUserPrompts, [originalPrompt, edited]);
  await until(
    async () => (await api.getRun(next.runId)).state === "completed",
    "explicit edited continuation",
  );
  assert.equal(await endpoint.marker(), "once");
  const history = await api.getConversation(id);
  assert.equal(history.turns.length, 2);
  assert.equal(history.turns[0].turnId, first.turnId);
  assert.equal(history.turns[1].prompt, edited);
  const explicit = await pool.query(
    "select entry_id from messages where run_id=$1 and source_input_id=$1::uuid::text and role='user'",
    [next.runId],
  );
  assert.equal(explicit.rowCount, 1);
  const {
    rows: [placed],
  } = await pool.query(
    "select count(*)::int n from messages where pi_session_id=$1 and source_input_id=any($2)",
    [id, ids],
  );
  assert.equal(placed.n, 0);
  return {
    ...(await browserMessage("/study", "done")),
    returnedInputs: 2,
    returnedPlacements: 0,
    runsBeforeExplicitSend: 1,
    explicitEditedPlacements: 1,
    modelCallsInStoppedRun: stopped.modelCalls,
    physicalExecutions: 1,
    orderedCancellations: counts().cancels,
    legacyNativeRowsAdded: 0,
  };
}
