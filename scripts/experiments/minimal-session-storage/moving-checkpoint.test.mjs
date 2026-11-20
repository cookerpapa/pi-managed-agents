import assert from "node:assert/strict";
import test from "node:test";
import {
  checkpoint,
  foldLive,
  hydrate,
  movingStarts,
  plainAnswer,
  replayableMetadata,
  useSynchronousPolicy,
} from "./moving-checkpoint.mjs";

const scope = { writerId: "writer", runId: "run" },
  fact = {
    kind: "durable_commit",
    scope,
    factId: "fact",
    commit: { seq: 11, writes: [] },
    tools: [],
    display: [],
  };
const record = { fact, offset: 101n };
const create = (id, conversation, live) => ({
  type: "document.create",
  record: { id, kind: "pi.live", scope: { kind: "conversation", conversationId: conversation } },
  content: { kind: "base", value: live },
});
const state = () => ({
  writer: "writer",
  seq: 11,
  lastFact: "fact",
  inboxes: {
    4: { conversation: 1, value: { items: [] } },
    5: { conversation: 7, value: { items: [] } },
  },
  liveDocs: foldLive({}, [create(2, 1, {}), create(3, 7, {})]),
});

test("checkpoints only bounded settled document metadata and preserves the original delta base", () => {
  const v = state(),
    snap = checkpoint(v, record);
  assert.equal(hydrate(snap, "writer", 102).seq, 11);
  assert.throws(() => hydrate(snap, "another-writer", 102));
  for (const live of [
    { generation: { message: { text: "hidden" } } },
    { tools: [{ status: "running", output: "hidden" }] },
    { tools: [{ status: "done", details: "hidden" }] },
    { unknown: "hidden" },
  ]) {
    const changed = foldLive(v.liveDocs, [
      { type: "document.change", id: 3, content: { kind: "base", value: live } },
    ]);
    assert.equal(checkpoint({ ...v, liveDocs: changed }, record), undefined);
    assert.deepEqual(v.liveDocs[3].value, {});
  }
  assert.equal(
    checkpoint(
      { ...v, inboxes: { ...v.inboxes, 5: { conversation: 7, value: { items: ["pending"] } } } },
      record,
    ),
    undefined,
  );
  assert.equal(checkpoint({ ...v, pendingTools: { 4: {} } }, record), undefined);
});

test("cold position ignores idle snapshots, honors the oldest pending child and blocks an expired pin or changed writer locally", async () => {
  const snapshot = checkpoint(state(), record);
  const rows = [
    {
      partition: 0,
      id: "idle",
      lease_id: "writer",
      next_offset: "600",
      projection_first: "102",
      projection_state: snapshot,
      projection_pin: null,
    },
    {
      partition: 0,
      id: "active",
      lease_id: "writer",
      next_offset: "600",
      projection_first: "102",
      projection_state: snapshot,
      projection_pin: "450",
    },
    {
      partition: 1,
      id: "changed",
      lease_id: "old",
      next_offset: "110",
      projection_first: "102",
      projection_state: snapshot,
      projection_pin: null,
    },
  ];
  const plan = (low) =>
    movingStarts({ query: async () => ({ rows }) }, "topic", [
      { partition: 0, low, high: 700n },
      { partition: 1, low: 0n, high: 200n },
    ]);
  let p = await plan(400n);
  assert.equal(p[0][2], 450n);
  assert.equal(p[0][3].length, 2);
  assert(p[1][2] instanceof Error);
  p = await plan(451n);
  assert(p[0][2] instanceof Error);
  rows[1].projection_pin = null;
  p = await plan(500n);
  assert.equal(p[0][2], 600n);
});

test("control metadata and non-text content keep the conservative synchronous policy", () => {
  const f = {
    ...fact,
    commit: {
      writes: [
        {
          type: "entry",
          value: {
            model: [
              { role: "assistant", stopReason: "stop", content: [{ type: "text", text: "ok" }] },
            ],
          },
        },
      ],
    },
  };
  const frame = { ops: [{ kind: "message" }] };
  assert(plainAnswer(frame, f));
  for (const extra of [
    { display: [{}] },
    { inputQueueSessions: ["s"] },
    { modelEvidence: [{}] },
    { cancellations: [{}] },
  ])
    assert(!plainAnswer(frame, { ...f, ...extra }));
  assert(!plainAnswer({ ...frame, anchor: [] }, f));
  assert(!plainAnswer({ ops: [{ kind: "finish" }] }, f));
  assert(useSynchronousPolicy({ ...f, tools: [{}] }));
  assert(!useSynchronousPolicy({ ...f, tools: [{}] }, true));
});

test("a copy of the admitted input can be reconstructed without relaxing its initial admission or a later input receipt", () => {
  const op = (id) => ({
    kind: "message",
    args: [0, 0, 0, 0, JSON.stringify([{ role: "user", input: id }])],
  });
  assert(replayableMetadata({ ops: [op("run")] }, fact));
  assert(!replayableMetadata({ ops: [op("follow-up")] }, fact));
  assert(!replayableMetadata({ ops: [op("run")], strict: true }, fact));
  assert(replayableMetadata({ ops: [{ kind: "pin" }] }, fact));
  assert(!replayableMetadata({ ops: [{ kind: "finish" }] }, fact));
});
