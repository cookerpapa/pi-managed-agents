import assert from "node:assert/strict";
import test from "node:test";
import { precedingRunDelay, summarizeTerminal } from "./tail-probe.mjs";

test("one Run's non-overlapping stages add up even when the ACK arrives after delivery", () => {
  const phases = summarizeTerminal([
    {
      sentAt: 1000,
      claimedAt: 1011,
      work: { finishAckAt: 1054 },
      terminal: {
        at: 1090,
        tail: { created: 1040, received: 1050, start: 1080, own: 5, other: 20, pg: 12, pool: 7 },
      },
    },
  ]);
  const x = phases.example;
  assert.equal(x.claim + x.worker + x.untilReceive + x.local + x.terminal, x.total);
  assert.equal(x.total, 90);
  assert.equal(x.afterAck, -4);
  assert.equal(x.between, 5);
});

test("eligibility uses the precise preceding Session completion, not the length of its own claim", () => {
  const a = {
    id: "a",
    tenant_id: "T",
    session_id: "S",
    mailbox_position: 2,
    created_at: new Date(1000),
    started_at: new Date(1010),
    settled_at: new Date(1500),
  };
  const b = {
    ...a,
    id: "b",
    mailbox_position: 3,
    created_at: new Date(1100),
    started_at: new Date(1550),
    settled_at: new Date(1600),
  };
  const result = precedingRunDelay(
    [b, a],
    [
      { id: "a", work: { finishAckAt: 1300 } },
      { id: "b", work: { finishAckAt: 1590 } },
    ],
  );
  assert.equal(result.hadPredecessorWait, 1);
  assert.deepEqual(result.example, { wait: 450, previous: 400, ready: 50, afterAck: 200 });
});
