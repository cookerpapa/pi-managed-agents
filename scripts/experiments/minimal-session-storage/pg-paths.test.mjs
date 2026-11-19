import assert from "node:assert/strict";
import test from "node:test";
import { orderFrames } from "./pg-path-replay.mjs";
import { attachIdleWitness, matchDurations } from "./pg-path-trials.mjs";

test("merge preserves each real partition prefix even when producer clocks go backwards", () => {
  const frames = [
    { partition: 0, offset: 8, created: 10 },
    { partition: 1, offset: 5, created: 7 },
    { partition: 0, offset: 9, created: 6 },
  ];
  assert.deepEqual(
    orderFrames(frames).map((f) => f.offset),
    [5, 8, 9],
  );
  assert.throws(() =>
    orderFrames([
      { partition: 0, offset: 9, created: 5 },
      { partition: 0, offset: 8, created: 7 },
    ]),
  );
});
test("an observed idle transition splits the same query, and an out-of-window observation does not", () => {
  const span = ["assistant", 100, 115];
  const row = { afterExecute: 11 };
  attachIdleWitness(row, span, 112);
  assert.equal(row.executeToIdle, 8);
  assert.equal(row.afterIdle, 3);
  for (const idle of [undefined, 91, 127]) {
    const unmatched = { afterExecute: 11 };
    attachIdleWitness(unmatched, span, idle);
    assert.equal(unmatched.afterIdle, undefined);
  }
});
test("match every PG protocol triplet on its backend or reject the timing block", () => {
  const spans = [
    ["assistant", 100, 115],
    ["finish", 116, 131],
  ];
  const durations = [
    [10, 0.5],
    [101, 0.2],
    [102, 0.5],
    [104, 1],
    [117, 0.1],
    [118, 0.2],
    [119, 0.4],
  ];
  const rows = matchDurations(spans, durations);
  assert.equal(rows[0].server, 1.7);
  assert.equal(rows[0].afterExecute, 11);
  assert.equal(rows[1].client, 15);
  assert.throws(() => matchDurations(spans, durations.slice(0, -1)), /Incomplete/);
  assert.throws(
    () => matchDurations([["assistant", 0, 1], spans[1]], durations),
    /match client span/,
  );
});
