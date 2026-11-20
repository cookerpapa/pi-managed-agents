import assert from "node:assert/strict";
import test from "node:test";
import { messageTape, MessageRateMeter } from "./complete-message-rate.mjs";

test("the unit counts 200 paired entries per Run and excludes opening, input, final answer and settlement", async () => {
  let lane = 0;
  const partitions = new Map();
  const tape = messageTape("test", 1, JSON.stringify, (id) => {
    if (!partitions.has(id)) partitions.set(id, lane++ % 32);
    return partitions.get(id);
  });
  assert.equal(tape.owners.length, 32);
  assert.equal(tape.frames.length, 204 * 32);
  const owner = tape.owners[0],
    meter = new MessageRateMeter([owner], 2315, true);
  meter.start(performance.timeOrigin + performance.now(), async () => ({ lag: 0 }));
  try {
    let call,
      pairs = 0;
    for (const frame of tape.frames.filter((f) => f.session === owner.sessionId)) {
      const fact = JSON.parse(frame.value);
      if (fact.commit?.seq >= 3 && fact.commit.seq <= 202) {
        const m = fact.commit.writes[0].value.model[0];
        if (m.role === "assistant") {
          call = m.content[0].id;
          assert.equal(Buffer.byteLength(m.content[0].arguments.content), 4096);
        } else {
          assert.equal(m.toolCallId, call);
          assert.equal(Buffer.byteLength(m.content[0].text), 4096);
          pairs++;
        }
      }
      meter.apply(fact);
    }
    assert.equal(pairs, 100);
    assert.equal(meter.latency.length, 200);
    assert((await meter.done.promise).rps > 0);
  } finally {
    await meter.stop();
  }
});
