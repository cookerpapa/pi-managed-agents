import { withoutUsageUpdates } from "./native-display-tape.mjs";
import { visibleOutput } from "./visible-output.mjs";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import { MemoryStorage } from "@earendil-works/pi-durable";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { nativeBashTrace } from "./native-bash-trace.mjs";

for (const [callsPerRun, expectedSummaries] of [
  [20, 0],
  [200, 1],
])
  test(`the ${callsPerRun}-call trace retains paired calls, expected compactions and one final answer`, async () => {
    const calls = [],
      results = [];
    let finals = 0,
      summaries = 0,
      commits = 0;
    const retained = await nativeBashTrace(
      { runId: randomUUID() },
      (commit) => {
        assert.equal(commit.seq, ++commits);
        for (const w of commit.writes) {
          if (w.type !== "entry") continue;
          if (w.value.kind === "pi.compaction") summaries++;
          const m = w.value.model?.[0];
          if (m?.role === "assistant") {
            const call = m.content.find((b) => b.type === "toolCall");
            if (call) calls.push(call.id);
            else finals++;
          }
          if (m?.role === "toolResult") {
            assert.equal(Buffer.byteLength(m.content[0].text), 1024);
            results.push(m.toolCallId);
          }
        }
      },
      { callsPerRun, callIdAt: (i) => "call-" + i, onlyLatest: true, waitForCommit: true },
    );
    assert.deepEqual(
      calls,
      Array.from({ length: callsPerRun }, (_, i) => "call-" + i),
    );
    assert.deepEqual(results, calls);
    assert.equal(summaries, expectedSummaries);
    if (callsPerRun === 20) assert.equal(commits + 1, 187);
    assert.equal(finals, 1);
    assert.deepEqual(
      retained.map((c) => c.seq),
      [commits],
    );
  });

test("counts one visible record with text and two calls, ignores control data and counts each result", () => {
  const assistant = {
    type: "entry",
    value: {
      kind: "pi.assistant",
      model: [
        {
          role: "assistant",
          content: [
            { type: "thinking", thinking: "private" },
            { type: "text", text: "Checking." },
            { type: "toolCall", id: "a" },
            { type: "toolCall", id: "b" },
          ],
        },
      ],
    },
  };
  assert.deepEqual(visibleOutput([{ type: "task" }, assistant]), {
    texts: 1,
    calls: 2,
    results: 0,
  });
  assert.deepEqual(visibleOutput([{ type: "document.change", id: 1 }]), {
    texts: 0,
    calls: 0,
    results: 0,
  });
  assert.deepEqual(
    visibleOutput([
      {
        type: "entry",
        value: { kind: "pi.tool-result", model: [{ role: "toolResult", content: [] }] },
      },
    ]),
    { texts: 0, calls: 0, results: 1 },
  );
});

test("omitting co-committed usage updates preserves visible output but loses Pi's native totals on replay", async () => {
  const full = new MemoryStorage(),
    ablation = new MemoryStorage();
  let doc,
    updates = 0,
    visible = 0;
  await nativeBashTrace(
    { runId: randomUUID() },
    async (c) => {
      const original = visibleOutput(c.writes);
      if (original.calls || original.results || original.texts) visible++;
      for (const w of c.writes)
        if (w.type === "document.create" && w.record.kind === "pi.usage") doc = w.record.id;
      const remaining = c.writes.filter((w) => !(w.type === "document.change" && w.id === doc));
      updates += c.writes.length - remaining.length;
      assert(remaining.length > 0);
      assert.deepEqual(visibleOutput(remaining), original);
      await full.commit(c.writes, BACKGROUND_CONTEXT);
      await ablation.commit(remaining, BACKGROUND_CONTEXT);
    },
    { callsPerRun: 20, callIdAt: (i) => "c-" + i, onlyLatest: true, waitForCommit: true },
  );
  assert.equal(visible, 41);
  assert.equal(updates, 21);
  const intact = (await full.document(doc, "current", BACKGROUND_CONTEXT)).value;
  const stripped = (await ablation.document(doc, "current", BACKGROUND_CONTEXT)).value;
  assert(Object.keys(intact.models).length);
  assert.deepEqual(stripped, { models: {}, tools: {} });
});

test("experimental removal keeps UI references aligned with the original content", () => {
  const writes = [
    { type: "task" },
    { type: "document.change", id: 9 },
    { type: "entry", value: { kind: "pi.assistant" } },
  ];
  const frame = {
    usageOrdinals: [1],
    value: JSON.stringify({
      commit: { writes },
      display: [{ events: [{ source: { kind: "write", ordinal: 2 } }] }],
    }),
  };
  const replay = JSON.parse(withoutUsageUpdates(frame).value);
  assert.equal(replay.display[0].events[0].source.ordinal, 1);
  assert.deepEqual(replay.commit.writes[1], writes[2]);
  assert.equal(JSON.parse(frame.value).commit.writes.length, 3);
});
