import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
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
