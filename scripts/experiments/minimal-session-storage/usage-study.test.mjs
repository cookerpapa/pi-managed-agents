import assert from "node:assert/strict";
import test from "node:test";
import { randomUUID } from "node:crypto";
import { captureNativeUsage } from "./native-usage-fixture.mjs";
import { coCommittedUsage } from "./usage-study.mjs";

test("two model replies co-commit two cumulative values even with two tools and streamed text", async (t) => {
  const { commits, finalUsage } = await captureNativeUsage(randomUUID());
  const view = {},
    seen = [],
    updates = [];
  let liveId,
    pureLive = 0;
  for (const commit of commits) {
    const w = commit.writes,
      entries = w.filter((x) => x.type === "entry");
    const live = w.find((x) => x.type === "document.create" && x.record.kind === "pi.live");
    if (live) liveId = live.record.id;
    const pure = w.length > 0 && w.every((x) => x.type === "document.change" && x.id === liveId);
    if (pure) pureLive++;
    const opening = w.some((x) => x.type === "document.create" && x.record.kind === "pi.inbox");
    const frame = {
      ops: opening ? [{ kind: "inbox" }] : entries.length ? [{ kind: "message" }] : [],
    };
    coCommittedUsage(view, { scope: { tenantId: "t", piSessionId: "s" }, commit }, frame, true);
    if (!frame.usage) continue;
    assert(!pure);
    assert.equal(frame.usage.length, 1);
    seen.push(commit.seq);
    if (opening) assert.deepEqual(frame.usage[0].value, { models: {}, tools: {} });
    else {
      const replies = entries
        .flatMap((e) => e.value.model ?? [])
        .filter((m) => m.role === "assistant");
      assert.equal(replies.length, 1);
      updates.push(frame.usage[0].value);
    }
  }
  assert(pureLive > 0);
  assert.equal(seen.length, 3);
  assert.equal(updates.length, 2);
  assert.deepEqual(updates.at(-1), finalUsage);
  t.diagnostic(
    JSON.stringify({
      nativeCommits: commits.length,
      usageCreate: seen[0],
      usageChanges: seen.slice(1),
      pureLive,
    }),
  );
});
