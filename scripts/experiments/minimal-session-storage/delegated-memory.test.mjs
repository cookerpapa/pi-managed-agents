import assert from "node:assert/strict";
import { it } from "vitest";
import { MemoryStorage } from "@earendil-works/pi-durable";
import { BACKGROUND_CONTEXT as ctx } from "@earendil-works/chord/context";
import { DelegatedMemoryStorage } from "./delegated-memory.mjs";

it("keeps a mixed native batch in order across two loaded histories, including as-of config and paged descendants", async () => {
  const live = new DelegatedMemoryStorage(),
    full = new MemoryStorage();
  const conv = (id, parent) => ({
    type: "conversation",
    value: { id, ...(parent ? { parent } : {}) },
  });
  const entry = (id, conversationId, head) => ({
    type: "entry",
    value: { id, conversationId, kind: "example", ...(head ? { head } : {}) },
  });
  const config = (id, lane) => ({
    kind: "pi.agent",
    id,
    scope: { kind: "conversation", conversationId: lane },
    history: "rewindable",
    fork: "asOf",
  });
  const states = [
    conv(1),
    entry(2, 1),
    conv(3, { conversationId: 1, at: 2 }),
    {
      type: "document.create",
      record: config(4, 3),
      content: { kind: "base", version: 1, value: { model: "first" } },
    },
    entry(5, 3, 2),
    {
      type: "document.change",
      id: 4,
      content: { kind: "base", version: 1, value: { model: "later" } },
    },
    conv(7),
    entry(8, 7),
  ];
  for (const [i, w] of states.entries()) full.prepareCommit([w], i + 1).apply();
  for (const [i, w] of states.slice(0, 2).entries()) live.prepareCommit([w], i + 1).apply();
  live.prepareCommit([], 8).apply();
  live.nextId = 9;
  let attempts = 0;
  live.loadArchive = async ({ lane }) => {
    if (++attempts === 1) throw Error("temporary read failure");
    const archive = new MemoryStorage();
    for (const [i, w] of states.entries()) {
      if (lane === 3 ? i >= 6 : i < 6) continue;
      archive.prepareCommit([w], i + 1).apply();
    }
    return { store: archive, entries: lane === 3 ? 2 : 1, docs: lane === 3 ? [4] : [] };
  };
  const a = { lane: 3, at: 5 },
    b = { lane: 7, at: 8 };
  await assert.rejects(live.ensureSource(a), /temporary/);
  assert.equal(live.archiveReads.length, 0);
  await Promise.all([live.ensureSource(a), live.ensureSource(b), live.ensureSource(a)]);
  assert.equal(attempts, 3);
  const batch = [
    conv(10, { conversationId: 3, at: 5 }),
    conv(11, { conversationId: 7, at: 8 }),
    { type: "document.copy", record: config(12, 10), source: { id: 4, at: 5 } },
    entry(13, 10),
    entry(14, 1),
    entry(15, 11),
    conv(16, { conversationId: 10, at: 13 }),
    entry(17, 16, 13),
  ];
  const expected = full.prepareCommit(batch, 12),
    actual = live.prepareCommit(batch, 12);
  assert.deepEqual(actual.writes, expected.writes);
  assert.equal(await live.entry(13, ctx), undefined);
  expected.apply();
  actual.apply();
  for (const lane of [1, 3, 7, 10, 11, 16]) {
    let cursor;
    do {
      const page = await live.scanEntries({ conversationId: lane }, 1, cursor, ctx);
      assert.deepEqual(page, await full.scanEntries({ conversationId: lane }, 1, cursor, ctx));
      cursor = page.next;
    } while (cursor);
    assert.deepEqual(
      await live.findLatestHeadMarker(lane, undefined, ctx),
      await full.findLatestHeadMarker(lane, undefined, ctx),
    );
    for (const id of [2, 5, 8, 13, 14, 15, 17])
      assert.deepEqual(await live.entry(lane, id, ctx), await full.entry(lane, id, ctx));
  }
  assert.equal((await live.document(12, "current", ctx)).value.model, "first");
  assert.equal((await live.document(4, "current", ctx)).value.model, "later");
  assert.deepEqual(await live.entry(15, ctx), await full.entry(15, ctx));
  await live.close(ctx);
  await full.close(ctx);
});
