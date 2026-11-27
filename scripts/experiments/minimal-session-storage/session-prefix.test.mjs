import assert from "node:assert/strict";
import test from "node:test";
import { MemoryStorage, InboxDoc, Harness, createRegistry } from "@earendil-works/pi-durable";
import { createModels } from "@earendil-works/pi-ai/models";
import { BACKGROUND_CONTEXT as ctx } from "@earendil-works/chord/context";
import {
  PrefixCache,
  scopeKey,
  priorState,
  readPrefix,
  record,
  restoreSettled,
} from "./session-prefix.mjs";

const entry = (id, text) => ({
  id,
  conversationId: 1,
  kind: "pi.user",
  model: [{ role: "user", content: [{ type: "text", text }], timestamp: 1 }],
});

test("settled Tool pairs may reuse an ID in later turns, incomplete pairs and image bytes stay cold", () => {
  const cache = new PrefixCache({ minBytes: 1 });
  const call = (id, args = { path: "/work/file" }) =>
    record(
      {
        id,
        conversationId: 1,
        kind: "pi.assistant",
        model: [
          {
            role: "assistant",
            content: [{ type: "toolCall", id: "repeat", name: "read", arguments: args }],
          },
        ],
      },
      id,
    );
  const reply = (id, content = [{ type: "text", text: "done" }]) =>
    record(
      {
        id,
        conversationId: 1,
        kind: "pi.tool-result",
        model: [{ role: "toolResult", toolCallId: "repeat", toolName: "read", content }],
        data: { diagnostics: [] },
      },
      id,
    );
  const offer = (rows) =>
    cache.put(
      "T/S/1",
      "v",
      rows,
      rows.reduce((n, r) => n + r.bytes, 0),
    );
  assert(offer([call(2), reply(3), call(4), reply(5)]));
  assert(!offer([call(2)]));
  assert(!offer([reply(3)]));
  assert(!offer([call(2), reply(3, [{ type: "image", data: "bytes", mimeType: "image/png" }])]));
  assert(!offer([call(2, { image: { mimeType: "image/png", data: "bytes" } }), reply(3)]));
});

test("prefixes require the exact tenant, physical root and accepted version and expire by bytes or time", () => {
  let time = 0;
  const evicted = [];
  const a = scopeKey("owner", "original"),
    b = scopeKey("owner", "fork"),
    other = scopeKey("another", "original");
  const r = record(entry(4, "original"), 1),
    size = r.bytes;
  const c = new PrefixCache({
    minBytes: size,
    maxBytes: size * 2,
    ttlMs: 10,
    now: () => time,
    onEvict: (k, v) => evicted.push([k, v]),
  });
  assert(c.put(a, "v1", [r], size));
  assert.equal(c.get(a, "v0"), undefined);
  assert.equal(c.get(b, "v1"), undefined);
  assert.equal(c.get(other, "v1"), undefined);
  assert(c.put(b, "v1", [r], size));
  assert(c.get(a, "v1"));
  assert(c.put(other, "v2", [r], size));
  assert.deepEqual(evicted, [[b, "v1"]]);
  assert.equal(c.bytes, 2 * size);
  time = 11;
  c.expire();
  assert.equal(c.bytes, 0);
  assert.equal(c.items.size, 0);
  assert(!c.put(a, "small", [r], size - 1));
  const image = record(
    { ...entry(5, ""), model: [{ role: "user", content: [{ type: "image", data: "test" }] }] },
    1,
  );
  assert(!c.put(a, "image", [image], size));
});

test("a cache hit restores entry commit positions and rechecks current Inbox and prefix coverage", async () => {
  const one = record(entry(4, "one"), 2),
    two = record(entry(10, "two"), 7);
  const row = {
    tenant_id: "T",
    session_id: "S",
    mailbox_position: 2,
    inbox_id: 3,
    inbox_items: [],
    prior_head: {
      version: "accepted",
      seq: 9,
      nextId: 14,
      floor: 1,
      count: 2,
      bytes: one.bytes + two.bytes,
    },
  };
  const c = new PrefixCache({ minBytes: 1 });
  const db = {
    query: async () => ({
      rows: [one, two].map((r) => ({
        entry_id: r.entry.id,
        commit_seq: r.seq,
        content: JSON.stringify(r.entry),
      })),
    }),
  };
  c.put(scopeKey("T", "S"), "unaccepted", [one], one.bytes);
  const cold = await readPrefix(db, row, priorState(row), c);
  assert.equal(cold.status, "cold");
  c.put(scopeKey("T", "S"), "accepted", cold.records, one.bytes + two.bytes);
  const hot = await readPrefix({ query: assert.fail }, row, priorState(row), c);
  assert.equal(hot.readBytes, 0);
  const store = new MemoryStorage();
  restoreSettled(store, row, row.prior_head, hot.records);
  assert.equal((await store.entry(1, 4, ctx)).commitSeq, 2);
  assert.equal((await store.entry(1, 10, ctx)).commitSeq, 7);
  const h = await Harness.open(
    store,
    { models: createModels({ providers: [] }), registry: createRegistry() },
    ctx,
  );
  try {
    assert.deepEqual((await h.snapshot(InboxDoc, 1, ctx)).items, []);
    assert.deepEqual(
      (await (await h.conversation(1, ctx)).context(ctx)).messages.map((m) => m.content[0].text),
      ["one", "two"],
    );
  } finally {
    await h.close(ctx);
  }
  assert.throws(() => priorState({ ...row, inbox_items: null }), /Inbox/);
  assert.throws(() => priorState({ ...row, inbox_items: [{}] }), /Inbox/);
  await assert.rejects(
    readPrefix({ query: async () => ({ rows: [] }) }, row, priorState(row)),
    /Incomplete/,
  );
});

test("a segmented image snapshot requires its exact authority and does not retain waiters or cross a Flow", async () => {
  const { SegmentWarmHistory } = await import("./segment-warm-history.mjs");
  let now = 10;
  const notices = [];
  const warm = new SegmentWarmHistory({
    minBytes: 1,
    maxBytes: 2048,
    ttlMs: 1000,
    now: () => now,
    advertise: (...n) => notices.push(n),
  });
  const scope = {
    tenantId: "tenant",
    sessionId: "session",
    runId: "turn",
    writerId: "writer",
    fencingToken: 2,
  };
  const job = { id: "turn", tenant_id: "tenant", logical_id: "session", flow_id: "flow", scope };
  const terminal = {
    scope,
    factId: "seal",
    kind: "work_finished",
    nativeSequence: 5,
    terminal: { type: "turn.completed" },
  };
  try {
    const builder = warm.capture(job);
    builder.seed([]);
    builder.append({
      seq: 4,
      writes: [
        { type: "entry", value: entry(7, "picture") },
        { type: "task", value: { id: 11 } },
      ],
    });
    builder.finish({ ...terminal, scope: { ...scope, writerId: "retired" } });
    builder.finish({ ...terminal, terminal: { type: "turn.cancelled" } });
    assert.equal(builder.get(null, "seal"), undefined);
    builder.finish(terminal);
    builder.append({ seq: 6, writes: [{ type: "entry", value: entry(9, "late") }] });
    assert.deepEqual(
      builder.get(null, "seal").records.map((r) => r.entry.id),
      [7],
    );
    for (const other of [{ tenant_id: "other" }, { logical_id: "other" }, { flow_id: "new" }])
      assert.equal(warm.capture({ ...job, ...other }).get(null, "seal"), undefined);
    assert.equal(builder.get(null, "old"), undefined);
    now += 1001;
    warm.cache.expire();
    assert.equal(builder.get(null, "seal"), undefined);
    assert.deepEqual(notices.at(-1), ["tenant", "flow", "seal", "drop"]);
    const huge = warm.capture(job);
    huge.append({ seq: 4, writes: [{ type: "entry", value: entry(7, "x".repeat(4096)) }] });
    huge.finish(terminal);
    assert.equal(warm.cache.bytes, 0);
  } finally {
    warm.close();
  }
});
