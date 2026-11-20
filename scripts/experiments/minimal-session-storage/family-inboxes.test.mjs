import test from "node:test";
import assert from "node:assert/strict";
import { foldInboxes, emptyInboxRoutes, reopenEmptyInboxes } from "./family-inboxes.mjs";
const create = (id, lane) => ({
  type: "document.create",
  record: { id, scope: { kind: "conversation", conversationId: lane }, kind: "pi.inbox" },
  content: { kind: "base", value: { items: [] } },
});
const change = (id, items) => ({
  type: "document.change",
  id,
  content: { kind: "base", value: { items } },
});
test("two inboxes in one native commit retain final ordinals without changing the accepted view", () => {
  const prior = foldInboxes({}, [create(3, 1), create(31, 27)]).documents;
  const draft = foldInboxes(prior, [change(31, [1]), change(3, [2]), change(31, [1, 3])]);
  assert.deepEqual(
    draft.changed.map(({ conversation, ordinal, value }) => [conversation, ordinal, value.items]),
    [
      [1, 1, [2]],
      [27, 2, [1, 3]],
    ],
  );
  assert.deepEqual(prior, reopenEmptyInboxes({ 3: 1, 31: 27 }));
  assert.equal(emptyInboxRoutes(draft.documents), undefined);
  assert.deepEqual(
    emptyInboxRoutes(foldInboxes(draft.documents, [change(3, []), change(31, [])]).documents),
    { 3: 1, 31: 27 },
  );
  assert.throws(() => foldInboxes(prior, [create(40, 27)]), /Duplicate/);
});
