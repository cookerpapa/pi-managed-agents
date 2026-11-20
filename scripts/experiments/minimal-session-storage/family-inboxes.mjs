import assert from "node:assert/strict";
import { applyImmutableBatches } from "@earendil-works/chord/delta";

// A draft shares the prior map until a write touches an Inbox. Never mutate the
// committed root or a sibling's queue before the enclosing prefix is accepted.
export function foldInboxes(prior = {}, writes) {
  let documents = prior;
  const changed = new Map();
  for (const [ordinal, w] of writes.entries()) {
    const created = w.type === "document.create" && w.record.kind === "pi.inbox";
    if (w.type === "document.retire" && Object.hasOwn(documents, w.id))
      throw new Error("Conversation Inbox retired during active execution");
    if (!created && (w.type !== "document.change" || !Object.hasOwn(documents, w.id))) continue;
    const id = created ? w.record.id : w.id;
    if (created) {
      assert(w.record.scope.kind === "conversation" && w.content.kind === "base");
      assert(
        !Object.hasOwn(documents, id) &&
          !Object.values(documents).some((d) => d.conversation === w.record.scope.conversationId),
        "Duplicate Conversation Inbox",
      );
    }
    const conversation = created ? w.record.scope.conversationId : documents[id].conversation;
    const value =
      w.content.kind === "base"
        ? w.content.value
        : applyImmutableBatches(documents[id].value, [w.content.ops]);
    assert(Array.isArray(value.items), "Invalid native Inbox");
    if (documents === prior) documents = { ...prior };
    documents[id] = { conversation, value };
    changed.delete(id); // Reinsert in the order of each document's final write.
    changed.set(id, { id, conversation, value, ordinal });
  }
  return { documents, changed: [...changed.values()] };
}

export function emptyInboxRoutes(documents) {
  const routes = {};
  for (const [id, doc] of Object.entries(documents ?? {})) {
    if (doc.value.items.length) return;
    routes[id] = doc.conversation;
  }
  if (Object.values(routes).includes(1)) return routes;
}

export function reopenEmptyInboxes(routes) {
  return Object.fromEntries(
    Object.entries(routes).map(([id, conversation]) => [
      id,
      { conversation, value: { items: [] } },
    ]),
  );
}
