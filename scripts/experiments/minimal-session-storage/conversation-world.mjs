import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { applyImmutableBatches } from "@earendil-works/chord/delta";

export const worldKind = "pi-cloud.runtime_world_state";

const tokenPrefix = "world-equality:";
export const worldToken = (id) => tokenPrefix + createHash("sha256").update(id).digest("hex");

export function worldBaseWrite(id, state, conversation = 1) {
  return {
    type: "document.create",
    record: {
      id,
      kind: worldKind,
      scope: { kind: "conversation", conversationId: conversation },
      history: "rewindable",
      fork: "current",
    },
    content: { kind: "base", version: 1, value: { state } },
  };
}

// Clone only on a world write. A rejected PG prefix must leave the active view intact.
export function foldWorld(view, writes, observed) {
  let docs = view.worldDocs;
  const put = (id, doc) => {
    if (docs === view.worldDocs) docs = { ...docs };
    if (doc) docs[id] = doc;
    else delete docs[id];
  };
  for (const w of writes) {
    const create = w.type === "document.create" && w.record.kind === worldKind;
    const id = create ? w.record.id : w.id;
    if (!create && !docs?.[id]) continue;
    const prior = docs?.[id];
    if (w.type === "document.retire") {
      // Retiring a native document does not undo a confirmed physical use.
      put(id, null);
      continue;
    }
    if (!create && w.type !== "document.change") continue;
    if (create)
      assert(
        w.record.scope.kind === "conversation" &&
          w.record.fork === "current" &&
          w.content.kind === "base",
      );
    const lane = create ? w.record.scope.conversationId : prior.lane;
    const value =
      w.content.kind === "base"
        ? w.content.value
        : applyImmutableBatches(prior.value, [w.content.ops]);
    const state = value.state;
    put(id, { lane, value });
    if (state?.sandbox?.status !== "active" || !state.sandbox.continuityId) continue;
    assert(
      typeof state.sandbox.continuityId === "string" && state.sandbox.continuityId.length <= 256,
    );
    // A token rehydrated for native comparison is not new execution evidence.
    if (!state.sandbox.continuityId.startsWith(tokenPrefix) && prior?.value.state !== state)
      observed?.(lane, state);
  }
  if (docs !== view.worldDocs) view.worldDocs = Object.keys(docs).length ? docs : undefined;
}

export function projectWorld(view, fact, frame) {
  let changed;
  foldWorld(view, fact.commit.writes, (lane, state) => (changed ??= new Map()).set(lane, state));
  if (!changed) return;
  for (const [lane, state] of changed)
    frame.ops.push({
      kind: "world",
      count: 1,
      args: [
        fact.scope.tenantId,
        fact.scope.piSessionId,
        fact.scope.runId,
        fact.scope.writerId,
        lane,
      ],
      state,
    });
}

export function worldRoutes(view) {
  if (!view.worldDocs || !Object.keys(view.worldDocs).length) return {};
  return {
    worldRoutes: Object.fromEntries(Object.entries(view.worldDocs).map(([id, d]) => [id, d.lane])),
  };
}

// A fresh normal Harness binds a neutral native document. Admission supplies
// continuity, and replay starts from the identical Worker-side document.
export function reopenWorld(routes) {
  return Object.fromEntries(
    Object.entries(routes).map(([id, lane]) => [id, { lane, value: { state: null } }]),
  );
}
