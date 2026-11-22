// Pi resolves document.copy into a native create before publishing the batch.
// Its pinned AgentDoc normally checkpoints each edit as a base value.
import assert from "node:assert/strict";
import { applyImmutableBatches } from "@earendil-works/chord/delta";

export function agentLane(routes, w) {
  if (w.type === "document.create" || w.type === "document.copy") {
    if (w.record.kind !== "pi.agent") return;
    assert(
      w.type === "document.create" &&
        w.record.history === "rewindable" &&
        w.record.fork === "asOf" &&
        w.record.scope.kind === "conversation",
      "Invalid native agent document",
    );
    return w.record.scope.conversationId;
  }
  if (w.type === "document.change" || w.type === "document.retire") return routes?.[w.id];
}

export async function agentVersion(db, s, scope, seq, w, lane, ordinal) {
  const id = w.record?.id ?? w.id;
  let content = null;
  if (w.type === "document.create") {
    assert(
      w.content.kind === "base" && !Object.values(s.agentRoutes ?? {}).includes(lane),
      "Duplicate agent configuration",
    );
    s.agentRoutes = { ...s.agentRoutes, [id]: lane };
  }
  if (w.type === "document.retire") {
    s.agentRoutes = { ...s.agentRoutes };
    delete s.agentRoutes[id];
  } else if (w.content.kind === "base") {
    content = w.content;
  } else {
    let previous = s.agentValues?.[id];
    if (!previous) {
      const {
        rows: [v],
      } = await db.query("select document_id,content from agent_config_at($1,$2,$3,$4)", [
        scope.tenantId,
        scope.piSessionId,
        lane,
        seq - 1,
      ]);
      assert(v && Number(v.document_id) === id, "Missing agent delta base");
      previous = JSON.parse(v.content);
    }
    assert(previous?.version === w.content.version, "Invalid agent delta version");
    content = {
      kind: "base",
      version: w.content.version,
      value: applyImmutableBatches(previous.value, [w.content.ops]),
    };
  }
  s.agentValues = { ...s.agentValues };
  if (content) s.agentValues[id] = content;
  else delete s.agentValues[id];
  return {
    document: id,
    conversation: lane,
    role: "pi.agent",
    ordinal,
    content: JSON.stringify(content),
  };
}

// A cold import creates the native document directly in the independent storage.
export function agentBaseWrite(id, content, lane = 1) {
  assert(
    Number.isSafeInteger(id) && id > 0 && content?.kind === "base",
    "Invalid retained agent configuration",
  );
  return {
    type: "document.create",
    record: {
      id,
      kind: "pi.agent",
      scope: { kind: "conversation", conversationId: lane },
      history: "rewindable",
      fork: "asOf",
    },
    content,
  };
}
