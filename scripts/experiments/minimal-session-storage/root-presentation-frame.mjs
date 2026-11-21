import assert from "node:assert/strict";
import { applyImmutableBatches } from "@earendil-works/chord/delta";
import { NATIVE_HUMAN_INPUT_DOCUMENT } from "@pi-cloud/protocol";

// Optional root reader metadata. Display-only text never becomes a message row.
export function foldRootPresentation(view, f, frame) {
  const receipts = {},
    requests = {},
    tasks = {},
    submissions = {};
  let doc, latest;
  for (const w of f.commit.writes) {
    if (w.type === "document.create" && w.record.kind === "pi-cloud.compact-request") {
      const receipt = w.content.value;
      assert(receipt.runId === f.scope.runId && receipt.requestId === w.record.key);
      latest = receipt.requestId;
      requests[latest] = receipt;
    }
    if (
      w.type === "task" &&
      w.value.kind === "pi.compaction" &&
      w.value.input.reason === "manual"
    ) {
      const { status, outcome } = w.value.state;
      const result = outcome?.result;
      tasks[w.value.id] = {
        kind: "pi.compaction",
        state: outcome
          ? {
              status,
              outcome: {
                status: outcome.status,
                ...(result
                  ? { result: { entryId: result.entryId, submissionId: result.submissionId } }
                  : {}),
              },
            }
          : { status },
      };
    }
    if (w.type === "submission" && w.value.requestId?.startsWith("compaction:")) {
      const { status, entry, reason } = w.value;
      submissions[w.value.id] = { status, entry, reason };
    }
    if (w.type === "submission" && w.value.requestId?.startsWith("input-")) {
      const { id, status, entry, reason } = w.value;
      receipts[w.value.requestId] = { id, status, entry, reason };
    }
    if (
      w.type === "document.create" &&
      w.record.kind === NATIVE_HUMAN_INPUT_DOCUMENT &&
      w.record.key === f.scope.runId &&
      w.record.scope.conversationId === 1
    ) {
      view.rootInputDoc = { id: w.record.id, value: w.content.value };
      doc = view.rootInputDoc.value;
    }
    if (w.type === "document.change" && view.rootInputDoc?.id === w.id) {
      doc =
        w.content.kind === "base"
          ? w.content.value
          : applyImmutableBatches(view.rootInputDoc.value, [w.content.ops]);
      view.rootInputDoc = { id: w.id, value: doc };
    }
  }
  if (latest || Object.keys(tasks).length || Object.keys(submissions).length) {
    frame.nativeSeq = f.commit.seq;
    frame.ops.push({
      kind: "root-compact",
      args: [f.scope.tenantId, f.scope.piSessionId, f.scope.runId, f.scope.writerId],
      requests,
      tasks,
      submissions,
      latest,
      count: 1,
    });
    frame.strict = true;
  }
  if (doc)
    assert(
      doc.runId === f.scope.runId && doc.inputs && doc.receipts,
      "Root input metadata changed Run",
    );
  if (doc || Object.keys(receipts).length) {
    frame.nativeSeq = f.commit.seq;
    frame.ops.push({
      kind: "root-input",
      args: [f.scope.tenantId, f.scope.piSessionId, f.scope.runId, f.scope.writerId],
      receipts,
      document: doc ? JSON.stringify(doc) : undefined,
      count: 1,
    });
    frame.strict = true;
  }
}
