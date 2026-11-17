import { describe, expect, it } from "vitest";
import { collectInputConfirmations } from "../src/durable/input-confirmations.ts";
import type { CurrentStateRow } from "../src/durable/current-state.ts";

const scope = { tenantId: "tenant", sessionId: "physical" };
const fingerprint = "a".repeat(64);
function row(
  nativeId: number | null,
  receipts: Record<string, { fingerprint: string }> = {},
): CurrentStateRow {
  return {
    id: "10",
    kind: "document",
    seq: "4",
    ordinal: 0,
    delta_count: 2,
    created_seq: "2",
    retired_seq: null,
    cloud_origin: { cloudSession: "child", run: "root-run", turn: "child-turn" },
    body: JSON.stringify({
      type: "document.create",
      record: {
        kind: "pi-cloud.human-inputs",
        key: "root-run",
        scope: { kind: "conversation", conversationId: 2 },
      },
      content: {
        kind: "base",
        value: { runId: "root-run", inputs: { input: { nativeId, fingerprint } }, receipts },
      },
    }),
  };
}

describe("native input projection confirmations", () => {
  it("does not confirm metadata preparation before native submission", () => {
    expect(collectInputConfirmations(scope, [row(null)], [])).toEqual([]);
  });
  it("confirms the submitted identity in its logical child rather than the root", () => {
    expect(collectInputConfirmations(scope, [row(11)], [row(null)])).toEqual([
      {
        tenantId: "tenant",
        conversationId: 2,
        runId: "root-run",
        action: "submit",
        operationId: "input",
        fingerprint,
      },
    ]);
  });
  it("confirms withdrawal or editing from the exact new receipt, not queue membership", () => {
    expect(
      collectInputConfirmations(scope, [row(11, { edit: { fingerprint } })], [row(11)]),
    ).toEqual([
      {
        tenantId: "tenant",
        conversationId: 2,
        runId: "root-run",
        action: "mutate",
        operationId: "edit",
        fingerprint,
      },
    ]);
    expect(
      collectInputConfirmations(
        scope,
        [row(11, { edit: { fingerprint } })],
        [row(11, { edit: { fingerprint } })],
      ),
    ).toEqual([]);
  });
  it("retirement does not invent an operation confirmation", () => {
    expect(collectInputConfirmations(scope, [{ ...row(11), retired_seq: "5" }], [row(11)])).toEqual(
      [],
    );
  });
});
