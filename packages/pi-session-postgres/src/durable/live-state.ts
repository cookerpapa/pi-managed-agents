import { createHash } from "node:crypto";
import { validateImageReferencePayload } from "@pi-cloud/protocol";
import { retainedAttachmentReferences } from "@pi-cloud/session-attachments";
import {
  changeCurrentDocument,
  currentStateFingerprint,
  isPendingLiveRow,
  type CurrentStateRow,
  type CurrentStateSource,
} from "./current-state.ts";
import type { DurableBatch, StoredDurableBatch } from "./types.ts";
import type { DurableRecordOrigin } from "@pi-cloud/protocol";

export type NativeReplayPosition = Readonly<{ topic: string; partition: number; offset: bigint }>;

export function nativeCommitFingerprint(
  batch: StoredDurableBatch | DurableBatch,
  origins: readonly DurableRecordOrigin[],
  cloudMetadata?: Readonly<Record<string, unknown>>,
): string {
  return createHash("sha256")
    .update(JSON.stringify({ batch, origins, cloudMetadata }))
    .digest("hex");
}

type LiveReceipt = Readonly<{
  seq: number;
  nextId: number;
  fingerprint: string;
  writeCount: number;
  hashes: Record<string, string>;
}>;

/** Disposable partition-owner state. A transaction draft shares an immutable
 * receipt prefix until it replaces it. Only the serial owner appends new proofs. */
export class NativeLiveState {
  through: number | undefined;
  projectedThrough: number | undefined;
  nextId: number | undefined;
  readonly rows = new Map<number, CurrentStateRow>();
  receipts = new Map<number, LiveReceipt>();
  #receiptBytes = 0;

  statistics() {
    return {
      documents: this.rows.size,
      deferredReceipts: this.receipts.size,
      estimatedBytes: [...this.rows.values()].reduce(
        (n, row) => n + 256 + row.body.length * 2,
        this.#receiptBytes,
      ),
    };
  }

  fork(): NativeLiveState {
    const next = new NativeLiveState();
    next.adopt(this);
    return next;
  }
  adopt(next: NativeLiveState): void {
    this.through = next.through;
    this.projectedThrough = next.projectedThrough;
    this.nextId = next.nextId;
    this.receipts = next.receipts;
    this.#receiptBytes = next.#receiptBytes;
    this.rows.clear();
    for (const [id, row] of next.rows) this.rows.set(id, row);
  }
  receiptsMaterialized(through: number): void {
    this.projectedThrough = through;
    this.receipts = new Map();
    this.#receiptBytes = 0;
  }
  initialize(head: number, checkpoints: readonly CurrentStateSource[]): void {
    if (this.through !== undefined) return;
    this.through = head;
    for (const row of checkpoints) {
      if (row.replay_seq === null) continue;
      this.through = Math.min(this.through, Number(row.replay_seq) - 1);
      this.rows.set(Number(row.id), row);
    }
  }

  /** Only documents whose first replay pin already committed may take this path. */
  prepareProgress(stored: StoredDurableBatch) {
    const batch = stored as DurableBatch;
    if (
      this.through === undefined ||
      this.projectedThrough === undefined ||
      this.through < this.projectedThrough ||
      this.nextId === undefined ||
      !batch.writes.length ||
      !batch.writes.every((w) => w.type === "document.change" && this.rows.has(w.id))
    )
      return;
    validateImageReferencePayload(stored);
    if (retainedAttachmentReferences(stored).length) return;
    const fingerprint = nativeCommitFingerprint(stored, []);
    const prior = this.receipts.get(batch.seq);
    if (prior) {
      if (prior.fingerprint !== fingerprint)
        throw new Error("Native commit position changed bytes");
      return { duplicate: true as const };
    }
    if (batch.seq <= this.through) return;
    if (batch.seq !== this.through + 1 || batch.nextId < this.nextId)
      throw new Error("Native commit sequence or ID watermark is not contiguous");
    const changed = new Map<number, CurrentStateRow>();
    for (const [ordinal, write] of batch.writes.entries()) {
      if (write.type !== "document.change") return;
      const previous = changed.get(write.id) ?? this.rows.get(write.id)!;
      const row = changeCurrentDocument(previous, write, batch.seq, ordinal);
      if (!isPendingLiveRow(row)) return;
      changed.set(write.id, row);
    }
    const receipt: LiveReceipt = {
      seq: batch.seq,
      nextId: batch.nextId,
      fingerprint,
      writeCount: batch.writes.length,
      hashes: Object.fromEntries(
        [...changed].map(([id, row]) => [id, currentStateFingerprint(row)]),
      ),
    };
    return {
      duplicate: false as const,
      changed,
      apply: () => {
        for (const [id, row] of changed) this.rows.set(id, row);
        this.receipts.set(batch.seq, receipt);
        this.#receiptBytes += 256 + 160 * changed.size;
        this.through = batch.seq;
        this.nextId = batch.nextId;
      },
    };
  }
}

export type NativeLiveProjection = Readonly<{
  state: NativeLiveState;
  position: NativeReplayPosition;
}>;
