import { applyImmutableBatches } from "@earendil-works/chord/delta";
import type { StorageWrite } from "@earendil-works/pi-durable";
import type { DurableSessionCommit } from "@pi-cloud/protocol";

// Only non-Entry current values belong in the seal. The candidate PG history and
// Inbox already contain those bodies. State is scoped to one admitted incarnation.
type CurrentRecord = { seq: number; write: StorageWrite };
type FoldedRecord = CurrentRecord | "inbox";
export type SealedNativeState = {
  seq: number;
  nextId: number;
  records: CurrentRecord[];
  heads: { conversationId: number; id: number; floor: number }[];
  sequences: [string, number][];
};
export class AcceptedFamilyCheckpoint {
  #records = new Map<number, FoldedRecord>();
  #sequences = new Map<string, number>();
  #heads: SealedNativeState["heads"] = [];
  #writer?: string;
  #seq = 0;
  #lastFact?: string;
  #nextId = 2;
  get seq() {
    return this.#seq;
  }
  observe(f: {
    factId: string;
    scope: { writerId: string };
    commit: DurableSessionCommit;
    display: ReadonlyArray<{
      scope: { sessionId: string };
      coveredThrough?: number;
      events: ReadonlyArray<{ event: { seq: number } }>;
    }>;
  }) {
    if (
      this.#writer === f.scope.writerId &&
      this.#seq === f.commit.seq &&
      this.#lastFact === f.factId
    )
      return;
    if (
      (this.#writer !== undefined && this.#writer !== f.scope.writerId) ||
      f.commit.seq !== this.#seq + 1 ||
      f.commit.nextId < this.#nextId
    )
      throw Error("Incomplete admitted family prefix");
    const changes = new Map<number, FoldedRecord>();
    const heads: SealedNativeState["heads"] = [];
    for (const w of f.commit.writes as unknown as readonly StorageWrite[]) {
      switch (w.type) {
        case "entry":
          if (w.value.head !== undefined)
            heads.push({
              conversationId: w.value.conversationId,
              id: w.value.id,
              floor: w.value.head,
            });
          break;
        case "conversation":
        case "task":
        case "submission":
          changes.set(w.value.id, { seq: f.commit.seq, write: w });
          break;
        case "document.create":
          changes.set(
            w.record.id,
            w.record.kind === "pi.inbox" ? "inbox" : { seq: f.commit.seq, write: w },
          );
          break;
        case "document.change": {
          const row = changes.get(w.id) ?? this.#records.get(w.id);
          if (row === "inbox") break;
          if (row?.write.type !== "document.create")
            throw Error("Missing original native document");
          const content =
            w.content.kind === "base"
              ? w.content
              : {
                  kind: "base" as const,
                  version: w.content.version,
                  value: applyImmutableBatches(row.write.content.value, [w.content.ops]),
                };
          changes.set(w.id, { seq: row.seq, write: { ...row.write, content } });
          break;
        }
        default:
          throw Error("Unsupported native document lifecycle in cold handoff: " + w.type);
      }
    }
    this.#heads.push(...heads);
    for (const [id, value] of changes) this.#records.set(id, value);
    for (const d of f.display)
      this.#sequences.set(
        d.scope.sessionId,
        Math.max(
          this.#sequences.get(d.scope.sessionId) ?? 0,
          d.coveredThrough ?? 0,
          ...d.events.map((e) => e.event.seq),
        ),
      );
    this.#lastFact = f.factId;
    this.#writer = f.scope.writerId;
    this.#seq = f.commit.seq;
    this.#nextId = f.commit.nextId;
  }
  snapshot(): SealedNativeState {
    return {
      seq: this.#seq,
      nextId: this.#nextId,
      heads: this.#heads.slice(),
      records: [...this.#records.values()].filter((v) => v !== "inbox"),
      sequences: [...this.#sequences],
    };
  }
}
