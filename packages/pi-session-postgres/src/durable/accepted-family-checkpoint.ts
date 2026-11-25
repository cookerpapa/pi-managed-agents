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
  static resume(writer: string, state: SealedNativeState, inboxIds: readonly number[]) {
    const next = new AcceptedFamilyCheckpoint();
    if (
      !Number.isSafeInteger(state.seq) ||
      state.seq < 1 ||
      !Number.isSafeInteger(state.nextId) ||
      state.nextId < 2
    )
      throw Error("Invalid predecessor native head");
    next.#writer = writer;
    next.#seq = state.seq;
    next.#nextId = state.nextId;
    next.#heads = state.heads.slice();
    next.#sequences = new Map(state.sequences);
    const put = (id: number, value: FoldedRecord) => {
      if (!Number.isSafeInteger(id) || id < 1 || id >= state.nextId || next.#records.has(id))
        throw Error("Invalid predecessor native identity");
      next.#records.set(id, value);
    };
    for (const row of state.records) {
      if (!Number.isSafeInteger(row.seq) || row.seq < 1 || row.seq > state.seq)
        throw Error("Invalid predecessor record position");
      const w = row.write;
      switch (w.type) {
        case "conversation":
        case "task":
        case "submission":
          put(w.value.id, row);
          break;
        case "document.create":
          put(w.record.id, row);
          break;
        default:
          throw Error("Invalid predecessor record type");
      }
    }
    for (const id of inboxIds) put(id, "inbox");
    return next;
  }

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
    const changes = new Map<number, FoldedRecord | null>();
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
          const row = changes.has(w.id) ? changes.get(w.id) : this.#records.get(w.id);
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
        case "document.retire": {
          // Pi retires the task-scoped return route when the recovered Tool settles.
          const row = changes.has(w.id) ? changes.get(w.id) : this.#records.get(w.id);
          if (
            !row ||
            row === "inbox" ||
            row.write.type !== "document.create" ||
            row.write.record.scope.kind !== "task"
          )
            throw Error("Retired native document has no matching task scope");
          changes.set(w.id, null);
          break;
        }
        default:
          throw Error("Unsupported native document lifecycle in cold handoff: " + w.type);
      }
    }
    this.#heads.push(...heads);
    for (const [id, value] of changes) {
      if (value === null) this.#records.delete(id);
      else this.#records.set(id, value);
    }
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
