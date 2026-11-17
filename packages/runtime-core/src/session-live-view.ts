import type { PiCloudEvent } from "@pi-cloud/protocol";
import { SessionEventHub } from "./session-event-hub.ts";
import { CompactEventTail } from "./compact-event-tail.ts";

export type LiveSessionTailSnapshot = Readonly<{
  canonicalThroughSequence: number;
  highWaterMark: number;
  /** Internal presentation spans, not original Kafka records. */
  events: readonly PiCloudEvent[];
}>;
type Tail = { content: CompactEventTail; touched: number; metadataBytes: number };
const key = (tenant: string, session: string) => `${tenant}\0${session}`;

/** The single consumer feeds live delivery and a compact, disposable view. */
export class SessionLiveView {
  owner?: (tenant: string, session: string) => Promise<string | undefined>;
  readonly eventHub = new SessionEventHub();
  readonly #tails = new Map<string, Tail>();
  readonly #empty = new Map<string, Tail>();
  #emptyBytes = 0;
  #emptyEvictions = 0;
  #accepted = 0;
  #duplicates = 0;
  #evicted = 0;
  readonly #sweep: NodeJS.Timeout;
  constructor(
    readonly waitForSession: (tenant: string, session: string) => Promise<void>,
    readonly emptyLimits = { maximumEntries: 4096, maximumBytes: 4 * 1024 * 1024 },
  ) {
    if (
      ![emptyLimits.maximumEntries, emptyLimits.maximumBytes].every(
        (v) => Number.isSafeInteger(v) && v > 0,
      )
    )
      throw new Error("Empty live-holder budgets must be positive integers");
    this.#sweep = setInterval(() => {
      for (const [id, t] of this.#empty) if (t.touched < Date.now() - 300000) this.#dropEmpty(id);
    }, 60000);
    this.#sweep.unref();
  }
  reset(): void {
    this.#tails.clear();
    this.#empty.clear();
    this.#emptyBytes = 0;
    this.eventHub.resyncAll();
  }
  snapshot(tenant: string, session: string): LiveSessionTailSnapshot {
    const content = this.#tails.get(key(tenant, session))?.content;
    return {
      canonicalThroughSequence: content?.coveredThrough ?? 0,
      highWaterMark: content?.highWaterMark ?? 0,
      events: content?.events ?? [],
    };
  }
  #tail(tenant: string, session: string) {
    const id = key(tenant, session);
    let t = this.#tails.get(id);
    if (!t) {
      t = {
        content: new CompactEventTail(),
        touched: Date.now(),
        metadataBytes: 512 + id.length * 2,
      };
      this.#tails.set(id, t);
    }
    t.touched = Date.now();
    return t.content;
  }
  #dropEmpty(id: string): void {
    const tail = this.#empty.get(id);
    if (!tail) return;
    this.#empty.delete(id);
    this.#emptyBytes -= tail.metadataBytes;
    if (this.#tails.get(id) === tail && !tail.content.size) {
      this.#tails.delete(id);
      this.#emptyEvictions++;
    }
  }
  #trackEmpty(tenant: string, session: string): void {
    const id = key(tenant, session),
      tail = this.#tails.get(id)!;
    const previous = this.#empty.get(id);
    if (previous) {
      this.#empty.delete(id);
      this.#emptyBytes -= previous.metadataBytes;
    }
    if (tail.content.size) return;
    this.#empty.set(id, tail);
    this.#emptyBytes += tail.metadataBytes;
    while (
      this.#empty.size > this.emptyLimits.maximumEntries ||
      this.#emptyBytes > this.emptyLimits.maximumBytes
    )
      this.#dropEmpty(this.#empty.keys().next().value!);
  }
  accept(tenant: string, event: PiCloudEvent): void {
    const content = this.#tail(tenant, event.sessionId);
    if (!content.accept(event)) {
      this.#duplicates++;
      this.#trackEmpty(tenant, event.sessionId);
      return;
    }
    this.#accepted++;
    this.#trackEmpty(tenant, event.sessionId);
    // Normal subscribers receive each original event immediately, not its span.
    this.eventHub.publish(tenant, event);
    if (["turn.completed", "turn.failed", "turn.cancelled"].includes(event.type))
      this.cover(tenant, event.sessionId, event.seq);
  }
  cover(tenant: string, session: string, through: number): void {
    const content = this.#tail(tenant, session),
      before = content.size;
    content.cover(through);
    this.#evicted += before - content.size;
    this.#trackEmpty(tenant, session);
  }
  statistics() {
    return {
      activeSessionTails: this.#tails.size,
      activeTextTails: this.#tails.size - this.#empty.size,
      emptyHolders: this.#empty.size,
      emptyEstimatedBytes: this.#emptyBytes,
      evictedEmptyHolders: this.#emptyEvictions,
      cachedEvents: [...this.#tails.values()].reduce((n, t) => n + t.content.size, 0),
      cachedBytes: [...this.#tails.values()].reduce((n, t) => n + t.content.bytes, 0),
      acceptedEvents: this.#accepted,
      duplicateEvents: this.#duplicates,
      evictedEvents: this.#evicted,
    };
  }
  close(): void {
    clearInterval(this.#sweep);
    this.reset();
    this.eventHub.onApplicationShutdown();
  }
}
