import type { Kysely } from "kysely";
import type { Database } from "@pi-cloud/database";
import type { AcceptedFactScope } from "./accepted-fact.ts";
import type { KafkaAcceptedFactRecord } from "./kafka-accepted-fact-consumer.ts";

type Admission = Readonly<{
  allowed: boolean;
  writerCutoff?: bigint;
  runCutoff?: bigint;
  first?: { sessionId: string; topic: string; partition: number; offset: bigint };
}>;
type Entry = { writerId: string; value: Admission; bytes: number };
type Loading = { writerId: string; invalidated: boolean; promise: Promise<Admission> };
export type BoundaryCacheLimits = Readonly<{ maximumEntries: number; maximumBytes: number }>;
export const DEFAULT_BOUNDARY_CACHE_LIMITS: BoundaryCacheLimits = Object.freeze({
  maximumEntries: 65_536,
  maximumBytes: 32 * 1024 * 1024,
});

/** Coherent, disposable identity snapshots. A miss never grants authority. */
export class ExecutionStreamBoundary {
  readonly #entries = new Map<string, Entry>();
  readonly #writers = new Map<string, Set<string>>();
  readonly #loading = new Map<string, Loading>();
  #generation = 0;
  #revision = 0;
  #bytes = 0;
  #peakBytes = 0;
  #hits = 0;
  #misses = 0;
  #loads = 0;
  #coalesced = 0;
  #evictions = 0;
  constructor(
    readonly database: Kysely<Database>,
    readonly limits: BoundaryCacheLimits = DEFAULT_BOUNDARY_CACHE_LIMITS,
  ) {
    if (
      ![limits.maximumEntries, limits.maximumBytes].every((v) => Number.isSafeInteger(v) && v > 0)
    )
      throw new Error("Identity-cache budgets must be positive integers");
  }
  #identity(scope: AcceptedFactScope): string {
    return JSON.stringify([
      scope.tenantId,
      scope.piSessionId,
      scope.writerId,
      scope.fencingToken,
      scope.runId,
    ]);
  }
  reset(): void {
    this.#generation++;
    for (const loading of this.#loading.values()) loading.invalidated = true;
    this.#loading.clear();
    this.#entries.clear();
    this.#writers.clear();
    this.#bytes = 0;
  }
  #delete(key: string): void {
    const entry = this.#entries.get(key);
    if (!entry) return;
    this.#entries.delete(key);
    this.#bytes -= entry.bytes;
    const keys = this.#writers.get(entry.writerId)!;
    keys.delete(key);
    if (!keys.size) this.#writers.delete(entry.writerId);
  }
  #put(key: string, writerId: string, value: Admission): void {
    this.#delete(key);
    // Budget includes keys, a conservative object/index allowance and first-position metadata.
    // This is an estimate, not a promise about V8's allocator or process RSS.
    const bytes =
      768 +
      2 *
        (key.length +
          writerId.length +
          (value.first?.topic.length ?? 0) +
          (value.first?.sessionId.length ?? 0));
    if (bytes > this.limits.maximumBytes) return;
    while (
      this.#entries.size >= this.limits.maximumEntries ||
      this.#bytes + bytes > this.limits.maximumBytes
    ) {
      this.#delete(this.#entries.keys().next().value!);
      this.#evictions++;
    }
    this.#entries.set(key, { writerId, value, bytes });
    let keys = this.#writers.get(writerId);
    if (!keys) {
      keys = new Set();
      this.#writers.set(writerId, keys);
    }
    keys.add(key);
    this.#bytes += bytes;
    this.#peakBytes = Math.max(this.#peakBytes, this.#bytes);
  }
  #cached(key: string): Admission | undefined {
    const entry = this.#entries.get(key);
    if (!entry) return;
    this.#entries.delete(key);
    this.#entries.set(key, entry);
    return entry.value;
  }
  close(scope: AcceptedFactScope, offset: bigint): void {
    this.#revision++;
    const key = this.#identity(scope),
      old = this.#entries.get(key)?.value;
    const loading = this.#loading.get(key);
    if (loading) loading.invalidated = true;
    this.#put(key, scope.writerId, {
      ...(old ?? { allowed: true }),
      runCutoff: old?.runCutoff !== undefined && old.runCutoff < offset ? old.runCutoff : offset,
    });
  }
  closeWriter(writerId: string, offset: bigint): void {
    this.#revision++;
    for (const loading of this.#loading.values())
      if (loading.writerId === writerId) loading.invalidated = true;
    // Updating an entry neither creates a second permission nor globally invalidates others.
    for (const key of [...(this.#writers.get(writerId) ?? [])]) {
      const entry = this.#entries.get(key);
      if (!entry) continue;
      const cutoff = entry.value.writerCutoff;
      entry.value = {
        ...entry.value,
        writerCutoff: cutoff !== undefined && cutoff < offset ? cutoff : offset,
      };
    }
  }
  observes(scope: AcceptedFactScope): boolean {
    const value = this.#entries.get(this.#identity(scope))?.value;
    return (
      value?.allowed === true && value.writerCutoff === undefined && value.runCutoff === undefined
    );
  }
  async #read(scope: AcceptedFactScope): Promise<Admission> {
    const key = this.#identity(scope),
      generation = this.#generation;
    for (;;) {
      if (generation !== this.#generation)
        throw new Error("Execution identity cache reset during lookup");
      const cached = this.#cached(key);
      if (cached) {
        this.#hits++;
        return cached;
      }
      this.#misses++;
      let loading = this.#loading.get(key);
      if (loading) this.#coalesced++;
      else {
        this.#loads++;
        const slot: Loading = {
          writerId: scope.writerId,
          invalidated: false,
          promise: Promise.resolve({ allowed: false }),
        };
        slot.promise = this.#load(scope)
          .then((value) => {
            if (
              generation === this.#generation &&
              !slot.invalidated &&
              this.#loading.get(key) === slot
            )
              this.#put(key, scope.writerId, value);
            return value;
          })
          .finally(() => {
            if (this.#loading.get(key) === slot) this.#loading.delete(key);
          });
        this.#loading.set(key, slot);
        loading = slot;
      }
      const value = await loading.promise;
      if (generation !== this.#generation)
        throw new Error("Execution identity cache reset during lookup");
      if (loading.invalidated) continue;
      return value;
    }
  }
  async #load(scope: AcceptedFactScope): Promise<Admission> {
    const run = await this.database
      .selectFrom("runs as run")
      .innerJoin("session_leases as writer", (join) =>
        join
          .on("writer.lease_id", "=", scope.writerId)
          .onRef("writer.tenant_id", "=", "run.tenant_id"),
      )
      .select([
        "run.session_id",
        "run.output_sealed_at",
        "run.output_seal_offset",
        "run.output_first_topic",
        "run.output_first_partition",
        "run.output_first_offset",
        "run.lease_id",
        "writer.writer_seal_offset",
        "writer.fencing_token",
      ])
      .where("run.id", "=", scope.runId)
      .where("run.tenant_id", "=", scope.tenantId)
      .where("writer.pi_session_id", "=", scope.piSessionId)
      .executeTakeFirst();
    if (!run || Number(run.fencing_token) !== scope.fencingToken) return { allowed: false };
    if (run.lease_id !== scope.writerId) {
      if (run.writer_seal_offset === null)
        throw new Error("Previous execution owner has no closure proof");
      return { allowed: false };
    }
    return {
      allowed: true,
      ...(run.writer_seal_offset == null ? {} : { writerCutoff: BigInt(run.writer_seal_offset) }),
      ...(run.output_sealed_at === null
        ? {}
        : { runCutoff: run.output_seal_offset == null ? -1n : BigInt(run.output_seal_offset) }),
      ...(run.output_first_offset == null
        ? {}
        : {
            first: {
              sessionId: run.session_id,
              topic: run.output_first_topic!,
              partition: run.output_first_partition!,
              offset: BigInt(run.output_first_offset),
            },
          }),
    };
  }
  async isOpen(
    record: KafkaAcceptedFactRecord,
    canonical: boolean,
    hasPrefix?: (rootSessionId: string) => boolean,
  ): Promise<boolean> {
    const generation = this.#generation;
    let value: Admission;
    for (;;) {
      const revision = this.#revision;
      value = await this.#read(record.fact.scope);
      if (generation !== this.#generation)
        throw new Error("Execution identity cache reset during admission");
      // Covers the microtask gap after a coalesced read finished but before
      // this caller resumed. Unrelated closure causes only a cache hit, not eviction.
      if (revision === this.#revision) break;
    }
    if (!value.allowed) return false;
    if (value.writerCutoff !== undefined && record.offset >= value.writerCutoff) return false;
    if (value.runCutoff !== undefined) return !canonical && record.offset < value.runCutoff;
    if (canonical && value.first && !hasPrefix?.(value.first.sessionId)) {
      if (
        value.first.topic !== record.topic ||
        value.first.partition !== record.partition ||
        value.first.offset !== record.offset
      )
        throw new Error("Unsealed execution prefix is missing or changed Kafka partition");
    }
    return true;
  }
  statistics() {
    return {
      entries: this.#entries.size,
      estimatedBytes: this.#bytes,
      peakEstimatedBytes: this.#peakBytes,
      pendingLoads: this.#loading.size,
      hits: this.#hits,
      misses: this.#misses,
      loads: this.#loads,
      coalesced: this.#coalesced,
      evictions: this.#evictions,
    };
  }
}
