import type { Message } from "@confluentinc/kafka-javascript";

export type PrefetchLimits = Readonly<{
  maximumRecords: number;
  maximumBytes: number;
  partitionRecords: number;
  partitionBytes: number;
}>;

export const DEFAULT_PREFETCH_LIMITS: PrefetchLimits = Object.freeze({
  maximumRecords: 4096,
  maximumBytes: 32 * 1024 * 1024,
  partitionRecords: 256,
  partitionBytes: 4 * 1024 * 1024,
});

export type BufferedRecord = Readonly<{
  message: Message;
  generation: number;
  epoch: number;
  bytes: number;
}>;

/** Volatile delivery only. Counts include active handlers until release().
 * The last admitted record may exceed a byte watermark by its own size. */
export class PartitionPrefetch {
  readonly #parts = new Map<
    number,
    { queue: BufferedRecord[]; records: number; bytes: number; pressured: boolean }
  >();
  #records = 0;
  #bytes = 0;
  #pressured = false;
  #peakRecords = 0;
  #peakBytes = 0;
  constructor(readonly limits: PrefetchLimits = DEFAULT_PREFETCH_LIMITS) {
    if (
      (Object.keys(DEFAULT_PREFETCH_LIMITS) as Array<keyof PrefetchLimits>).some(
        (key) => !Number.isSafeInteger(limits[key]) || limits[key] < 1,
      )
    )
      throw new Error("Prefetch limits must be positive integers");
  }
  push(message: Message, generation: number, epoch: number): void {
    const size = (value: unknown) =>
      Buffer.isBuffer(value)
        ? value.byteLength
        : typeof value === "string"
          ? Buffer.byteLength(value)
          : 0;
    const bytes =
      512 +
      size(message.value) +
      size(message.key) +
      (message.headers ?? []).reduce(
        (total, header) =>
          total +
          Object.entries(header).reduce(
            (n, [key, value]) => n + Buffer.byteLength(key) + size(value),
            0,
          ),
        0,
      );
    let part = this.#parts.get(message.partition);
    if (!part) {
      part = { queue: [], records: 0, bytes: 0, pressured: false };
      this.#parts.set(message.partition, part);
    }
    part.queue.push({ message, generation, epoch, bytes });
    part.records++;
    part.bytes += bytes;
    this.#records++;
    this.#bytes += bytes;
    this.#peakRecords = Math.max(this.#peakRecords, this.#records);
    this.#peakBytes = Math.max(this.#peakBytes, this.#bytes);
    this.#pressure(message.partition);
  }
  take(partition: number): BufferedRecord | undefined {
    return this.#parts.get(partition)?.queue.shift();
  }
  /** The caller retains the budget of every returned record until the handler settles. */
  takeReady(first: BufferedRecord, limit = 16): BufferedRecord[] {
    const queue = this.#parts.get(first.message.partition)!.queue;
    let extra = 0,
      bytes = first.bytes;
    while (1 + extra < limit && extra < queue.length) {
      const next = queue[extra]!;
      if (
        next.generation !== first.generation ||
        next.epoch !== first.epoch ||
        bytes + next.bytes > 256 * 1024
      )
        break;
      bytes += next.bytes;
      extra++;
    }
    return [first, ...queue.splice(0, extra)];
  }
  queued(partition: number): number {
    return this.#parts.get(partition)?.queue.length ?? 0;
  }
  release(record: BufferedRecord): void {
    const part = this.#parts.get(record.message.partition)!;
    part.records--;
    part.bytes -= record.bytes;
    this.#records--;
    this.#bytes -= record.bytes;
    if (!part.records) this.#parts.delete(record.message.partition);
    this.#pressure(record.message.partition);
  }
  clearQueued(partition?: number): void {
    for (const id of partition === undefined ? [...this.#parts.keys()] : [partition]) {
      let record;
      while ((record = this.take(id))) this.release(record);
    }
  }
  #pressure(partition: number): void {
    const limits = this.limits;
    if (this.#records >= limits.maximumRecords || this.#bytes >= limits.maximumBytes)
      this.#pressured = true;
    else if (this.#records <= limits.maximumRecords / 2 && this.#bytes <= limits.maximumBytes / 2)
      this.#pressured = false;
    const part = this.#parts.get(partition);
    if (!part) return;
    if (part.records >= limits.partitionRecords || part.bytes >= limits.partitionBytes)
      part.pressured = true;
    else if (part.records <= limits.partitionRecords / 2 && part.bytes <= limits.partitionBytes / 2)
      part.pressured = false;
  }
  paused(partition: number): boolean {
    return this.#pressured || this.#parts.get(partition)?.pressured === true;
  }
  statistics() {
    return {
      records: this.#records,
      estimatedBytes: this.#bytes,
      queuedRecords: [...this.#parts.values()].reduce((n, part) => n + part.queue.length, 0),
      peakRecords: this.#peakRecords,
      peakEstimatedBytes: this.#peakBytes,
    };
  }
}
