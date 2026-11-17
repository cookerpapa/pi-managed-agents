import kafkaNative from "@confluentinc/kafka-javascript";
import { PartitionPrefetch, type PrefetchLimits } from "./partition-prefetch.ts";
import type {
  KafkaJS as K,
  KafkaConsumer,
  LibrdKafkaError,
  Message,
  Producer,
} from "@confluentinc/kafka-javascript";

type Assignment = { topic: string; partition: number; offset?: number };
export type PartitionReaderOptions = {
  prefetch?: PrefetchLimits;
  /** Optional diagnostics only; observer failure cannot change delivery. */
  onStatistics?(statistics: {
    receivedRecords: number;
    receivedMessageBytes: number;
    fetchRequests: number;
  }): void;
  brokers: readonly string[];
  clientId: string;
  groupId: string;
  topic: string;
  logger: K.Logger;
  rebalance(
    error: { code: number },
    assignments: Assignment[],
    functions: {
      assign(value: Assignment[]): void;
      unassign(value: Assignment[]): void;
    },
  ): Promise<void>;
  /** Emitted only after every delivered record preceding this EOF has settled. */
  onPosition(partition: number, offset: bigint): void;
};

/** Pinned public native API supplies EOF and group transaction fencing.
 * Bounded prefetch retains data while each partition runs one handler.
 */
export class KafkaPartitionReader {
  readonly #client: KafkaConsumer;
  readonly #options: PartitionReaderOptions;
  readonly #assigned = new Set<number>();
  readonly #active = new Map<number, Promise<void>>();
  readonly #paused = new Set<number>();
  readonly #nativePaused = new Set<number>();
  readonly #seeking = new Map<number, { epoch: number; ready: boolean }>();
  readonly #prefetch: PartitionPrefetch;
  readonly #epochs = new Map<number, number>();
  readonly #eof = new Map<number, { offset: bigint; epoch: number }>();
  readonly #starts = new Map<number, number>();
  readonly #committed = new Map<number, bigint>();
  readonly #commitLocks = new Map<number, Promise<void>>();
  #generation = 0;
  #failed: unknown;
  #closing = false;
  #disconnecting: Promise<void> | undefined;
  #connected = false;
  #polling = false;
  #pollGeneration = 0;
  #pollEpochs = new Map<number, number>();
  #loop: Promise<void> | undefined;
  #handler: K.EachBatchHandler | undefined;
  readonly #pendingCommits = new Map<
    number,
    {
      offset: K.TopicPartitionOffsetAndMetadata;
      generation: number;
      resolve(): void;
      reject(error: unknown): void;
    }
  >();
  #commitTimer: NodeJS.Timeout | undefined;
  #commitFlush: Promise<void> | undefined;
  #commit:
    | {
        offsets: K.TopicPartitionOffsetAndMetadata[];
        resolve(): void;
        reject(error: unknown): void;
      }
    | undefined;

  constructor(options: PartitionReaderOptions) {
    this.#options = options;
    this.#prefetch = new PartitionPrefetch(options.prefetch);
    this.#client = new kafkaNative.KafkaConsumer({
      "metadata.broker.list": options.brokers.join(","),
      "client.id": options.clientId,
      "group.id": options.groupId,
      "enable.auto.commit": false,
      "statistics.interval.ms": options.onStatistics ? 1000 : 0,
      "enable.auto.offset.store": false,
      "enable.partition.eof": true,
      "isolation.level": "read_committed",
      "partition.assignment.strategy": "range",
      "allow.auto.create.topics": false,
      "session.timeout.ms": 10_000,
      "heartbeat.interval.ms": 1_000,
      "queued.max.messages.kbytes": 32 * 1024,
      "queued.min.messages": 1,
      "fetch.queue.backoff.ms": 5,
      "fetch.wait.max.ms": 25,
      "fetch.max.bytes": 1024 * 1024,
      offset_commit_cb: true,
      rebalance_cb: (error: LibrdKafkaError, assignments: Assignment[]) => {
        this.#generation++;
        this.#assigned.clear();
        this.#starts.clear();
        this.#committed.clear();
        this.#eof.clear();
        this.#paused.clear();
        this.#nativePaused.clear();
        this.#seeking.clear();
        this.#prefetch.clearQueued();
        const generation = this.#generation;
        void options
          .rebalance(error, assignments, {
            assign: (values) => {
              if (this.#closing || generation !== this.#generation) return;
              this.#client.assign(values);
              // librdkafka preserves pause state for a retained partition.
              // Its old-generation handler deliberately cannot resume it;
              // reset transport pauses when the new assignment is installed.
              // Recovery may explicitly pause blocked partitions afterwards.
              if (values.length) this.#client.resume(values);
              for (const value of values) {
                this.#assigned.add(value.partition);
                if (value.offset !== undefined) this.#starts.set(value.partition, value.offset);
              }
              this.#updatePauses();
            },
            unassign: () => {
              this.#client.unassign();
            },
          })
          .catch((failure) => this.#fail(failure));
      },
    });
    this.#client.setDefaultConsumeTimeout(25);
    if (options.onStatistics)
      this.#client.on("event.stats", (event: { message: string }) => {
        try {
          const value = JSON.parse(event.message) as {
            rxmsgs: number;
            rxmsg_bytes: number;
            brokers: Record<string, { req?: { Fetch?: number } }>;
          };
          options.onStatistics!({
            receivedRecords: value.rxmsgs,
            receivedMessageBytes: value.rxmsg_bytes,
            fetchRequests: Object.values(value.brokers).reduce(
              (n, broker) => n + (broker.req?.Fetch ?? 0),
              0,
            ),
          });
        } catch {
          /* Disposable evidence never changes consumption. */
        }
      });
    this.#client.on("event.error", (error) => {
      if (!this.#closing) this.#fail(error);
    });
    this.#client.on("partition.eof", (event) => {
      if (
        this.#closing ||
        !this.#polling ||
        this.#pollGeneration !== this.#generation ||
        this.#pollEpochs.get(event.partition) !== this.#epoch(event.partition) ||
        this.#seeking.has(event.partition) ||
        this.#failed !== undefined ||
        !this.#assigned.has(event.partition) ||
        event.offset < 0
      )
        return;
      this.#eof.set(event.partition, {
        offset: BigInt(event.offset),
        epoch: this.#epoch(event.partition),
      });
      this.#advance(event.partition);
    });
    this.#client.on("offset.commit", (error, offsets) => {
      const pending = this.#commit;
      if (!pending) return;
      if (error) {
        pending.reject(error);
        return;
      }
      const failed = offsets.find((offset) => offset.error);
      if (failed) {
        pending.reject(failed.error);
        return;
      }
      if (
        pending.offsets.every((wanted) =>
          offsets.some(
            (actual) =>
              actual.topic === wanted.topic &&
              actual.partition === wanted.partition &&
              BigInt(actual.offset) >= BigInt(wanted.offset),
          ),
        )
      )
        pending.resolve();
    });
  }

  #fail(error: unknown): void {
    if (this.#failed !== undefined || this.#closing) return;
    this.#failed = error ?? new Error("Kafka reader failed");
    this.#generation++;
    this.#prefetch.clearQueued();
    for (const request of this.#pendingCommits.values()) request.reject(error);
    this.#pendingCommits.clear();
    this.#commit?.reject(error);
    this.#options.logger.error(error instanceof Error ? error.name : "KafkaReaderError");
  }

  #epoch(partition: number): number {
    return this.#epochs.get(partition) ?? 0;
  }

  #advance(partition: number): void {
    const eof = this.#eof.get(partition);
    if (
      this.#polling ||
      this.#active.has(partition) ||
      this.#prefetch.queued(partition) ||
      this.#seeking.has(partition) ||
      this.#paused.has(partition) ||
      this.#failed !== undefined ||
      !eof ||
      eof.epoch !== this.#epoch(partition) ||
      !this.#assigned.has(partition)
    )
      return;
    this.#options.onPosition(partition, eof.offset);
    this.#eof.delete(partition);
  }

  async connect(): Promise<void> {
    await new Promise<void>((resolve, reject) =>
      this.#client.connect({}, (error) => {
        if (error) reject(error);
        else {
          this.#connected = true;
          resolve();
        }
      }),
    );
  }

  async subscribe(input: K.ConsumerSubscribeTopics): Promise<void> {
    this.#client.subscribe(input.topics);
  }

  async run(input: K.ConsumerRunConfig): Promise<void> {
    if (this.#loop || !input.eachBatch)
      throw new Error("Kafka partition reader requires one batch handler");
    this.#handler = input.eachBatch;
    this.#loop = this.#poll().catch((error) => {
      if (!this.#closing) this.#fail(error);
    });
  }

  async #poll(): Promise<void> {
    while (!this.#closing && this.#failed === undefined) {
      this.#finishSeeks();
      const generation = this.#generation;
      this.#pollGeneration = generation;
      this.#pollEpochs = new Map(
        [...this.#assigned]
          .filter((partition) => !this.#seeking.has(partition))
          .map((partition) => [partition, this.#epoch(partition)]),
      );
      const epochs = this.#pollEpochs;
      this.#polling = true;
      let messages: Message[];
      try {
        messages = await new Promise<Message[]>((resolve, reject) =>
          this.#client.consume(1, (error, values) => (error ? reject(error) : resolve(values))),
        );
      } finally {
        this.#polling = false;
      }
      if (!this.#closing && this.#failed === undefined && generation === this.#generation) {
        for (const message of messages) {
          if (epochs.get(message.partition) !== this.#epoch(message.partition)) {
            const offset = this.#starts.get(message.partition);
            if (offset !== undefined && this.#assigned.has(message.partition))
              this.seek({
                topic: message.topic,
                partition: message.partition,
                offset: String(offset),
              });
            continue;
          }
          this.#enqueue(message);
        }
        for (const partition of this.#eof.keys()) this.#advance(partition);
      } else if (!this.#closing && this.#failed === undefined) {
        // A poll can span assignment. No new-generation record has been delivered
        // by this single poller yet; rewind its new assignments rather than drop
        // a first record returned while the callback was installing them.
        for (const [partition, offset] of this.#starts)
          this.seek({ topic: this.#options.topic, partition, offset: String(offset) });
      }
      if (!messages.length) await new Promise<void>((resolve) => setTimeout(resolve, 5));
    }
  }

  #enqueue(message: Message): void {
    if (!this.#assigned.has(message.partition) || this.#failed !== undefined) return;
    this.#prefetch.push(message, this.#generation, this.#epoch(message.partition));
    this.#updatePauses();
    this.#drain(message.partition);
  }

  #drain(partition: number): void {
    if (
      this.#closing ||
      this.#failed !== undefined ||
      !this.#assigned.has(partition) ||
      this.#active.has(partition) ||
      this.#paused.has(partition) ||
      this.#seeking.has(partition)
    )
      return;
    const buffered = this.#prefetch.take(partition);
    if (!buffered) {
      this.#advance(partition);
      return;
    }
    const { message, generation, epoch } = buffered;
    if (generation !== this.#generation || epoch !== this.#epoch(partition)) {
      this.#prefetch.release(buffered);
      this.#updatePauses();
      this.#drain(partition);
      return;
    }
    const current = () =>
      !this.#closing &&
      this.#failed === undefined &&
      generation === this.#generation &&
      epoch === this.#epoch(partition);
    const record: K.KafkaMessage = {
      key: typeof message.key === "string" ? Buffer.from(message.key) : (message.key ?? null),
      value: message.value,
      timestamp: String(message.timestamp ?? 0),
      attributes: 0,
      offset: String(message.offset),
      headers: Object.assign({}, ...(message.headers ?? [])),
    };
    const task = Promise.resolve()
      .then(() =>
        this.#handler!({
          batch: {
            topic: message.topic,
            partition,
            highWatermark: String(message.offset + 1),
            messages: [record],
            isEmpty: () => false,
            firstOffset: () => String(message.offset),
            lastOffset: () => String(message.offset),
            offsetLag: () => "0",
            offsetLagLow: () => "0",
          },
          isRunning: () => !this.#closing,
          isStale: () => !current(),
          resolveOffset: (offset) => {
            if (current())
              this.#client.offsetsStore([
                { topic: message.topic, partition, offset: Number(BigInt(offset) + 1n) },
              ]);
          },
          heartbeat: async () => {},
          pause: () => {
            this.pause([{ topic: message.topic, partitions: [partition] }]);
            return () => this.resume([{ topic: message.topic, partitions: [partition] }]);
          },
          commitOffsetsIfNecessary: async () => {},
        }),
      )
      .catch((error) => this.#fail(error))
      .finally(() => {
        this.#prefetch.release(buffered);
        if (this.#active.get(partition) !== task) return;
        this.#active.delete(partition);
        this.#updatePauses();
        this.#drain(partition);
      });
    this.#active.set(partition, task);
  }

  pause(topics: K.TopicPartitions[]): void {
    const partitions = topics.flatMap((topic) =>
      topic.partitions.map((partition) => ({ topic: topic.topic, partition })),
    );
    for (const { partition } of partitions) this.#paused.add(partition);
    this.#updatePauses();
  }

  resume(topics: K.TopicPartitions[]): void {
    const partitions = topics.flatMap((topic) =>
      topic.partitions.map((partition) => ({ topic: topic.topic, partition })),
    );
    for (const { partition } of partitions) this.#paused.delete(partition);
    this.#updatePauses();
    for (const { partition } of partitions) this.#drain(partition);
  }

  #updatePauses(): void {
    if (this.#closing || this.#failed !== undefined) return;
    for (const partition of this.#assigned) {
      const paused =
        this.#paused.has(partition) ||
        this.#seeking.has(partition) ||
        this.#prefetch.paused(partition);
      if (paused === this.#nativePaused.has(partition)) continue;
      const target = [{ topic: this.#options.topic, partition }];
      if (paused) {
        this.#client.pause(target);
        this.#nativePaused.add(partition);
      } else {
        this.#client.resume(target);
        this.#nativePaused.delete(partition);
      }
    }
  }

  statistics() {
    return {
      ...this.#prefetch.statistics(),
      activePartitions: this.#active.size,
      pausedPartitions: this.#nativePaused.size,
      pendingOffsetCommits: this.#pendingCommits.size,
    };
  }

  seek(position: K.TopicPartitionOffset): void {
    this.#epochs.set(position.partition, this.#epoch(position.partition) + 1);
    const epoch = this.#epoch(position.partition),
      generation = this.#generation;
    this.#starts.set(position.partition, Number(position.offset));
    const seeking = { epoch, ready: false };
    this.#seeking.set(position.partition, seeking);
    this.#prefetch.clearQueued(position.partition);
    this.#eof.delete(position.partition);
    this.#updatePauses();
    this.#client.seek({ ...position, offset: Number(position.offset) }, 5000, (error) => {
      if (
        this.#closing ||
        generation !== this.#generation ||
        this.#seeking.get(position.partition) !== seeking
      )
        return;
      if (error) {
        this.#fail(error);
        return;
      }
      seeking.ready = true;
      // Do not resume in the middle of an old consume() call: it could return
      // a new-position record under that poll's old epoch and skip it.
      if (!this.#polling) this.#finishSeeks();
    });
  }

  #finishSeeks(): void {
    const ready = [...this.#seeking]
      .filter(([, seek]) => seek.ready)
      .map(([partition]) => partition);
    for (const partition of ready) this.#seeking.delete(partition);
    if (!ready.length) return;
    this.#updatePauses();
    for (const partition of ready) this.#drain(partition);
  }

  commitOffset(position: K.TopicPartitionOffsetAndMetadata): Promise<void> {
    if (this.#closing) return Promise.reject(new Error("Kafka consumer is closing"));
    if (this.#failed !== undefined) return Promise.reject(this.#failed);
    const offset = { ...position },
      generation = this.#generation;
    // Join a batch only AFTER this partition's publication guard is available.
    // A blocked partition never reserves the shared native callback.
    return this.#withCommitLock(offset.partition, async () => {
      if (
        this.#closing ||
        this.#failed !== undefined ||
        generation !== this.#generation ||
        !this.#assigned.has(offset.partition)
      )
        throw new Error("Kafka commit owner changed");
      if (BigInt(offset.offset) <= (this.#committed.get(offset.partition) ?? -1n)) return;
      await new Promise<void>((resolve, reject) => {
        this.#pendingCommits.set(offset.partition, { offset, generation, resolve, reject });
        this.#scheduleCommits();
      });
    });
  }

  #scheduleCommits(): void {
    if (
      this.#closing ||
      this.#failed !== undefined ||
      this.#commitTimer ||
      this.#commitFlush ||
      !this.#pendingCommits.size
    )
      return;
    this.#commitTimer = setTimeout(() => {
      this.#commitTimer = undefined;
      this.#commitFlush = this.#flushCommits().finally(() => {
        this.#commitFlush = undefined;
        this.#scheduleCommits();
      });
      void this.#commitFlush.catch((error) => this.#fail(error));
    }, 0);
  }

  async #flushCommits(): Promise<void> {
    const requests = [...this.#pendingCommits.values()];
    this.#pendingCommits.clear();
    const valid = requests.filter((request) => {
      if (
        this.#closing ||
        this.#failed !== undefined ||
        request.generation !== this.#generation ||
        !this.#assigned.has(request.offset.partition)
      ) {
        request.reject(new Error("Kafka commit owner changed"));
        return false;
      }
      return true;
    });
    if (!valid.length) return;
    try {
      const offsets = valid.map((request) => request.offset);
      await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(
          () => finish(new Error("Kafka dispatch commit acknowledgement timed out")),
          5000,
        );
        const finish = (error?: unknown) => {
          clearTimeout(timer);
          this.#commit = undefined;
          if (error !== undefined) reject(error);
          else resolve();
        };
        this.#commit = { offsets, resolve: () => finish(), reject: finish };
        try {
          this.#client.commit(
            offsets.map((offset) => ({ ...offset, offset: Number(offset.offset) })),
          );
        } catch (error) {
          finish(error);
        }
      });
      for (const request of valid) {
        if (
          this.#closing ||
          this.#failed !== undefined ||
          request.generation !== this.#generation ||
          !this.#assigned.has(request.offset.partition)
        ) {
          request.reject(new Error("Kafka commit owner changed"));
          continue;
        }
        this.#committed.set(request.offset.partition, BigInt(request.offset.offset));
        request.resolve();
      }
    } catch (error) {
      for (const request of valid) request.reject(error);
      if (valid.some((request) => request.generation === this.#generation)) this.#fail(error);
    }
  }

  seedCommitted(offsets: ReadonlyMap<number, bigint>): void {
    for (const [partition, offset] of offsets)
      this.#committed.set(partition, offset < 0n ? 0n : offset);
  }

  async #withCommitLock<T>(partition: number, run: () => Promise<T>): Promise<T> {
    const previous = this.#commitLocks.get(partition) ?? Promise.resolve();
    const next = Promise.withResolvers<void>();
    const tail = previous.then(() => next.promise);
    this.#commitLocks.set(partition, tail);
    await previous;
    try {
      return await run();
    } finally {
      next.resolve();
      if (this.#commitLocks.get(partition) === tail) this.#commitLocks.delete(partition);
    }
  }

  /** Couple publication commit to the current group member, without advancing
   * or regressing the Projector's independent effect-dispatch offset. */
  async commitPublication(
    partition: number,
    producer: Producer,
    send: () => Promise<void>,
  ): Promise<void> {
    if (this.#closing) throw new Error("Kafka consumer is closing");
    if (this.#failed !== undefined) throw this.#failed;
    const generation = this.#generation;
    await this.#withCommitLock(partition, async () => {
      if (this.#closing || generation !== this.#generation || !this.#assigned.has(partition))
        throw new Error("Publication partition assignment changed");
      const offset = this.#committed.get(partition);
      if (offset === undefined) throw new Error("Publication dispatch boundary is not initialized");
      await send();
      if (this.#closing || generation !== this.#generation || !this.#assigned.has(partition))
        throw new Error("Publication owner changed before transaction commit");
      await new Promise<void>((resolve, reject) =>
        producer.sendOffsetsToTransaction(
          [{ topic: this.#options.topic, partition, offset: Number(offset) }],
          this.#client,
          5000,
          (error) => (error ? reject(error) : resolve()),
        ),
      );
      if (this.#closing || generation !== this.#generation || !this.#assigned.has(partition))
        throw new Error("Publication owner changed during group transaction");
      await new Promise<void>((resolve, reject) =>
        producer.commitTransaction(5000, (error) => (error ? reject(error) : resolve())),
      );
    });
  }

  disconnect(): Promise<void> {
    return (this.#disconnecting ??= this.#disconnect());
  }

  async #disconnect(): Promise<void> {
    this.#closing = true;
    this.#generation++;
    clearTimeout(this.#commitTimer);
    this.#commitTimer = undefined;
    this.#prefetch.clearQueued();
    for (const request of this.#pendingCommits.values())
      request.reject(new Error("Kafka consumer stopped before commit"));
    this.#pendingCommits.clear();
    this.#commit?.reject(new Error("Kafka consumer stopped during commit"));
    await this.#loop;
    await Promise.allSettled([...this.#active.values()]);
    await this.#commitFlush;
    // Session seal writers are external to KafkaLogConsumer's partition writer
    // set. Every group transaction borrows this native client through these
    // locks, so join them before disposal even during consumer-only restart.
    await Promise.allSettled([...this.#commitLocks.values()]);
    if (this.#connected)
      await new Promise<void>((resolve, reject) =>
        this.#client.disconnect((error) => (error ? reject(error) : resolve())),
      );
  }
}
