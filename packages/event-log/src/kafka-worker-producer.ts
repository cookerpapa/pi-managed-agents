import kafkaNative, {
  type Producer,
  type DeliveryReport,
  type LibrdKafkaError,
} from "@confluentinc/kafka-javascript";
import { PublicationNotSentError } from "@pi-cloud/protocol";
import type { KafkaPublicationRecord } from "./kafka-publication-writer.ts";

type Receipt = { factId: string; durable: true };
type Pending = {
  factId: string;
  timer: NodeJS.Timeout;
  resolve(value: Receipt): void;
  reject(error: unknown): void;
};

/** One boot-scoped transport. A complete native commit is one atomic Kafka record;
 * business-epoch admission belongs to Projector, not the producer identity. */
export class KafkaWorkerProducer {
  readonly #failure = new AbortController();
  readonly #pending = new Map<number, Pending>();
  #next = 0;
  #producer: Producer | undefined;
  #opening: Promise<void> | undefined;
  #closing: Promise<void> | undefined;
  #closed = false;
  get failureSignal(): AbortSignal {
    return this.#failure.signal;
  }
  constructor(
    readonly options: {
      brokers: readonly string[];
      clientId: string;
      topic: string;
      onFailure?(error: unknown): void;
    },
  ) {}

  assertHealthy(): void {
    this.#failure.signal.throwIfAborted();
    if (this.#closed || !this.#producer)
      throw new PublicationNotSentError("Worker producer is not ready");
  }
  #fail(error: unknown): void {
    if (this.#failure.signal.aborted) return;
    this.#failure.abort(error);
    for (const pending of this.#pending.values()) {
      clearTimeout(pending.timer);
      pending.reject(error);
    }
    this.#pending.clear();
    try {
      this.options.onFailure?.(error);
    } catch {
      /* failure remains latched */
    }
  }
  start(): Promise<void> {
    if (this.#closed)
      return Promise.reject(new PublicationNotSentError("Worker producer is closed"));
    return (this.#opening ??= this.#connect().catch((error) => {
      this.#fail(error);
      throw error;
    }));
  }
  async #connect(): Promise<void> {
    const producer = new kafkaNative.Producer({
      "metadata.broker.list": this.options.brokers.join(","),
      "client.id": this.options.clientId,
      "enable.idempotence": true,
      acks: -1,
      dr_cb: true,
      "message.timeout.ms": 10_000,
      "queue.buffering.max.messages": 4096,
      "queue.buffering.max.kbytes": 65536,
      "linger.ms": 1,
      "retry.backoff.ms": 5,
      "retry.backoff.max.ms": 1000,
      "allow.auto.create.topics": false,
    });
    producer.on("event.error", () => {
      /* A broker observation is not delivery/authority proof. */
    });
    producer.on("delivery-report", (error: LibrdKafkaError, report: DeliveryReport) => {
      const pending = this.#pending.get(report.opaque);
      if (!pending) return;
      if (error) {
        this.#fail(error);
        return;
      }
      clearTimeout(pending.timer);
      this.#pending.delete(report.opaque);
      pending.resolve({ factId: pending.factId, durable: true });
    });
    try {
      await new Promise<void>((resolve, reject) =>
        producer.connect({ topic: this.options.topic, timeout: 5000 }, (error) =>
          error ? reject(error) : resolve(),
        ),
      );
      if (this.#closed) throw new PublicationNotSentError("Worker producer closed during startup");
      producer.setPollInterval(5);
      this.#producer = producer;
    } catch (error) {
      await new Promise<void>((resolve) => producer.disconnect(() => resolve()));
      throw error;
    }
  }
  append(partition: number, record: KafkaPublicationRecord): Promise<Receipt> {
    try {
      this.assertHealthy();
    } catch (error) {
      return Promise.reject(error);
    }
    const id = ++this.#next;
    return new Promise<Receipt>((resolve, reject) => {
      const timer = setTimeout(
        () => this.#fail(new Error("Worker Kafka delivery acknowledgement is uncertain")),
        15_000,
      );
      timer.unref();
      this.#pending.set(id, { factId: record.factId, timer, resolve, reject });
      try {
        this.#producer!.produce(
          this.options.topic,
          partition,
          Buffer.from(record.value),
          Buffer.from(record.sessionId),
          undefined,
          id,
          [{ "pi-cloud-fact-id": Buffer.from(record.factId) }],
        );
      } catch (error) {
        clearTimeout(timer);
        this.#pending.delete(id);
        if ((error as { code?: number }).code === kafkaNative.CODES.ERRORS.ERR__QUEUE_FULL)
          reject(
            new PublicationNotSentError("Worker Kafka queue is full; record was not enqueued", {
              cause: error,
            }),
          );
        else {
          this.#fail(error);
          reject(error);
        }
      }
    });
  }
  close(): Promise<void> {
    return (this.#closing ??= (async () => {
      this.#closed = true;
      await this.#opening?.catch(() => {});
      const producer = this.#producer;
      this.#producer = undefined;
      if (!producer) return;
      await new Promise<void>((resolve) =>
        producer.flush(10_000, (error) => {
          if (error) this.#fail(error);
          resolve();
        }),
      );
      if (this.#pending.size)
        this.#fail(new Error("Worker producer closed with uncertain publications"));
      await new Promise<void>((resolve) => producer.disconnect(() => resolve()));
    })());
  }
}
