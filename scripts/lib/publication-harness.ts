import {
  KafkaWorkerProducer,
  DEFAULT_PRODUCER_CAPACITY,
  type ProducerCapacity,
} from "@pi-cloud/event-log";
import {
  ExecutionLogTopic,
  type ExecutionLogTopicOptions,
} from "../../packages/runtime-core/src/execution-log-topic.ts";
import { kafkaProducerLane } from "../../packages/runtime-core/src/kafka-accepted-fact.ts";
import { PublicationIngressRejected } from "../../packages/runtime-core/src/session-publication-ingress.ts";
import type { AcceptedFact } from "../../packages/runtime-core/src/accepted-fact.ts";

/** Isolated transport measurements. Synthetic grants never authorize production
 * work; production grants are admitted by the single Session Projector. */
export class PublicationHarness {
  readonly #topic: ExecutionLogTopic;
  readonly #producer: KafkaWorkerProducer;
  readonly #tails = new Map<string, Promise<void>>();
  #closed = false;
  #bytes = 0;
  #facts = 0;
  constructor(
    readonly options: ExecutionLogTopicOptions & { topic: string; capacity?: ProducerCapacity },
  ) {
    this.#topic = new ExecutionLogTopic(options);
    this.#producer = new KafkaWorkerProducer(options);
  }
  async start(): Promise<void> {
    await this.#topic.start();
    await this.#producer.start();
  }
  async append(fact: AcceptedFact) {
    if (this.#closed) throw new Error("Fixture is closed");
    const value = JSON.stringify(fact),
      bytes = Buffer.byteLength(value),
      capacity = this.options.capacity ?? DEFAULT_PRODUCER_CAPACITY;
    if (
      this.#bytes + bytes > capacity.maximumPendingBytes ||
      this.#facts >= capacity.maximumPendingFacts
    )
      throw new PublicationIngressRejected("capacity");
    this.#bytes += bytes;
    this.#facts++;
    try {
      const key = `${fact.scope.tenantId}:${fact.scope.piSessionId}`;
      const pending = (this.#tails.get(key) ?? Promise.resolve()).then(() =>
        this.#producer.append(kafkaProducerLane(fact.scope.piSessionId, this.options.partitions), {
          factId: fact.factId,
          sessionId: fact.scope.piSessionId,
          value,
        }),
      );
      const tail = pending.then(
        () => {},
        () => {},
      );
      this.#tails.set(key, tail);
      try {
        return await pending;
      } finally {
        if (this.#tails.get(key) === tail) this.#tails.delete(key);
      }
    } finally {
      this.#bytes -= bytes;
      this.#facts--;
    }
  }
  async retire(tenant: string, session: string): Promise<void> {
    await this.#tails.get(`${tenant}:${session}`);
  }
  statistics() {
    return { pendingBytes: this.#bytes, pendingFacts: this.#facts };
  }
  async close(): Promise<void> {
    this.#closed = true;
    await Promise.allSettled(this.#tails.values());
    await this.#producer.close();
    this.#tails.clear();
    await this.#topic.close();
  }
}
