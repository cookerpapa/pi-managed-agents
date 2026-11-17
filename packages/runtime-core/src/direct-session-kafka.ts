import { KafkaWorkerProducer, ACCEPTED_FACT_TOPIC } from "@pi-cloud/event-log";
import { MAX_EXECUTION_FACT_BYTES, PublicationNotSentError } from "@pi-cloud/protocol";
import type {
  AcceptedFact,
  AcceptedFactBus,
  AcceptedFactScope,
  ExecutionPublication,
} from "./accepted-fact.ts";
import { kafkaProducerLane, prepareAcceptedFact } from "./kafka-accepted-fact.ts";
import { ExecutionLogTopic } from "./execution-log-topic.ts";
import { encodeSessionLogFact } from "./session-log-envelope.ts";

export function assertSessionPublication(
  publication: ExecutionPublication,
  fact: AcceptedFact,
): void {
  const root = publication.scope;
  const check = (scope: AcceptedFactScope) => {
    for (const key of ["tenantId", "piSessionId", "runId", "writerId", "fencingToken"] as const)
      if (scope[key] !== root[key])
        throw new PublicationNotSentError("Record belongs to another Session writer");
  };
  check(fact.scope);
  if (fact.kind === "execution_seal")
    throw new PublicationNotSentError("Workers publish only native intents and work records");
  prepareAcceptedFact(fact);
  if (fact.kind === "durable_commit") {
    for (const tool of fact.tools) check(tool.scope);
    for (const cancel of fact.cancellations) check(cancel.scope);
    for (const display of fact.display) check(display.scope);
  }
}

/** Per-Session local ordering over one Worker transport. PG claims/renewal own
 * execution; the Projector admits the durable candidate prefix. */
export class DirectSessionKafkaBus implements AcceptedFactBus {
  readonly #writers = new Map<
    string,
    {
      publication: ExecutionPublication;
      retired: boolean;
      signal?: AbortSignal;
      tail: Promise<void>;
    }
  >();
  readonly #topic: ExecutionLogTopic;
  readonly #producer: KafkaWorkerProducer;
  #starting: Promise<void> | undefined;
  #closed = false;
  constructor(
    readonly options: {
      brokers: readonly string[];
      clientId: string;
      partitions: number;
      replicas: number;
      topic?: string;
      onFailure?(error: unknown): void;
    },
  ) {
    this.#topic = new ExecutionLogTopic({ ...options, manageTopic: false });
    this.#producer = new KafkaWorkerProducer({
      ...options,
      topic: options.topic ?? ACCEPTED_FACT_TOPIC,
    });
  }
  #key(scope: Pick<AcceptedFactScope, "tenantId" | "piSessionId">): string {
    return JSON.stringify([scope.tenantId, scope.piSessionId]);
  }
  async open(publication: ExecutionPublication, signal?: AbortSignal): Promise<void> {
    if (this.#closed) throw new PublicationNotSentError("Session publication is closed");
    signal?.throwIfAborted();
    const key = this.#key(publication.scope);
    if (this.#writers.has(key))
      throw new PublicationNotSentError("Retire the local Session before opening another owner");
    if (publication.scope.leaseId !== publication.scope.writerId)
      throw new PublicationNotSentError("Session writer identity changed");
    const entry = {
      publication: structuredClone(publication),
      retired: false,
      ...(signal ? { signal } : {}),
      tail: Promise.resolve(),
    };
    this.#writers.set(key, entry);
    try {
      await this.#start();
      this.#producer.assertHealthy();
      signal?.throwIfAborted();
      if (this.#closed || entry.retired || this.#writers.get(key) !== entry)
        throw new PublicationNotSentError("Session owner retired while opening");
    } catch (error) {
      entry.retired = true;
      if (this.#writers.get(key) === entry) this.#writers.delete(key);
      throw error;
    }
  }
  append(fact: AcceptedFact) {
    const entry = this.#writers.get(this.#key(fact.scope));
    const scope = entry?.publication.scope;
    if (
      !entry ||
      entry.retired ||
      entry.signal?.aborted ||
      !scope ||
      scope.runId !== fact.scope.runId ||
      scope.writerId !== fact.scope.writerId ||
      scope.fencingToken !== fact.scope.fencingToken ||
      fact.kind === "execution_seal"
    )
      return Promise.reject(
        new PublicationNotSentError("Session writer is unavailable for this record"),
      );
    let value: string;
    try {
      assertSessionPublication(entry.publication, fact);
      value = encodeSessionLogFact(fact);
      if (Buffer.byteLength(value) > MAX_EXECUTION_FACT_BYTES)
        throw new Error("Session record exceeds its transport budget");
    } catch (cause) {
      return Promise.reject(new PublicationNotSentError("Session record is invalid", { cause }));
    }
    const pending = entry.tail.then(() => {
      if (this.#closed || entry.retired || entry.signal?.aborted)
        throw new PublicationNotSentError("Session writer retired before enqueue");
      return this.#producer.append(kafkaProducerLane(scope.piSessionId, this.options.partitions), {
        factId: fact.factId,
        sessionId: scope.piSessionId,
        value,
      });
    });
    entry.tail = pending.then(
      () => {},
      () => {},
    );
    return pending;
  }
  async forget(scope: AcceptedFactScope): Promise<void> {
    const key = this.#key(scope),
      entry = this.#writers.get(key);
    if (
      !entry ||
      entry.publication.scope.writerId !== scope.writerId ||
      entry.publication.scope.runId !== scope.runId
    )
      return;
    entry.retired = true;
    this.#writers.delete(key);
    await entry.tail;
  }
  publicationFailure(scope: AcceptedFactScope): unknown {
    return this.publicationFailureSignal(scope)?.reason;
  }
  publicationFailureSignal(scope: AcceptedFactScope): AbortSignal | undefined {
    const entry = this.#writers.get(this.#key(scope));
    return entry?.publication.scope.writerId === scope.writerId
      ? this.#producer.failureSignal
      : undefined;
  }
  async publicationAvailability() {
    await this.checkHealth();
    return {
      partitionCount: this.options.partitions,
      partitions: await this.#topic.availablePartitions(),
    };
  }
  #start(): Promise<void> {
    return (this.#starting ??= (async () => {
      await this.#topic.start();
      await this.#producer.start();
    })());
  }
  async checkHealth(): Promise<void> {
    if (this.#closed) throw new Error("Session publication is closed");
    await this.#start();
    await this.#topic.checkHealth();
    this.#producer.assertHealthy();
  }
  async close(): Promise<void> {
    this.#closed = true;
    const entries = [...this.#writers.values()];
    this.#writers.clear();
    for (const entry of entries) entry.retired = true;
    await Promise.allSettled(entries.map((entry) => entry.tail));
    await this.#starting?.catch(() => {});
    await this.#producer.close();
    await this.#topic.close();
  }
}
