import { createHash } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import kafkaNative from "@confluentinc/kafka-javascript";
import {
  MAX_TOOL_CATALOG_BYTES,
  TOOL_CATALOG_KEY,
  TOOL_CATALOG_TOPIC,
  parseToolCatalogPublication,
  parseControlToolCatalog,
  type ControlToolCatalog,
  type ControlToolCatalogSource,
  type ToolCatalogPublication,
  type ToolCatalogWatermark,
  type ToolImplementation,
} from "@pi-cloud/protocol";
import { KafkaLogConsumer } from "./kafka-log-consumer.ts";

/** Disposable scoped views of one PG-selected directory publication authority. */
export class ToolCatalogView implements ControlToolCatalogSource {
  readonly #values = new Map<string, ToolCatalogPublication>();
  readonly #listeners = new Set<() => void>();
  #revision = 0;

  get ready(): boolean {
    return this.#values.has(TOOL_CATALOG_KEY);
  }
  get snapshot(): ToolCatalogPublication | undefined {
    return this.#values.get(TOOL_CATALOG_KEY);
  }
  snapshotFor(scope: string): ToolCatalogPublication | undefined {
    return this.#values.get(scope);
  }
  read(): ControlToolCatalog {
    if (!this.snapshot) throw new Error("Tool catalog has not been restored");
    return this.snapshot.catalog;
  }
  subscribe(listener: () => void): () => void {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  }
  accept(input: unknown): boolean {
    const next = parseToolCatalogPublication(input);
    if (
      createHash("sha256").update(JSON.stringify(next.catalog.tools)).digest("hex") !==
      next.catalog.revision
    )
      throw new Error("Tool catalog digest does not match its definitions");
    const old = this.#values.get(next.scope);
    if (old && next.generation < old.generation) return false;
    if (old?.generation === next.generation) {
      if (!isDeepStrictEqual(old, next))
        throw new Error("Tool catalog generation changed its contents");
      return false;
    }
    for (const [scope, value] of this.#values) {
      if (scope === next.scope) continue;
      const names = new Set(value.catalog.tools.map((tool) => tool.name));
      if (next.catalog.tools.some((tool) => names.has(tool.name)))
        throw new Error("Tool identity collides across catalog scopes");
    }
    const captured = structuredClone(next);
    const freeze = (value: unknown): void => {
      if (!value || typeof value !== "object" || Object.isFrozen(value)) return;
      for (const child of Object.values(value)) freeze(child);
      Object.freeze(value);
    };
    freeze(captured);
    this.#values.set(next.scope, captured);
    this.#revision++;
    for (const listener of this.#listeners) listener();
    return true;
  }
  forActor(actor: {
    tenantId: string;
    userId?: string | null;
    root: boolean;
  }): ControlToolCatalogSource {
    let revision = -1;
    let value: ControlToolCatalog;
    return {
      read: () => {
        const platform = this.snapshot;
        if (!platform) throw new Error("Tool catalog has not been restored");
        if (revision === this.#revision) return value;
        const tools = [...platform.catalog.tools];
        if (actor.root && actor.userId) {
          for (const entry of this.#values.values()) {
            const access = entry.mcp;
            if (
              entry.releaseId === platform.releaseId &&
              access?.enabled &&
              access.tenantId === actor.tenantId &&
              (access.visibility === "organization" || access.ownerUserId === actor.userId)
            )
              tools.push(...entry.catalog.tools);
          }
        }
        tools.sort((a, b) => a.name.localeCompare(b.name));
        value = parseControlToolCatalog({
          version: 4,
          tools,
          revision: createHash("sha256").update(JSON.stringify(tools)).digest("hex"),
        });
        revision = this.#revision;
        return value;
      },
      subscribe: (listener) => this.subscribe(listener),
    };
  }
  publicationFor(name: string): ToolCatalogPublication | undefined {
    const platform = this.snapshot;
    if (!platform) return;
    for (const entry of this.#values.values())
      if (
        entry.releaseId === platform.releaseId &&
        (!entry.mcp || entry.mcp.enabled) &&
        entry.catalog.tools.some((tool) => tool.name === name)
      )
        return entry;
  }
  implementation(name: string): ToolImplementation | undefined {
    const entry = this.publicationFor(name);
    if (!entry) return;
    return {
      releaseId: entry.releaseId,
      generation: entry.generation,
      replay: entry.replayable.includes(name) ? "safe" : "never",
      ...(entry.scope !== TOOL_CATALOG_KEY ? { catalogScope: entry.scope } : {}),
    };
  }
  replayable(name: string): boolean {
    return this.implementation(name)?.replay === "safe";
  }
}

async function ensureTopic(brokers: readonly string[], replicas: number): Promise<void> {
  const client = new kafkaNative.KafkaJS.Kafka({
    kafkaJS: {
      brokers: [...brokers],
      clientId: "tool-catalog-bootstrap",
      logLevel: kafkaNative.KafkaJS.logLevel.NOTHING,
    },
  });
  const admin = client.admin();
  try {
    await admin.connect();
    await admin.createTopics({
      topics: [
        {
          topic: TOOL_CATALOG_TOPIC,
          numPartitions: 1,
          replicationFactor: replicas,
          configEntries: [
            { name: "cleanup.policy", value: "compact" },
            { name: "retention.ms", value: "-1" },
            { name: "min.insync.replicas", value: String(Math.max(1, replicas - 1)) },
            { name: "max.message.bytes", value: String(MAX_TOOL_CATALOG_BYTES + 4096) },
            { name: "segment.bytes", value: String(16 * 1024 * 1024) },
          ],
        },
      ],
    });
  } finally {
    await admin.disconnect();
  }
}

/** Every boot uses its own group: catalogs are broadcast, not work sharing.
 * Rebuild from retained snapshots; a PG-selected minimum generation is the
 * startup watermark, avoiding transactional control offsets as fake records. */
export class KafkaToolCatalogConsumer {
  readonly view = new ToolCatalogView();
  readonly #consumer: KafkaLogConsumer<ToolCatalogPublication>;
  #closed = false;
  #bootstrapped = false;

  constructor(
    readonly options: {
      brokers: readonly string[];
      replicas: number;
      instanceId: string;
      minimumGenerations(): Promise<readonly ToolCatalogWatermark[]>;
      onSnapshot?(snapshot: ToolCatalogPublication): void;
    },
  ) {
    this.#consumer = new KafkaLogConsumer({
      brokers: options.brokers,
      clientId: `catalog-${options.instanceId}`,
      groupId: `tool-catalog-${options.instanceId}`,
      topic: TOOL_CATALOG_TOPIC,
      commitMessages: false,
      replayOffsets: async (bounds) => new Map(bounds.map((p) => [p.partition, p.low])),
      decode: (body) => {
        if (Buffer.byteLength(body) > MAX_TOOL_CATALOG_BYTES)
          throw new Error("Tool catalog exceeds its transport budget");
        return parseToolCatalogPublication(JSON.parse(body.toString()));
      },
      handler: async ({ fact }, current) => {
        if (current?.() === false) return;
        if (this.view.accept(fact)) options.onSnapshot?.(fact);
      },
    });
  }
  get ready(): boolean {
    if (!this.#bootstrapped || this.#closed) return false;
    try {
      this.#consumer.checkHealth();
      return this.view.ready;
    } catch {
      return false;
    }
  }
  async start(timeoutMs = 120_000): Promise<void> {
    await ensureTopic(this.options.brokers, this.options.replicas);
    const minimum = [...(await this.options.minimumGenerations())];
    if (!minimum.some((item) => item.scope === TOOL_CATALOG_KEY))
      minimum.push({ scope: TOOL_CATALOG_KEY, generation: 1 });
    await this.#consumer.start();
    await this.#consumer.waitUntilAssigned(timeoutMs);
    const until = Date.now() + timeoutMs;
    while (!this.#closed && Date.now() < until) {
      if (
        minimum.every(
          (item) => (this.view.snapshotFor(item.scope)?.generation ?? 0) >= item.generation,
        )
      ) {
        this.#bootstrapped = true;
        return;
      }
      await new Promise((resolve) => setTimeout(resolve, 25));
    }
    throw new Error("Tool catalog did not reach the selected startup generation");
  }
  async close(): Promise<void> {
    this.#closed = true;
    this.#bootstrapped = false;
    await this.#consumer.close();
    const kafka = new kafkaNative.KafkaJS.Kafka({
      kafkaJS: {
        brokers: [...this.options.brokers],
        clientId: "catalog-retirement",
        logLevel: kafkaNative.KafkaJS.logLevel.NOTHING,
      },
    });
    const admin = kafka.admin();
    try {
      await admin.connect();
      await admin.deleteGroups([`tool-catalog-${this.options.instanceId}`]);
    } catch {
      /* An empty ephemeral group may already have expired. */
    } finally {
      await admin.disconnect();
    }
  }
}

/** Kafka fences overlapping catalog publishers. Selection is checked both
 * before and after producer initialization; unlike Session effects, retrying a
 * complete immutable configuration snapshot is idempotent. */
export class KafkaToolCatalogPublisher {
  #producer: import("@confluentinc/kafka-javascript").Producer | undefined;
  #tail: Promise<unknown> = Promise.resolve();
  #closed = false;
  constructor(
    readonly options: {
      brokers: readonly string[];
      replicas: number;
      instanceId: string;
      authorize(snapshot: ToolCatalogPublication): Promise<boolean>;
    },
  ) {}

  publish(input: ToolCatalogPublication): Promise<void> {
    const snapshot = structuredClone(parseToolCatalogPublication(input));
    const body = Buffer.from(JSON.stringify(snapshot));
    if (body.byteLength > MAX_TOOL_CATALOG_BYTES)
      return Promise.reject(new Error("Tool catalog exceeds its transport budget"));
    const next = this.#tail
      .catch(() => {})
      .then(async () => {
        if (this.#closed || !(await this.options.authorize(snapshot)))
          throw new Error("Tool catalog release is no longer selected");
        try {
          if (!this.#producer) {
            await ensureTopic(this.options.brokers, this.options.replicas);
            const producer = new kafkaNative.Producer({
              "metadata.broker.list": this.options.brokers.join(","),
              "client.id": `catalog-publisher-${this.options.instanceId}`,
              "transactional.id": `${TOOL_CATALOG_TOPIC}-publisher`,
              "enable.idempotence": true,
              acks: -1,
              "allow.auto.create.topics": false,
              "message.max.bytes": MAX_TOOL_CATALOG_BYTES + 4096,
              "transaction.timeout.ms": 15000,
            });
            producer.on("event.error", () => {});
            this.#producer = producer;
            await new Promise<void>((resolve, reject) =>
              producer.connect({}, (error) => (error ? reject(error) : resolve())),
            );
            await new Promise<void>((resolve, reject) =>
              producer.initTransactions(10000, (error) => (error ? reject(error) : resolve())),
            );
            producer.setPollInterval(10);
          }
          if (this.#closed || !(await this.options.authorize(snapshot)))
            throw new Error("Tool catalog selection changed during initialization");
          const producer = this.#producer;
          await new Promise<void>((resolve, reject) =>
            producer.beginTransaction((error) => (error ? reject(error) : resolve())),
          );
          producer.produce(TOOL_CATALOG_TOPIC, 0, body, snapshot.scope);
          await new Promise<void>((resolve, reject) =>
            producer.commitTransaction(10000, (error) => (error ? reject(error) : resolve())),
          );
        } catch (error) {
          const producer = this.#producer;
          this.#producer = undefined;
          if (producer) await new Promise<void>((resolve) => producer.disconnect(() => resolve()));
          throw error;
        }
      });
    this.#tail = next;
    return next;
  }
  async close(): Promise<void> {
    this.#closed = true;
    await this.#tail.catch(() => {});
    const producer = this.#producer;
    this.#producer = undefined;
    if (producer) await new Promise<void>((resolve) => producer.disconnect(() => resolve()));
  }
}
