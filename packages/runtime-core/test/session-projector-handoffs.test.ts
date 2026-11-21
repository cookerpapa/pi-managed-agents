import { afterEach, beforeEach, describe, expect, it, vi, type Mock } from "vitest";
import type { Database } from "@pi-cloud/database";
import type { Kysely } from "kysely";
import type { PiCloudEvent } from "@pi-cloud/protocol";
import type { ExecutionProjectionResult } from "../src/execution-stream-projection.ts";

const f = vi.hoisted(() => ({
  project: vi.fn(),
  applies: vi.fn(),
  observes: vi.fn(),
  parse: vi.fn(),
  start: vi.fn<(resource: string, partitions?: number) => void>(),
  close: vi.fn<(resource: string) => Promise<void>>(),
  decode: undefined as undefined | ((value: Buffer) => any),
  prepareReady: undefined as
    undefined | ((records: any[], current: () => boolean) => Promise<number>),
  decodedHandler: undefined as
    | undefined
    | ((record: any, current: () => boolean, grant: () => Promise<boolean>) => Promise<void>),
  handler: undefined as
    | undefined
    | ((record: any, current?: () => boolean, commit?: () => Promise<boolean>) => Promise<void>),
}));
vi.mock("@pi-cloud/event-log", async (original) => ({
  ...(await original<typeof import("@pi-cloud/event-log")>()),
  KafkaLogConsumer: class {
    constructor(options: {
      handler: typeof f.handler;
      decode: typeof f.decode;
      prepareReady?: typeof f.prepareReady;
    }) {
      f.prepareReady = options.prepareReady;
      f.decodedHandler = options.handler;
      f.handler = (record, current, commit) =>
        options.handler!(
          {
            ...record,
            fact: { fact: record.fact, tools: toolCommandsForFact(record.fact), controlTools: [] },
          },
          current,
          commit,
        );
      f.decode = (value) => options.decode!(value).fact;
    }
    async start() {
      f.start("consumer");
    }
    async close() {
      await f.close("consumer");
    }
  },
}));
vi.mock("../src/kafka-accepted-fact.ts", () => ({
  ACCEPTED_FACT_TOPIC: "test",
  kafkaProducerLane: () => 0,
  decodeSessionLogFact: f.parse,
}));
vi.mock("../src/execution-log-topic.ts", () => ({
  ExecutionLogTopic: class {
    async start() {
      f.start("producer");
    }
    async close() {
      await f.close("producer");
    }
  },
}));
vi.mock("../src/execution-stream-projection.ts", async (original) => ({
  ...(await original<typeof import("../src/execution-stream-projection.ts")>()),
  ExecutionStreamProjector: class {
    project = f.project;
    accepts = f.applies;
    observes = f.observes;
  },
}));
vi.mock("../src/session-outbox-relay.ts", () => ({
  SessionOutboxRelay: class {
    start() {
      f.start("relay");
    }
    async close() {
      await f.close("relay");
    }
  },
}));
vi.mock("../src/kafka-safe-retention.ts", () => ({
  KafkaSafeRetention: class {
    start(partitions: number) {
      f.start("retention", partitions);
    }
    async close() {
      await f.close("retention");
    }
  },
}));
import { SessionProjector } from "../src/session-projector.ts";
import { toolInvocationFixture } from "./tool-invocation-fixture.ts";
import { toolCommandsForFact } from "../src/tool-invocation-fact.ts";

let projector: SessionProjector;
type ToolDelivery = ConstructorParameters<typeof SessionProjector>[0]["toolCommands"]["consume"];
let route: Mock<ToolDelivery>;
let shutdownAsserted = false;
beforeEach(() => {
  shutdownAsserted = false;
  f.close.mockReset().mockResolvedValue(undefined);
  f.start.mockReset();
  f.project.mockReset().mockResolvedValue(undefined);
  f.applies.mockReset().mockResolvedValue(true);
  f.observes.mockReset().mockReturnValue(false);
  f.parse
    .mockReset()
    .mockImplementation((value) => ({ fact: JSON.parse(value), tools: [], controlTools: [] }));
  route = vi.fn<ToolDelivery>(async () => {});
  projector = new SessionProjector({
    database: {} as Kysely<Database>,
    publicationDatabase: {} as Kysely<Database>,
    notificationConnectionString: "postgresql://unused/test",
    brokers: ["unused"],
    clientId: "test",
    publicationToken: "test-publication-token",
    partitions: 2,
    replicas: 3,
    retentionMs: 7_200_000,
    advertisedBaseUrl: "http://projector",
    toolCommands: { consume: route, close: () => f.close("tools") },
    nativeChanges: { consume: async () => {}, close: () => f.close("subagents") },
  });
});
afterEach(async () => {
  if (!shutdownAsserted) await projector.close();
});
function event(
  sessionId = "session",
  type: PiCloudEvent["type"] = "assistant.text.delta",
): PiCloudEvent {
  return {
    schemaVersion: 1,
    eventId: "e-" + sessionId,
    sessionId,
    turnId: "turn",
    agentId: "root",
    seq: 1,
    occurredAt: new Date().toISOString(),
    type,
    payload: { text: "visible" },
  } as PiCloudEvent;
}
function record(sessionId = "session", kind = "agent_event", partition = 0) {
  return {
    topic: "test",
    partition,
    offset: 10n,
    fact: { kind, scope: { tenantId: "tenant", sessionId }, event: event(sessionId) },
  };
}
function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}

describe("Unified Projector handoff boundaries (transport/PG simulated)", () => {
  it("materializes the admitted history prefix before delivery and leaves the following Tool to its own grant", async () => {
    const ordinary = vi.fn(),
      batch = vi.fn(async (_records: readonly unknown[], _current: () => boolean) => {}),
      notify = vi.fn(),
      grant = vi.fn(async () => true),
      current = () => true;
    const options = projector.options;
    await projector.close();
    projector = new SessionProjector({
      ...options,
      nativeChanges: { consume: notify },
      controlTools: { consume: ordinary, historyBatch: { accepts: () => true, consume: batch } },
    });
    const { fact: tool } = toolInvocationFixture();
    const entry = {
      ...tool,
      tools: [],
      commit: { ...tool.commit, writes: [{ type: "entry", value: { id: 4, conversationId: 1 } }] },
    };
    const decoded = (offset: bigint, fact: typeof tool | typeof entry) => ({
      ...record(),
      offset,
      fact: { fact, tools: fact === tool ? toolCommandsForFact(tool) : [] },
    });
    const first = decoded(10n, entry),
      stale = decoded(11n, entry),
      last = decoded(12n, entry),
      effect = decoded(13n, tool);
    f.project
      .mockResolvedValueOnce(undefined)
      .mockResolvedValueOnce({ admitted: false })
      .mockResolvedValueOnce(undefined);
    batch.mockRejectedValueOnce(new Error("candidate unavailable"));
    await expect(f.prepareReady!([first, stale, last, effect], current)).rejects.toThrow(
      "candidate unavailable",
    );
    expect(notify).not.toHaveBeenCalled();
    expect(grant).not.toHaveBeenCalled();
    f.project
      .mockResolvedValueOnce(undefined)
      .mockResolvedValueOnce({ admitted: false })
      .mockResolvedValueOnce(undefined);
    expect(await f.prepareReady!([first, stale, last, effect], current)).toBe(3);
    expect(batch.mock.lastCall?.[0]).toEqual([
      expect.objectContaining({ offset: 10n }),
      expect.objectContaining({ offset: 12n }),
    ]);
    for (const r of [first, stale, last]) await f.decodedHandler!(r, current, grant);
    expect(f.project).toHaveBeenCalledTimes(6);
    expect(notify).toHaveBeenCalledTimes(2);
    expect(ordinary).not.toHaveBeenCalled();
    expect(grant).not.toHaveBeenCalled();
    expect(await f.prepareReady!([effect, last], current)).toBe(1);
    await f.decodedHandler!(effect, current, grant);
    expect(ordinary).toHaveBeenCalledOnce();
    expect(grant).toHaveBeenCalledOnce();
    expect(route).toHaveBeenCalledOnce();

    let assigned = true;
    f.project.mockImplementationOnce(async () => {
      assigned = false;
    });
    expect(await f.prepareReady!([first, last], () => assigned)).toBe(1);
    expect(batch).toHaveBeenCalledTimes(2);
  });
  it("admits Session activity through the epoch view without native projection or a Tool dispatch receipt", async () => {
    const consume = vi.fn(),
      commit = vi.fn(async () => true);
    projector.options.sessionActivity = { consume, reset: vi.fn() };
    const scope = toolInvocationFixture().scope;
    const fact = {
      kind: "session_activity",
      factId: crypto.randomUUID(),
      scope,
      occurredAt: new Date().toISOString(),
      resources: [{ workspaceId: crypto.randomUUID() }],
    };
    const record = { fact, topic: "test", partition: 0, offset: 12n };
    await f.handler!(record, () => true, commit);
    expect(consume).toHaveBeenCalledOnce();
    expect(f.project).not.toHaveBeenCalled();
    expect(f.applies).toHaveBeenCalledWith(record);
    expect(commit).not.toHaveBeenCalled();
    expect(route).not.toHaveBeenCalled();
    await f.handler!(record, () => false, commit);
    expect(consume).toHaveBeenCalledOnce();
    f.applies.mockResolvedValue(false);
    await f.handler!({ ...record, offset: 13n }, () => true, commit);
    expect(consume).toHaveBeenCalledOnce();
  });
  it("holds a replacement record before PG projection or dispatch acknowledgement", async () => {
    const resume = deferred<void>();
    projector.options.projectionGate = {
      async run(operation) {
        await resume.promise;
        return operation();
      },
    };
    const commit = vi.fn(async () => true);
    const work = f.handler!(record(), () => true, commit);
    await Promise.resolve();
    expect(f.project).not.toHaveBeenCalled();
    expect(commit).not.toHaveBeenCalled();
    resume.resolve();
    await work;
    expect(f.project).toHaveBeenCalledTimes(1);
  });
  it("projects a combined invocation before dispatch commit and never reissues it on history replay", async () => {
    const { fact } = toolInvocationFixture();
    const r = { fact, topic: "test", partition: 0, offset: 12n };
    const projection = deferred<void>();
    const dispatch = deferred<boolean>();
    const commit = vi.fn(() => dispatch.promise);
    f.project.mockImplementationOnce(() => projection.promise);
    const pending = f.handler!(r, () => true, commit);
    await vi.waitFor(() => expect(f.project).toHaveBeenCalledOnce());
    expect(commit).not.toHaveBeenCalled();
    expect(route).not.toHaveBeenCalled();
    projection.resolve();
    await vi.waitFor(() => expect(commit).toHaveBeenCalledOnce());
    expect(route).not.toHaveBeenCalled();
    dispatch.resolve(true);
    await pending;
    expect(route).toHaveBeenCalledWith(
      { ...r, fact: toolCommandsForFact(fact)[0] },
      expect.any(Function),
    );
    await f.handler!(
      r,
      () => true,
      async () => false,
    );
    expect(f.project).toHaveBeenCalledTimes(2);
    expect(route).toHaveBeenCalledOnce();
  });

  it.each(["projection", "commit", "sealed", "rebalance"])(
    "does not dispatch the combined command across a %s failure",
    async (failure) => {
      const { fact } = toolInvocationFixture();
      const r = { fact, topic: "test", partition: 0, offset: 12n };
      let current = true;
      if (failure === "projection") f.project.mockRejectedValueOnce(new Error("PG unavailable"));
      if (failure === "sealed") f.applies.mockResolvedValueOnce(false);
      const commit = vi.fn(async () => {
        if (failure === "commit") throw new Error("commit unknown");
        if (failure === "rebalance") current = false;
        return true;
      });
      const pending = f.handler!(r, () => current, commit);
      if (failure === "projection" || failure === "commit") await expect(pending).rejects.toThrow();
      else await pending;
      expect(route).not.toHaveBeenCalled();
      if (failure === "projection" || failure === "sealed") expect(commit).not.toHaveBeenCalled();
    },
  );

  it("routes a Tool only after confirmed dispatch commit and skips replayed commands", async () => {
    const commit = deferred<boolean>();
    const r = { ...record(), fact: toolInvocationFixture().fact };
    const handling = f.handler!(
      r,
      () => true,
      () => commit.promise,
    );
    await vi.waitFor(() => expect(f.project).toHaveBeenCalledOnce());
    expect(route).not.toHaveBeenCalled();
    commit.resolve(true);
    await handling;
    expect(route).toHaveBeenCalledOnce();
    await f.handler!(
      r,
      () => true,
      async () => false,
    );
    expect(route).toHaveBeenCalledOnce();
  });

  it("shares one dispatch commit between an ordinary external effect and Cube commands in the same record", async () => {
    const seen: string[] = [];
    projector.options.controlTools = {
      consume: async (_record, _current, admit) => {
        expect(await admit!()).toBe(true);
        seen.push("image");
      },
    };
    route.mockImplementation(async () => {
      seen.push("cube");
    });
    const commit = vi.fn(async () => {
      seen.push("commit");
      return true;
    });
    await f.handler!({ ...record(), fact: toolInvocationFixture().fact }, () => true, commit);
    expect(commit).toHaveBeenCalledTimes(1);
    expect(seen).toEqual(["commit", "image", "cube"]);
  });

  it("does not route a Tool when its dispatch commit fails", async () => {
    await expect(
      f.handler!(
        { ...record(), fact: toolInvocationFixture().fact },
        () => true,
        async () => {
          throw new Error("commit lost");
        },
      ),
    ).rejects.toThrow("commit lost");
    expect(route).not.toHaveBeenCalled();
  });
  it("uses the producer-verified partition count without a second offset query at startup", async () => {
    await projector.start();
    expect(f.start.mock.calls).toEqual([["producer"], ["consumer"], ["relay"], ["retention", 2]]);
  });
  it("closes every owner after a failed relay/consumer drain and shares concurrent shutdown", async () => {
    const relayFailure = new Error("Relay drain failed"),
      consumerFailure = new Error("Consumer close failed");
    f.close.mockImplementation(async (resource) => {
      if (resource === "relay") throw relayFailure;
      if (resource === "consumer") throw consumerFailure;
    });
    const viewClose = vi.spyOn(projector.eventStore, "close");
    const first = projector.close(),
      second = projector.close();
    const results = await Promise.allSettled([first, second]);
    shutdownAsserted = true;
    expect(first).toBe(second);
    expect(results[0]).toMatchObject({
      status: "rejected",
      reason: { errors: [relayFailure, consumerFailure] },
    });
    expect(f.close.mock.calls.map(([resource]) => resource)).toEqual([
      "relay",
      "consumer",
      "tools",
      "subagents",
      "retention",
      "producer",
    ]);
    expect(viewClose).toHaveBeenCalledOnce();
  });
  it("decodes once and passes the same decoded record to projections without JSON round trips", async () => {
    const r = record();
    r.fact = f.decode!(Buffer.from(JSON.stringify(r.fact)));
    expect(f.parse).toHaveBeenCalledOnce();
    Object.defineProperty(r.fact, "toJSON", {
      value: () => {
        throw new Error("Decoded facts must not be reserialized");
      },
    });
    await f.handler!(r);
    expect(f.parse).toHaveBeenCalledOnce();
    expect(f.project.mock.calls[0]![0].fact).toBe(r.fact);
    expect(route).not.toHaveBeenCalled();
  });

  it("waits for seal projection before displaying the terminal without a second execution-stop RPC", async () => {
    const commit = deferred<ExecutionProjectionResult>();
    f.project.mockReturnValue(commit.promise);
    const display = vi.spyOn(projector.eventHub, "publish");
    const handling = f.handler!(record("session", "execution_seal"));
    await vi.waitFor(() => expect(f.project).toHaveBeenCalledOnce());
    expect(display).not.toHaveBeenCalled();
    expect(route).not.toHaveBeenCalled();
    commit.resolve({ terminal: event("session", "turn.completed"), canonicalThroughSequence: 1 });
    await handling;
    expect(display).toHaveBeenCalledOnce();
    expect(route).not.toHaveBeenCalled();
    expect(projector.eventStore.statistics().cachedEvents).toBe(0);
  });

  it("stops a retired partition handler after a PG await, before UI or Tool delivery", async () => {
    const commit = deferred<ExecutionProjectionResult>();
    f.project.mockReturnValue(commit.promise);
    let current = true;
    const display = vi.spyOn(projector.eventHub, "publish");
    const handling = f.handler!(record(), () => current);
    await vi.waitFor(() => expect(f.project).toHaveBeenCalledOnce());
    current = false;
    commit.resolve({});
    await handling;
    expect(display).not.toHaveBeenCalled();
    expect(route).not.toHaveBeenCalled();
  });

  it("replays seal projection without duplicating its live terminal or execution closure", async () => {
    f.project.mockResolvedValue({
      terminal: event("session", "turn.completed"),
      canonicalThroughSequence: 1,
    });
    const r = record("session", "execution_seal"),
      display = vi.spyOn(projector.eventHub, "publish");
    await f.handler!(r);
    await f.handler!(r);
    expect(f.project).toHaveBeenCalledTimes(2); // PG idempotency is tested with real PG elsewhere.
    expect(route).not.toHaveBeenCalled();
    expect(display).toHaveBeenCalledOnce();
  });

  it("does not put unrelated partition handlers behind a pending owner-delivery ACK", async () => {
    const delivery = deferred<void>();
    route.mockImplementationOnce(() => delivery.promise);
    const { fact } = toolInvocationFixture();
    f.project.mockImplementation(async (record) => ({
      events: [event(record.partition ? "b" : "a")],
    }));
    const a = f.handler!(
      { ...record("a", "durable_commit", 0), fact },
      () => true,
      async () => true,
    );
    await vi.waitFor(() => expect(route).toHaveBeenCalledOnce());
    await f.handler!(
      { ...record("b", "durable_commit", 1), fact },
      () => true,
      async () => true,
    );
    expect(route).toHaveBeenCalledTimes(2);
    expect(projector.eventStore.snapshot(fact.scope.tenantId, "b").events).toHaveLength(1);
    delivery.resolve();
    await a;
  });
});
