import { afterEach, expect, it, vi } from "vitest";
import type { Kysely } from "kysely";
import type { Database } from "@pi-cloud/database";
const f = vi.hoisted(() => ({
  handler: undefined as any,
  accepts: vi.fn(async () => false),
  project: vi.fn(async () => ({ admitted: false })),
}));
vi.mock("@pi-cloud/event-log", async (original) => ({
  ...(await original<typeof import("@pi-cloud/event-log")>()),
  KafkaLogConsumer: class {
    constructor(options: any) {
      f.handler = options.handler;
    }
    async close() {}
    ownsPartition() {
      return true;
    }
  },
}));
vi.mock("../src/execution-stream-projection.ts", () => ({
  factEvents: () => [],
  ExecutionStreamProjector: class {
    accepts = f.accepts;
    project = f.project;
    observes() {
      return false;
    }
  },
}));
vi.mock("../src/execution-log-topic.ts", () => ({
  ExecutionLogTopic: class {
    async close() {}
  },
}));
vi.mock("../src/session-outbox-relay.ts", () => ({
  SessionOutboxRelay: class {
    async close() {}
  },
}));
vi.mock("../src/kafka-safe-retention.ts", () => ({
  KafkaSafeRetention: class {
    async close() {}
  },
}));
import { SessionProjector } from "../src/session-projector.ts";

const scope = {
  tenantId: "tenant",
  sessionId: "logical",
  piSessionId: "physical",
  runId: "run",
  writerId: "old",
  fencingToken: 1,
  turnId: "turn",
};
const owners: SessionProjector[] = [];
afterEach(async () => {
  await Promise.all(owners.splice(0).map((p) => p.close()));
  vi.clearAllMocks();
});
function fixture() {
  const tools = vi.fn(async () => {}),
    activity = vi.fn(),
    probes = vi.fn(),
    forget = vi.fn();
  const projector = new SessionProjector({
    database: {} as Kysely<Database>,
    publicationDatabase: {} as Kysely<Database>,
    brokers: ["fixture"],
    clientId: "fixture",
    partitions: 1,
    replicas: 3,
    notificationConnectionString: "postgresql://fixture/db",
    retentionMs: 600000,
    advertisedBaseUrl: "http://fixture:3000",
    publicationToken: "fixture",
    toolCommands: { consume: tools },
    sessionActivity: { consume: activity, reset() {} },
    executionProbes: { consume: probes, reset() {} },
    runtimeRetention: { forget, reset() {} },
  });
  owners.push(projector);
  return { tools, activity, probes, forget };
}

it.each(["session_activity", "path_probe", "work_finished"])(
  "rejects obsolete %s before side effects",
  async (kind) => {
    const effects = fixture();
    await f.handler(
      {
        topic: "log",
        partition: 0,
        offset: 12n,
        fact: { fact: { kind, factId: "late", scope }, tools: [] },
      },
      () => true,
      async () => true,
    );
    expect(f.accepts).toHaveBeenCalledOnce();
    expect(f.project).not.toHaveBeenCalled();
    for (const effect of Object.values(effects)) expect(effect).not.toHaveBeenCalled();
  },
);

it("a rejected native commit cannot dispatch its cancellation or ordinary Tools", async () => {
  const effects = fixture();
  const dispatch = vi.fn(async () => true);
  await f.handler(
    {
      topic: "log",
      partition: 0,
      offset: 12n,
      fact: {
        fact: {
          kind: "durable_commit",
          factId: "late",
          scope,
          commit: { seq: 10, nextId: 2, writes: [] },
          display: [],
          tools: [],
          cancellations: [{ kind: "tool_cancel", scope, operationId: "old-operation" }],
        },
        tools: [{ kind: "tool_command", scope }],
      },
    },
    () => true,
    dispatch,
  );
  expect(f.project).toHaveBeenCalledOnce();
  expect(dispatch).not.toHaveBeenCalled();
  for (const effect of Object.values(effects)) expect(effect).not.toHaveBeenCalled();
});
