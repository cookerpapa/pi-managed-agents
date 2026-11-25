import { ProtocolError } from "@platformatic/kafka";
import type { Database } from "@pi-cloud/database";
import { TOOL_REPLY_RETENTION_MS, toolReplyTopic } from "@pi-cloud/event-log";
import type { Kysely } from "kysely";
import { expect, it, vi } from "vitest";
import { KafkaSafeRetention } from "../src/kafka-safe-retention.ts";

const boot = "10000000-0000-4000-8000-000000000001";
const other = "10000000-0000-4000-8000-000000000002";

function fixture(topics = [toolReplyTopic(boot), toolReplyTopic(other)]) {
  const query: Record<string, any> = {};
  for (const name of ["select", "where"]) query[name] = vi.fn(() => query);
  query.execute = vi.fn(async () => [{ boot_id: boot }]);
  const database = {
    selectFrom: vi.fn(() => query),
  } as unknown as Kysely<Database>;
  const admin = {
    listOffsets: vi.fn(async ({ topics }: any): Promise<any[]> =>
      topics.map(({ name }: { name: string }) => ({
        name,
        partitions: [{ partitionIndex: 0, offset: 4n }],
      })),
    ),
    deleteRecords: vi.fn(async () => []),
    listTopics: vi.fn(async () => topics),
    listGroups: vi.fn(
      async () =>
        new Map([
          [`tool-reply-${boot}`, {}],
          ["unrelated", {}],
        ]),
    ),
    deleteTopics: vi.fn(async () => {}),
    deleteGroups: vi.fn(async () => {}),
    close: vi.fn(async () => {}),
  };
  const reaper = new KafkaSafeRetention({
    database,
    brokers: [],
    brokerTimed: true,
    topic: "log",
    clientId: "test",
    graceMs: 1000,
    admin: admin as any,
  });
  return { reaper, admin, query };
}

it("only reaps positively retired old boots, including groups left after Topic deletion", async () => {
  const { reaper, admin, query } = fixture([]);
  await reaper.sweep(1_000_000);
  expect(query.where).toHaveBeenCalledWith("state", "=", "completed");
  expect(query.where).toHaveBeenCalledWith(
    "completed_at",
    "<",
    new Date(1_000_000 - TOOL_REPLY_RETENTION_MS),
  );
  expect(admin.deleteTopics).not.toHaveBeenCalled();
  expect(admin.deleteGroups).toHaveBeenCalledExactlyOnceWith({ groups: [`tool-reply-${boot}`] });
});

it("treats concurrent deletion as success without swallowing other Kafka failures", async () => {
  const { reaper, admin } = fixture();
  admin.deleteTopics.mockRejectedValue(new AggregateError([new ProtocolError(3)]));
  admin.deleteGroups.mockRejectedValue(new AggregateError([new ProtocolError(69)]));
  await expect(reaper.sweep()).resolves.toBeUndefined();
  expect(admin.deleteTopics).toHaveBeenCalledExactlyOnceWith({ topics: [toolReplyTopic(boot)] });
  admin.deleteTopics.mockRejectedValue(new Error("network lost"));
  await expect(reaper.sweep(Date.now() + 86_400_001)).rejects.toThrow("network lost");
});

it("lets Kafka age the records and only removes empty retired mailboxes once a day", async () => {
  const topic = toolReplyTopic(boot);
  const { reaper, admin } = fixture([topic]);
  admin.listOffsets
    .mockResolvedValueOnce([{ name: topic, partitions: [{ partitionIndex: 0, offset: 19n }] }])
    .mockResolvedValueOnce([{ name: topic, partitions: [{ partitionIndex: 0, offset: 20n }] }]);
  await reaper.sweep(0);
  expect(admin.deleteTopics).not.toHaveBeenCalled();
  expect(admin.deleteGroups).not.toHaveBeenCalled();
  await reaper.sweep(60_000);
  expect(admin.listTopics).toHaveBeenCalledTimes(1);
  await reaper.sweep(86_400_000);
  expect(admin.deleteTopics).toHaveBeenCalledExactlyOnceWith({ topics: [topic] });
  expect(admin.deleteRecords).not.toHaveBeenCalled();
  await reaper.close();
});
