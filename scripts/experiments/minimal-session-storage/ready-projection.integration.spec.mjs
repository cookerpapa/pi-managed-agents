import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import native from "@confluentinc/kafka-javascript";
import pg from "pg";
import { expect, it, vi } from "vitest";
import { KafkaLogConsumer } from "../../../packages/event-log/src/kafka-log-consumer.ts";
import { ReadyProjection } from "./ready-projection.mjs";
import { projectionTemplates } from "./projection-statements.mjs";

const url = process.env.PI_CLOUD_POSTGRES_INTEGRATION_URL;
const brokers = process.env.PI_CLOUD_KAFKA_INTEGRATION_BROKERS?.split(",");
const now = () => true;

// Use the same guarded SQL as the Pi actor. The small fact fixture lets us
// place the process failure precisely, without executing a shell command.
async function fixture(test) {
  const schema = "ready_" + randomUUID().replaceAll("-", "");
  const topic = "prefix-" + randomUUID(),
    admin = new pg.Pool({ connectionString: url, max: 1 });
  const db = new pg.Pool({ connectionString: url, max: 4, options: "-c search_path=" + schema });
  const scope = {
    tenantId: randomUUID(),
    piSessionId: randomUUID(),
    runId: randomUUID(),
    writerId: randomUUID(),
  };
  const { tenantId, piSessionId, runId, writerId } = scope;
  const notifications = [];
  const fresh = () => {
    const view = new ReadyProjection(db, topic, new Map(), projectionTemplates);
    view.configure(16);
    return view;
  };
  const record = (n, kind = "message") => ({
    partition: 0,
    offset: BigInt(n),
    topic,
    fact: {
      scope,
      kind: "durable_commit",
      factId: "fact-" + n,
      commit: { seq: n + 1, writes: [] },
      sample: kind,
      ...(kind === "tool" ? { tools: [{ operationId: "test-operation" }] } : {}),
    },
  });
  const fold = async (r, d) => {
    const { fact: f } = r,
      s = f.scope;
    if (f.sample !== "live") {
      const entry = {
        id: f.commit.seq,
        conversation: 1,
        role: "assistant",
        ordinal: 0,
        content: f.factId,
      };
      await d
        .acquire(f)
        .query(projectionTemplates.message, [
          s.tenantId,
          s.piSessionId,
          s.runId,
          f.commit.seq,
          JSON.stringify([entry]),
          s.writerId,
        ]);
    }
    if (f.sample === "queued") {
      await d
        .acquire(f)
        .query(projectionTemplates.inbox, [
          s.tenantId,
          s.piSessionId,
          42,
          f.commit.seq,
          1,
          JSON.stringify(["pending"]),
          s.runId,
          s.writerId,
        ]);
    }
    d.views.set(s.runId, { seq: f.commit.seq, lastFact: f.factId, lastOffset: r.offset });
    d.after.push(() => notifications.push(f.factId));
  };
  const cursor = async () =>
    BigInt(
      (
        await db.query(
          "select next_offset from accepted_fact_projection_offsets where topic=$1 and partition=0",
          [topic],
        )
      ).rows[0].next_offset,
    );
  try {
    await admin.query("create schema " + schema);
    await db.query(await readFile(new URL("./schema.sql", import.meta.url), "utf8"));
    expect((await db.query("show synchronous_commit")).rows[0].synchronous_commit).toBe("on");
    const view = fresh();
    await view.install();
    await view.reset(new Map([[0, 0n]]), 16);
    await db.query(
      "insert into runs(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,model_ref,state,lease_id) values ($1,$2,$3,1,$1,'fixture','local','running',$4)",
      [runId, tenantId, piSessionId, writerId],
    );
    await test({ view, db, admin, scope, topic, record, fold, cursor, fresh, notifications });
  } finally {
    await db.end();
    try {
      await admin.query("drop schema if exists " + schema + " cascade");
    } finally {
      await admin.end();
    }
  }
}

it.skipIf(!url)(
  "rolls back the entire prefix on a disconnected transaction and reconstructs a committed prefix after reply loss",
  async () => {
    await fixture(
      async ({ view, db, admin, scope, topic, record, fold, cursor, fresh, notifications }) => {
        const prefix = [record(0, "live"), record(1), record(2, "queued")];
        const beforeCommit = async () => {
          const c = await db.connect();
          c.on("error", () => {});
          await c.query("begin");
          return {
            async query(sql, args) {
              await c.query(sql, args);
              // The server processed all rows, but another connection still sees the old state.
              expect(await cursor()).toBe(0n);
              await admin.query("select pg_terminate_backend($1)", [c.processID]);
              return c.query("commit");
            },
            release() {
              c.release(true);
            },
          };
        };
        await expect(view.apply(prefix, now, fold, undefined, beforeCommit)).rejects.toThrow();
        expect(await cursor()).toBe(0n);
        expect((await db.query("select * from messages")).rowCount).toBe(0);
        expect((await db.query("select * from inboxes")).rowCount).toBe(0);
        expect(
          (await db.query("select projection_first from runs")).rows[0].projection_first,
        ).toBeNull();
        expect(notifications).toEqual([]);
        expect(view.sessions.size).toBe(0);
        const loseReply = async () => {
          const c = await db.connect();
          return {
            async query(sql, args) {
              await c.query(sql, args);
              throw new Error("reply lost");
            },
            release() {
              c.release();
            },
          };
        };
        await expect(view.apply(prefix, now, fold, undefined, loseReply)).rejects.toThrow(
          "reply lost",
        );
        expect(await cursor()).toBe(3n);
        expect(notifications).toEqual([]);
        await view.apply(prefix, now, fold);
        expect(view.sessions.get(scope.runId).seq).toBe(3);
        expect(notifications).toEqual([]);
        expect((await db.query("select * from messages")).rowCount).toBe(2);
        expect((await db.query("select items from inboxes")).rows).toEqual([
          { items: ["pending"] },
        ]);

        const replacement = fresh();
        const bounds = [{ partition: 0, low: 0n, high: 4n }];
        expect((await replacement.recover(bounds)).get(0)).toBe(0n);
        await replacement.apply(prefix, now, fold);
        expect(replacement.sessions.get(scope.runId).seq).toBe(3);
        expect(
          (await replacement.recover([{ partition: 0, low: 1n, high: 4n }])).get(0),
        ).toBeInstanceOf(Error);
        await db.query("update runs set lease_id=$1 where id=$2", [randomUUID(), scope.runId]);
        replacement.revoke();
        await replacement.recover(bounds);
        await replacement.apply(prefix, now, fold);
        expect(replacement.sessions.size).toBe(0);
        const stale = record(3, "tool");
        await replacement.apply([stale], now, fold);
        expect(replacement.canDeliver(stale)).toBe(false);
        expect(await cursor()).toBe(4n);
        expect(notifications).toEqual([]);
        expect(
          (
            await db.query(
              "select count(*)::int n from accepted_fact_projection_offsets where topic=$1",
              [topic],
            )
          ).rows[0].n,
        ).toBe(1);
      },
    );
  },
  30000,
);

it.skipIf(!url)(
  "isolates a stale Run without acknowledging its valid predecessor or overwriting a conflicting message",
  async () => {
    await fixture(async ({ view, db, scope, record, fold, cursor, notifications }) => {
      const a = record(0),
        stale = record(1, "tool");
      stale.fact.scope = { ...scope, runId: randomUUID(), piSessionId: randomUUID() };
      await expect(view.apply([a, stale], now, fold)).rejects.toMatchObject({ code: "PZ001" });
      expect(await cursor()).toBe(0n);
      expect(notifications).toEqual([]);
      expect((await db.query("select * from messages")).rowCount).toBe(0);
      await view.apply([a], now, fold);
      await view.apply([stale], now, fold);
      expect(view.canDeliver(stale)).toBe(false);
      const conflict = record(2);
      conflict.fact.commit.seq = 1;
      await expect(view.apply([conflict], now, fold)).rejects.toMatchObject({ code: "PZ002" });
      expect(await cursor()).toBe(2n);
      expect(notifications).toEqual(["fact-0"]);
      expect((await db.query("select content from messages")).rows).toEqual([
        { content: "fact-0" },
      ]);
    });
  },
);

it.skipIf(!url || !brokers)(
  "recovers a pending dispatch before its Kafka grant and suppresses it after the grant",
  async () => {
    await fixture(async ({ view, db, scope, topic, record, fold, cursor, fresh }) => {
      const kafka = new native.KafkaJS.Kafka({
        kafkaJS: { brokers, logLevel: native.KafkaJS.logLevel.NOTHING },
      });
      const admin = kafka.admin(),
        producer = kafka.producer({ kafkaJS: { acks: -1 } });
      const effects = [],
        entered = Promise.withResolvers(),
        release = Promise.withResolvers();
      const grantOnly = Promise.withResolvers(),
        leaveGrant = Promise.withResolvers();
      const create = (projection, phase) =>
        new KafkaLogConsumer({
          brokers,
          topic,
          groupId: topic,
          clientId: topic + phase,
          groupRecovery: true,
          orderedEffects: true,
          batchRead: true,
          onRevoked: () => projection.revoke(),
          replayOffsets: (bounds) => projection.recover(bounds),
          decode: (v) => JSON.parse(v.toString()),
          prepareReady: (records, current) => projection.apply(records, current, fold),
          handler: async (r, current, grant) => {
            if (!r.fact.tools?.length || !projection.canDeliver(r)) return;
            if (phase === 1) {
              entered.resolve();
              await release.promise;
            }
            if (!current() || !(await grant())) return;
            if (r.offset === 2n && phase === 2) {
              grantOnly.resolve();
              await leaveGrant.promise;
              if (!current()) return;
            }
            effects.push(r.offset);
          },
        });
      let reader;
      const send = async (items) =>
        producer.send({
          topic,
          messages: items.map((r) => ({ partition: 0, value: JSON.stringify(r.fact) })),
        });
      try {
        await admin.connect();
        await admin.createTopics({
          topics: [
            {
              topic,
              numPartitions: 1,
              replicationFactor: 3,
              configEntries: [{ name: "min.insync.replicas", value: "2" }],
            },
          ],
        });
        await producer.connect();
        reader = create(view, 1);
        await reader.start();
        await reader.waitUntilAssigned(20000);
        await send([record(0), record(1, "tool")]);
        await entered.promise;
        expect(await cursor()).toBe(2n);
        expect(effects).toEqual([]);
        let closing = reader.close();
        release.resolve();
        await closing;
        reader = create(fresh(), 2);
        await reader.start();
        await reader.waitUntilAssigned(20000);
        await vi.waitFor(() => expect(effects).toEqual([1n]), { timeout: 8000 });
        await send([record(2, "tool")]);
        await grantOnly.promise;
        expect(await cursor()).toBe(3n);
        expect(
          (await admin.fetchOffsets({ groupId: topic, topics: [topic] }))[0].partitions[0].offset,
        ).toBe("3");
        closing = reader.close();
        leaveGrant.resolve();
        await closing;
        reader = create(fresh(), 3);
        await reader.start();
        await reader.waitUntilAssigned(20000);
        await send([record(3)]);
        await reader.waitUntilInitialReplay([4n], 10000);
        expect(effects).toEqual([1n]);
        expect(await cursor()).toBe(4n);
        expect(
          (await db.query("select count(*)::int n from messages where run_id=$1", [scope.runId]))
            .rows[0].n,
        ).toBe(4);
      } finally {
        release.resolve();
        leaveGrant.resolve();
        await reader?.close();
        await producer.disconnect();
        try {
          await admin.deleteTopics({ topics: [topic] });
        } finally {
          await admin.disconnect();
        }
      }
    });
  },
  60000,
);
