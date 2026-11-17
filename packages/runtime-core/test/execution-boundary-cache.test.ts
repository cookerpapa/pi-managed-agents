import { expect, it } from "vitest";
import type { Kysely } from "kysely";
import type { Database } from "@pi-cloud/database";
import type { AcceptedFactScope } from "../src/accepted-fact.ts";
import type { KafkaAcceptedFactRecord } from "../src/kafka-accepted-fact-consumer.ts";
import { ExecutionStreamBoundary } from "../src/execution-stream-boundary.ts";

const scope = (): AcceptedFactScope => ({
  tenantId: crypto.randomUUID(),
  sessionId: crypto.randomUUID(),
  piSessionId: crypto.randomUUID(),
  writerId: crypto.randomUUID(),
  runId: crypto.randomUUID(),
  turnId: crypto.randomUUID(),
  fencingToken: 1,
});
const record = (s: AcceptedFactScope, offset = 12n) =>
  ({ topic: "log", partition: 0, offset, fact: { scope: s } }) as KafkaAcceptedFactRecord;
function fixture(maximumEntries = 10, maximumBytes = 65536) {
  const rows = new Map<string, any>(),
    reads = new Map<string, number>(),
    held = new Map<
      string,
      {
        entered: ReturnType<typeof Promise.withResolvers<void>>;
        release: ReturnType<typeof Promise.withResolvers<void>>;
      }
    >();
  let failure: Error | undefined;
  const id = (s: AcceptedFactScope) => s.runId + ":" + s.writerId;
  const database = {
    selectFrom() {
      const fields = new Map<string, unknown>();
      const q: any = {
        innerJoin(_table: unknown, join: any) {
          join({
            on(k: string, _op: unknown, v: unknown) {
              fields.set(k, v);
              return this;
            },
            onRef() {
              return this;
            },
          });
          return q;
        },
        select() {
          return q;
        },
        where(k: string, _op: unknown, v: unknown) {
          fields.set(k, v);
          return q;
        },
        async executeTakeFirst() {
          const key = fields.get("run.id") + ":" + fields.get("writer.lease_id");
          reads.set(key, (reads.get(key) ?? 0) + 1);
          if (failure) throw failure;
          const row = rows.get(key),
            snapshot = row ? { ...row } : undefined,
            gate = held.get(key);
          held.delete(key);
          if (gate) {
            gate.entered.resolve();
            await gate.release.promise;
          }
          return snapshot;
        },
      };
      return q;
    },
  } as unknown as Kysely<Database>;
  const cache = new ExecutionStreamBoundary(database, { maximumEntries, maximumBytes });
  return {
    cache,
    reads,
    rows,
    id,
    set(s: AcceptedFactScope, extra: Record<string, unknown> = {}) {
      rows.set(id(s), {
        session_id: s.sessionId,
        lease_id: s.writerId,
        fencing_token: String(s.fencingToken),
        writer_seal_offset: null,
        output_sealed_at: null,
        output_seal_offset: null,
        output_first_topic: null,
        output_first_partition: null,
        output_first_offset: null,
        ...extra,
      });
    },
    hold(s: AcceptedFactScope) {
      const g = { entered: Promise.withResolvers<void>(), release: Promise.withResolvers<void>() };
      held.set(id(s), g);
      return g;
    },
    fail(error?: Error) {
      failure = error;
    },
  };
}

it("evicts complete identities by recency and reloads the durable seal without clearing healthy entries", async () => {
  const f = fixture(2),
    a = scope(),
    b = scope(),
    c = scope();
  for (const s of [a, b, c]) f.set(s);
  expect(await f.cache.isOpen(record(a), false)).toBe(true);
  await f.cache.isOpen(record(b), false);
  await f.cache.isOpen(record(a), false);
  await f.cache.isOpen(record(c), false);
  expect(f.cache.statistics().entries).toBe(2);
  expect(f.cache.statistics().evictions).toBe(1);
  await f.cache.isOpen(record(b), false);
  expect(f.reads.get(f.id(b))).toBe(2);
  f.set(a, { writer_seal_offset: "10", output_seal_offset: "10", output_sealed_at: new Date() });
  f.cache.closeWriter(a.writerId, 10n);
  expect(await f.cache.isOpen(record(a, 12n), false)).toBe(false);
  const before = f.reads.get(f.id(b));
  f.cache.closeWriter(a.writerId, 10n);
  expect(await f.cache.isOpen(record(b), false)).toBe(true);
  expect(f.reads.get(f.id(b))).toBe(before);
});

it("bounds estimated bytes and does not turn an uncached or failed lookup into permission", async () => {
  const f = fixture(100, 3000);
  for (let i = 0; i < 100; i++) {
    const s = scope();
    f.set(s);
    await f.cache.isOpen(record(s), false);
    expect(f.cache.statistics().estimatedBytes).toBeLessThanOrEqual(3000);
  }
  expect(f.cache.statistics().evictions).toBeGreaterThan(90);
  const missing = scope();
  expect(await f.cache.isOpen(record(missing), false)).toBe(false);
  f.fail(new Error("PG unavailable"));
  await expect(f.cache.isOpen(record(scope()), false)).rejects.toThrow("PG unavailable");
  const small = fixture(1, 1),
    s = scope();
  small.set(s);
  expect(await small.cache.isOpen(record(s), false)).toBe(true);
  expect(small.cache.statistics().entries).toBe(0);
  await small.cache.isOpen(record(s), false);
  expect(small.reads.get(small.id(s))).toBe(2);
});

it("coalesces simultaneous identity loads", async () => {
  const f = fixture(),
    s = scope();
  f.set(s);
  const gate = f.hold(s);
  const calls = Array.from({ length: 20 }, () => f.cache.isOpen(record(s), false));
  await gate.entered.promise;
  expect(f.reads.get(f.id(s))).toBe(1);
  gate.release.resolve();
  expect((await Promise.all(calls)).every(Boolean)).toBe(true);
  expect(f.cache.statistics().coalesced).toBe(19);
});

it("rejects a delayed open snapshot when its writer closes during the read", async () => {
  const f = fixture(),
    s = scope();
  f.set(s);
  const gate = f.hold(s);
  const pending = f.cache.isOpen(record(s, 30n), false);
  await gate.entered.promise;
  f.set(s, { writer_seal_offset: "20", output_seal_offset: "20", output_sealed_at: new Date() });
  f.cache.closeWriter(s.writerId, 20n);
  gate.release.resolve();
  expect(await pending).toBe(false);
  expect(f.reads.get(f.id(s))).toBe(2);
  expect(f.cache.observes(s)).toBe(false);
  expect(await f.cache.isOpen(record(s, 19n), false)).toBe(true);
  expect(await f.cache.isOpen(record(s, 19n), true)).toBe(false);
});

it("does not install a reset generation's late result or remove a successor lookup", async () => {
  const f = fixture(),
    s = scope();
  f.set(s);
  const gate = f.hold(s);
  const pending = f.cache.isOpen(record(s), false);
  const outcome = Promise.allSettled([pending]);
  await gate.entered.promise;
  f.cache.reset();
  f.set(s, { writer_seal_offset: "10", output_seal_offset: "10", output_sealed_at: new Date() });
  expect(await f.cache.isOpen(record(s), false)).toBe(false);
  gate.release.resolve();
  expect((await outcome)[0]!.status).toBe("rejected");
  expect(f.cache.statistics().entries).toBe(1);
  const reads = f.reads.get(f.id(s));
  expect(await f.cache.isOpen(record(s), false)).toBe(false);
  expect(f.reads.get(f.id(s))).toBe(reads);
});

it("reloads active admission independently of an already reconstructed native prefix", async () => {
  const f = fixture(1),
    s = scope(),
    other = scope();
  f.set(s, { output_first_topic: "log", output_first_partition: 0, output_first_offset: "10" });
  f.set(other);
  expect(await f.cache.isOpen(record(s, 10n), true)).toBe(true);
  await f.cache.isOpen(record(other), false);
  expect(await f.cache.isOpen(record(s, 11n), true, (id) => id === s.sessionId)).toBe(true);
  f.cache.reset();
  await expect(f.cache.isOpen(record(s, 11n), true)).rejects.toThrow("prefix is missing");
});

it("an old closure neither closes a new writer nor permits a forged fence", async () => {
  const f = fixture(),
    old = scope(),
    next = { ...old, writerId: crypto.randomUUID(), fencingToken: 2 };
  f.set(old);
  f.set(next);
  await f.cache.isOpen(record(old), false);
  await f.cache.isOpen(record(next), false);
  f.cache.close(old, 20n);
  f.cache.closeWriter(old.writerId, 20n);
  expect(await f.cache.isOpen(record(old, 21n), false)).toBe(false);
  expect(await f.cache.isOpen(record(next, 21n), false)).toBe(true);
  expect(f.cache.observes(next)).toBe(true);
  expect(await f.cache.isOpen(record({ ...next, fencingToken: 3 }), false)).toBe(false);
});
