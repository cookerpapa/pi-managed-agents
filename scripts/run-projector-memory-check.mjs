import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdir, writeFile } from "node:fs/promises";
import { resolve, dirname } from "node:path";
import {
  ExecutionStreamBoundary,
  DEFAULT_BOUNDARY_CACHE_LIMITS,
} from "../packages/runtime-core/src/execution-stream-boundary.ts";
import { SessionLiveView } from "../packages/runtime-core/src/session-live-view.ts";

assert.equal(typeof global.gc, "function", "Run with node --expose-gc --import tsx");
const output = resolve(process.argv[2] ?? ".cache/projector-memory.json");
let databaseReads = 0;
const database = {
  selectFrom() {
    const values = new Map();
    const q = {
      innerJoin(_table, join) {
        join({
          on(k, _op, v) {
            values.set(k, v);
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
      where(k, _op, v) {
        values.set(k, v);
        return q;
      },
      async executeTakeFirst() {
        databaseReads++;
        return {
          session_id: values.get("run.id"),
          lease_id: values.get("writer.lease_id"),
          fencing_token: "1",
          writer_seal_offset: null,
          output_sealed_at: null,
          output_seal_offset: null,
          output_first_topic: null,
          output_first_partition: null,
          output_first_offset: null,
        };
      },
    };
    return q;
  },
};
const cache = new ExecutionStreamBoundary(database);
const tenantId = randomUUID(),
  turnId = randomUUID();
const identity = () => {
  const sessionId = randomUUID();
  return {
    tenantId,
    sessionId,
    piSessionId: sessionId,
    writerId: randomUUID(),
    runId: randomUUID(),
    turnId,
    fencingToken: 1,
  };
};
const fact = (s) => ({ topic: "memory", partition: 0, offset: 0n, fact: { scope: s } });
global.gc();
const before = process.memoryUsage().heapUsed,
  samples = [];
let last;
for (let i = 1; i <= 100000; i++) {
  last = identity();
  assert(await cache.isOpen(fact(last), false));
  if ([10000, 50000, 100000].includes(i)) {
    global.gc();
    const state = cache.statistics();
    assert(state.entries <= DEFAULT_BOUNDARY_CACHE_LIMITS.maximumEntries);
    assert(state.estimatedBytes <= DEFAULT_BOUNDARY_CACHE_LIMITS.maximumBytes);
    samples.push({
      identities: i,
      heapGrowthBytes: process.memoryUsage().heapUsed - before,
      cache: state,
    });
  }
}
assert.equal(
  samples[1].cache.entries,
  samples[2].cache.entries,
  "Cache did not reach a bounded working set",
);
const reads = databaseReads;
for (let i = 0; i < 1000; i++) assert(await cache.isOpen(fact(last), false));
assert.equal(databaseReads, reads, "Hot identity reloaded PG");
cache.reset();
global.gc();
const afterReset = {
  heapGrowthBytes: process.memoryUsage().heapUsed - before,
  cache: cache.statistics(),
};
assert.equal(afterReset.cache.entries, 0);
assert.equal(afterReset.cache.estimatedBytes, 0);
const view = new SessionLiveView(async () => {}),
  active = randomUUID();
const event = (sessionId, text) => ({
  schemaVersion: 1,
  eventId: randomUUID(),
  sessionId,
  turnId,
  agentId: "root",
  seq: 1,
  occurredAt: new Date().toISOString(),
  type: "assistant.text.delta",
  payload: { text },
});
let live;
try {
  view.accept(tenantId, event(active, "x".repeat(1024 * 1024)));
  for (let i = 0; i < 20000; i++) {
    const id = randomUUID();
    view.accept(tenantId, event(id, "done"));
    view.cover(tenantId, id, 1);
  }
  live = { beforeSettlement: view.statistics() };
  assert.equal(live.beforeSettlement.activeTextTails, 1);
  assert(live.beforeSettlement.emptyHolders <= 4096);
  assert(live.beforeSettlement.emptyEstimatedBytes <= 4 * 1024 * 1024);
  assert.equal(view.snapshot(tenantId, active).events[0].payload.text.length, 1024 * 1024);
  view.cover(tenantId, active, 1);
  live.afterSettlement = view.statistics();
  assert.equal(live.afterSettlement.cachedBytes, 0);
} finally {
  view.close();
}
await mkdir(dirname(output), { recursive: true, mode: 0o700 });
const report = {
  format: "pma.projector-memory-check.v1",
  at: new Date().toISOString(),
  node: process.version,
  scope:
    "Mock PG authority; real cache/live-view implementations. Forced-GC heap deltas, not a process RSS bound.",
  limits: DEFAULT_BOUNDARY_CACHE_LIMITS,
  samples,
  afterReset,
  hotReads: 1000,
  hotSqlQueries: databaseReads - reads,
  live,
};
await writeFile(output, JSON.stringify(report, null, 2) + "\n", { mode: 0o600 });
console.log(JSON.stringify(report));
