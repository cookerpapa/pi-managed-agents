// Each cache contains only immutable Entry bodies. PG confirms the terminal
// version before restoring any of them into a freshly authorized Pi instance.
import { PrefixCache } from "./session-prefix.mjs";
export const expensiveHistoryBytes = 16 * 1024 * 1024;
// Count the exact PG wire representation without allocating a second giant
// JSON string for ordinary base64 image data. A value already over budget can
// be rejected using its string length as a lower bound.
function entryBytes(entry, remaining) {
  let omitted = 0;
  const envelope = JSON.stringify(entry, (_key, value) => {
    if (typeof value !== "string" || value.length < 4096) return value;
    if (value.length > remaining - omitted) {
      omitted = remaining + 1;
      return "";
    }
    if (/["\\\u0000-\u001f\ud800-\udfff]/u.test(value)) return value;
    omitted += Buffer.byteLength(value);
    return "";
  });
  return Buffer.byteLength(envelope) + omitted;
}
export class SegmentWarmHistory {
  constructor({
    minBytes = 256 * 1024,
    maxBytes = 256 * 1024 * 1024,
    ttlMs = 300000,
    now,
    advertise,
  } = {}) {
    if (
      ![minBytes, maxBytes].every((v) => Number.isSafeInteger(v) && v >= 0) ||
      !Number.isSafeInteger(ttlMs) ||
      ttlMs <= 0
    )
      throw Error("Invalid warm history cache limits");
    this.closed = false;
    this.advertise = advertise;
    this.cache = new PrefixCache({
      minBytes,
      maxBytes,
      ttlMs,
      now,
      onEvict: (key, v) => this.notice(key, v, "drop"),
    });
    this.timer = setInterval(() => this.cache.expire(), Math.min(ttlMs, 60000));
    this.timer.unref();
  }
  notice(key, version, action) {
    const [tenant, flow] = key.split("/");
    this.advertise?.(tenant, flow, version, action);
  }
  advertiseAll() {
    this.cache.expire();
    for (const [key, item] of this.cache.items)
      this.notice(key, item.version, item.bytes >= expensiveHistoryBytes ? "large" : "keep");
  }
  close() {
    this.closed = true;
    clearInterval(this.timer);
    for (const key of this.cache.items.keys()) this.cache.remove(key);
  }
  capture(job) {
    if (this.closed || this.cache.maxBytes === 0) return;
    const key = job.tenant_id + "/" + job.flow_id;
    const budget = this.cache.maxBytes;
    let inherited = [],
      local = [],
      localBytes = 0,
      inheritedBytes = 0,
      floor = 1,
      finished = false,
      oversize = false;
    const measure = (rows) => rows.reduce((n, r) => n + r.bytes, 0);
    const bound = () => {
      if (localBytes + inheritedBytes > budget) {
        oversize = true;
        local = [];
        inherited = [];
      }
    };
    return {
      get: (_, version) => {
        const item = this.cache.get(key, version);
        if (item?.session === job.logical_id) return item;
      },
      seed(ancestors, previous = [], initialFloor = 1) {
        floor = initialFloor;
        inherited = ancestors;
        local = [...previous];
        inheritedBytes = measure(inherited);
        localBytes = measure(local);
        bound();
      },
      append(batch) {
        if (oversize || finished) return;
        for (const w of batch.writes) {
          if (w.type !== "entry" || w.value.conversationId !== 1) continue;
          const entry = w.value;
          if (entry.head !== undefined) {
            if (entry.head < floor) {
              oversize = true;
              local = [];
              inherited = [];
              return;
            }
            floor = entry.head;
            inherited = inherited.filter((r) => r.entry.id >= floor);
            inheritedBytes = measure(inherited);
            local = local.filter((r) => r.entry.id >= floor);
            localBytes = measure(local);
          }
          if (entry.id < floor) continue;
          const bytes = entryBytes(entry, budget - localBytes - inheritedBytes);
          local.push({ entry, seq: batch.seq, bytes });
          localBytes += bytes;
          bound();
          if (oversize) return;
        }
      },
      finish: (fact) => {
        if (
          finished ||
          oversize ||
          this.closed ||
          fact.kind !== "work_finished" ||
          fact.terminal.type !== "turn.completed" ||
          fact.scope.tenantId !== job.tenant_id ||
          fact.scope.sessionId !== job.logical_id ||
          fact.scope.fencingToken !== job.scope.fencingToken ||
          fact.scope.writerId !== job.scope.writerId ||
          fact.scope.runId !== job.id
        )
          return;
        finished = true;
        this.cache.remove(key);
        if (
          this.cache.retain(
            key,
            fact.factId,
            {
              session: job.logical_id,
              records: Object.freeze(local),
              localBytes,
              inherited: Object.freeze(inherited),
              seq: fact.nativeSequence,
              floor,
            },
            localBytes + inheritedBytes,
          )
        ) {
          this.notice(
            key,
            fact.factId,
            localBytes + inheritedBytes >= expensiveHistoryBytes ? "large" : "keep",
          );
        }
      },
    };
  }
}
