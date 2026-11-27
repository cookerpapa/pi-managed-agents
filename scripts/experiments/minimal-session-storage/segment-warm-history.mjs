// Each cache contains only immutable Entry bodies. PG confirms the terminal
// version before restoring any of them into a freshly authorized Pi instance.
import { SegmentDiskHistory } from "./segment-disk-history.mjs";
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
    now = Date.now,
    diskDirectory,
    diskMaxBytes = 20 * 1024 ** 3,
    diskTtlMs = 24 * 60 * 60 * 1000,
    advertise,
  } = {}) {
    if (
      ![minBytes, maxBytes, diskMaxBytes].every((v) => Number.isSafeInteger(v) && v >= 0) ||
      !Number.isSafeInteger(ttlMs) ||
      ttlMs <= 0 ||
      !Number.isSafeInteger(diskTtlMs) ||
      diskTtlMs <= 0
    )
      throw Error("Invalid warm history cache limits");
    this.closed = false;
    this.advertise = advertise;
    this.now = now;
    this.cache = new PrefixCache({
      minBytes,
      maxBytes,
      ttlMs,
      now,
      onEvict: (key, v) => this.notice(key, v),
    });
    if (diskDirectory && diskMaxBytes)
      this.disk = new SegmentDiskHistory({
        directory: diskDirectory,
        maxBytes: diskMaxBytes,
        ttlMs: diskTtlMs,
        now,
        changed: (key, version) => this.notice(key, version),
      });
    this.timer = setInterval(
      () => {
        this.cache.expire();
        this.disk?.refresh();
      },
      Math.min(ttlMs, diskTtlMs, 60000),
    );
    this.timer.unref();
  }
  notice(key, version) {
    const [tenant, flow] = key.split("/");
    const memory = this.cache.items.get(key),
      disk = this.disk?.heads.get(key);
    if (!this.closed && memory?.version === version && memory.expires > this.now())
      this.advertise?.(
        tenant,
        flow,
        version,
        memory.bytes >= expensiveHistoryBytes ? "large" : "keep",
        memory.expires,
      );
    else if (!this.closed && disk?.version === version && disk.expires > this.now())
      this.advertise?.(
        tenant,
        flow,
        version,
        disk.bytes >= expensiveHistoryBytes ? "disk-large" : "disk",
        disk.expires,
      );
    else this.advertise?.(tenant, flow, version, "drop");
  }
  advertiseAll() {
    this.cache.expire();
    for (const [key, item] of this.cache.items) this.notice(key, item.version);
    for (const [key, item] of this.disk?.heads ?? [])
      if (this.cache.items.get(key)?.version !== item.version) this.notice(key, item.version);
  }
  close() {
    this.closed = true;
    clearInterval(this.timer);
    for (const key of this.cache.items.keys()) this.cache.remove(key);
    for (const [key, item] of this.disk?.heads ?? []) this.notice(key, item.version);
    return this.disk?.close();
  }
  get hasSnapshots() {
    return this.cache.items.size > 0 || Boolean(this.disk?.heads.size);
  }
  async sourceSnapshot(tenant, flow, head, range) {
    if (this.closed || !head?.version) return;
    const key = tenant + "/" + flow;
    let hit = this.cache.get(key, head.version);
    if (!hit && this.disk) {
      const diskHead = this.disk.heads.get(key);
      if (diskHead?.version === head.version)
        hit = await this.disk.get(key, diskHead.session, head.version, head, range);
    }
    if (
      !hit ||
      hit.seq !== head.seq ||
      hit.floor !== head.floor ||
      (hit.locals ?? hit.records.length) !== head.count ||
      hit.localBytes !== head.bytes
    )
      return;
    return hit;
  }
  capture(job) {
    const disk = this.disk?.disabled ? undefined : this.disk;
    if (this.closed || (!disk && this.cache.maxBytes === 0)) return;
    const key = job.tenant_id + "/" + job.flow_id;
    const budget = Math.max(this.cache.maxBytes, disk?.maxBytes ?? 0);
    let base = null,
      additions = [];
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
        additions = [];
      }
    };
    return {
      get: (_, version, head) => {
        base = version;
        const item = this.cache.get(key, version);
        if (item?.session === job.logical_id) return item;
        return disk?.get(key, job.logical_id, version, head);
      },
      seed(ancestors, previous = [], initialFloor = 1, acceptedBase = base) {
        base = acceptedBase;
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
              additions = [];
              return;
            }
            floor = entry.head;
            inherited = inherited.filter((r) => r.entry.id >= floor);
            inheritedBytes = measure(inherited);
            additions = additions.filter((r) => r.entry.id >= floor);
            local = local.filter((r) => r.entry.id >= floor);
            localBytes = measure(local);
          }
          if (entry.id < floor) continue;
          const bytes = entryBytes(entry, budget - localBytes - inheritedBytes);
          const record = { entry, seq: batch.seq, bytes };
          local.push(record);
          if (disk) additions.push(record);
          localBytes += bytes;
          bound();
          if (oversize) return;
        }
      },
      finish: (fact) => {
        if (
          finished ||
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
        if (oversize || localBytes + inheritedBytes < this.cache.minBytes) {
          if (base) return disk?.forget(key, base);
          return;
        }
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
          this.notice(key, fact.factId);
        }
        if (disk) {
          return disk.put(
            {
              k: key,
              session: job.logical_id,
              version: fact.factId,
              fence: job.scope.fencingToken,
              seq: fact.nativeSequence,
              floor,
              bytes: inheritedBytes + localBytes,
              localBytes,
              locals: local.length,
              ancestors: inherited.length,
            },
            base,
            inherited,
            local,
            additions,
          );
        }
      },
    };
  }
}
