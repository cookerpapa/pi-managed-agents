import { existsSync } from "node:fs";
import { join } from "node:path";
import { Worker } from "node:worker_threads";

// All SQLite work, hashing and JSON encoding run on one dedicated I/O thread.
// Batch backpressure also bounds the bytes awaiting structured-clone delivery.
export class SegmentDiskHistory {
  heads = new Map();
  pending = new Map();
  tail = Promise.resolve();
  next = 0;
  writes = 0;
  reserved = 0;
  pendingVersions = new Set();
  constructor({ directory, maxBytes, ttlMs, now, changed }) {
    Object.assign(this, { directory, maxBytes, ttlMs, now, changed });
    this.opening = existsSync(join(directory, "history.sqlite"));
    if (this.opening)
      this.refresh().finally(() => {
        this.opening = false;
      });
  }
  queue(work) {
    const current = this.tail
      .then(async () => (this.disabled ? undefined : work()))
      .catch(() => {
        if (!this.warned) {
          this.warned = true;
          console.warn("segment.history_disk_unavailable");
        }
      });
    this.tail = current;
    return current;
  }
  start() {
    if (this.worker) return;
    const w = new Worker(new URL("./segment-history-sqlite.mjs", import.meta.url), {
      execArgv: [],
      workerData: {
        directory: this.directory,
        maxBytes: this.maxBytes,
        ttlMs: this.ttlMs,
      },
    });
    this.worker = w;
    w.on("message", (message) => {
      const request = this.pending.get(message.id);
      if (!request) return;
      this.pending.delete(message.id);
      clearTimeout(request.timeout);
      if (message.error)
        request.reject(Object.assign(Error("Cache I/O failed"), { code: message.error }));
      else request.resolve(message.result);
      if (!this.pending.size) w.unref();
    });
    const failed = () => {
      this.disabled = true;
      for (const request of this.pending.values()) {
        clearTimeout(request.timeout);
        request.reject(Error("Cache I/O stopped"));
      }
      this.pending.clear();
      for (const [key, h] of this.heads) {
        this.heads.delete(key);
        this.changed(key, h.version);
      }
    };
    w.on("error", failed);
    w.once("exit", failed);
    w.unref();
  }
  rpc(op, args = {}) {
    if (this.disabled) throw Error("Cache is unavailable");
    this.start();
    const request = Promise.withResolvers(),
      id = ++this.next;
    request.timeout = setTimeout(() => {
      // A frozen cache device must not keep a new Turn waiting indefinitely.
      this.disabled = true;
      this.pending.delete(id);
      request.reject(Error("Cache I/O timed out"));
      void this.worker.terminate();
    }, 30000);
    this.pending.set(id, request);
    this.worker.ref();
    try {
      this.worker.postMessage({ id, op, ...args });
    } catch (e) {
      this.pending.delete(id);
      clearTimeout(request.timeout);
      request.reject(e);
    }
    return request.promise;
  }
  discard(key) {
    const old = this.heads.get(key);
    if (old) {
      this.heads.delete(key);
      this.changed(key, old.version);
    }
  }
  refresh() {
    if (this.closed || (!this.worker && !this.opening)) return this.tail;
    if (this.refreshing) return this.refreshing;
    this.refreshing = this.queue(async () => {
      const list = await this.rpc("list", { now: this.now() });
      const keys = new Set(list.map((h) => h.k));
      for (const key of this.heads.keys()) if (!keys.has(key)) this.discard(key);
      for (const h of list) {
        const old = this.heads.get(h.k);
        if (old?.version !== h.version) this.discard(h.k);
        this.heads.set(h.k, h);
        if (old?.version !== h.version || old?.expires !== h.expires) this.changed(h.k, h.version);
      }
    }).finally(() => {
      this.refreshing = undefined;
    });
    return this.refreshing;
  }
  usage() {
    if (this.closed || !this.worker) return;
    return this.queue(() => this.rpc("usage"));
  }
  forget(key, version) {
    if (this.closed || !this.worker) return;
    return this.queue(async () => {
      await this.rpc("forget", { key, version });
      if (this.heads.get(key)?.version === version) this.discard(key);
    });
  }
  get(key, session, version, head, range) {
    if (
      this.closed ||
      (!this.opening &&
        this.heads.get(key)?.version !== version &&
        !this.pendingVersions.has(key + "/" + version))
    )
      return;
    return this.queue(async () => {
      const known = this.heads.get(key);
      if (!known || known.session !== session || known.version !== version) return;
      if (known.expires <= this.now()) {
        this.discard(key);
        return;
      }
      try {
        const h = await this.rpc("read", {
          key,
          session,
          version,
          now: this.now(),
          range,
          expected: head
            ? { seq: head.seq, floor: head.floor, locals: head.count, localBytes: head.bytes }
            : null,
        });
        if (!h) {
          this.discard(key);
          await this.rpc("forget", { key, version });
          return;
        }
        const records = [],
          inherited = [];
        let bytes = 0,
          localBytes = 0,
          readBytes = 0,
          done = false;
        while (!done) {
          const batch = await this.rpc("batch", { key });
          done = batch.done;
          readBytes += batch.bytes;
          for (const { entry, seq, bytes: weight, inherited: ancestor } of batch.rows) {
            (ancestor ? inherited : records).push({ entry, seq, bytes: weight });
            bytes += weight;
            if (!ancestor) localBytes += weight;
          }
          if (bytes > this.maxBytes) throw Error("Cache coverage invalid");
        }
        if (
          !range &&
          (bytes !== h.bytes ||
            localBytes !== h.localBytes ||
            records.length !== h.locals ||
            inherited.length !== h.ancestors)
        )
          throw Error("Cache coverage invalid");
        return { ...h, records, inherited, diskReadBytes: readBytes };
      } catch {
        this.discard(key);
        await this.rpc("abort");
        await this.rpc("forget", { key, version });
      }
    });
  }
  put(head, base, ancestors, local, additions) {
    if (
      this.closed ||
      this.disabled ||
      head.bytes > this.maxBytes ||
      this.writes >= 16 ||
      (this.writes && this.reserved + head.bytes > 256 * 1024 * 1024)
    )
      return;
    const publication = head.k + "/" + head.version;
    if (this.pendingVersions.has(publication)) return;
    this.pendingVersions.add(publication);
    this.writes++;
    this.reserved += head.bytes;
    return this.queue(async () => {
      for (;;) {
        try {
          return await this.write(head, base, ancestors, local, additions);
        } catch (e) {
          await this.rpc("abort");
          if (e.code !== "full") throw e;
          const key = await this.rpc("evict", { except: head.k });
          if (!key) throw e;
          this.discard(key);
        }
      }
    }).finally(() => {
      this.pendingVersions.delete(publication);
      this.writes--;
      this.reserved -= head.bytes;
    });
  }
  async write(head, base, ancestors, local, additions) {
    const result = await this.rpc("begin", { head, base, now: this.now() });
    if (result.skip) return;
    for (const [inherited, rows] of result.delta
      ? [[false, additions]]
      : [
          [true, ancestors],
          [false, local],
        ]) {
      let batch = [],
        bytes = 0;
      for (const { entry, seq, bytes: weight } of rows) {
        batch.push({ entry, seq, bytes: weight });
        bytes += weight;
        if (bytes >= 4 * 1024 * 1024 || batch.length >= 128) {
          await this.rpc("append", { inherited, rows: batch });
          batch = [];
          bytes = 0;
        }
      }
      if (batch.length) await this.rpc("append", { inherited, rows: batch });
    }
    const receipt = await this.rpc("commit");
    for (const key of receipt.evicted) this.discard(key);
    this.discard(head.k);
    this.heads.set(head.k, receipt.end);
    this.changed(head.k, head.version);
    return receipt;
  }
  async close() {
    if (this.closed) return;
    this.closed = true;
    await this.tail;
    if (this.worker) {
      try {
        if (!this.disabled) await this.rpc("close");
      } finally {
        await this.worker.terminate();
      }
    }
  }
}
