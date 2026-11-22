// Candidate Pi Storage. Native entry reads on a continued branch use one
// MemoryStorage containing its frozen ancestors and its subsequent entries.
// Ordinary runtime records, the allocator and the published batch stay shared.
import assert from "node:assert/strict";
import { MemoryStorage } from "@earendil-works/pi-durable";

export class DelegatedMemoryStorage extends MemoryStorage {
  #entries = new Map(); // conversation -> the native store for its visible entries
  #sources = new Map(); // only explicitly mounted, immutable source conversations
  #docs = new Map();
  #archives = new Set();
  #loads = new Map();
  #closed = false;
  archiveReads = [];

  async ensureSource(source) {
    assert(!this.#closed, "Delegated storage is closed");
    if (!source || !this.loadArchive) return;
    const { lane, at } = source;
    if (!this.deferred?.has(lane) && (await super.conversation(lane))) {
      if (await this.entry(lane, at, {})) return;
    }
    const cached = this.#sources.get(lane);
    if (cached && (await cached.entry(lane, at, {}))) return;
    const key = lane + "/" + at;
    let load = this.#loads.get(key);
    if (!load) {
      load = (async () => {
        const { store, entries, docs } = await this.loadArchive(source);
        if (this.#closed) {
          await store.close();
          throw Error("Delegated storage closed during history read");
        }
        assert(await store.entry(lane, at, {}), "Missing delegated fork anchor");
        this.#sources.set(lane, store);
        this.#entries.set(lane, store);
        this.#archives.add(store);
        for (const id of docs) this.#docs.set(id, store);
        this.archiveReads.push({ lane, at, entries });
      })().finally(() => this.#loads.delete(key));
      this.#loads.set(key, load);
    }
    await load;
  }
  async conversation(id, ctx) {
    return (await super.conversation(id, ctx)) ?? this.#sources.get(id)?.conversation(id, ctx);
  }
  async entry(laneOrId, idOrCtx, ctx) {
    if (ctx !== undefined) {
      const view = this.#entries.get(laneOrId);
      return view ? view.entry(laneOrId, idOrCtx, ctx) : super.entry(laneOrId, idOrCtx, ctx);
    }
    const own = await super.entry(laneOrId, idOrCtx);
    if (own) return own;
    for (const view of this.#archives) {
      const match = await view.entry(laneOrId, idOrCtx);
      if (match) return match;
    }
  }
  scanEntries(query, limit, cursor, ctx) {
    const view = this.#entries.get(query.conversationId);
    return view
      ? view.scanEntries(query, limit, cursor, ctx)
      : super.scanEntries(query, limit, cursor, ctx);
  }
  findLatestHeadMarker(lane, at, ctx) {
    const view = this.#entries.get(lane);
    return view
      ? view.findLatestHeadMarker(lane, at, ctx)
      : super.findLatestHeadMarker(lane, at, ctx);
  }
  scanDocuments(query, limit, cursor, ctx) {
    const view =
      query.scope.kind === "conversation" && this.#sources.get(query.scope.conversationId);
    return view
      ? view.scanDocuments(query, limit, cursor, ctx)
      : super.scanDocuments(query, limit, cursor, ctx);
  }
  findDocument(address, at, ctx) {
    const view =
      address.scope.kind === "conversation" && this.#sources.get(address.scope.conversationId);
    return view ? view.findDocument(address, at, ctx) : super.findDocument(address, at, ctx);
  }
  async close(ctx) {
    this.#closed = true;
    for (const view of this.#archives) await view.close(ctx);
    this.#archives.clear();
    this.#entries.clear();
    this.#sources.clear();
    this.#docs.clear();
    this.loadArchive = undefined;
    return super.close(ctx);
  }
  document(id, at, ctx) {
    const view = this.#docs.get(id);
    return view ? view.document(id, at, ctx) : super.document(id, at, ctx);
  }
  prepareCommit(writes, seq = this.nextSeq) {
    if (!this.#archives.size) return super.prepareCommit(writes, seq);
    const changes = new Map(),
      parts = new Map();
    const locations = [],
      local = [];
    for (const w of writes) {
      const v = w.value;
      if (w.type === "conversation" && v.parent) {
        const view =
          changes.get(v.parent.conversationId) ?? this.#entries.get(v.parent.conversationId);
        if (view) {
          changes.set(v.id, view);
          if (!parts.has(view)) parts.set(view, []);
          parts.get(view).push(w);
        }
      }
      const dst =
        w.type === "entry" &&
        (changes.get(v.conversationId) ?? this.#entries.get(v.conversationId));
      if (dst) {
        if (!parts.has(dst)) parts.set(dst, []);
        locations.push([dst, parts.get(dst).length]);
        parts.get(dst).push(w);
      } else {
        const source = w.type === "document.copy" && this.#docs.get(w.source.id);
        // Let native Pi validate and normalize the immutable source copy in
        // precisely the same way as a locally resident configuration.
        locations.push([null, local.length]);
        local.push(source ? source.prepareCommit([w], seq).writes[0] : w);
      }
    }
    const primary = super.prepareCommit(local, seq);
    if (!parts.size) return primary;
    const staged = new Map([...parts].map(([dst, items]) => [dst, dst.prepareCommit(items, seq)]));
    const ordered = locations.map(([dst, i]) => (dst ? staged.get(dst) : primary).writes[i]);
    return {
      seq: primary.seq,
      writes: Object.freeze(ordered),
      apply: () => {
        for (const p of staged.values()) p.apply();
        const applied = primary.apply();
        for (const [lane, dst] of changes) this.#entries.set(lane, dst);
        for (const w of ordered)
          if (w.type === "entry") this.nextId = Math.max(this.nextId, w.value.id + 1);
        return applied;
      },
    };
  }
}
