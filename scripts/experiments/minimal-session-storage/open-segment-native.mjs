import { MemoryStorage } from "@earendil-works/pi-durable";
import { readSealedFamily, hydrateSealedFamily } from "@pi-cloud/pi-session-postgres/durable";
import { loadPriorRoot } from "./retained-family-read.mjs";
export const historyScope = (s, flow) => ({
  ...s,
  piSessionId: flow,
  sessionId: flow,
  leaseSessionId: s.sessionId,
});
// Imported messages remain physically in their ancestor. Only the destination's
// native roots/documents and its subsequent writes enter its own Kafka stream.
export async function openSegmentNative(
  store,
  job,
  topic,
  publish,
  { bashIntentLog = false } = {},
) {
  const s = historyScope(job.scope, job.flow_id);
  const capture =
    job.recovering || job.started_at != null ? undefined : job.warmHistory?.capture(job);
  const load = {
    path: job.recovering ? "recovery" : "cold",
    diskEntryBytes: 0,
    pgEntryBytes: job.recovering ? null : 0,
  };
  class Published extends MemoryStorage {
    async commit(writes) {
      job.authority.assertActive();
      const batch = this.prepareCommit(writes);
      await publish({ seq: batch.seq, nextId: this.nextId, writes: batch.writes });
      const applied = batch.apply();
      capture?.append(batch);
      return applied;
    }
  }
  const mem = new Published();
  if (job.recovering || Number(job.mailbox_position) > 1) {
    let entries = [];
    const importEntries = (rows) => {
      entries = rows.map((r) => ({ type: "entry", value: r.entry }));
    };
    const restored = {
      prepareCommit(writes, seq) {
        const first = entries.length ? [...entries.splice(0), ...writes] : writes;
        return mem.prepareCommit(first, seq);
      },
      mintId: () => mem.mintId(),
      get nextId() {
        return mem.nextId;
      },
      set nextId(value) {
        mem.nextId = value;
      },
    };
    if (job.recovering) {
      importEntries(await store.contextPrefix(s.tenantId, job.logical_id, true));
      const family = await readSealedFamily(store.db, s, topic, { bashIntentLog });
      await hydrateSealedFamily(family, restored);
      return { storage: mem, resumed: true, family, load };
    }
    await loadPriorRoot(store.pool, s, restored, {
      cache: capture,
      beforeRestore: async (prefix, head) => {
        let received = 0;
        const inherited =
          prefix.hit?.inherited ??
          (await store.contextPrefix(
            s.tenantId,
            job.logical_id,
            true,
            (bytes) => {
              received = bytes;
            },
            head.floor,
          ));
        importEntries(inherited);
        capture?.seed(inherited, prefix.records, head.floor, prefix.hit ? head.version : null);
        load.diskEntryBytes = prefix.hit?.diskReadBytes ?? 0;
        load.path =
          prefix.hit?.diskReadBytes !== undefined
            ? "disk"
            : prefix.status === "hit"
              ? "warm"
              : "cold";
        load.pgEntryBytes = prefix.readBytes + received;
      },
    });
  } else {
    const imported = [];
    await store.hydrate(
      s.tenantId,
      job.logical_id,
      {
        prepareCommit(writes) {
          return { apply: () => imported.push(...writes) };
        },
      },
      (records, received, hit) => {
        if (hit) {
          load.path = hit.diskBytes ? "fork-disk" : "fork-warm";
          load.diskEntryBytes = hit.diskBytes;
        }
        capture?.seed(records);
        load.pgEntryBytes = received;
      },
      job.warmHistory,
    );
    if (imported.length) {
      const batch = mem.prepareCommit(imported, 1);
      job.authority.assertActive();
      const nextId =
        Math.max(
          ...imported.filter((w) => w.type !== "entry").map((w) => (w.record ?? w.value).id),
        ) + 1;
      await publish({ seq: 1, nextId, writes: batch.writes.filter((w) => w.type !== "entry") });
      batch.apply();
    }
  }
  return {
    storage: mem,
    resumed: false,
    load,
    rememberCompletion: (fact) => capture?.finish(fact),
  };
}
