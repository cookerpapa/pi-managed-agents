import assert from "node:assert/strict";

// The pressure tape has one model bucket. Native Pi writes the complete cumulative
// value for each response; fixture counters can be checked without replaying the tape.
export function responseUsage(n) {
  return {
    input: 320 * n,
    output: 32 * n,
    cacheRead: 128 * n,
    cacheWrite: 0,
    totalTokens: 480 * n,
    reasoning: 8 * n,
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
  };
}
export function usageValue(n) {
  return { models: n ? { "faux/local-load": responseUsage(n) } : {}, tools: {} };
}

// Both benchmark arms identify exactly the same doc from the native create.
// Keep only its identity in the replay checkpoint. Never derive usage from text.
export function coCommittedUsage(view, fact, frame, persist) {
  const scope = fact.scope;
  for (const [ordinal, write] of fact.commit.writes.entries()) {
    const opening = write.type === "document.create" && write.record.kind === "pi.usage";
    if (opening) {
      assert.equal(write.record.scope.kind, "conversation");
      assert.equal(write.record.history, "latest");
      view.usageDocs = { ...view.usageDocs, [write.record.id]: write.record.scope.conversationId };
    } else if (write.type !== "document.change" || !Object.hasOwn(view.usageDocs ?? {}, write.id)) {
      continue;
    }
    const id = opening ? write.record.id : write.id;
    assert.equal(write.content.kind, "base");
    // The experiment covers opening and response co-commits. Reuse the
    // existing guarded mutation in this frame for tenant/Session admission.
    assert(frame.ops.some((op) => op.kind === "message" || op.kind === "inbox"));
    if (persist)
      (frame.usage ??= []).push({
        tenant: scope.tenantId,
        session: scope.piSessionId,
        conversation: view.usageDocs[id],
        document: id,
        seq: fact.commit.seq,
        ordinal,
        value: write.content.value,
      });
  }
}

// PostgreSQL cannot upsert a conflicting key twice in one statement.
export function canGroupUsage(frames, count) {
  if (count < 2) return false;
  const keys = new Set();
  for (const frame of frames)
    for (const op of frame.usage ?? []) {
      const key = op.tenant + "/" + op.session + "/" + op.conversation;
      if (keys.has(key)) return false;
      keys.add(key);
    }
  return true;
}

export const usageTableSql = `create table usage_latest(
    tenant_id uuid, pi_session_id uuid, conversation_id bigint, doc_id bigint not null,
    commit_seq bigint not null, ordinal int not null, value jsonb not null,
    primary key(tenant_id,pi_session_id,conversation_id))`;

export async function usageStudy(pool, profile, owners, token) {
  const { installForkStudy, setForkLayout, ForkStorage } = await import("./fork-storage.mjs");
  await installForkStudy(pool, token);
  const storage = new ForkStorage(pool);
  const bulk = profile.startsWith("usage-rate-bulk");
  const rates = profile.endsWith("smoke") ? [bulk ? 0 : 2315] : [2315, 6000, 0];
  const cases = rates.flatMap((rate) => {
    const arms = profile.endsWith("smoke")
      ? [false, true]
      : profile.endsWith("confirm")
        ? [true, false, false, true]
        : [false, true, true, false];
    return arms.map((enabled) => ({
      rate,
      policy: "recorded-history",
      persistUsage: bulk || enabled,
      ...(bulk ? { bulkUsage: enabled } : {}),
    }));
  });
  await pool.query(usageTableSql);
  return {
    cases,
    async before() {
      await setForkLayout(pool, true, false);
      await pool.query("truncate usage_latest");
      await storage.register(
        owners[0].tenantId,
        owners.map((o) => o.piSessionId),
      );
    },
    async verify(persist) {
      const { rows } = await pool.query("select * from usage_latest order by pi_session_id");
      assert.deepEqual(
        rows.map((r) => r.pi_session_id),
        persist ? owners.map((o) => o.piSessionId).sort() : [],
      );
      for (const r of rows) {
        assert.equal(r.tenant_id, owners[0].tenantId);
        assert.equal(Number(r.conversation_id), 1);
        assert.equal(Number(r.doc_id), 5);
        assert.equal(Number(r.commit_seq), 203);
        assert.equal(r.ordinal, 1);
        assert.deepEqual(r.value, usageValue(101));
      }
      return rows.length;
    },
  };
}
