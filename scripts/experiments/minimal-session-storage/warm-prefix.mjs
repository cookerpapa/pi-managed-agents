import assert from "node:assert/strict";
import { randomBytes, randomUUID } from "node:crypto";
import { distribution } from "../../lib/runtime-load-scheduler.mjs";
import { userEntry } from "../shared-history-roots/fixture.mjs";
import { referenceContext } from "../shared-history-roots/native-reader.mjs";

// Read-cost comparison only. This fixture freezes the preceding Run's entries,
// builds a fresh native Harness each time and never reuses an active owner.
export async function measureWarmPrefix(db) {
  const result = [];
  for (const bytes of [512, 32768]) {
    const tenant = randomUUID(),
      session = randomUUID(),
      previous = randomUUID(),
      next = randomUUID();
    const count = 128,
      newInput = "Continue with the cache experiment.";
    const history = Array.from({ length: count }, (_, i) =>
      userEntry(i + 2, randomBytes(bytes / 2).toString("hex")),
    );
    const nextEntry = (text) => userEntry(count + 2, text);
    const expected = await referenceContext([...history, nextEntry(newInput)]);
    try {
      await db.query(
        `insert into runs(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,model_ref,state,settled_at)
        values($3,$1,$2,1,$3,'history','none','completed',clock_timestamp()),($4,$1,$2,2,$4,$5,'none','queued',null)`,
        [tenant, session, previous, next, newInput],
      );
      await db.query(
        `insert into messages(tenant_id,pi_session_id,entry_id,conversation_id,run_id,role,content,commit_seq,ordinal)
        select $1,$2,n+1,1,$3,'user',body,n,0 from unnest($4::text[]) with ordinality as x(body,n)`,
        [tenant, session, previous, history.map((e) => JSON.stringify(e))],
      );
      const read = async () =>
        (
          await db.query(
            "select content from message_entries where tenant_id=$1 and pi_session_id=$2 and run_id=$3 order by commit_seq,ordinal",
            [tenant, session, previous],
          )
        ).rows.map((r) => JSON.parse(r.content));
      const key = (r) => JSON.stringify([r.tenant_id, r.session_id, r.prior_id, r.settled_at]);
      const metadata = async () =>
        (
          await db.query(
            `select n.tenant_id,n.session_id,n.initial_input,p.id as prior_id,p.settled_at
        from runs n join runs p on p.tenant_id=n.tenant_id and p.session_id=n.session_id and p.mailbox_position=n.mailbox_position-1
        where n.tenant_id=$1 and n.id=$2 and p.state='completed'`,
            [tenant, next],
          )
        ).rows[0];
      const cache = { key: key(await metadata()), entries: await read() };
      const timings = { cold: [], hit: [] },
        reads = { cold: [], hit: [] };
      for (const variant of [
        "warmup",
        ...Array.from({ length: 6 }, () => ["cold", "hit", "hit", "cold"]).flat(),
      ]) {
        const start = performance.now(),
          m = await metadata();
        const entries = variant === "hit" && cache.key === key(m) ? cache.entries : await read();
        const assembled = performance.now();
        const context = await referenceContext([...entries, nextEntry(m.initial_input)]);
        const end = performance.now();
        assert.deepEqual(context, expected);
        if (variant !== "warmup") {
          reads[variant].push(assembled - start);
          timings[variant].push(end - start);
        }
      }
      const changedInput = "New input after the cache was filled.";
      await db.query("update runs set initial_input=$2 where id=$1", [next, changedInput]);
      const m = await metadata();
      assert.equal(cache.key, key(m));
      assert.deepEqual(
        await referenceContext([...cache.entries, nextEntry(m.initial_input)]),
        await referenceContext([...history, nextEntry(changedInput)]),
      );
      for (const variant of ["cold", "hit"])
        result.push({
          variant,
          entries: count,
          textBytes: count * bytes,
          metadataAndReadMs: distribution(reads[variant]),
          throughNativeContextMs: distribution(timings[variant]),
        });
    } finally {
      await db.query("delete from messages where tenant_id=$1", [tenant]);
      await db.query("delete from runs where tenant_id=$1", [tenant]);
    }
  }
  return result;
}
