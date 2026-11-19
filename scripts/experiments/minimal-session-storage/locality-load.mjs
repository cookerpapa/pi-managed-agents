import assert from "node:assert/strict";
import { randomUUID, randomBytes } from "node:crypto";
import { answer } from "./fixture.mjs";
import { extendWitness, claimPreferredWithHead, toolEvidence } from "./session-prefix.mjs";

const userEntry = (id, text) => ({
  id,
  conversationId: 1,
  kind: "pi.user",
  model: [{ role: "user", content: [{ type: "text", text }], timestamp: 1 }],
});
export function sessionForArrival(cohort, sc, index) {
  if (!cohort.length || (sc.halfFresh && index % 2 === 0)) return randomUUID();
  if (sc.forkAt !== undefined) return cohort.at(-1).session;
  const i = sc.halfFresh ? Math.floor(index / 2) : index;
  return cohort[i % cohort.length].session;
}

export async function checkPreferredClaim(db, tenant) {
  const fifo = randomUUID(),
    target = randomUUID(),
    session = randomUUID(),
    seed = randomUUID(),
    version = randomUUID();
  const c = await db.connect();
  try {
    await c.query("begin");
    await c.query(
      `insert into runs(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,model_ref,state,created_at,history_head)
      values ($2,$1,$2,1,$2,'fifo','local-load','queued',clock_timestamp()-interval '1 minute',null),
      ($4,$1,$3,1,$4,'prefix','local-load','completed',clock_timestamp(),jsonb_build_object('version',$6::text)),
      ($5,$1,$3,2,$5,'target','local-load','queued',clock_timestamp(),null)`,
      [tenant, fifo, session, seed, target, version],
    );
    await c.query("savepoint before_claim");
    for (const [scope, token, expected] of [
      [tenant, version, target],
      [tenant, randomUUID(), fifo],
      [randomUUID(), version, fifo],
    ]) {
      const { rows } = await c.query(claimPreferredWithHead, [1, scope, target, token]);
      assert.deepEqual(
        rows.map((r) => r.id),
        [expected],
      );
      await c.query("rollback to savepoint before_claim");
    }
  } finally {
    await c.query("rollback");
    c.release();
  }
}
export async function seedCohort(db, tenant, sc) {
  const sessions = [];
  for (let s = 0; s < (sc.cohort ?? 0); s++) {
    const session = randomUUID(),
      run = randomUUID();
    let witness = "",
      bytes = 0;
    const bodies = [];
    for (let left = sc.seedBytes ?? 0; left > 0; left -= 32768) {
      const text = randomBytes(Math.ceil(Math.min(left, 32768) / 2))
        .toString("hex")
        .slice(0, Math.min(left, 32768));
      const body = JSON.stringify(userEntry(4 + bodies.length, text));
      bytes += Buffer.byteLength(body);
      bodies.push(body);
      witness = extendWitness(witness, "user", text, sc.verifyFull);
    }
    if (bodies.length) {
      const client = await db.connect();
      try {
        await client.query("begin");
        await client.query(
          `insert into runs(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,
          model_ref,state,settled_at,history_head) values($1,$2,$3,1,$1,'synthetic prefix','local-load','completed',clock_timestamp(),$4)`,
          [
            run,
            tenant,
            session,
            {
              version: randomUUID(),
              seq: 1,
              nextId: 4 + bodies.length,
              floor: 1,
              count: bodies.length,
              bytes,
            },
          ],
        );
        await client.query(
          `insert into messages(tenant_id,pi_session_id,entry_id,conversation_id,run_id,role,content,commit_seq,ordinal)
          select $1,$2,n+3,1,$3,'user',body,1,n from unnest($4::text[]) with ordinality x(body,n)`,
          [tenant, session, run, bodies],
        );
        await client.query("insert into inboxes values ($1,$2,1,3,1,0,'[]')", [tenant, session]);
        await client.query("commit");
      } catch (error) {
        await client.query("rollback");
        throw error;
      } finally {
        client.release();
      }
    }
    sessions.push({ session, witness, seeded: bodies.length > 0 });
  }
  return sessions;
}

// A quiescent maintenance fixture rotates an accepted history version in the
// same transaction as its head marker. Old rows deliberately remain present.
export async function resetCohortHead(db, tenant, session) {
  const c = await db.connect();
  try {
    await c.query("begin");
    const {
      rows: [r],
    } = await c.query(
      "select id,state,history_head from runs where tenant_id=$1 and session_id=$2 order by mailbox_position desc limit 1 for update",
      [tenant, session],
    );
    assert.equal(r.state, "completed");
    const h = r.history_head,
      text = "new active range " + randomBytes(160 * 1024).toString("hex"),
      id = h.nextId;
    const body = JSON.stringify({ ...userEntry(id, text), kind: "fixture.reset", head: id });
    await c.query(
      `insert into messages(tenant_id,pi_session_id,entry_id,conversation_id,run_id,role,content,commit_seq,ordinal)
      values($1,$2,$3,1,$4,'fixture.reset',$5,$6,0)`,
      [tenant, session, id, r.id, body, h.seq + 1],
    );
    await c.query("update runs set history_head=$2 where id=$1", [
      r.id,
      {
        version: randomUUID(),
        seq: h.seq + 1,
        nextId: id + 1,
        floor: id,
        count: 1,
        bytes: Buffer.byteLength(body),
      },
    ]);
    await c.query("commit");
    return text;
  } catch (error) {
    await c.query("rollback");
    throw error;
  } finally {
    c.release();
  }
}

// Copy just a settled cutoff to a new physical root in the private fixture.
export async function forkSeed(db, tenant, source) {
  const c = await db.connect(),
    session = randomUUID(),
    run = randomUUID();
  try {
    await c.query("begin isolation level repeatable read");
    const {
      rows: [p],
    } = await c.query(
      "select id,history_head from runs where tenant_id=$1 and session_id=$2 and mailbox_position=1 and state='completed'",
      [tenant, source.session],
    );
    assert(p?.history_head);
    await c.query(
      `insert into runs(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,model_ref,state,settled_at,history_head)
      values($1,$2,$3,1,$1,'fork cutoff','local-load','completed',clock_timestamp(),$4)`,
      [run, tenant, session, { ...p.history_head, version: randomUUID() }],
    );
    await c.query(
      `insert into messages(tenant_id,pi_session_id,entry_id,conversation_id,run_id,role,content,commit_seq,ordinal)
      select tenant_id,$3,entry_id,conversation_id,$4,role,content,commit_seq,ordinal from messages
      where tenant_id=$1 and pi_session_id=$2 and run_id=$5`,
      [tenant, source.session, session, run, p.id],
    );
    await c.query("insert into inboxes values($1,$2,1,3,1,0,'[]')", [tenant, session]);
    await c.query("commit");
    return { session, witness: source.witness, seeded: true };
  } catch (error) {
    await c.query("rollback");
    throw error;
  } finally {
    c.release();
  }
}
export function verifyModelHistory(sc, rows, records, cohort, byRun) {
  const sessions = new Map(cohort.map((s) => [s.session, { ...s, position: s.seeded ? 1 : 0 }]));
  const request = new Map(records.map((e) => [e.id, e])),
    leases = new Set();
  for (const r of rows.sort((a, b) => Number(a.mailbox_position) - Number(b.mailbox_position))) {
    assert(r.history_head?.version && r.history_head.seq > 0);
    assert(!leases.has(r.lease_id));
    leases.add(r.lease_id);
    const e = request.get(r.id),
      s = sessions.get(r.session_id) ?? { position: 0, witness: "" };
    if (sc.scene === "steer") {
      assert.equal(e.work.toolExecutions, 1);
      if (sc.verifyFull) {
        s.tools ??= [];
        assert.deepEqual(
          e.work.observedTools,
          s.tools,
          "Mismatch in previously settled Tool calls and replies",
        );
        s.tools.push(
          ...toolEvidence(byRun.get(r.id).flatMap((m) => JSON.parse(m.content).model ?? [])),
        );
      }
    }
    assert.equal(Number(r.mailbox_position), s.position + 1);
    if (e.resetText) s.witness = extendWitness("", "user", e.resetText, sc.verifyFull);
    const expected = extendWitness(s.witness, "user", e.text, sc.verifyFull);
    assert.equal(
      e.work.observed,
      expected,
      "Model received an incorrect previous prefix or current input",
    );
    s.witness = extendWitness(
      expected,
      "assistant",
      sc.scene === "steer" ? "" : answer(e.id, 1),
      sc.verifyFull,
    );
    for (const item of e.work.inputs)
      if (!item.withdrawn) s.witness = extendWitness(s.witness, "user", item.text, sc.verifyFull);
    if (sc.scene !== "plain")
      s.witness = extendWitness(s.witness, "assistant", answer(e.id, 2), sc.verifyFull);
    s.position++;
    sessions.set(r.session_id, s);
  }
}

const opts = {
  integrated: true,
  cacheMode: "prefer",
  wakeMode: "directed",
  batch: 32,
  slots: 128,
  omitNotify: true,
  probeUnlisted: true,
  windowMs: 5,
  tickMs: 200,
  sink: "single",
};
export async function runLocality(wave, suite) {
  const sink = process.env.PI_CLOUD_SESSION_BENCH_PROJECTION ?? opts.sink;
  assert(["full", "single"].includes(sink));
  const run = (label, spec) => wave({ ...opts, sink, label, ...spec });
  if (suite === "tail-history") {
    for (const [rate, seconds] of [
      [140, 60],
      [220, 24],
    ])
      await run("returning-" + rate, {
        tail: true,
        pollSize: 1,
        sink: "single",
        cohort: 128,
        seedBytes: 512 * 1024,
        rate,
        count: rate * seconds,
        concurrency: 384,
      });
    return;
  }
  if (suite === "tail-reference") {
    await run("reference-160", {
      tail: true,
      sampleServer: true,
      pollSize: 1,
      sink: "single",
      rate: 160,
      count: 160 * 90,
      concurrency: 384,
    });
    return;
  }
  if (suite === "tail-switch") {
    await run("alternating-poll", {
      tail: true,
      sampleServer: true,
      switchMs: 4000,
      sink: "single",
      rate: 260,
      count: 260 * 64,
      concurrency: 384,
    });
    return;
  }
  if (suite === "tail" || suite === "tail-stress") {
    const rate = suite === "tail" ? 160 : 260;
    for (const [i, [pollSize, sink]] of [
      [1, "single"],
      [32, "single"],
      [32, "terminal-only"],
      [1, "terminal-only"],
      [1, "terminal-only"],
      [32, "terminal-only"],
      [32, "single"],
      [1, "single"],
    ].entries()) {
      await run("tail-" + i, {
        tail: true,
        sampleServer: true,
        pollSize,
        sink,
        rate,
        concurrency: 384,
        count: rate * (i === 0 && suite === "tail" ? 50 : 16),
      });
    }
    return;
  }
  if (suite === "smoke") {
    const common = {
      cohort: 1,
      seedBytes: 320 * 1024,
      offline: ["w1"],
      count: 4,
      concurrency: 1,
      verifyFull: true,
    };
    const warm = await run("verified-warm", { ...common, changeModelAt: 2, redeliver: true });
    assert.equal(warm.locality.hits, 3);
    assert(warm.router.preferred > 0);
    const cut = await run("changed-head", { ...common, resetAt: 2 });
    assert.deepEqual(cut.locality.path, ["cold", "hit", "cold", "hit"]);
    const switcher = await run("old-worker-offline", { ...common, switchAt: 2 });
    assert.deepEqual(switcher.locality.path, ["cold", "hit", "cold", "hit"]);
    const fork = await run("new-fork-scope", { ...common, forkAt: 2 });
    assert.deepEqual(fork.locality.path, ["cold", "hit", "cold", "hit"]);
    const small = await run("short-is-fifo", { ...common, seedBytes: 4096 });
    assert.equal(small.locality.hits, 0);
    assert.equal(small.router.preferred, 0);
    const tight = await run("cache-eviction", {
      ...common,
      cohort: 2,
      count: 8,
      cacheBudget: 360 * 1024,
    });
    assert.equal(tight.locality.hits, 0);
    await run("ordered-successors", {
      ...common,
      cohort: 8,
      concurrency: 32,
      count: 96,
      offline: [],
    });
    await run("inbox-follow", {
      ...common,
      scene: "follow",
      inspect: true,
      depth: 2,
      count: 3,
      concurrency: 1,
    });
    const steered = await run("inbox-steer", {
      ...common,
      scene: "steer",
      inspect: true,
      count: 3,
      concurrency: 1,
    });
    assert.deepEqual(steered.locality.path, ["cold", "hit", "hit"]);
    return;
  }
  if (suite === "projection") {
    for (const [i, sink] of ["full", "single", "single", "full"].entries())
      await run("projection-" + i, {
        sink,
        halfFresh: true,
        cohort: 128,
        seedBytes: 512 * 1024,
        rate: 140,
        count: 2800,
        concurrency: 384,
      });
    return;
  }
  if (suite === "confirm") {
    for (const [kind, rate, seconds] of [
      ["fresh", 120, 60],
      ["history", 110, 60],
      ["mixed", 100, 120],
      ["mixed", 116, 60],
    ])
      await run("confirm-" + kind + "-" + rate, {
        ...(kind === "fresh"
          ? {}
          : { cohort: 128, seedBytes: 512 * 1024, halfFresh: kind === "mixed" }),
        rate,
        count: seconds * rate,
        concurrency: 384,
      });
    return;
  }
  if (suite === "busy") {
    for (const [i, scene] of ["follow", "steer", "steer", "follow"].entries())
      await run("two-requests-" + i, {
        scene,
        cohort: 64,
        seedBytes: 512 * 1024,
        concurrency: 64,
        count: 384,
        depth: 1,
      });
    return;
  }
  if (suite === "interactive") {
    for (const [i, cacheMode] of ["off", "local", "prefer", "prefer", "local", "off"].entries())
      await run("interactive-" + i, {
        cohort: 64,
        seedBytes: 512 * 1024,
        cacheMode,
        rate: 40,
        count: 640,
        concurrency: 128,
      });
    return;
  }
  if (suite === "burst") {
    for (const [i, concurrency] of [64, 128, 256, 256, 128, 64].entries())
      await run("new-burst-" + i, { concurrency, count: 1024 });
    for (const [i, cacheMode] of ["off", "local", "prefer", "prefer", "local", "off"].entries())
      await run("history-burst-" + i, {
        cohort: 64,
        seedBytes: 512 * 1024,
        cacheMode,
        concurrency: 64,
        count: 640,
      });
    return;
  }
  if (suite === "large") {
    for (const [i, cacheMode] of ["off", "prefer", "prefer", "off"].entries())
      await run("four-mib-" + i, {
        cohort: 16,
        seedBytes: 4 * 1024 * 1024,
        cacheMode,
        concurrency: 16,
        count: 192,
      });
    return;
  }
  assert(["fresh", "history", "mixed", "tail-reader"].includes(suite));
  const history = suite === "history" || suite === "mixed",
    spec = history ? { cohort: 128, seedBytes: 512 * 1024, halfFresh: suite === "mixed" } : {};
  const rates = (
    process.env.PI_CLOUD_SESSION_BENCH_RATES ??
    (history ? "50,80,110,140,170,200,240" : "90,140,190,240,290,340")
  )
    .split(",")
    .map(Number);
  const seconds = Number(process.env.PI_CLOUD_SESSION_BENCH_SECONDS ?? 20);
  assert(Number.isInteger(seconds) && seconds >= 10 && seconds <= 120);
  for (const rate of rates) {
    assert(Number.isInteger(rate) && rate > 0 && rate <= 500);
    const r = await run(suite + "-" + rate + "-" + seconds, {
      ...spec,
      ...(suite === "tail-reader" ? { tail: true } : {}),
      rate,
      count: rate * seconds,
      concurrency: 384,
    });
    if (suite !== "tail-reader" && (r.dropped || r.drainAfterOfferMs > 5000)) break;
  }
}
