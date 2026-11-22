import assert from "node:assert/strict";
import { randomUUID, randomBytes } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import { distribution } from "../../lib/runtime-load-scheduler.mjs";
import { ForkStorage, installForkStudy, setForkLayout } from "./fork-storage.mjs";

// Seed independent base64 payloads in bounded chunks outside the timed interval.
// Every 200 entries form a completed turn. The designated early/middle/last
// anchors also start a new turn after the final response.
async function seed(pool, storage, t, n, bytes, register = true, stride = 1, lanes = 1) {
  const session = randomUUID(),
    middle = Math.max(1, Math.floor(n / 2));
  const anchors = new Map(),
    runs = [],
    positions = [];
  let run = randomUUID();
  for (let i = 0; i < n; i++) {
    if (!i || positions.at(-1).final) {
      run = randomUUID();
      runs.push(run);
    }
    const id = i * stride + 4,
      final = i === 0 || i === middle || i === n - 1 || (i + 1) % 200 === 0;
    const lane = (i % lanes) + 1;
    positions.push({ id, run, final, lane });
    if (final) anchors.set(i, { entry: id, run, lane });
  }
  if (register) await storage.register(t, [session]);
  await pool.query(
    `insert into runs(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,model_ref,state,history_head)
    select r,$1,$2,i,r,'seed','fixture','completed','{}'::jsonb from unnest($3::uuid[]) with ordinality u(r,i)`,
    [t, session, runs],
  );
  if (register)
    await pool.query("insert into fork_completed_runs select $1,$2,r from unnest($3::uuid[]) r", [
      t,
      session,
      runs,
    ]);
  for (let first = 0; first < n; first += 128) {
    const slice = positions.slice(first, first + 128);
    const contents = slice.map((x) =>
      JSON.stringify({
        id: x.id,
        conversationId: x.lane,
        kind: x.final ? "pi.assistant" : "pi.user",
        version: 1,
        model: [
          {
            role: x.final ? "assistant" : "user",
            timestamp: 1,
            ...(x.final
              ? { stopReason: "stop", api: "faux", provider: "faux", model: "local-load" }
              : {}),
            content: [
              {
                type: "text",
                text: randomBytes(Math.ceil((bytes * 3) / 4))
                  .toString("base64")
                  .slice(0, bytes),
              },
            ],
          },
        ],
      }),
    );
    await pool.query(
      `insert into messages(tenant_id,pi_session_id,entry_id,conversation_id,run_id,role,content,commit_seq,ordinal)
      select $1,$2,id,lane,r,role,body,id,0 from unnest($3::bigint[],$4::uuid[],$5::text[],$6::text[],$7::int[]) d(id,r,role,body,lane)`,
      [
        t,
        session,
        slice.map((v) => v.id),
        slice.map((v) => v.run),
        slice.map((v) => (v.final ? "assistant" : "user")),
        contents,
        slice.map((v) => v.lane),
      ],
    );
  }
  return { session, first: anchors.get(0), mid: anchors.get(middle), last: anchors.get(n - 1) };
}

export async function forkStudy(pool, profile, owners, token) {
  await installForkStudy(pool, token);
  const storage = new ForkStorage(pool),
    t = owners[0].tenantId,
    smoke = profile === "fork-rate-smoke",
    reverse = profile === "fork-rate-confirm";
  let source,
    created = 0;
  const layouts = ["baseline", "enabled", "fanout", "busy"];
  const ablate = profile === "fork-rate-index";
  const cases = ablate
    ? [0, 6000].flatMap((rate, i) =>
        (i
          ? ["lean", "baseline", "enabled", "enabled", "baseline", "lean"]
          : ["baseline", "enabled", "lean", "lean", "enabled", "baseline"]
        ).map((layout) => ({ rate, policy: "recorded-history", layout })),
      )
    : (smoke ? [2315] : [2315, 6000, 0]).flatMap((rate, i) => {
        const order = reverse !== Boolean(i % 2) ? [...layouts].reverse() : layouts;
        return order.map((layout) => ({ rate, policy: "recorded-history", layout }));
      });
  return {
    cases,
    async before(layout) {
      const enabled = layout !== "baseline";
      await setForkLayout(pool, enabled, layout !== "lean");
      source = await seed(pool, storage, t, smoke ? 16 : 1024, 256, enabled);
      created = 0;
      if (!enabled) return;
      const frozen = await storage.fork(t, source.session, source.first);
      const {
        rows: [r],
      } = await pool.query("select root_id from fork_sessions where tenant_id=$1 and id=$2", [
        t,
        frozen,
      ]);
      await storage.register(
        t,
        owners.map((o) => o.sessionId),
        r.root_id,
      );
      if (["fanout", "busy"].includes(layout)) {
        for (const o of owners)
          for (let n = 0; n < (smoke ? 1 : 4); n++) {
            await storage.fork(t, o.sessionId, source.first);
            created++;
          }
      }
    },
    start(layout, at) {
      let stopping = false,
        error,
        missed = 0;
      const latency = [];
      const loop = (async () => {
        if (layout !== "busy") return;
        let due = at;
        while (!stopping) {
          await delay(Math.max(0, due - Date.now()));
          if (stopping) break;
          const begin = performance.now();
          if (latency.length % 2 === 0) await storage.fork(t, source.session, source.last);
          else
            await storage.fork(t, owners[latency.length % owners.length].sessionId, source.first);
          latency.push(performance.now() - begin);
          due += 50;
          while (due < Date.now()) {
            due += 50;
            missed++;
          }
        }
      })().catch((e) => {
        error = e;
      });
      return async () => {
        stopping = true;
        await loop;
        if (error) throw error;
        return {
          existingForks: created,
          concurrentForks: latency.length,
          missedForkOffers: missed,
          forkMs: distribution(latency),
        };
      };
    },
    async verify(layout) {
      if (layout === "baseline") return;
      const o = owners[0],
        {
          rows: [last],
        } = await pool.query(
          "select entry_id from messages where run_id=$1 and role='assistant' order by entry_id desc limit 1",
          [o.runId],
        );
      const dest = await storage.fork(t, o.sessionId, {
        entry: Number(last.entry_id),
        run: o.runId,
        lane: 1,
      });
      const visible = await storage.page(t, dest, 250);
      assert.equal(visible.length, 203); // inherited seed anchor and 202 local entries
      assert.equal(visible.at(-1).run_id, source.first.run);
    },
    async finish() {
      return forkTimings(pool, storage, t, smoke, ablate);
    },
  };
}

async function forkTimings(pool, storage, t, smoke, ablate) {
  await setForkLayout(pool, true, !ablate);
  const correctness = await verifyForkOwnership(pool, storage, t);
  const results = [];
  let burst;
  for (const [entries, bytes] of smoke
    ? [
        [16, 256],
        [256, 1024],
      ]
    : ablate
      ? [
          [200, 4096],
          [32768, 32768],
        ]
      : [
          [16, 4096],
          [200, 4096],
          [4096, 4096],
          [32768, 768],
          [32768, 32768],
        ]) {
    const data = await seed(pool, storage, t, entries, bytes);
    await pool.query("vacuum analyze messages");
    const countBodies = async () =>
      (
        await pool.query(
          "select count(*)::int n from messages where tenant_id=$1 and pi_session_id=$2",
          [t, data.session],
        )
      ).rows[0].n;
    assert.equal(await countBodies(), entries);
    for (const [position, point] of [
      ["middle", data.mid],
      ["end", data.last],
    ]) {
      const create = [],
        page = [],
        nested = [];
      const samples = smoke ? 3 : entries >= 32768 ? 25 : 50;
      for (let i = -1; i < samples; i++) {
        let start = performance.now();
        const dest = await storage.fork(t, data.session, point);
        const forkMs = performance.now() - start;
        start = performance.now();
        const rows = await storage.page(t, dest, 30);
        const readMs = performance.now() - start;
        assert.equal(Number(rows[0].entry_id), point.entry);
        start = performance.now();
        const grand = await storage.fork(t, dest, point);
        const nestedMs = performance.now() - start;
        assert.equal(Number((await storage.page(t, grand, 1))[0].entry_id), point.entry);
        if (i >= 0) {
          create.push(forkMs);
          page.push(readMs);
          nested.push(nestedMs);
        }
      }
      results.push({
        entries,
        bytes,
        position,
        samples,
        forkMs: distribution(create),
        firstPageMs: distribution(page),
        nestedForkMs: distribution(nested),
      });
      console.log(JSON.stringify(results.at(-1)));
    }
    if (!smoke && entries * bytes >= 1024 ** 3) {
      const clients = 8,
        perClient = 8,
        latencies = [],
        begin = performance.now();
      assert(pool.options.max >= clients);
      await Promise.all(
        Array.from({ length: clients }, async () => {
          for (let i = 0; i < perClient; i++) {
            const at = performance.now();
            await storage.fork(t, data.session, data.last);
            latencies.push(performance.now() - at);
          }
        }),
      );
      burst = {
        clients,
        completed: latencies.length,
        forksPerSecond: (latencies.length * 1000) / (performance.now() - begin),
        latencyMs: distribution(latencies),
      };
    }
    const total = (await pool.query("select count(*)::int n from messages where tenant_id=$1", [t]))
      .rows[0].n;
    assert.equal(total, entries, "Fork copied message bodies");
    await setForkLayout(pool, true, !ablate);
  }
  const tree = await measureNested(pool, storage, t, smoke ? 8 : 128);
  const sparse = ablate ? await measureSparseLane(pool, storage, t) : undefined;
  return { coveringIndex: !ablate, correctness, results, tree, burst, sparse };
}

async function measureSparseLane(pool, storage, t) {
  const source = await seed(pool, storage, t, 32768, 1024, true, 1, 32);
  await pool.query("vacuum analyze messages");
  const trials = [];
  for (const cover of [false, true, true, false]) {
    await pool.query("drop index if exists fork_message_coordinates");
    if (cover)
      await pool.query(
        "create index fork_message_coordinates on messages(tenant_id,pi_session_id,conversation_id,entry_id) include(commit_seq,ordinal,role,run_id)",
      );
    const elapsed = [];
    for (let i = -1; i < 35; i++) {
      const at = performance.now(),
        id = await storage.fork(t, source.session, source.last);
      if (i >= 0) elapsed.push(performance.now() - at);
      if (i === 0) assert.equal((await storage.page(t, id, 2000)).length, 1024);
    }
    trials.push({ cover, totalEntries: 32768, selectedEntries: 1024, ms: distribution(elapsed) });
  }
  return trials;
}

async function append(pool, t, stream, id, seq, lane = 1) {
  const run = randomUUID(),
    writer = randomUUID();
  await pool.query(
    "insert into runs(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,model_ref,state,lease_id) select $1,$2,$3,coalesce(max(mailbox_position),0)+1,$1,'append','fixture','running',$4 from runs where tenant_id=$2 and session_id=$3",
    [run, t, stream, writer],
  );
  const content = JSON.stringify({
    id,
    conversationId: lane,
    kind: "pi.assistant",
    version: 1,
    model: [
      {
        role: "assistant",
        stopReason: "stop",
        model: "local-load",
        provider: "faux",
        api: "faux",
        timestamp: 1,
        content: [{ type: "text", text: run }],
      },
    ],
  });
  const {
    rows: [proof],
  } = await pool.query("select * from prefix_message($1,$2,$3,$4,$5,$6)", [
    t,
    stream,
    run,
    seq,
    JSON.stringify([{ id, conversation: lane, role: "assistant", content, ordinal: 0 }]),
    writer,
  ]);
  assert(proof.accepted && proof.writes === 1);
  await pool.query("select prefix_finish($1,$2,$3,$4,$5)", [t, stream, run, writer, "{}"]);
  return { entry: id, run, lane, writer };
}

async function verifyForkOwnership(pool, storage, t) {
  const s = await seed(pool, storage, t, 5, 64, true, 2),
    key = randomUUID();
  const pending = randomUUID(),
    writer = randomUUID();
  await pool.query(
    "insert into runs(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,model_ref,state,lease_id) values ($1,$2,$3,100,$1,'pending','fixture','running',$4)",
    [pending, t, s.session, writer],
  );
  const [a, b] = await Promise.all([
    storage.fork(t, s.session, s.mid, key),
    storage.fork(t, s.session, s.mid, key),
  ]);
  assert.equal(a, b);
  await assert.rejects(storage.retire(t, s.session), /still running/);
  const stableEntry = {
    id: 16,
    conversation: 1,
    role: "assistant",
    content: JSON.stringify({
      id: 16,
      conversationId: 1,
      model: [{ role: "assistant", stopReason: "stop" }],
    }),
    ordinal: 0,
  };
  const storeOnce = (entry) =>
    pool.query("select * from prefix_message($1,$2,$3,17,$4,$5)", [
      t,
      s.session,
      pending,
      JSON.stringify([entry]),
      writer,
    ]);
  assert.equal((await storeOnce(stableEntry)).rows[0].writes, 1);
  assert.equal((await storeOnce({ ...stableEntry, conversation: 2 })).rows[0].writes, 0);
  await assert.rejects(
    storage.fork(t, s.session, { entry: 16, run: pending, lane: 1 }),
    /Missing completed assistant/,
  );
  await pool.query("select prefix_finish($1,$2,$3,$4,$5)", [t, s.session, pending, writer, "{}"]);
  await assert.rejects(storage.fork(t, s.session, s.last, key), /request changed/);
  const childSize = (await storage.page(t, a)).length;
  assert.equal(childSize, 3);
  await append(pool, t, s.session, 5, 20); // inside the frozen ID range, outside its commit horizon
  assert.equal((await storage.page(t, a)).length, childSize);
  const sibling = await append(pool, t, s.session, 7, 21, 2);
  const side = await storage.fork(t, s.session, sibling);
  assert.deepEqual(
    (await storage.page(t, side)).map((r) => r.run_id),
    [sibling.run],
  );
  const current = await storage.fork(t, s.session, s.mid);
  assert.deepEqual(
    (await storage.page(t, current)).map((r) => Number(r.entry_id)),
    [8, 6, 5, 4],
  );
  await storage.retire(t, current);
  const x = await append(pool, t, a, 12, 30);
  const nested = await storage.fork(t, a, x);
  const fresh = await storage.retire(t, s.session, s.first);
  await append(pool, t, fresh, 13, 31);
  assert(!(await storage.page(t, s.session)).some((r) => Number(r.entry_id) === s.mid.entry));
  assert((await storage.page(t, a)).some((r) => Number(r.entry_id) === s.mid.entry));
  await storage.retire(t, s.session);
  await storage.collect(t);
  assert.equal(await storage.fork(t, s.session, s.mid, key), a);
  await storage.retire(t, a);
  await storage.collect(t);
  const obsolete = await pool.query("select * from prefix_message($1,$2,$3,$4,$5,$6)", [
    t,
    a,
    x.run,
    31,
    JSON.stringify([
      { id: 31, conversation: 1, role: "assistant", content: "obsolete", ordinal: 0 },
    ]),
    x.writer,
  ]);
  assert.equal(obsolete.rows[0].accepted, false);
  assert.deepEqual(
    (await storage.page(t, nested)).map((r) => Number(r.entry_id)),
    [12, 8, 6, 4],
  );
  await verifyModelContext(
    await storage.page(t, nested),
    await storage.agentConfiguration(t, nested),
  );
  for (const id of [nested, side]) await storage.retire(t, id);
  await storage.collect(t);
  for (const table of [
    "messages",
    "runs",
    "pi_history_nodes",
    "pi_history_streams",
    "fork_completed_runs",
    "fork_requests",
    "fork_sessions",
  ]) {
    assert.equal(
      (await pool.query("select count(*)::int n from " + table + " where tenant_id=$1", [t]))
        .rows[0].n,
      0,
    );
  }
  const race = await seed(pool, storage, t, 8, 64),
    blocker = await pool.connect();
  let fork, retire;
  try {
    await blocker.query("begin");
    await blocker.query("select id from fork_sessions where tenant_id=$1 and id=$2 for update", [
      t,
      race.session,
    ]);
    fork = storage.fork(t, race.session, race.last);
    void fork.catch(() => {});
    const deadline = Date.now() + 4000;
    while (
      !(
        await pool.query(
          "select exists(select from pg_stat_activity where $1=any(pg_blocking_pids(pid))) waiting",
          [blocker.processID],
        )
      ).rows[0].waiting
    ) {
      assert(Date.now() < deadline, "Fork did not contend on its source");
      await delay(10);
    }
    retire = storage.retire(t, race.session);
    void retire.catch(() => {});
    await blocker.query("commit");
  } finally {
    await blocker.query("rollback");
    blocker.release();
  }
  const target = await fork;
  await retire;
  await storage.collect(t);
  assert.equal((await storage.page(t, target)).length, 8);
  await storage.retire(t, target);
  await storage.collect(t);
  return {
    idempotent: true,
    activeSourceFork: true,
    activeDeleteRejected: true,
    obsoleteAppendRejected: true,
    lateLowerIdExcluded: true,
    siblingExcluded: true,
    sourcePruned: true,
    independentAppend: true,
    sourceDeleted: true,
    nestedSurvives: true,
    lastReferenceCollected: true,
    concurrentDelete: true,
    piColdContext: true,
  };
}

async function verifyModelContext(rows, agentConfig) {
  const { Harness, MemoryStorage, createRegistry } = await import("@earendil-works/pi-durable");
  const { BACKGROUND_CONTEXT: ctx } = await import("@earendil-works/chord/context");
  const { createModels } = await import("@earendil-works/pi-ai/models");
  const { fauxProvider, fauxAssistantMessage } =
    await import("@earendil-works/pi-ai/providers/faux");
  const { restoreForkPrefix } = await import("./session-prefix.mjs");
  const history = [...rows]
    .reverse()
    .map((r) => ({ entry: JSON.parse(r.content), seq: Number(r.commit_seq) }));
  const expected = history.map((r) => [r.entry.model[0].role, r.entry.model[0].content[0].text]);
  const store = new MemoryStorage(),
    models = createModels(),
    fake = fauxProvider({ tokensPerSecond: 0, models: [{ id: "local-load" }] });
  restoreForkPrefix(store, history, agentConfig);
  models.setProvider(fake.provider);
  let observed;
  fake.setResponses([
    (context) => {
      observed = context.messages.map((m) => [m.role, m.content[0].text]);
      return fauxAssistantMessage("continued");
    },
  ]);
  const harness = await Harness.open(
    store,
    {
      models,
      registry: createRegistry(),
      settings: { retry: { maxRetries: 0 }, compaction: { enabled: false } },
    },
    ctx,
  );
  try {
    const model = fake.getModel(),
      agent = { model: { provider: model.provider, modelId: model.id } },
      root = await harness.root(ctx, { agent });
    await root.configure(agent, ctx);
    const input = await root.submit(
      {
        type: "input",
        content: [{ type: "text", text: "continue fork" }],
        requestId: randomUUID(),
      },
      ctx,
    );
    await root.waitForIdle(ctx);
    assert.equal((await input.wait(ctx)).status, "done");
    assert.deepEqual(observed, [...expected, ["user", "continue fork"]]);
  } finally {
    await harness.close(ctx);
  }
}

async function measureNested(pool, storage, t, depth) {
  const data = await seed(pool, storage, t, 16, 256),
    times = [],
    ids = [data.session];
  let target = await storage.fork(t, data.session, data.last);
  for (let i = 0; i < depth; i++) {
    ids.push(target);
    const point = await append(pool, t, target, 100 + i, 100 + i),
      begin = performance.now();
    target = await storage.fork(t, target, point);
    times.push(performance.now() - begin);
  }
  const fresh = await storage.page(t, target, depth + 30);
  assert.equal(fresh.length, depth + 16);
  const middle = await storage.fork(t, target, data.mid);
  assert.equal((await storage.page(t, middle)).length, 9);
  for (const id of ids) await storage.retire(t, id);
  await storage.collect(t);
  assert.equal((await storage.page(t, target, depth + 30)).length, depth + 16);
  await storage.retire(t, target);
  await storage.retire(t, middle);
  await storage.collect(t);
  return {
    depth,
    first: times[0],
    last: times.at(-1),
    appendThenForkMs: distribution(times),
    clippedAcrossAncestors: true,
  };
}
