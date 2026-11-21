import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { fanoutFixture, targets, offered, partitions } from "./fanout-fixture.mjs";
import { installWorkspaceRoutes } from "./workspace-route-spans.mjs";
import { wall } from "./pg-path-replay.mjs";
const md5 = (s) => createHash("md5").update(s).digest("hex");
export async function runFanout({
  projector,
  emitter,
  receiver,
  configure,
  guard,
  db,
  report,
  save,
  token,
}) {
  const fixture = fanoutFixture(token),
    topic = "pma-session-bench-" + token;
  assert.equal(
    (await db.query("select current_database() db")).rows[0].db,
    "pma_sessbench_" + token,
  );
  const call = async (actor, kind, reply, body = {}) => {
    const result = actor.wait(reply);
    actor.write({ kind, ...body });
    return guard(result, 360000);
  };
  await configure({ label: "fanout-park", scene: "plain", slots: 0 });
  await installWorkspaceRoutes(db);
  const { url } = await call(receiver, "fanoutListen", "fanoutListening");
  const connected = await call(projector, "fanoutConnect", "fanoutConnected", { url });
  const study = (report.fanout = {
    targets,
    connectionSetup: {
      before: connected.before,
      after: connected.after,
      elapsedMs: connected.elapsedMs,
    },
    partitions,
    offered,
    slots: 3072,
    packetReservation: 4096,
    trials: [],
  });
  const owners = fixture.owners;
  async function seed(arm, displayRounds, mixed) {
    await call(projector, "fanoutReset", "fanoutResetDone");
    await db.query("truncate workspace_route_spans,messages,inboxes,runs");
    await db.query(
      `insert into runs(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,model_ref,state,lease_id)
      select r,$1,s,1,r,'fixture','fixture','running',w from unnest($2::uuid[],$3::uuid[],$4::uuid[]) a(r,s,w)`,
      [
        owners[0].scope.tenantId,
        ...["runId", "sessionId", "writerId"].map((k) => owners.map((o) => o.scope[k])),
      ],
    );
    await db.query(
      `insert into workspace_route_spans(run_id,conversation_id,tenant_id,pi_session_id,session_id,turn_id,topic,partition,first_offset,binding_id,writer_id)
      select id,1,tenant_id,session_id::text,session_id,id,$1,p,0,b,lease_id from runs join
      unnest($2::uuid[],$3::int[],$4::uuid[]) a(r,p,b) on a.r=id`,
      [
        topic,
        owners.map((o) => o.scope.runId),
        owners.map((o) => o.partition),
        owners.map((o) => o.binding),
      ],
    );
    if (!mixed) {
      await call(emitter, "fanoutTape", "fanoutTapeReady", {
        arm,
        count: targets,
        seed: true,
        displayRounds,
      });
      await call(emitter, "emitMessages", "messagesEmitted", { start: wall(), frameRate: offered });
    }
    await call(projector, "fanoutWarm", "fanoutWarmDone", { mixed });
  }
  async function verify(arm, rounds) {
    const { rows } =
      await db.query(`select id,count(*)::int n,md5(string_agg(v->>'operationId',',' order by k::int)) digest
      from runs cross join lateral jsonb_each(projection_tools) j(k,v) group by id`);
    assert.equal(rows.length, targets);
    const byId = new Map(rows.map((r) => [r.id, r]));
    for (const [i, o] of owners.entries()) {
      const r = byId.get(o.scope.runId);
      assert.equal(r?.n, rounds);
      assert.equal(
        r.digest,
        md5(Array.from({ length: rounds }, (_, n) => fixture.op(arm, n * targets + i)).join(",")),
      );
    }
  }
  const suite = process.env.PI_CLOUD_FANOUT_SUITE ?? "matrix";
  for (const [
    i,
    {
      mode,
      ms,
      rounds,
      preload = false,
      cap,
      grantWidth = 1,
      batchProjection = false,
      display = false,
      mixed = false,
      liveSource = false,
      rate = offered,
    },
  ] of cases(suite).entries()) {
    const arm = mixed ? 1 : i + 1,
      count = rounds * targets;
    if (mode !== "direct") await seed(arm, display ? rounds : 0, mixed);
    await call(receiver, "fanoutArm", "fanoutArmed", { arm, delay: ms, mixed });
    let source, result, bytes, ends, mix;
    if (mode === "direct") {
      ({ result } = await call(projector, "fanoutDirect", "fanoutDirectDone", {
        mode,
        arm,
        count,
        topic,
        start: wall() + 150,
      }));
    } else {
      if (!liveSource)
        ({ bytes, mix } = await call(emitter, "fanoutTape", "fanoutTapeReady", {
          arm,
          count,
          mixed,
          callsPerRun: rounds,
          displayRounds: display ? rounds : 0,
        }));
      const start = wall() + 250;
      await call(projector, "fanoutBegin", "fanoutBegun", {
        mode,
        arm,
        count,
        start,
        preload,
        recordCount: mix?.records,
        runs: targets,
        rate,
        liveSource,
        mixed,
        grantWidth,
        batchProjection,
        display,
        cap,
      });
      if (liveSource)
        ({ source, bytes, mix } = await call(emitter, "fanoutLiveTape", "fanoutLiveDone", {
          arm,
          callsPerRun: rounds,
          start,
        }));
      else
        ({ source } = await call(emitter, "emitMessages", "messagesEmitted", {
          frameRate: rate,
          start,
        }));
      source.rate = ((mix?.records ?? count) * 1000) / (source.acknowledgedAt - start);
      if (preload) await call(projector, "fanoutRelease", "fanoutReleased", { start: wall() });
      ({ result, ends } = await call(projector, "fanoutFinish", "fanoutFinished"));
      assert.equal(ends.length, partitions);
      if (mode === "offset-control") {
        const { rows } = await db.query(
          "select count(*)::int n from runs where projection_tools <> '{}'::jsonb",
        );
        assert.equal(rows[0].n, 0);
      } else if (mixed) {
        assert.equal(result.handled, mix.records);
        if (study.trials.length) assert.equal(mix.hash, study.trials[0].mix.hash);
        assert.equal(
          (
            await db.query(
              "select count(*)::int n from runs where state='completed' and projection_tools='{}'::jsonb",
            )
          ).rows[0].n,
          targets,
        );
        assert.equal(
          (await db.query("select count(*)::int n from messages where role='toolResult'")).rows[0]
            .n,
          count,
        );
        if (rounds > 1) await verifyLoopHistory(db, owners, mix, rounds);
      } else await verify(arm, rounds);
    }
    const deliveryRounds = mode.endsWith("-control") ? 0 : rounds;
    const { result: network } = await call(receiver, "fanoutStats", "fanoutStatsDone", {
      rounds: deliveryRounds,
    });
    assert(network.total === deliveryRounds * targets && result.acks === network.total);
    assert(result.peakActive <= (cap ?? 3072) && result.peakReservedBytes <= 12 * 1024 * 1024);
    assert(result.peakWaiting <= partitions, "Only the partition callbacks may await capacity");
    if (suite.startsWith("grants")) {
      assert.equal(
        Object.entries(result.grantSizes).reduce((sum, [size, n]) => sum + Number(size) * n, 0),
        count,
      );
      assert(
        Object.keys(result.grantSizes).every((k) => Number(k) >= 1 && Number(k) <= grantWidth),
      );
      assert.equal(result.pgTransactions.poolLimit, report.projectorConnections);
      assert(
        result.pgTransactions.openConnections > 0 &&
          result.pgTransactions.openConnections <= report.projectorConnections,
      );
      const grants = Object.values(result.grantSizes).reduce((sum, n) => sum + n, 0);
      if (mixed)
        assert(
          count - (result.grantSizes[1] ?? 0) <= mix.adjacent,
          "Batch crossed an intervening record",
        );
      if (!mixed) assert.equal(result.pgTransactions.total, batchProjection ? grants : count);
      if (!mixed)
        assert.equal(
          result.pgTransactions[display ? "sync" : "async"],
          result.pgTransactions.total,
        );
      assert.equal(result.displayed, display ? count : 0);
      assert(
        result.nativeCommits.positions >=
          Object.values(result.grantSizes).reduce((sum, n) => sum + n, 0),
      );
    }
    const trial = {
      mode,
      delay: ms,
      rate,
      display,
      liveSource,
      mix,
      grantWidth,
      batchProjection,
      count,
      preload,
      bytes,
      source,
      result,
      network,
    };
    study.trials.push(trial);
    await save();
    console.log(
      JSON.stringify({
        mode,
        grantWidth,
        batchProjection,
        transactions: result.pgTransactions?.total,
        ms,
        count,
        preload,
        input: Math.round(source?.rate ?? 0),
        rps: Math.round(result.rps),
        peakLag: result.peakLag,
        peak: result.peakActive,
        rssMiB: Math.round(result.peakRss / 1048576),
        pgMs: result.phases.pg.mean,
        grantMs: result.phases.grant.mean,
        ackMs: result.phases.ack.mean,
      }),
    );
  }
}

// These independent database checks run after the timed projection and receipt window.
async function verifyLoopHistory(db, owners, mix, rounds) {
  const count = rounds * owners.length;

  const counts = Object.fromEntries(
    (
      await db.query("select content::jsonb->>'kind' k,count(*)::int n from messages group by 1")
    ).rows.map((r) => [r.k, r.n]),
  );
  assert.deepEqual(counts, mix.entries);
  assert.equal(counts["pi.assistant"], count + targets);
  assert.equal(counts["pi.user"], targets);
  assert.equal(counts["pi.compaction"], targets);
  const pairs = await db.query(`with history as (
            select run_id,commit_seq,ordinal,role,content::jsonb->'model'->0 msg from messages where role in ('assistant','toolResult')
          ), calls as (select run_id,commit_seq,ordinal,msg->'content'->0->>'id' id from history where role='assistant' and msg->'content'->0->>'type'='toolCall'),
          replies as (select run_id,commit_seq,ordinal,msg->>'toolCallId' id,length(msg->'content'->0->>'text') size from history where role='toolResult')
          select calls.run_id as run,count(*)::int n,md5(string_agg(calls.id,',' order by calls.commit_seq,calls.ordinal)) digest
          from calls join replies using(run_id,id) where (replies.commit_seq,replies.ordinal)>(calls.commit_seq,calls.ordinal) and size=1024 group by calls.run_id`);
  assert.equal(pairs.rowCount, targets);
  const byRun = new Map(pairs.rows.map((r) => [r.run, r]));
  for (const [i, owner] of owners.entries()) {
    const pair = byRun.get(owner.scope.runId);
    assert.equal(pair?.n, rounds);
    assert.equal(
      pair.digest,
      md5(Array.from({ length: rounds }, (_, r) => "call-" + (targets * r + i)).join(",")),
    );
  }
}

function cases(suite) {
  const trial = (mode, ms, rounds, options) => ({ mode, ms, rounds, ...options });
  if (suite === "grants-tool-loop")
    return [
      { liveSource: true },
      { rate: 2500 },
      { rate: 3500 },
      { rate: 4600 },
      { rate: 6500 },
      { preload: true },
      { rate: 3500 },
    ].map((options) =>
      trial("concurrent", 100, 200, {
        grantWidth: 8,
        batchProjection: true,
        display: true,
        mixed: true,
        ...options,
      }),
    );
  if (suite === "grants-display-wide")
    return [32, 64, 128, 128, 64, 32].map((grantWidth) =>
      trial("concurrent", 100, 8, {
        grantWidth,
        batchProjection: true,
        display: true,
        preload: true,
      }),
    );
  if (suite === "grants-display-ackmix")
    return [
      [8, true],
      [8, false],
      [32, false],
    ].map(([grantWidth, liveSource]) =>
      trial("concurrent", 100, 1, {
        grantWidth,
        liveSource,
        batchProjection: true,
        display: true,
        mixed: true,
      }),
    );
  if (suite === "grants-display-mix")
    return [8, 32].map((grantWidth) =>
      trial("concurrent", 100, 1, {
        grantWidth,
        batchProjection: true,
        display: true,
        mixed: true,
      }),
    );
  if (suite === "grants-display-smoke")
    return [1, 8].map((grantWidth) =>
      trial("concurrent", 100, 2, { grantWidth, batchProjection: true, display: true }),
    );
  if (suite === "grants-display" || suite === "grants-display-backlog") {
    const preload = suite.endsWith("backlog"),
      widths = preload ? [4, 8, 16, 24, 32, 32, 24, 16, 8, 4] : [4, 8, 16, 32, 32, 16, 8, 4];
    return widths.map((grantWidth) =>
      trial("concurrent", 100, 8, { grantWidth, batchProjection: true, display: true, preload }),
    );
  }
  if (suite === "grants-pg-minute")
    return [false, true].map((batchProjection) =>
      trial("concurrent", 100, 69, { grantWidth: 8, batchProjection }),
    );
  if (suite === "grants-pg" || suite === "grants-pg-backlog") {
    const preload = suite === "grants-pg-backlog";
    const paired = [
      [8, false],
      [8, true],
      [32, true],
      [32, false],
      [32, false],
      [32, true],
      [8, true],
      [8, false],
    ];
    return (preload ? paired.slice(0, 4) : paired).map(([grantWidth, batchProjection]) =>
      trial("concurrent", 100, 23, { grantWidth, batchProjection, preload }),
    );
  }
  if (suite === "grants" || suite === "grants-long") {
    const widths = suite === "grants" ? [1, 8, 32, 32, 8, 1] : [8, 32];
    return widths.map((grantWidth) =>
      trial("concurrent", 100, suite === "grants" ? 8 : 23, { grantWidth }),
    );
  }
  if (suite === "sustain")
    return [
      trial("direct", 500, 32),
      trial("offset-control", 0, 8),
      trial("pg-control", 0, 8),
      trial("concurrent", 500, 2, { cap: 128 }),
      trial("concurrent", 100, 23),
      trial("concurrent", 0, 23),
    ];
  if (suite === "confirm")
    return [
      trial("pg-control", 0, 8),
      trial("offset-control", 0, 8),
      trial("concurrent", 500, 2, { cap: 128 }),
      trial("concurrent", 500, 3),
      trial("serial", 100, 1),
    ];
  assert(["smoke", "matrix"].includes(suite));
  const smoke = suite === "smoke",
    delays = smoke ? [0, 20] : [0, 20, 100, 500];
  const controls = delays.map((ms) => trial("direct", ms, smoke ? 1 : 8));
  const pairs = delays.flatMap((ms, i) =>
    (i % 2 ? ["concurrent", "serial"] : ["serial", "concurrent"]).map((mode) =>
      trial(mode, ms, smoke ? 1 : 2),
    ),
  );
  return [
    ...controls,
    ...pairs,
    ...(smoke ? [] : [trial("concurrent", 500, 12), trial("concurrent", 0, 12, { preload: true })]),
  ];
}
