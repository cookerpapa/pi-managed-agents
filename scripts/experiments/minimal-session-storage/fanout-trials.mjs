import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { fanoutFixture, targets, offered } from "./fanout-fixture.mjs";
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
    partitions: 32,
    offered,
    slots: 3072,
    packetReservation: 4096,
    trials: [],
  });
  const owners = fixture.owners;
  async function seed(arm) {
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
    await call(emitter, "fanoutTape", "fanoutTapeReady", { arm, count: targets, seed: true });
    await call(emitter, "emitMessages", "messagesEmitted", { start: wall(), frameRate: offered });
    await call(projector, "fanoutWarm", "fanoutWarmDone");
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
  for (const [i, { mode, ms, rounds, preload = false, cap }] of cases(suite).entries()) {
    const arm = i + 1,
      count = rounds * targets;
    if (mode !== "direct") await seed(arm);
    await call(receiver, "fanoutArm", "fanoutArmed", { arm, delay: ms });
    let source, result, bytes, ends;
    if (mode === "direct") {
      ({ result } = await call(projector, "fanoutDirect", "fanoutDirectDone", {
        mode,
        arm,
        count,
        topic,
        start: wall() + 150,
      }));
    } else {
      ({ bytes } = await call(emitter, "fanoutTape", "fanoutTapeReady", { arm, count }));
      const start = wall() + 250;
      await call(projector, "fanoutBegin", "fanoutBegun", {
        mode,
        arm,
        count,
        start,
        preload,
        cap,
      });
      ({ source } = await call(emitter, "emitMessages", "messagesEmitted", {
        frameRate: offered,
        start,
      }));
      source.rate = (count * 1000) / (source.acknowledgedAt - start);
      if (preload) await call(projector, "fanoutRelease", "fanoutReleased", { start: wall() });
      ({ result, ends } = await call(projector, "fanoutFinish", "fanoutFinished"));
      assert.equal(ends.length, 32);
      if (mode === "offset-control") {
        const { rows } = await db.query(
          "select count(*)::int n from runs where projection_tools <> '{}'::jsonb",
        );
        assert.equal(rows[0].n, 0);
      } else await verify(arm, rounds);
    }
    const deliveryRounds = mode.endsWith("-control") ? 0 : rounds;
    const { result: network } = await call(receiver, "fanoutStats", "fanoutStatsDone", {
      rounds: deliveryRounds,
    });
    assert(network.total === deliveryRounds * targets && result.acks === network.total);
    assert(result.peakActive <= (cap ?? 3072) && result.peakReservedBytes <= 12 * 1024 * 1024);
    assert(result.peakWaiting <= 32, "Only the partition callbacks may await capacity");
    const trial = { mode, delay: ms, count, preload, bytes, source, result, network };
    study.trials.push(trial);
    await save();
    console.log(
      JSON.stringify({
        mode,
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

function cases(suite) {
  const trial = (mode, ms, rounds, options) => ({ mode, ms, rounds, ...options });
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
