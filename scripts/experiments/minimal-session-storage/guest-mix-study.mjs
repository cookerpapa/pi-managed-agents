// Matched native Tool proposal/result traffic. The control produces a fixed
// text result in the harness; it never runs an untrusted command on a Worker.
import assert from "node:assert/strict";
import { createHash, randomUUID } from "node:crypto";
import { kafkaProducerLane } from "../../../packages/runtime-core/src/kafka-accepted-fact.ts";
import { distribution } from "../../lib/runtime-load-scheduler.mjs";
import { startGuestSampler } from "./guest-load-sampler.mjs";
import { wall } from "./pg-path-replay.mjs";
import { installGuestResources } from "./guest-continuation-study.mjs";
import { withFamilyGuest } from "./retained-family-cube.mjs";
import { studyCubeConfig } from "./study-cube-config.mjs";

const cwd = "/workspace/load";
function targetIndex(index, targets) {
  return (Math.floor(index / 4) + Math.floor(index / 32)) % targets;
}
export async function mixCohort(db, tenant, count, targets) {
  const lanes = Array.from({ length: 32 }, () => []),
    cohort = [];
  for (let i = 0; i < count; i++) {
    const lane = lanes[i % 32];
    while (!lane.length) {
      const session = randomUUID();
      lanes[kafkaProducerLane(session, 32)].push(session);
    }
    cohort.push({ session: lane.pop(), witness: "", seeded: false });
  }
  await db.query(
    `insert into study_session_environments(tenant,session,value)
      select $1,s,jsonb_build_object('workspace',w,'cwd',$4::text,'processGeneration',1)
       from unnest($2::uuid[],$3::text[]) as ids(s,w)`,
    [
      tenant,
      cohort.map((c) => c.session),
      cohort.map((c, i) => "mix-" + targetIndex(i, targets)),
      cwd,
    ],
  );
  return cohort;
}
// A Guest epoch is not necessarily the host epoch. Each independent sample bounds
// host-minus-Guest offset using the round trip around printing the Guest's clock.
async function clockBounds(guest) {
  let low = -Infinity,
    high = Infinity;
  for (let i = 0; i < 4; i++) {
    const start = wall(),
      guestMs = 1000 * Number(await guest.command('printf "%s\\n" "$EPOCHREALTIME"')),
      end = wall();
    assert(guestMs > 0 && Number.isFinite(guestMs));
    low = Math.max(low, start - guestMs);
    high = Math.min(high, end - guestMs);
  }
  return { low, high };
}
async function summarize(items, quarters, guests, before) {
  const clocks = await Promise.all(
    guests.map(async (guest, i) => {
      const after = await clockBounds(guest);
      const low = Math.max(before[i].low, after.low),
        high = Math.min(before[i].high, after.high);
      return { low, high, before: before[i], after };
    }),
  );
  const spans = {},
    real = guests.map(() => []);
  const add = (key, n) => {
    assert(Number.isFinite(n), key);
    (spans[key] ??= []).push(n);
  };
  for (const e of items) {
    const { mix, observedBash } = e.work;
    assert.equal(mix.remote, e.realGuest);
    assert(Number.isFinite(e.resultProjectedAt));
    const m = observedBash.match(/^\/workspace\/load\nload-marker\n(\d+\.\d+) (\d+\.\d+)$/);
    assert(m, "Wrong working directory, marker or clock in Tool Result");
    if (!mix.remote) {
      assert(!e.toolTiming && !e.guestOperationId);
      continue;
    }
    const target = targetIndex(e.index, guests.length);
    real[target].push(e.id);
    const { low, high } = clocks[target],
      offset = (low + high) / 2;
    const t = e.toolTiming;
    assert(t && t.op === e.guestOperationId);
    const projected = t.timing,
      began = Number(m[1]) * 1000,
      ended = Number(m[2]) * 1000;
    assert(ended >= began && mix.replyAt >= e.submitStarted && e.submitEnded >= e.submitStarted);
    add("prepareReplyWait", mix.waitSetupMs);
    add("producerAck", mix.producerAckMs);
    add("intentToProjector", projected.received - mix.publishedAt);
    add("selectAndFreezeRoute", projected.routeReady - projected.received);
    add("projectIntent", projected.projected - projected.routeReady);
    add("dispatchOffsetAck", projected.granted - projected.projected);
    add("grantToAdapter", e.submitStarted - projected.granted);
    add("adapterToAccept", e.submitEnded - e.submitStarted);
    if (e.guestHttp?.start?.headers && e.guestHttp?.input?.headers) {
      add("envdStartHeaders", e.guestHttp.start.headers - e.guestHttp.start.created);
      add("envdStartToSendInput", e.guestHttp.input.created - e.guestHttp.start.created);
      add("sendInputResponse", e.guestHttp.input.headers - e.guestHttp.input.created);
    }
    add("adapterToWorkerReply", mix.replyAt - e.submitStarted);
    add("shell", ended - began);
    if (low <= high) {
      add("adapterToShell", began + offset - e.submitStarted);
      add("shellEndToWorker", mix.replyAt - ended - offset);
    }
    add("replyToCanonicalProjection", e.resultProjectedAt - mix.replyAt);
    add("intentToCanonicalProjection", e.resultProjectedAt - mix.publishedAt);
  }
  for (const [i, guest] of guests.entries()) {
    const digest = createHash("sha256")
      .update(
        real[i]
          .sort()
          .map((id) => id + "\n")
          .join(""),
      )
      .digest("hex");
    assert.equal(
      (await guest.command("LC_ALL=C sort " + cwd + "/executions | sha256sum")).split(" ")[0],
      digest,
      "Missing, extra or duplicate physical execution",
    );
  }
  const physical = real.reduce((n, ids) => n + ids.length, 0);
  return {
    quarters,
    real: physical,
    controls: items.length - physical,
    clocks,
    spans: Object.fromEntries(Object.entries(spans).map(([k, v]) => [k, distribution(v)])),
  };
}
export async function runGuestMix(wave, db, endpoints, report, save) {
  const rate = Number(process.env.PI_CLOUD_GUEST_MIX_RATE ?? 4);
  const count = Number(process.env.PI_CLOUD_GUEST_MIX_COUNT ?? 64);
  const targets = Number(process.env.PI_CLOUD_GUEST_MIX_TARGETS ?? 1);
  const confirm = process.env.PI_CLOUD_GUEST_MIX_CONFIRM === "1";
  assert(
    Number.isSafeInteger(rate) &&
      rate > 0 &&
      Number.isSafeInteger(count) &&
      count >= 32 &&
      count % 32 === 0,
  );
  assert([1, 2].includes(targets));
  await installGuestResources(db);
  const config = await studyCubeConfig();
  async function run(guests) {
    try {
      for (const [i, guest] of guests.entries()) {
        const target = randomUUID();
        endpoints.set(target, guest);
        await db.query("insert into study_guest_targets values($1,$2)", ["mix-" + i, target]);
        await guest.command(
          "install -d -m 0755 -o 1000 -g 1000 " +
            cwd +
            " && printf 'load-marker\\n' > " +
            cwd +
            "/marker && install -m 0644 -o 1000 -g 1000 /dev/null " +
            cwd +
            "/executions",
        );
      }
      const cases = [
        { quarters: 4, count: 8, rate: 2 },
        ...(confirm ? [4, 4] : [0, 1, 2, 4, 4, 2, 1, 0]).map((quarters) => ({
          quarters,
          count,
          rate,
        })),
      ];
      for (const [i, c] of cases.entries()) {
        await Promise.all(guests.map((g) => g.command("truncate -s 0 " + cwd + "/executions")));
        const before = await Promise.all(guests.map(clockBounds));
        const stops = await Promise.all(guests.map(startGuestSampler));
        let stopped;
        const finishSampling = () => (stopped ??= Promise.all(stops.map((stop) => stop())));
        try {
          const result = await wave(
            {
              label: "guest-mix-" + i,
              guestQuarters: c.quarters,
              guestTargets: targets,
              guestTools: true,
              integrated: true,
              cacheMode: "off",
              wakeMode: "directed",
              count: c.count,
              rate: c.rate,
              concurrency: 128,
              slots: 64,
              batch: 32,
              omitNotify: true,
              probeUnlisted: true,
              windowMs: 5,
              tickMs: 200,
              sink: "single",
              prefixWidth: 16,
              projectionPolicy: "recorded-history",
            },
            async (items) => {
              const resources = await finishSampling();
              return { ...(await summarize(items, c.quarters, guests, before)), resources };
            },
          );
          assert.equal(result.completed + result.dropped, c.count);
          if (!result.dropped) assert.equal(result.guestMix.real, (c.count * c.quarters) / 4);
        } catch (error) {
          const partial = report.interruptedGuestMix;
          if (partial) {
            partial.resources = await finishSampling().catch(() => null);
            partial.physicalAtCapture = await Promise.all(
              guests.map((g) =>
                g.command("wc -l < " + cwd + "/executions").then(Number, () => null),
              ),
            );
            for (const failure of partial.failures) {
              assert.match(failure.runId, /^[a-f0-9-]{36}$/);
              failure.physicalMatchesAtCapture = await Promise.all(
                guests.map((g) =>
                  g
                    .command("grep -Fxc " + failure.runId + " " + cwd + "/executions || true")
                    .then(Number, () => null),
                ),
              );
            }
            await save();
          }
          throw error;
        } finally {
          await finishSampling().catch(() => {});
        }
      }
    } finally {
      endpoints.clear();
    }
  }
  await withFamilyGuest(config, (first) =>
    targets === 1 ? run([first]) : withFamilyGuest(config, (second) => run([first, second])),
  );
}
