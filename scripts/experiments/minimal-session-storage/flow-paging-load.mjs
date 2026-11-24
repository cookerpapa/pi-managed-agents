// Isolate API vs projection contention with one preloaded RF3 tape and fresh groups.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import native from "@confluentinc/kafka-javascript";
import { KafkaLogConsumer } from "@pi-cloud/event-log";
import { FamilyPresentation } from "./family-presentation.mjs";
import { testKafka } from "../../lib/test-kafka.mjs";
import { observePagingWaits, provePagingVisibility } from "./flow-paging-waits.mjs";
import {
  withMatchedPageFixture,
  measurePageCpu,
  pageCostTrials,
  loadMatchedPages,
} from "./matched-page-cost.mjs";
import { studyReplicaConsistency, observeReplay, splitTrials } from "./replica-study.mjs";
import { withPagingFixture } from "./flow-paging-fixture.mjs";

function latency(ms) {
  const xs = ms.toSorted((a, b) => a - b);
  const p = (n) => Number(xs[Math.ceil(n * xs.length) - 1]?.toFixed(2));
  return { n: xs.length, p50: p(0.5), p95: p(0.95), p99: p(0.99) };
}
async function readers(api, level, deadline) {
  const latencies = [],
    counts = { latest: 0, older: 0, newer: 0 };
  let running = true;
  const tasks = Array.from({ length: 8 }, async (_, i) => {
    let phase = 0,
      before,
      after;
    const id = api.windows[i % api.windows.length];
    await delay(i * 12);
    while (running && performance.now() < deadline) {
      const start = performance.now(),
        action = ["latest", "older", "newer"][phase];
      const p = await api.page(id, phase === 1 ? { before } : phase === 2 ? { after } : {});
      assert(p.turns.length > 0);
      if (phase === 0) before = p.history.before;
      if (phase === 1) after = p.history.after;
      const end = performance.now();
      latencies.push({ start, end });
      counts[action]++;
      phase = (phase + 1) % 3;
      if (level === "paced") await delay(Math.max(0, 1000 - (end - start)));
    }
  });
  const all = Promise.all(tasks);
  // Rejections are observed immediately, with the actual failure returned by stop.
  all.catch(() => {});
  return {
    stop: async (start, end) => {
      running = false;
      await all;
      const within = latencies.filter((r) => r.start >= start && r.end <= end);
      return {
        latencyMs: latency(within.map((r) => r.end - r.start)),
        pagesPerSecond: +((1000 * within.length) / (end - start)).toFixed(2),
        counts,
      };
    },
  };
}
export async function studyFlowPaging(
  url,
  tenant,
  groups,
  scopes,
  visiblePerRun,
  setCpuLimit,
  pageCost = false,
  referenceRoot,
  standbyUrl,
) {
  const presenters = new Map();
  for (const group of groups)
    for (const { fact } of group) {
      let presenter = presenters.get(fact.scope.runId);
      if (!presenter) {
        presenter = new FamilyPresentation();
        presenter.bind(1, fact.scope);
        presenters.set(fact.scope.runId, presenter);
      }
      const prepared = presenter.prepare(fact.commit);
      fact.display = prepared.display;
      prepared.confirmed();
    }
  const live = scopes[0],
    total = groups.reduce((n, g) => n + g.length, 0);
  const fixture = pageCost
    ? (url, tenant, work) =>
        withMatchedPageFixture(
          url,
          tenant,
          work,
          referenceRoot,
          standbyUrl,
          pageCost === "standby-control",
        )
    : withPagingFixture;
  await fixture(url, tenant, async (api) => {
    let arms = api.arms;
    if (standbyUrl) {
      const primary = arms.branch,
        standby = { ...primary, page: primary.split.page };
      const latest = await primary.page({}),
        older = await primary.page({ before: latest.history.before }),
        third = await primary.page({ before: older.history.before });
      const queries = [latest, older, third].map((p) => ({ before: p.history.before }));
      arms = { primary: { ...primary, queries }, standby: { ...standby, queries } };
      if (standbyUrl && pageCost === "standby-control")
        arms.frozen = { ...primary, page: primary.split.frozenPage, queries };
      for (const q of queries) {
        const expected = await primary.page(q);
        assert.deepEqual(await standby.page(q), expected);
        if (arms.frozen) assert.deepEqual(await arms.frozen.page(q), expected);
        for (const a of Object.values(arms))
          await Promise.all(Array.from({ length: 8 }, () => a.page(q)));
      }
      await studyReplicaConsistency(url, standbyUrl, tenant, api);
    } else if (pageCost === "selector") {
      const candidate = arms.branch,
        reference = { ...candidate, page: candidate.reference.page };
      arms = { candidate, reference };
      for (const q of candidate.queries) {
        assert.deepEqual(await reference.page(q), await candidate.page(q));
        for (const arm of [reference, candidate])
          await Promise.all(Array.from({ length: 8 }, () => arm.page(q)));
      }
    } else if (pageCost) await measurePageCpu(arms);
    const kafka = await testKafka({});
    const { topic } = api;
    const k = new native.KafkaJS.Kafka({
      kafkaJS: { brokers: kafka.brokers, logLevel: native.KafkaJS.logLevel.NOTHING },
    });
    const admin = k.admin(),
      producer = k.producer({ kafkaJS: { acks: -1, idempotent: true } });
    try {
      await admin.connect();
      await admin.createTopics({
        topics: [
          {
            topic,
            numPartitions: groups.length,
            replicationFactor: 3,
            configEntries: [{ name: "min.insync.replicas", value: "2" }],
          },
        ],
      });
      await producer.connect();
      for (let start = 0; start < groups[0].length; start += 12) {
        await producer.send({
          topic,
          messages: groups.flatMap((g, partition) =>
            g.slice(start, start + 12).map((r) => ({
              partition,
              key: r.fact.scope.piSessionId,
              value: JSON.stringify(r.fact),
            })),
          ),
        });
      }
      const ends = (await admin.fetchTopicOffsets(topic)).map((r) => [
        r.partition,
        Number(r.offset),
      ]);
      assert.equal(
        ends.reduce((n, r) => n + r[1], 0),
        total,
      );
      if (!pageCost)
        await api.db.query("insert into sessions(id,tenant_id,flow_id) values($1,$2,$3)", [
          api.live,
          tenant,
          live.piSessionId,
        ]);
      if (setCpuLimit) await provePagingVisibility(api, tenant, live.piSessionId);
      for (const level of pageCost ? [] : ["paced", "saturated"]) {
        const t = performance.now(),
          load = await readers(api, level, t + 3500);
        await delay(3500);
        console.log(
          JSON.stringify({
            mode: "read-only-" + level,
            ...(await load.stop(t, performance.now())),
          }),
        );
      }
      const trials = standbyUrl
        ? splitTrials(pageCost === "standby-control")
        : pageCost
          ? pageCostTrials(pageCost)
          : setCpuLimit
            ? [
                [2, "writer"],
                [2, "saturated"],
                [4, "saturated"],
                [4, "writer"],
                [4, "writer"],
                [4, "saturated"],
                [2, "saturated"],
                [2, "writer"],
              ]
            : ["writer", "paced", "saturated", "saturated", "paced", "writer"].map((mode) => [
                2,
                mode,
              ]);
      for (const [layout, mode] of trials) {
        const arm = pageCost ? arms[layout] : api;
        const { db, projector } = arm;
        if (setCpuLimit) await setCpuLimit(layout);
        const ids = scopes.map((s) => s.runId);
        await db.query("delete from messages where run_id=any($1::uuid[])", [ids]);
        await db.query("delete from inboxes where pi_session_id=any($1::uuid[])", [
          scopes.map((s) => s.piSessionId),
        ]);
        await db.query("delete from runs where id=any($1::uuid[])", [ids]);
        await db.query(
          `insert into runs(id,tenant_id,session_id,lease_id,initial_input,idempotency_key,model_ref,state,mailbox_position)
     select "runId",$1,"piSessionId","writerId",'Inspect',"runId",'fixture','running',1 from jsonb_to_recordset($2::jsonb) a("runId" uuid,"piSessionId" uuid,"writerId" uuid)`,
          [tenant, JSON.stringify(scopes)],
        );
        await projector.view.reset(new Map(ends.map(([n]) => [n, 0n])), 16, "recorded-history");
        const prepared = new WeakSet(),
          finished = Promise.withResolvers();
        finished.promise.catch(() => {});
        let livePage;
        let count = 0,
          t0,
          end,
          loads,
          firstError;
        const begin = () => {
          if (!t0) t0 = performance.now();
        };
        const reader = new KafkaLogConsumer({
          brokers: kafka.brokers,
          clientId: "page-study",
          groupId: "page-" + randomUUID(),
          topic,
          decode: (b) => JSON.parse(b.toString()),
          batchRead: true,
          commitMessages: false,
          replayOffsets: async (bounds) => new Map(bounds.map((b) => [b.partition, 0n])),
          prepareReady: async (rs, current) => {
            begin();
            try {
              const n = await projector.prepareHistory(rs, current);
              for (const r of rs.slice(0, n)) prepared.add(r.fact);
              return n || 1;
            } catch (e) {
              firstError = e;
              finished.reject(e);
              throw e;
            }
          },
          handler: async (r, current) => {
            begin();
            try {
              if (!prepared.has(r.fact))
                assert(
                  (await projector.nativeStore.projection.project(r.fact, r, current)).admitted,
                );
              if (!pageCost && mode !== "writer" && count === Math.floor(total * 0.6)) {
                livePage = api.page(api.live).then((p) => {
                  assert.equal(p.turns.length, 1);
                  const n = p.turns[0].transcript?.items.length ?? 0;
                  assert(n > 0);
                  return n;
                });
                livePage.catch((e) => finished.reject(e));
              }
              if (++count === total) {
                end = performance.now();
                finished.resolve();
              }
            } catch (e) {
              firstError = e;
              finished.reject(e);
              throw e;
            }
          },
        });
        let timeout, scrollJob;
        const counters = async () => {
          const {
            rows: [r],
          } = await db.query("select pg_read_file('/sys/fs/cgroup/cpu.stat') body");
          return Object.fromEntries(
            r.body
              .trim()
              .split("\n")
              .map((v) => v.split(" ")),
          );
        };
        const chromeBefore = api.browserReads?.();
        let cpuBefore, cpuAfter, observer, replay, countsBefore;
        try {
          if (setCpuLimit) observer = await observePagingWaits(url, layout);
          if (standbyUrl) {
            countsBefore = await api.arms.branch.split.counts();
            replay = await observeReplay(url, standbyUrl);
          }
          cpuBefore = await counters();
          await reader.start();
          const limit = performance.now() + 90000;
          while (!t0 && !firstError && performance.now() < limit) await delay(10);
          assert(t0, "No partition delivery");
          if (mode !== "writer") {
            loads = pageCost ? loadMatchedPages(arm, mode) : await readers(api, mode, limit);
            if (!pageCost) scrollJob = api.scroll();
          }
          await Promise.race([
            finished.promise,
            new Promise((_, reject) => {
              timeout = setTimeout(() => reject(Error("Projection did not drain")), 90000);
            }),
          ]);
          cpuAfter = await counters();
          const pages = loads ? await loads.stop(t0, end) : null;
          loads = undefined;
          await scrollJob;
          const {
            rows: [t],
          } = await db.query(
            "select count(*)::int n from messages where run_id=any($1::uuid[]) and role='toolResult'",
            [ids],
          );
          assert.equal(t.n, 20 * scopes.length);
          const progress = (
            await db.query(
              "select partition,next_offset from accepted_fact_projection_offsets where topic=$1",
              [topic],
            )
          ).rows;
          for (const [p, h] of ends)
            assert.equal(Number(progress.find((r) => r.partition === p).next_offset), h);
          let replicaEvidence;
          if (replay) {
            const after = await api.arms.branch.split.counts();
            const routes = Object.fromEntries(
              Object.keys(after).map((k) => [k, after[k] - countsBefore[k]]),
            );
            replicaEvidence = { routes, ...replay.summary(t0, end) };
          }
          const visible = visiblePerRun * scopes.length;
          console.log(
            JSON.stringify({
              mode,
              runs: scopes.length,
              total,
              visible,
              visiblePerSecond: Math.round((visible * 1000) / (end - t0)),
              seconds: +((end - t0) / 1000).toFixed(2),
              pgCommits: projector.view.stats.commits,
              asyncCommits: projector.view.stats.asyncTransactions,
              ...(replicaEvidence ? { replicaEvidence } : {}),
              ...(observer ? { waitStudy: observer.summary(t0, end) } : {}),
              pgCpuSeconds: +((cpuAfter.usage_usec - cpuBefore.usage_usec) / 1e6).toFixed(3),
              pgThrottlePeriods: cpuAfter.nr_throttled - cpuBefore.nr_throttled,
              allPartitionsDrained: true,
              livePageItems: livePage ? await livePage : null,
              toolResults: t.n,
              pages,
              ...(pageCost ? { layout } : { browserOlderPages: api.browserReads() - chromeBefore }),
            }),
          );
        } finally {
          clearTimeout(timeout);
          if (loads) await loads.stop(t0 ?? 0, performance.now());
          try {
            await reader.close();
          } finally {
            await observer?.stop();
            await replay?.close();
          }
        }
      }
    } finally {
      try {
        await producer.disconnect();
        await admin.disconnect();
      } finally {
        await kafka.close();
      }
    }
  });
}
