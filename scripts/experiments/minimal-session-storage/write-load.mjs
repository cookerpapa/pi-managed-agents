// Isolated, paired Projector/primary study. One real Pi tape, simulated 50 ms
// sandbox receipts and one product Chrome watching a representative live Run.
import assert from "node:assert/strict";
import { createHash, randomUUID } from "node:crypto";
import { monitorEventLoopDelay } from "node:perf_hooks";
import { setBashLogReader, setBashLogIndexes } from "./bash-intent-log.mjs";
import { seedActiveBashHistory } from "./bash-intent-read-cost.mjs";
import { seedToolsWindow } from "./scroll-demand-study.mjs";
import { setTimeout as delay } from "node:timers/promises";
import native from "@confluentinc/kafka-javascript";
import { KafkaLogConsumer } from "@pi-cloud/event-log";
import { kafkaProducerLane } from "@pi-cloud/runtime-core/kafka-accepted-fact";
import { testKafka } from "../../lib/test-kafka.mjs";
import { testPostgres } from "../../lib/test-postgres.mjs";
import { withPagingFixture } from "./flow-paging-fixture.mjs";
import { FamilyPresentation } from "./family-presentation.mjs";
import { nativeBashTrace, nativeBashFact } from "./native-bash-trace.mjs";
import { normalRootProjection } from "./normal-root-projection.mjs";
import { installWorkspaceRoutes } from "./workspace-route-spans.mjs";
import { encodeWorkspaceFact, decodeWorkspaceFact } from "./workspace-operations.mjs";
import { visibleOutput } from "./visible-output.mjs";
import { checkMemoryWait } from "./write-wait-check.mjs";
import { checkWriterGuard } from "./write-guard-check.mjs";
import { writeTrialFunctions } from "./write-trial-sql.mjs";

const visibleRecord = (writes) =>
  Object.values(visibleOutput(writes)).some((count) => count > 0) ? 1 : 0;
const sqlStudy = process.argv.includes("--intent-sql");
const appendStudy = sqlStudy || process.argv.includes("--intent-log");
const partitions = 32,
  tools = 20,
  runs = 64,
  tenant = randomUUID();
const runIds = [];
const owners = [];
const scopeFor = (sessionId) => ({
  tenantId: tenant,
  sessionId,
  piSessionId: sessionId,
  runId: randomUUID(),
  writerId: randomUUID(),
  fencingToken: 1,
});
async function makeTape(watched) {
  const seed = randomUUID();
  const commits = await nativeBashTrace({ runId: seed }, undefined, {
    callsPerRun: tools,
    callIdAt: (n) => "call-" + n,
  });
  const prefixes = commits.flatMap((c) =>
    c.writes
      .filter((w) => w.type === "entry" && w.value.kind === "pi.tool-result")
      .map((w) => w.value.model[0].content[0].text.slice(-20)),
  );
  assert.equal(new Set(prefixes).size, tools);
  const population = Array(partitions).fill(0);
  while (owners.length < runs) {
    const s = scopeFor(owners.length ? randomUUID() : watched),
      partition = kafkaProducerLane(s.sessionId, partitions);
    if (population[partition] >= runs / partitions) continue;
    population[partition]++;
    s.turnId = s.runId;
    runIds.push(s.runId);
    const presenter = new FamilyPresentation();
    presenter.bind(1, s);
    owners.push({ s, partition, presenter, observed: s.sessionId === watched });
  }
  const reply = "pi-cloud.tool-replies.v4." + randomUUID();
  const tape = [],
    digest = createHash("sha256");
  for (const commit of commits) {
    for (const { s, partition, presenter, observed } of owners) {
      const c = JSON.parse(JSON.stringify(commit).replaceAll(seed, s.runId));
      const d = presenter.prepare(c),
        fact = nativeBashFact(s, c, d.display, randomUUID(), reply);
      const value = encodeWorkspaceFact(fact);
      digest.update(value);
      d.confirmed();
      const visible = visibleRecord(c.writes);
      const marker =
        observed &&
        c.writes
          .find((w) => w.type === "entry" && w.value.kind === "pi.tool-result")
          ?.value.model[0].content[0].text.slice(-20);
      tape.push({ partition, key: s.sessionId, value, visible, marker });
    }
  }
  assert.equal(
    tape.reduce((n, r) => n + r.visible, 0),
    41 * runs,
  );
  console.log(
    JSON.stringify({
      kind: "tape",
      runs,
      tools,
      records: tape.length,
      sha256: digest.digest("hex"),
    }),
  );
  return { tape, prefixes };
}
async function watch(api, id, markers) {
  await api.chrome.navigate("about:blank");
  await api.resetTail();
  const pageUrl = new URL(api.address);
  pageUrl.searchParams.set("session", id);
  await api.chrome.navigate(pageUrl.href);
  await api.chrome.waitFor(
    "document.body.innerText.includes('Write measurement prompt') && document.querySelector('.product-chat-scroll')",
  );
  await api.chrome.evaluate(`(()=>{
    const markers=${JSON.stringify(markers)},seen=window.resultPaints={};
    const p=document.querySelector('.product-chat-scroll');
    function tick(){
      const viewport=p.getBoundingClientRect();
      for(const row of p.querySelectorAll('[data-tool-name=bash]')){
        const rect=row.getBoundingClientRect(); if(rect.bottom<=viewport.top || rect.top>=viewport.bottom)continue;
        const text=row.innerText;for(const marker of markers)if(!seen[marker]&&text.includes(marker))seen[marker]=Date.now();
      }
      requestAnimationFrame(tick);
    }tick();
  })()`);
}
async function createTopic(admin, topic) {
  for (let attempt = 0; ; attempt++)
    try {
      await admin.createTopics({
        topics: [
          {
            topic,
            numPartitions: partitions,
            replicationFactor: 3,
            configEntries: [{ name: "min.insync.replicas", value: "2" }],
          },
        ],
      });
      break;
    } catch (error) {
      if (attempt >= 3 || !error.errors?.every((e) => e.type === "ERR__DESTROY_BROKER"))
        throw error;
      await delay(300);
    }
  for (let attempt = 0; ; attempt++)
    try {
      const offsets = await admin.fetchTopicOffsets(topic);
      if (offsets.length === partitions) break;
      throw Error("Incomplete topic metadata");
    } catch (e) {
      if (attempt >= 30) throw e;
      await delay(300);
    }
}
async function counters(pool) {
  // Force each projection backend to publish its own counters before sampling.
  const clients = await Promise.all(Array.from({ length: 12 }, () => pool.connect()));
  try {
    await Promise.all(clients.map((c) => c.query("select pg_stat_force_next_flush()")));
    return (
      await clients[0]
        .query(`select (select n_tup_ins from pg_stat_user_tables where relname='messages' and schemaname=current_schema())::int inserts,n_tup_upd::int updates, pg_current_wal_insert_lsn()::text lsn,
      (select wal_sync_time from pg_stat_wal)::float8 as sync_ms,
      (select wal_sync from pg_stat_wal)::float8 as syncs,
      (select wal_write_time from pg_stat_wal)::float8 as write_ms,
      substring(pg_read_file('/sys/fs/cgroup/cpu.stat') from 'usage_usec ([0-9]+)')::float8/1000 as cpu_ms,
      substring(pg_read_file('/sys/fs/cgroup/cpu.stat') from 'throttled_usec ([0-9]+)')::float8/1000 as throttled_ms,
      coalesce((select calls from pg_stat_user_functions where schemaname=current_schema() and funcname='prefix_presented_message'),0)::int fused
      from pg_stat_user_tables where schemaname=current_schema() and relname='runs'`)
    ).rows[0];
  } finally {
    for (const c of clients) c.release();
  }
}
async function trial(api, install, mode, transport, data, offered, warmup = false) {
  const { db, chrome } = api,
    { admin, producer, brokers, topic } = transport;
  // Fresh groups start at the low watermark. Retire only the previous arm of
  // this runner-owned, disposable topic after its measured group fully closed.
  const old = (await admin.fetchTopicOffsets(topic)).filter(
    (p) => BigInt(p.low) < BigInt(p.offset),
  );
  if (old.length)
    await admin.deleteTopicRecords({
      topic,
      partitions: old.map((p) => ({ partition: p.partition, offset: p.offset })),
    });
  const pageArm = mode.endsWith("-page");
  const bashLog =
    {
      "intent-log": "intents",
      "intent-coverage": "coverage",
      "intent-folded": "folded",
      "folded-page": "folded",
    }[mode] ?? false;
  await install(appendStudy ? "combined" : pageArm ? "baseline" : mode);
  if (appendStudy) {
    await setBashLogIndexes(db, bashLog);
    await setBashLogReader(db, bashLog);
    await data.selectReadLayout?.(bashLog ? "bounded" : "combined");
  }
  await api.dependencyProbe(mode === "old-page" ? "original" : false);
  await db.query("delete from messages where run_id=any($1)", [runIds]);
  await db.query("delete from inboxes where pi_session_id=any($1)", [
    owners.map((o) => o.s.sessionId),
  ]);
  await db.query("delete from runs where id=any($1)", [runIds]);
  await db.query(
    `insert into runs(id,tenant_id,session_id,lease_id,mailbox_position,initial_input,idempotency_key,model_ref,state)
    select "runId","tenantId","sessionId","writerId",1,'Write measurement prompt',"runId",'fixture','running'
    from jsonb_to_recordset($1::jsonb) v("runId" uuid,"tenantId" uuid,"sessionId" uuid,"writerId" uuid)`,
    [JSON.stringify(owners.map((o) => o.s))],
  );
  const starts = new Map(
    (await admin.fetchTopicOffsets(topic)).map((v) => [v.partition, BigInt(v.offset)]),
  );
  const countsByOffset = Array.from({ length: partitions }, () => [0]);
  for (const r of data.tape) {
    const prefix = countsByOffset[r.partition];
    prefix.push(prefix.at(-1) + r.visible);
  }
  const projected = new Map(starts),
    brokerLag = [];
  let sampling = true,
    sampleJob;
  const effects = new Set(),
    acks = new Set(),
    pending = new Set(),
    ackTimes = [],
    grants = [];
  const eventLoop = monitorEventLoopDelay({ resolution: 10 });
  const submit = (_record, command) => {
    const id = command.request.operationId;
    assert(!effects.has(id));
    effects.add(id);
    const offeredAt = performance.now();
    const receipt = delay(50).then(() => {
      ackTimes.push(performance.now() - offeredAt);
      acks.add(id);
      pending.delete(receipt);
    });
    pending.add(receipt);
  };
  const projector = normalRootProjection(
    db,
    topic,
    (s) => ({ bindingId: s.runId, allocationId: s.runId }),
    () => ({ submit }),
    undefined,
    { partitions, bashLog },
  );
  if (data.sqlTape)
    projector.view.capturePrefix = (text, params) => data.sqlTape[params[1]].push({ text, params });
  await projector.view.reset(starts, 16, "recorded-history");
  const observed = owners[0].s.sessionId;
  await watch(api, observed, data.prefixes);
  const prepared = new WeakSet(),
    sentAt = new Map(),
    done = Promise.withResolvers();
  done.promise.catch(() => {});
  let handled = 0,
    admittedVisible = 0,
    sentVisible = 0,
    peakLag = 0,
    delivery = Promise.resolve(),
    end;
  const reader = new KafkaLogConsumer({
    brokers,
    topic,
    clientId: "write-study",
    groupId: "write-" + randomUUID(),
    batchRead: true,
    groupRecovery: true,
    orderedEffects: true,
    decode: (bytes) => decodeWorkspaceFact(bytes).fact,
    replayOffsets: async () => new Map(starts),
    prepareReady: async (records, current) => {
      try {
        const boundary = records.findIndex((r) => r.fact.scope.sessionId === observed);
        const plain = boundary < 0 ? records : records.slice(0, boundary);
        const n = plain.length ? await projector.prepareHistory(plain, current) : 0;
        for (const r of plain.slice(0, n)) prepared.add(r.fact);
        return n || 1;
      } catch (e) {
        done.reject(e);
        throw e;
      }
    },
    handler: async (r, current, grant) => {
      try {
        if (!prepared.has(r.fact)) {
          const result = await projector.nativeStore.projection.project(r.fact, r, current);
          assert(result.admitted);
          await projector.dispatch(r, current, async () => {
            const time = performance.now();
            const ok = await grant();
            grants.push(performance.now() - time);
            return ok;
          });
          if (r.fact.scope.sessionId === observed && (result.events.length || result.covers.length))
            delivery = delivery.then(() => api.publishFrame(tenant, result.events, result.covers));
        }
        projected.set(r.partition, r.offset + 1n);
        admittedVisible += visibleRecord(r.fact.commit.writes);
        if (++handled === data.tape.length) {
          end = performance.now();
          done.resolve();
        }
      } catch (e) {
        done.reject(e);
        throw e;
      }
    },
  });
  let timer,
    stopPaging = false,
    readJob;
  const pageDurations = [];
  try {
    await reader.start();
    await Promise.all([...starts.keys()].map((p) => reader.waitForPartition(p)));
    const before = await counters(db);
    const start = performance.now(),
      timeline = [],
      sends = new Set();
    eventLoop.enable();
    sampleJob = (async () => {
      while (sampling) {
        const offsets = await admin.fetchTopicOffsets(topic);
        let raw = 0,
          visible = 0;
        for (const p of offsets) {
          const high = Number(BigInt(p.offset) - starts.get(p.partition));
          const next = Math.min(high, Number(projected.get(p.partition) - starts.get(p.partition)));
          raw += high - next;
          visible += countsByOffset[p.partition][high] - countsByOffset[p.partition][next];
        }
        brokerLag.push({ ms: Math.round(performance.now() - start), raw, visible });
        await delay(400);
      }
    })();
    sampleJob.catch((e) => done.reject(e));
    if (pageArm) {
      readJob = (async () => {
        for (let index = 0; !stopPaging; index++) {
          const at = performance.now();
          const page = await api.page(data.pageWindow, {
            before: data.cursors[index % data.cursors.length],
          });
          assert(
            page.turns[0].transcript.items.some(
              (i) => i.kind === "tool" && i.input.command === "npm test",
            ),
          );
          pageDurations.push(performance.now() - at);
          await delay(Math.max(1, 500 - (performance.now() - at)));
        }
      })();
      readJob.catch((e) => done.reject(e));
    }
    // Offer equal-sized chunks from the identical tape. Capture queueing latency
    // from the producer send until a result row intersects Chrome's viewport.
    for (let at = 0; at < data.tape.length; at += 256) {
      const batch = data.tape.slice(at, at + 256),
        stamp = Date.now();
      for (const r of batch) {
        if (r.marker) sentAt.set(r.marker, stamp);
        sentVisible += r.visible;
      }
      peakLag = Math.max(peakLag, sentVisible - admittedVisible);
      const send = producer.send({
        topic,
        messages: batch.map(({ partition, key, value }) => ({ partition, key, value })),
      });
      sends.add(send);
      send.then(
        () => sends.delete(send),
        (err) => done.reject(err),
      );
      if (sends.size >= 12) await Promise.race(sends);
      if (at % 512 === 0)
        timeline.push({
          ms: Math.round(performance.now() - start),
          lag: sentVisible - admittedVisible,
        });
      await delay(
        Math.max(
          0,
          start +
            (1000 * (at + batch.length) * 41 * runs) / data.tape.length / offered -
            performance.now(),
        ),
      );
    }
    await Promise.all(sends);
    const inputEnded = performance.now();
    await Promise.race([
      done.promise,
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(Error("write tape did not drain")), 60000);
      }),
    ]);
    sampling = false;
    await sampleJob;
    stopPaging = true;
    await readJob;
    eventLoop.disable();
    await delivery;
    await chrome.waitFor("Object.keys(window.resultPaints).length===" + tools, 8000);
    const painted = await chrome.evaluate("window.resultPaints");
    const times = data.prefixes.map((m) => painted[m] - sentAt.get(m)).sort((a, b) => a - b);
    assert(times.every((v) => v >= 0));
    await Promise.all([...pending]);
    assert.equal(acks.size, runs * tools);
    assert.equal(admittedVisible, 41 * runs);
    const resultCount = (
      await db.query(
        "select count(*)::int n from messages where role='toolResult' and run_id=any($1)",
        [runIds],
      )
    ).rows[0].n;
    assert.equal(resultCount, runs * tools);
    const progress = await db.query(
      "select partition,next_offset from accepted_fact_projection_offsets where topic=$1",
      [topic],
    );
    const ends = await admin.fetchTopicOffsets(topic);
    for (const v of ends)
      assert.equal(progress.rows.find((r) => r.partition === v.partition).next_offset, v.offset);
    const after = await counters(db);
    const walBytes = Number(
      (await db.query("select pg_wal_lsn_diff($1,$2) bytes", [after.lsn, before.lsn])).rows[0]
        .bytes,
    );
    let appendCounters;
    if (appendStudy) {
      const {
        rows: [counts],
      } = await db.query(
        `select count(*)::int intents,count(*) filter(where m.display_through is not null)::int with_coverage,
        coalesce(avg(pg_column_size(m)),0)::float as mean_intent_bytes, count(*) filter(where m.content::jsonb->'args'?'command')::int copied_commands
        from messages m where m.run_id=any($1) and m.role='pma.bash-intent'`,
        [runIds],
      );
      assert.equal(counts.intents, bashLog ? tools * runs : 0);
      assert.equal(counts.copied_commands, 0);
      assert.equal(
        (await db.query("select from pending_bash_intents where run_id=any($1)", [runIds]))
          .rowCount,
        0,
      );
      const stable = (
        await db.query(
          "select count(*)::int n from messages where run_id=any($1) and entry_id is not null and display_through is not null",
          [runIds],
        )
      ).rows[0].n;
      if (bashLog === "coverage" || bashLog === "folded") {
        assert.equal(counts.with_coverage, tools * runs);
        assert(stable >= tools * runs);
      }
      appendCounters = { ...counts, covered_entries: stable };
    }
    const seconds = (end - start) / 1000;
    assert.equal(grants.length, effects.size);
    function mean(v) {
      return v.length ? v.reduce((s, n) => s + n, 0) / v.length : null;
    }
    function p95(v) {
      return v.toSorted((a, b) => a - b)[Math.ceil(0.95 * v.length) - 1];
    }
    const phases = {
      pgHoldMs: projector.view.stats.holdMs / projector.view.stats.commits,
      poolWaitMs: projector.view.stats.poolMs / projector.view.stats.commits,
      walSyncMs: after.sync_ms - before.sync_ms,
      walWriteMs: after.write_ms - before.write_ms,
      walSyncs: after.syncs - before.syncs,
      pgCpuMs: after.cpu_ms - before.cpu_ms,
      throttledMs: after.throttled_ms - before.throttled_ms,
      kafkaGrantMeanMs: mean(grants),
      kafkaGrantP95Ms: p95(grants),
      simulatedAckMeanMs: mean(ackTimes),
      loopP95Ms: eventLoop.percentile(95) / 1e6,
      pageReads: pageDurations.length,
      pageReadMeanMs: mean(pageDurations),
    };
    console.log(
      JSON.stringify({
        kind: "write-trial",
        mode,
        offered,
        warmup,
        phases,
        runUpdates: after.updates - before.updates,
        messageInserts: after.inserts - before.inserts,
        ...(appendCounters ? { appendCounters } : {}),
        fused: after.fused - before.fused,
        walBytes,
        seconds,
        visiblePerSecond: admittedVisible / seconds,
        actualInputPerSecond: (sentVisible * 1000) / (inputEnded - start),
        peakOfferedVisibleLag: peakLag,
        peakBrokerVisibleLag: Math.max(...brokerLag.map((s) => s.visible)),
        brokerLag,
        drainMs: Math.max(0, end - inputEnded),
        browserSamples: times.length,
        resultToVisibleP99Ms: times[Math.ceil(0.99 * times.length) - 1],
        browserMs: times,
        visible: admittedVisible,
        results: resultCount,
        acks: acks.size,
        transactions: projector.view.stats.commits,
        ...(appendStudy
          ? {
              snapshotAdvances: projector.view.stats.checkpointAdvances,
              firstRecoveryPins: projector.view.stats.firstPins,
            }
          : {}),
        timeline,
      }),
    );
  } finally {
    stopPaging = true;
    eventLoop.disable();
    await readJob;
    sampling = false;
    await sampleJob;
    clearTimeout(timer);
    await reader.close();
    await Promise.allSettled([delivery, ...pending]);
  }
}
const database = await testPostgres({});
try {
  await withPagingFixture(
    database.url,
    tenant,
    async (api) => {
      const { db } = api;
      await installWorkspaceRoutes(db);
      const install = await writeTrialFunctions(db);
      if (!appendStudy) {
        await checkWriterGuard(db, install);
        await checkMemoryWait(api, tenant, install);
      }
      if (process.argv.includes("--faults-only")) return;
      const observed = api.live;
      await db.query("insert into sessions(id,tenant_id,flow_id) values($1,$2,$1)", [
        observed,
        tenant,
      ]);
      const data = await makeTape(observed);
      let pageWindow;
      if (appendStudy && process.argv.includes("--confirm")) {
        const long = await seedActiveBashHistory(api, tenant);
        pageWindow = long.session;
        data.selectReadLayout = long.select;
        await long.select("combined");
      } else {
        pageWindow = await api.createScrollWindow("History reader under projection");
        await seedToolsWindow(api, tenant, undefined, { id: pageWindow, runs: 1, toolCount: 1024 });
      }
      data.pageWindow = pageWindow;
      data.cursors = [];
      for (let i = 0; i < 12; i++) {
        const page = await api.page(pageWindow, i ? { before: data.cursors.at(-1) } : {});
        assert(page.history.hasBefore);
        data.cursors.push(page.history.before);
      }
      const kafka = await testKafka({});
      const client = new native.KafkaJS.Kafka({
        kafkaJS: { brokers: kafka.brokers, logLevel: native.KafkaJS.logLevel.NOTHING },
      });
      const admin = client.admin(),
        producer = client.producer({ "linger.ms": 0, kafkaJS: { acks: -1, idempotent: true } });
      try {
        await admin.connect();
        await producer.connect();
        const topic = "write-study-" + randomUUID();
        await createTopic(admin, topic);
        const connection = { admin, producer, brokers: kafka.brokers, topic };
        if (sqlStudy) data.sqlTape = Array.from({ length: partitions }, () => []);
        await trial(api, install, "baseline", connection, data, 475, true);
        if (sqlStudy) {
          const { compareBashWriteSql } = await import("./bash-intent-sql-study.mjs");
          await compareBashWriteSql(
            database.url,
            api.db,
            data.sqlTape,
            owners.map((o) => o.s),
            topic,
          );
          return;
        }
        for (const offered of [475, 1200]) {
          const order = appendStudy
            ? process.argv.includes("--confirm")
              ? ["combined", "intent-folded", "combined-page", "folded-page"]
              : ["combined", "intent-log", "intent-coverage", "intent-folded"]
            : process.argv.includes("--confirm")
              ? ["baseline", "combined", "old-page", "exact-page"]
              : ["baseline", "fused", "combined", "cursor", "memory"];
          if (offered === 1200) order.reverse();
          for (const mode of [...order, ...order.reverse()])
            await trial(api, install, mode, connection, data, offered);
        }
      } finally {
        await Promise.allSettled([producer.disconnect(), admin.disconnect()]);
        await kafka.close();
      }
    },
    { liveStream: true, ioTiming: true, bashLogStudy: appendStudy },
  );
} finally {
  await database.close();
}
