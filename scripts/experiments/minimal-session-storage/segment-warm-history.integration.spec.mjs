// Real PG queue/leases, two independent slot pools, a real Pi context and a
// deterministic local model. Seed image bodies directly to time the read leg.
import assert from "node:assert/strict";
import { createHash, randomBytes, randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { crc32, deflateSync } from "node:zlib";
import pg from "pg";
import { it } from "vitest";
import { createDatabase, runMigrations } from "@pi-cloud/database";
import { BACKGROUND_CONTEXT as ctx } from "@earendil-works/chord/context";
import { Harness, createRegistry } from "@earendil-works/pi-durable";
import { createModels } from "@earendil-works/pi-ai/models";
import {
  fauxProvider,
  fauxAssistantMessage,
  fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import { createPrivateTenant } from "../../../packages/control-plane/src/tenant-administration.ts";
import { registerTestWorker } from "../../../packages/control-plane/test/fixtures/registered-worker.ts";
import { IndependentSessionRuntime, openSegmentNative } from "./independent-session-runtime.mjs";
import { installIndependentSessionLeases } from "./independent-session-leases.mjs";
import { installForkTables, setForkLayout } from "./fork-storage.mjs";
import { installWorkspaceRoutes } from "./workspace-route-spans.mjs";
import { installPagingProjection } from "./flow-paging-fixture.mjs";

function png() {
  const chunk = (name, data) => {
    const body = Buffer.concat([Buffer.from(name), data]),
      length = Buffer.alloc(4),
      sum = Buffer.alloc(4);
    length.writeUInt32BE(data.length);
    sum.writeUInt32BE(crc32(body));
    return Buffer.concat([length, body, sum]);
  };
  const side = 512,
    stride = 1 + side * 3,
    pixels = randomBytes(side * stride);
  for (let row = 0; row < side; row++) pixels[row * stride] = 0;
  const header = Buffer.alloc(13);
  header.writeUInt32BE(side);
  header.writeUInt32BE(side, 4);
  header[8] = 8;
  header[9] = 2;
  return Buffer.concat([
    Buffer.from("89504e470d0a1a0a", "hex"),
    chunk("IHDR", header),
    chunk("IDAT", deflateSync(pixels, { level: 0 })),
    chunk("IEND", Buffer.alloc(0)),
  ]).toString("base64");
}

const endpoint = process.env.PI_CLOUD_POSTGRES_INTEGRATION_URL;
it.skipIf(!endpoint).each(["memory", "ssd"])(
  "uses verified %s image history and bounds Worker affinity",
  async (tier) => {
    const directory = await mkdtemp(join(tmpdir(), "segment-disk-integration-"));
    let clockOffset = 0;
    const now = () => Date.now() + clockOffset;
    const controls = new Map();
    const admin = createDatabase({ connectionString: endpoint }),
      schema = "warm_" + randomUUID().replaceAll("-", "");
    const url = new URL(endpoint);
    url.searchParams.set("options", "-c search_path=" + schema + ",public");
    const pool = new pg.Pool({ connectionString: url.href, max: 12 }),
      topic = "warm";
    const rt = new IndependentSessionRuntime({ pool, topic });
    const root = randomUUID(),
      side = randomUUID(),
      token = randomUUID() + randomUUID();
    const workers = [],
      loops = [],
      gates = new Map(),
      abort = new AbortController();
    let scheduler,
      tenant,
      failure,
      offset = 0n,
      observedBytes = 0;
    const rawQuery = pool.query.bind(pool);
    pool.query = async (...args) => {
      const result = await rawQuery(...args);
      if (
        typeof args[0] === "string" &&
        args[0].includes("select entry_id,commit_seq,content from messages")
      )
        for (const row of result.rows) observedBytes += Buffer.byteLength(row.content);
      return result;
    };
    const readPage = rt.store.readPage.bind(rt.store);
    rt.store.readPage = async (...args) => {
      const rows = await readPage(...args);
      for (const row of rows) observedBytes += Buffer.byteLength(row.content);
      return rows;
    };
    async function until(read) {
      const until = Date.now() + 15000;
      while (Date.now() < until) {
        if (failure) throw failure;
        const value = await read();
        if (value) return value;
        await delay(5);
      }
      throw Error("Timed out waiting for a warm Session claim");
    }
    async function worker(diskDirectory = false) {
      const workerId = randomUUID(),
        bootId = randomUUID(),
        capacity = 1;
      await admin
        .insertInto("workers")
        .values({
          id: workerId,
          boot_id: bootId,
          supervisor_id: workerId,
          max_concurrent_sessions: capacity,
          state: "ready",
        })
        .execute();
      await registerTestWorker(admin, workerId);
      const w = rt.worker({
        workerId,
        bootId,
        capacity,
        leaseDurationMs: 30000,
        historyCache: { minBytes: 1, diskDirectory, now },
      });
      workers.push(w);
      const stop = new AbortController();
      controls.set(w, stop);
      loops.push(
        w
          .serve({
            scheduler: { url: scheduler.url, token },
            signal: AbortSignal.any([abort.signal, stop.signal]),
            onError: (e) => {
              failure = e;
            },
            execute: async (job) => {
              const gate = Promise.withResolvers();
              gates.set(job.id, { job, w, gate, at: performance.now() });
              await gate.promise;
            },
          })
          .catch((e) => {
            failure = e;
          }),
      );
      return w;
    }
    async function enqueue(session, n) {
      const id = randomUUID();
      await pool.query(
        "insert into turns(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,state) select $1,$2,s.flow_id,$4,$1,'Continue '||$4::bigint,'queued' from sessions s where s.id=$3 and s.tenant_id=$2",
        [id, tenant, session, n],
      );
      return id;
    }
    async function run(
      job,
      { coldCopy = false, sample = false, compact = false, addImage = false } = {},
    ) {
      let seq = 0,
        modelContext,
        images = 0;
      const publish = async (fact) => {
        assert(
          await rt.consume(
            { topic, partition: 0, offset: offset++, fact },
            () => true,
            async () => true,
          ),
        );
      };
      const priorBytes = observedBytes,
        started = performance.now();
      const opened = await openSegmentNative(
        rt.store,
        coldCopy ? { ...job, warmHistory: undefined } : job,
        topic,
        async (commit) => {
          assert(sample, "Read-only context comparison published a native write");
          seq = commit.seq;
          await publish({
            kind: "durable_commit",
            scope: job.scope,
            commit,
            factId: randomUUID(),
            occurredAt: new Date().toISOString(),
            display: [],
            tools: [],
            cancellations: [],
            origins: [],
          });
        },
      );
      const models = createModels(),
        fake = fauxProvider({ tokensPerSecond: 0 });
      models.setProvider(fake.provider);
      const harness = await Harness.open(
        opened.storage,
        { models, registry: createRegistry(), settings: { compaction: { enabled: false } } },
        ctx,
      );
      try {
        const convo = await harness.root(ctx);
        const { messages } = await convo.context(ctx);
        const loadMs = Math.round(performance.now() - started);
        const hash = createHash("sha256");
        for (const m of messages) {
          hash.update(JSON.stringify(m));
          if (Array.isArray(m.content))
            images += m.content.filter((b) => b.type === "image").length;
        }
        modelContext = hash.digest("hex");
        const bytes = observedBytes - priorBytes;
        assert.equal(opened.load.pgEntryBytes, bytes);
        const result = {
          path: opened.load.path,
          bytes,
          images,
          digest: modelContext,
          diskReadBytes: opened.load.diskEntryBytes,
          ms: loadMs,
        };
        if (sample) {
          if (addImage)
            await convo.commit(
              (tx) =>
                tx.appendEntry(convo.id, {
                  kind: "pi.user",
                  model: [
                    {
                      role: "user",
                      timestamp: 1,
                      content: [{ type: "image", mimeType: "image/png", data: png() }],
                    },
                  ],
                }),
              ctx,
            );
          if (compact) {
            await convo.commit(
              (tx) =>
                tx.appendEntry(convo.id, {
                  kind: "pi.compaction",
                  head: "self",
                  model: [{ role: "user", content: "Condensed image session", timestamp: 1 }],
                  data: { reason: "manual" },
                }),
              ctx,
            );
          }
          const m = fake.getModel();
          await convo.configure({ model: { provider: m.provider, modelId: m.id } }, ctx);
          const answer = fauxAssistantMessage("Done");
          fake.setResponses([answer]);
          const sub = await convo.submit(
            { type: "input", requestId: job.id, content: job.initial_input },
            ctx,
          );
          await convo.waitForIdle(ctx);
          const settled = await sub.wait(ctx);
          assert.equal(
            settled.status,
            "done",
            settled.reason + ": " + String(settled.detail ?? "").slice(0, 300),
          );
          const fact = {
            kind: "work_finished",
            scope: job.scope,
            factId: randomUUID(),
            agentId: "root",
            nativeSequence: seq,
            baseSequence: 0,
            terminal: { type: "turn.completed", payload: { stopReason: "stop" } },
            occurredAt: new Date().toISOString(),
          };
          const writing = opened.rememberCompletion(fact);
          await publish(fact);
          result.diskWrite = await writing;
        }
        return result;
      } finally {
        await harness.close(ctx);
      }
    }
    try {
      await runMigrations(admin, "up");
      tenant = (
        await createPrivateTenant(admin, {
          slug: schema.replace("_", "-"),
          ownerDisplayName: "Warm history",
        })
      ).tenantId;
      await pool.query("create schema " + schema);
      await installPagingProjection(pool);
      await installForkTables(pool);
      await setForkLayout(pool, true, false);
      await installWorkspaceRoutes(pool);
      await installIndependentSessionLeases(pool);
      await rt.store.register(tenant, [root, side]);
      await rt.replayOffsets([{ partition: 0, low: 0n, high: 0n }]);
      const first = randomUUID(),
        version = randomUUID(),
        count = process.env.PMA_WARM_LARGE === "1" ? 104 : 17;
      await pool.query(
        "insert into turns(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,state,settled_at) values($1,$2,$3,1,$1,'Pictures','completed',clock_timestamp())",
        [first, tenant, root],
      );
      let id = 3,
        totalBytes = 0,
        entries = 0;
      async function add(kind, message) {
        const value = { id, conversationId: 1, kind, model: [message] },
          body = JSON.stringify(value);
        await pool.query(
          "insert into messages(tenant_id,pi_session_id,entry_id,conversation_id,run_id,role,content,commit_seq,ordinal) values($1,$2,$3,1,$4,$5,$6,$3,0)",
          [tenant, root, id, first, message.role, body],
        );
        totalBytes += Buffer.byteLength(body);
        entries++;
        id++;
      }
      for (let i = 0; i < count - 1; i++) {
        const image = { type: "image", mimeType: "image/png", data: png() };
        if (i % 2 === 0) await add("pi.user", { role: "user", timestamp: i, content: [image] });
        else {
          const call = "image-" + i;
          await add(
            "pi.assistant",
            fauxAssistantMessage(
              fauxToolCall("read", { path: "/workspace/image-" + i + ".png" }, { id: call }),
              { stopReason: "toolUse" },
            ),
          );
          await add("pi.tool-result", {
            role: "toolResult",
            toolCallId: call,
            toolName: "read",
            timestamp: i,
            isError: false,
            content: [image],
          });
        }
      }
      await pool.query("update turns set history_head=$2 where id=$1", [
        first,
        JSON.stringify({
          version,
          seq: id - 1,
          nextId: id,
          floor: 1,
          count: entries,
          bytes: totalBytes,
        }),
      ]);
      await pool.query(
        "update flows set native_inbox_id=2,native_inbox_created=1,native_inbox_seq=1,native_inbox_ordinal=0 where tenant_id=$1 and flow_id=$2",
        [tenant, root],
      );
      scheduler = await rt.serveScheduler({ connectionString: url.href, token, tickMs: 100 });
      const a = await worker(tier === "ssd" ? directory : false);
      await until(() => scheduler.snapshot().waiting === 1);
      const second = await enqueue(root, 2),
        two = await until(() => gates.get(second));
      const initial = await run(two.job, { sample: true, addImage: true });
      assert.equal(initial.bytes, totalBytes);
      assert.equal(initial.images, count - 1);
      const b = await worker();
      await until(() => scheduler.snapshot().waiting === 1);
      if (tier === "ssd") {
        assert(initial.diskWrite && !initial.diskWrite.delta);
        // Advance only the cache clock. Queue and PG lease timing stay real.
        clockOffset += 31 * 60 * 1000;
        a.warmHistory.cache.expire();
        assert.equal(a.warmHistory.cache.bytes, 0);
        const third = await enqueue(root, 3);
        await delay(120);
        assert(!gates.has(third), "disk-only owner lost large-history affinity");
        two.gate.resolve();
        const three = await until(() => gates.get(third));
        assert.equal(three.w.workerId, a.workerId);
        const cold = await run(three.job, { coldCopy: true });
        const diskBefore = await a.warmHistory.disk.usage();
        const ssd = await run(three.job, { sample: true });
        const diskAfter = await a.warmHistory.disk.usage();
        const turnWriteBytes =
          diskBefore.threadWriteBytes === null || diskAfter.threadWriteBytes === null
            ? null
            : diskAfter.threadWriteBytes - diskBefore.threadWriteBytes;
        assert(turnWriteBytes === null || turnWriteBytes < 1024 * 1024);
        assert.equal(ssd.path, "disk");
        assert.equal(ssd.bytes, 0);
        assert.equal(ssd.images, count);
        assert.equal(ssd.digest, cold.digest);
        assert(ssd.diskWrite.delta && ssd.diskWrite.payloadBytes < 65536);
        assert(
          ssd.diskWrite.deviceWriteBytes === null || ssd.diskWrite.deviceWriteBytes < 1024 * 1024,
        );
        three.gate.resolve();
        await until(() => scheduler.snapshot().waiting === 2);
        const fourth = await enqueue(root, 4),
          four = await until(() => gates.get(fourth));
        assert.equal(four.w.workerId, a.workerId);
        const memory = await run(four.job, { sample: true });
        assert.equal(memory.path, "warm");
        assert.equal(memory.diskReadBytes, 0);
        four.gate.resolve();
        await until(() => scheduler.snapshot().waiting === 2);
        controls.get(a).abort();
        await loops[0];
        await a.close();
        const restarted = await worker(directory);
        await restarted.warmHistory.disk.tail;
        await until(() => scheduler.snapshot().waiting === 2);
        const fifth = await enqueue(root, 5),
          five = await until(() => gates.get(fifth));
        assert.equal(five.w.workerId, restarted.workerId);
        const beforeCompact = await run(five.job, { coldCopy: true }),
          afterRestart = await run(five.job, { sample: true, compact: true });
        assert.equal(afterRestart.path, "disk");
        assert.equal(afterRestart.bytes, 0);
        assert.equal(afterRestart.digest, beforeCompact.digest);
        five.gate.resolve();
        await until(() => scheduler.snapshot().waiting === 2);
        // This next prefix is under 1 KiB; ordinary FIFO can legitimately
        // claim it ahead of a cache notice. Isolate the disk reader here;
        // both Workers competed for the preceding large restart claim.
        controls.get(b).abort();
        await loops[1];
        await b.close();
        await until(() => scheduler.snapshot().waiting === 1);
        clockOffset += 31 * 60 * 1000;
        restarted.warmHistory.cache.expire();
        const sixth = await enqueue(root, 6),
          six = await until(() => gates.get(sixth));
        assert.equal(six.w.workerId, restarted.workerId);
        const compacted = await run(six.job, { sample: true });
        assert.equal(compacted.images, 0);
        assert.equal(compacted.path, "disk");
        assert(compacted.diskReadBytes < 65536);
        six.gate.resolve();
        await until(() => scheduler.snapshot().waiting === 1);
        // The Fork's immutable inherited range belongs to its own directory key.
        const anchor = Number(
          (
            await pool.query(
              "select max(entry_id) id from messages where run_id=$1 and role='assistant'",
              [second],
            )
          ).rows[0].id,
        );
        const peer = await rt.store.fork(tenant, root, { lane: 1, run: second, entry: anchor });
        const p1 = await enqueue(peer, 1),
          claimedPeer = await until(() => gates.get(p1));
        const firstPeer = await run(claimedPeer.job, { sample: true });
        assert.equal(firstPeer.path, "cold");
        assert.equal(firstPeer.images, count);
        claimedPeer.gate.resolve();
        await until(() => scheduler.snapshot().waiting === 1);
        clockOffset += 31 * 60 * 1000;
        restarted.warmHistory.cache.expire();
        const p2 = await enqueue(peer, 2),
          continuePeer = await until(() => gates.get(p2));
        const peerCold = await run(continuePeer.job, { coldCopy: true }),
          peerDisk = await run(continuePeer.job, { sample: true });
        assert.equal(peerDisk.path, "disk");
        assert.equal(peerCold.digest, peerDisk.digest);
        assert.equal(peerDisk.bytes, 0);
        assert.equal(peerDisk.images, count);
        assert(peerDisk.diskWrite.delta && peerDisk.diskWrite.payloadBytes < 65536);
        continuePeer.gate.resolve();
        await until(() => scheduler.snapshot().waiting === 1);
        clockOffset += 24 * 60 * 60 * 1000 + 1;
        restarted.warmHistory.cache.expire();
        await restarted.warmHistory.disk.refresh();
        const seventh = await enqueue(root, 7),
          seven = await until(() => gates.get(seventh));
        const expired = await run(seven.job, { sample: true });
        assert.equal(expired.path, "cold");
        assert(expired.bytes > 0);
        seven.gate.resolve();
        await rt.store.retire(tenant, root, { lane: 1, run: second, entry: anchor });
        const prunedTurn = await enqueue(root, 1),
          prunedJob = await until(() => gates.get(prunedTurn));
        const clipped = await run(prunedJob.job, { sample: true });
        assert.equal(clipped.path, "cold");
        assert.equal(clipped.digest, firstPeer.digest);
        prunedJob.gate.resolve();
        process.stdout.write(
          "ssd-image-continuation " +
            JSON.stringify({
              count,
              coldPgBytes: cold.bytes,
              coldMs: cold.ms,
              diskPgBytes: ssd.bytes,
              diskReadBytes: ssd.diskReadBytes,
              diskMs: ssd.ms,
              bootstrapBytes: initial.diskWrite.payloadBytes,
              turnDeviceWriteBytes: turnWriteBytes,
              mainFileGrowth: diskAfter.mainBytes - diskBefore.mainBytes,
              deltaPayloadBytes: ssd.diskWrite.payloadBytes,
              deltaDeviceWriteBytes: ssd.diskWrite.deviceWriteBytes,
              restartPgBytes: afterRestart.bytes,
              compactedDiskBytes: compacted.diskReadBytes,
              forkDiskPgBytes: peerDisk.bytes,
            }) +
            "\n",
        );
        return;
      }
      const start = performance.now(),
        third = await enqueue(root, 3);
      await delay(150);
      assert(!gates.has(third), "a FIFO probe stole the large Turn before its grace expired");
      const sideStart = performance.now();
      const unrelated = await enqueue(side, 1),
        other = await until(() => gates.get(unrelated));
      assert.equal(other.w.workerId, b.workerId);
      two.gate.resolve();
      await run(other.job, { sample: true });
      other.gate.resolve();
      const three = await until(() => gates.get(third));
      assert.equal(three.w.workerId, a.workerId);
      const cold = await run(three.job, { coldCopy: true }),
        warm = await run(three.job, { sample: true });
      assert.equal(warm.path, "warm");
      assert.equal(warm.bytes, 0);
      assert(cold.bytes >= totalBytes);
      assert.equal(warm.digest, cold.digest);
      assert.equal(warm.images, count);
      const delayed = performance.now(),
        fourth = await enqueue(root, 4),
        four = await until(() => gates.get(fourth));
      assert.equal(four.w.workerId, b.workerId);
      assert(
        four.at - delayed >= 1850 && four.at - delayed < 4500,
        "fallback exceeded its test window",
      );
      const fallback = await run(four.job, { sample: true });
      assert.equal(fallback.path, "cold");
      assert(fallback.bytes >= totalBytes);
      assert(
        (
          await pool.query(
            "select count(*)::int n from turns where id=$1 and lease_id is not null",
            [fourth],
          )
        ).rows[0].n === 1,
      );
      // A real eviction releases the preferred window before its deadline.
      three.gate.resolve();
      await until(() => scheduler.snapshot().waiting === 1);
      const fifth = await enqueue(root, 5);
      await delay(80);
      assert(!gates.has(fifth));
      const evictedAt = performance.now();
      b.warmHistory.cache.remove(tenant + "/" + root);
      const afterEvict = await until(() => gates.get(fifth));
      assert.equal(afterEvict.w.workerId, a.workerId);
      assert(afterEvict.at - evictedAt < 1200);
      assert.equal((await run(afterEvict.job, { sample: true })).path, "cold");
      afterEvict.gate.resolve();
      // A peer's first Turn reads only the frozen path even if the source is warm.
      const entry = Number(
        (
          await pool.query(
            "select max(entry_id) as id from messages where run_id=$1 and role='assistant'",
            [second],
          )
        ).rows[0].id,
      );
      const branch = await rt.store.fork(tenant, root, { lane: 1, run: second, entry });
      four.gate.resolve();
      three.gate.resolve();
      await until(() => scheduler.snapshot().waiting === 2);
      const branch1 = await enqueue(branch, 1),
        firstBranch = await until(() => gates.get(branch1));
      const inherited = await run(firstBranch.job, { sample: true });
      assert.equal(inherited.path, "cold");
      assert.equal(inherited.images, count);
      const branch2 = await enqueue(branch, 2);
      await delay(120);
      assert(!gates.has(branch2), "inherited image bytes also deserve affinity");
      firstBranch.gate.resolve();
      const secondBranch = await until(() => gates.get(branch2));
      assert.equal(secondBranch.w.workerId, firstBranch.w.workerId);
      const forkCold = await run(secondBranch.job, { coldCopy: true }),
        forkWarm = await run(secondBranch.job, { sample: true, compact: true });
      assert.equal(forkWarm.digest, forkCold.digest);
      assert.equal(forkWarm.images, count);
      assert.equal(forkWarm.bytes, 0);
      secondBranch.gate.resolve();
      await until(() => scheduler.snapshot().waiting === 2);
      const branch3 = await enqueue(branch, 3),
        compacted = await until(() => gates.get(branch3));
      assert.equal(compacted.w, secondBranch.w);
      const compactCold = await run(compacted.job, { coldCopy: true }),
        compactWarm = await run(compacted.job, { sample: true });
      assert.equal(compactWarm.path, "warm");
      assert.equal(compactWarm.images, 0);
      assert.equal(compactCold.digest, compactWarm.digest);
      assert(compactCold.bytes < 65536, "cold open reread ancestors below the compaction head");
      compacted.gate.resolve();
      await rt.store.retire(tenant, root, { lane: 1, run: second, entry });
      const pruned = await enqueue(root, 1),
        newFlow = await until(() => gates.get(pruned));
      assert.notEqual(newFlow.job.flow_id, root);
      const clipped = await run(newFlow.job, { sample: true });
      assert.equal(clipped.path, "cold");
      assert.equal(clipped.digest, inherited.digest);
      newFlow.gate.resolve();
      await until(() => scheduler.snapshot().waiting === 2);
      // With no scheduler to deliver the exact hint, the PG deadline still
      // permits an ordinary claimant after the grace period.
      await scheduler.close();
      scheduler = undefined;
      const missedAt = performance.now(),
        missed = await enqueue(root, 2);
      assert.equal(await b.claimNext(), undefined);
      const recovered = await until(() => b.claimNext());
      assert.equal(recovered.id, missed);
      const missedMs = Math.round(performance.now() - missedAt);
      assert(missedMs >= 1850 && missedMs < 4500);
      await recovered.release();
      process.stdout.write(
        "warm-image-continuation " +
          JSON.stringify({
            lostWakeFallbackMs: missedMs,
            forkWarmPgBytes: forkWarm.bytes,
            evictionFallbackMs: Math.round(afterEvict.at - evictedAt),
            compactColdBytes: compactCold.bytes,
            count,
            bodyMiB: +(cold.bytes / 1048576).toFixed(2),
            warmMs: warm.ms,
            coldMs: cold.ms,
            warmPgBytes: warm.bytes,
            coldPgBytes: cold.bytes,
            preferredClaimMs: Math.round(three.at - start),
            fallbackClaimMs: Math.round(four.at - delayed),
            unrelatedClaimMs: Math.round(other.at - sideStart),
          }) +
          "\n",
      );
    } finally {
      abort.abort();
      for (const e of gates.values()) e.gate.resolve();
      await Promise.allSettled(loops);
      for (const w of workers) await w.close();
      await scheduler?.close();
      await pool.query("drop schema if exists " + schema + " cascade");
      await rt.store.db.destroy();
      await admin.destroy();
      await rm(directory, { recursive: true, force: true });
    }
  },
  180000,
);
