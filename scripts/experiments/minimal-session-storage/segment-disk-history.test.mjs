import assert from "node:assert/strict";
import { mkdtemp, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { once } from "node:events";
import { spawn } from "node:child_process";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";
import { SegmentWarmHistory } from "./segment-warm-history.mjs";
import { SegmentDiskHistory } from "./segment-disk-history.mjs";
import { readPrefix } from "./session-prefix.mjs";

const job = (flow, fence) => ({
  tenant_id: "t",
  flow_id: flow,
  logical_id: "s-" + flow,
  id: "turn-" + fence,
  scope: {
    tenantId: "t",
    sessionId: "s-" + flow,
    runId: "turn-" + fence,
    writerId: "writer-" + fence,
    fencingToken: fence,
  },
});
const fact = (j) => ({
  kind: "work_finished",
  scope: j.scope,
  factId: "v" + j.scope.fencingToken,
  nativeSequence: j.scope.fencingToken,
  terminal: { type: "turn.completed" },
});
const entry = (id, text) => ({
  id,
  conversationId: 1,
  kind: "pi.user",
  model: [{ role: "user", content: text, timestamp: 1 }],
});
function append(b, j, text) {
  b.append({
    seq: j.scope.fencingToken,
    writes: [{ type: "entry", value: entry(j.scope.fencingToken + 1, text) }],
  });
}
async function dirTest(work) {
  const dir = await mkdtemp(join(tmpdir(), "ssd-unit-"));
  try {
    await work(dir);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}
test("SSD scope, delta, quota, TTL and RAM downgrade use the exact confirmed version", () =>
  dirTest(async (dir) => {
    let now = 1000;
    const changes = [],
      config = {
        diskDirectory: dir,
        diskMaxBytes: 900000,
        diskTtlMs: 86400000,
        maxBytes: 350000,
        minBytes: 1,
        ttlMs: 100,
        now: () => now,
        advertise: (...a) => changes.push(a),
      };
    const cache = new SegmentWarmHistory(config);
    try {
      const j = job("a", 1),
        b = cache.capture(j);
      b.seed([]);
      append(b, j, "x".repeat(300000));
      const write = await b.finish(fact(j));
      assert.equal(write.delta, false);
      now += 200;
      cache.cache.expire();
      assert.equal(changes.at(-1)[3], "disk");
      assert.equal(changes.at(-1)[4], 1000 + 86400000);
      const successor = job("a", 2),
        s = cache.capture(successor),
        hit = await s.get(null, "v1");
      assert(hit.diskReadBytes >= 300000);
      for (const cross of [{ tenant_id: "other" }, { logical_id: "other" }, { flow_id: "other" }])
        assert.equal(await cache.capture({ ...successor, ...cross }).get(null, "v1"), undefined);
      assert.equal(await s.get(null, "unknown"), undefined);
      await s.get(null, "v1");
      s.seed(hit.inherited, hit.records, hit.floor);
      append(s, successor, "next");
      const delta = await s.finish(fact(successor));
      assert(delta.delta && delta.payloadBytes < 1000);
      const other = job("b", 1),
        second = cache.capture(other);
      second.seed([]);
      append(second, other, "z".repeat(680000));
      assert((await second.finish(fact(other))).evicted.includes("t/a"));
      assert.equal(cache.disk.heads.has("t/a"), false);
      assert(cache.disk.heads.has("t/b"));
      assert((await stat(join(dir, "history.sqlite"))).size <= config.diskMaxBytes);
      now += 86400001;
      await cache.disk.refresh();
      assert.equal(cache.disk.heads.size, 0);
    } finally {
      await cache.close();
    }
  }));
test("a killed SSD writer leaves the previous manifest readable; a damaged Entry falls back to PG", () =>
  dirTest(async (dir) => {
    const opts = {
      directory: dir,
      maxBytes: 3 * 1024 * 1024,
      ttlMs: 86400000,
      now: Date.now,
      changed() {},
    };
    const h = {
      k: "t/a",
      session: "s",
      version: "old",
      fence: 1,
      seq: 3,
      floor: 1,
      locals: 1,
      ancestors: 0,
    };
    const image = entry(3, "x".repeat(5000)),
      weight = Buffer.byteLength(JSON.stringify(image));
    Object.assign(h, { localBytes: weight, bytes: weight });
    let cache = new SegmentDiskHistory(opts);
    try {
      assert(await cache.put(h, null, [], [{ entry: image, seq: 3, bytes: weight }], []));
    } finally {
      await cache.close();
    }
    const script = `import {SegmentDiskHistory} from ${JSON.stringify(new URL("./segment-disk-history.mjs", import.meta.url).href)};
    const d=new SegmentDiskHistory({...JSON.parse(process.argv[1]),now:Date.now,changed(){}});
    await d.tail;await d.rpc("begin",{head:JSON.parse(process.argv[2]),base:"old",now:Date.now()});
    await d.rpc("append",{inherited:false,rows:[{entry:{id:4,conversationId:1,model:[{content:"y".repeat(2200000)}]},seq:4,bytes:2200000}]});
    process.send("staged");setInterval(()=>{},1000);`;
    const child = spawn(
      process.execPath,
      [
        "--input-type=module",
        "-e",
        script,
        JSON.stringify(opts),
        JSON.stringify({ ...h, fence: 2, version: "uncommitted" }),
      ],
      { stdio: ["ignore", "ignore", "inherit", "ipc"] },
    );
    const exited = once(child, "exit");
    try {
      const [ready] = await Promise.race([
        once(child, "message", { signal: AbortSignal.timeout(10000) }),
        exited,
      ]);
      assert.equal(ready, "staged");
    } finally {
      child.kill("SIGKILL");
      await exited;
    }
    cache = new SegmentDiskHistory(opts);
    try {
      assert.deepEqual((await cache.get(h.k, h.session, h.version)).records[0].entry, image);
      assert.equal(await cache.get(h.k, h.session, "uncommitted"), undefined);
      const db = new DatabaseSync(join(dir, "history.sqlite"));
      db.exec("update entries set body=replace(body,'xxxxx','zzzzz')");
      db.close();
      let pgReads = 0;
      const read = await readPrefix(
        {
          query: async () => {
            pgReads++;
            return { rows: [{ entry_id: 3, commit_seq: 3, content: JSON.stringify(image) }] };
          },
        },
        { tenant_id: "t", session_id: "a" },
        { version: "old", seq: 3, floor: 1, nextId: 4, count: 1, bytes: weight },
        { get: (_, v) => cache.get(h.k, h.session, v) },
      );
      assert.equal(read.status, "cold");
      assert.equal(pgReads, 1);
      assert.deepEqual(read.records[0].entry, image);
      assert.equal(cache.heads.size, 0);
    } finally {
      await cache.close();
    }
  }));
test("an unavailable cache directory leaves memory continuation usable", () =>
  dirTest(async (dir) => {
    const path = join(dir, "plain-file");
    await writeFile(path, "keep");
    const cache = new SegmentWarmHistory({ diskDirectory: path, minBytes: 1 });
    try {
      const j = job("a", 1),
        b = cache.capture(j);
      b.seed([]);
      append(b, j, "text");
      await b.finish(fact(j));
      assert.equal(b.get(null, "v1").records.length, 1);
    } finally {
      await cache.close();
    }
  }));

test("the scheduler retains disk advertisements beyond the RAM TTL and selects the fastest available tier", async () => {
  const { openWakeRouter } = await import("./wake-channel.mjs");
  const { WebSocket } = await import("ws");
  const { randomUUID } = await import("node:crypto");
  const { setTimeout: delay } = await import("node:timers/promises");
  const router = await openWakeRouter("127.0.0.1", "/warm", (e) => {
    throw e;
  });
  router.configure({
    label: "test",
    cacheMode: "prefer",
    cacheTtlMs: 10,
    perTurn: true,
    wakeMode: "directed",
    tickMs: 60000,
    batch: 1,
  });
  const clients = [];
  try {
    const connect = async (id) => {
      const c = new WebSocket("ws://127.0.0.1:" + router.port + "/warm?stage=test&id=" + id);
      clients.push(c);
      await once(c, "message");
      return c;
    };
    const a = await connect("A"),
      b = await connect("B"),
      tenant = randomUUID(),
      flow = randomUUID(),
      version = randomUUID();
    const offer = (client, kind) =>
      client.send(JSON.stringify([tenant, flow, version, kind, Date.now() + 30000]));
    const ready = async (socket, count) => {
      socket.send("ready");
      for (let i = 0; i < 200 && router.snapshot().waiting !== count; i++) await delay(5);
      assert.equal(router.snapshot().waiting, count);
    };
    const claim = async (socket) => {
      const turn = randomUUID(),
        msg = once(socket, "message", { signal: AbortSignal.timeout(1500) });
      router.notifySegment([tenant, flow, turn, version, flow, Date.now() + 2000]);
      assert.equal(JSON.parse((await msg)[0])[2], turn);
    };
    offer(b, "disk-large");
    await ready(b, 1);
    offer(a, "large");
    await ready(a, 2);
    await delay(30);
    await claim(a);
    await claim(b);
    // The disk holder can take a Turn while the memory holder is busy.
    assert.equal(router.snapshot().preferred, 2);
  } finally {
    for (const c of clients) c.terminate();
    await router.close();
  }
});
