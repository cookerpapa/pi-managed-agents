// Dedicated fixture sockets; no shell executor, reply ledger or redelivery.
import assert from "node:assert/strict";
import { networkInterfaces } from "node:os";
import { once } from "node:events";
import WebSocket, { WebSocketServer } from "ws";
import { fanoutFixture, targets } from "./fanout-fixture.mjs";
import { wall } from "./pg-path-replay.mjs";

export async function fanoutReceiver(token, fail) {
  const fixture = fanoutFixture(token);
  const host = Object.values(networkInterfaces())
    .flat()
    .find((n) => n.family === "IPv4" && !n.internal)?.address;
  assert(host);
  const server = new WebSocketServer({ host, port: 0, perMessageDeflate: false, maxPayload: 4096 });
  await once(server, "listening");
  const sockets = new Map();
  let arm,
    delay,
    counts,
    offsets,
    firstAt,
    lastAt,
    pending = 0,
    total = 0,
    peak = 0,
    buckets;
  server.on("connection", (s, r) => {
    const [, path, slot] = r.url.split("/"),
      target = Number(slot);
    if (
      path !== token ||
      !Number.isInteger(target) ||
      target < 0 ||
      target >= targets ||
      sockets.has(target)
    ) {
      s.terminate();
      return;
    }
    sockets.set(target, s);
    s.on("error", fail);
    s.on("message", (raw) => {
      try {
        const q = JSON.parse(raw),
          index = counts[target] * targets + target,
          expected = fixture.owners[target];
        assert(
          q.command.request.activationId === expected.binding &&
            q.command.scope.runId === expected.scope.runId &&
            q.command.scope.piSessionId === expected.scope.piSessionId &&
            q.command.scope.writerId === expected.scope.writerId &&
            q.command.toolCallId === "task:" + (9 + counts[target]) &&
            q.command.request.toolCallId === "call-" + index &&
            q.reply.toolCallId === q.command.request.toolCallId &&
            BigInt(q.position.offset) > offsets[target] &&
            q.command.request.operationId === fixture.op(arm, index) &&
            q.command.request.args.command === "true" &&
            q.position.partition === expected.partition &&
            q.reply.topic === fixture.replyTopic,
          "Wrong target, duplicate or out-of-order delivery",
        );
        offsets[target] = BigInt(q.position.offset);
        counts[target]++;
        total++;
        pending++;
        peak = Math.max(peak, pending);
        lastAt = wall();
        firstAt ??= lastAt;
        const second = Math.floor(lastAt / 1000);
        buckets[second] = (buckets[second] ?? 0) + 1;
        const ack = () => {
          pending--;
          if (s.readyState === WebSocket.OPEN) s.send(q.command.request.operationId);
        };
        if (delay) setTimeout(ack, delay);
        else ack();
      } catch (e) {
        fail(e);
      }
    });
    s.send("ready");
  });
  return {
    url: "ws://" + host + ":" + server.address().port + "/" + token,
    arm(msg) {
      assert(!pending && sockets.size === targets);
      arm = msg.arm;
      delay = msg.delay;
      counts = new Uint32Array(targets);
      offsets = new BigInt64Array(targets).fill(-1n);
      firstAt = lastAt = undefined;
      total = 0;
      peak = 0;
      buckets = {};
    },
    stats(rounds) {
      assert(!pending && counts.every((n) => n === rounds), "Incomplete target coverage");
      assert([...sockets.values()].every((s) => s.readyState === WebSocket.OPEN));
      return { total, targets: sockets.size, peak, buckets, firstAt, lastAt };
    },
    async close() {
      for (const s of sockets.values()) s.terminate();
      await new Promise((resolve) => server.close(resolve));
    },
  };
}

// Capacity is acquired before the Kafka grant, including when a previous send to the target
// is still outstanding. No record, Kafka grant callback or writer lease is retained by an ACK.
export class FanoutSender {
  constructor({ slots = 3072, byteLimit = 12 * 1024 * 1024, packetLimit = 4096 } = {}) {
    assert(
      Number.isSafeInteger(slots) &&
        slots > 0 &&
        Number.isSafeInteger(packetLimit) &&
        packetLimit > 0 &&
        Number.isSafeInteger(byteLimit) &&
        byteLimit >= packetLimit,
    );
    this.slots = slots;
    this.byteLimit = byteLimit;
    this.packetLimit = packetLimit;
    this.active = new Map();
    this.sockets = [];
    this.count = 0;
    this.bytes = 0;
    this.waiters = new Set();
  }
  async connect(url) {
    for (let base = 0; base < targets; base += 64)
      await Promise.all(
        Array.from({ length: Math.min(64, targets - base) }, async (_, j) => {
          const i = base + j,
            s = new WebSocket(url + "/" + i, { perMessageDeflate: false, handshakeTimeout: 10000 });
          this.sockets[i] = s;
          const [msg] = await once(s, "message", { signal: AbortSignal.timeout(10000) });
          assert.equal(msg.toString(), "ready");
          s.on("message", (m) => {
            const t = this.active.get(i);
            if (!t || m.toString() !== t.op) {
              this.abort(Error("Mismatched fan-out acknowledgement"));
              return;
            }
            this.acks++;
            this.onAck?.(t);
            this.release(t);
          });
          s.on("error", (e) => this.abort(e));
          s.on("close", () => {
            if (!this.closing) this.abort(Error("Fan-out connection closed"));
          });
        }),
      );
  }
  reset() {
    assert.equal(this.count, 0);
    assert.equal(this.waiters.size, 0);
    this.peak = 0;
    this.peakBytes = 0;
    this.acks = 0;
    this.waits = 0;
    this.peakWaiting = 0;
  }
  canReserve(target) {
    return (
      !this.active.has(target) &&
      this.count < this.slots &&
      this.bytes + this.packetLimit <= this.byteLimit
    );
  }
  async reserve(target, current) {
    if (this.error) throw this.error;
    if (!current()) return;
    if (this.canReserve(target)) return this.take(target);
    const result = Promise.withResolvers();
    this.waits++;
    this.waiters.add({ target, current, result });
    this.peakWaiting = Math.max(this.waiters.size, this.peakWaiting);
    return result.promise;
  }
  take(target) {
    const t = { target, done: Promise.withResolvers() };
    this.active.set(target, t);
    this.count++;
    this.bytes += this.packetLimit;
    this.peak = Math.max(this.peak, this.count);
    this.peakBytes = Math.max(this.peakBytes, this.bytes);
    return t;
  }
  release(t) {
    if (this.active.get(t.target) !== t) return;
    clearTimeout(t.timeout);
    this.active.delete(t.target);
    this.bytes -= this.packetLimit;
    this.count--;
    t.done.resolve();
    for (const waiter of this.waiters) {
      if (this.error) waiter.result.reject(this.error);
      else if (!waiter.current()) waiter.result.resolve(undefined);
      else if (this.canReserve(waiter.target)) waiter.result.resolve(this.take(waiter.target));
      else continue;
      this.waiters.delete(waiter);
    }
  }
  start(t, packet, op) {
    assert(
      this.active.get(t.target) === t && !t.op && Buffer.byteLength(packet) <= this.packetLimit,
    );
    t.op = op;
    t.at = wall();
    t.timeout = setTimeout(() => this.abort(Error("Simulated receipt missing")), 15000);
    this.sockets[t.target].send(packet, (e) => {
      if (e) this.abort(e);
    });
  }
  abort(e) {
    this.error ??= e;
    for (const t of this.active.values()) this.release(t);
  }
  async drain() {
    await Promise.all([...this.active.values()].map((t) => t.done.promise));
    if (this.error) throw this.error;
  }
  async close() {
    this.closing = true;
    this.abort(Error("Fixture closed"));
    await Promise.all(
      this.sockets.map(async (s) => {
        if (s.readyState !== WebSocket.CLOSED) {
          const closed = once(s, "close");
          s.terminate();
          await closed;
        }
      }),
    );
  }
}
