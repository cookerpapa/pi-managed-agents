import assert from "node:assert/strict";
import { test } from "node:test";
import { setImmediate as turn } from "node:timers/promises";
import { FanoutSender } from "./fanout-network.mjs";
const current = () => true;
test("reservation limits apply before dispatch and a busy target cannot overtake itself", async () => {
  const sender = new FanoutSender({ slots: 3, byteLimit: 8, packetLimit: 4 });
  sender.reset();
  const a = await sender.reserve(0, current),
    b = await sender.reserve(1, current);
  let peer, next;
  const p = sender.reserve(2, current).then((t) => (peer = t)),
    q = sender.reserve(0, current).then((t) => (next = t));
  await turn();
  assert(!peer && !next);
  assert.equal(sender.bytes, 8);
  sender.release(b);
  await p;
  assert(peer && !next);
  sender.release(a);
  await q;
  assert.equal(next.target, 0);
  sender.release(peer);
  sender.release(next);
  await sender.drain();
  assert.equal(sender.peak, 2);
  assert.equal(sender.peakBytes, 8);
  assert.equal(sender.bytes, 0);
});
test("a revoked waiter cannot use released space; a lost receipt never starts a second send", async () => {
  const sender = new FanoutSender({ slots: 1, byteLimit: 16, packetLimit: 16 });
  sender.reset();
  let owned = true,
    calls = 0;
  const t = await sender.reserve(0, current),
    waiting = sender.reserve(1, () => owned);
  owned = false;
  sender.release(t);
  assert.equal(await waiting, undefined);
  sender.sockets[0] = {
    send(_payload, complete) {
      calls++;
      complete(Error("receipt lost"));
    },
  };
  const original = await sender.reserve(0, current);
  sender.start(original, "{}", "op");
  await assert.rejects(sender.drain(), /receipt lost/);
  await assert.rejects(sender.reserve(1, current), /receipt lost/);
  assert.equal(calls, 1);
  assert.equal(sender.acks, 0);
  assert.equal(sender.count, 0);
});
