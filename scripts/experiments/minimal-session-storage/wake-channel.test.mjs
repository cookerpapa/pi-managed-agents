import assert from "node:assert/strict";
import { once } from "node:events";
import test from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import { WebSocket } from "ws";
import { connectClaimWorker, openWakeRouter } from "./wake-channel.mjs";

async function waitFor(predicate) {
  const end = Date.now() + 2000;
  while (!predicate()) {
    assert(Date.now() < end, "Timed out waiting for a local socket event");
    await delay(2);
  }
}
const stage = { label: "unit", wakeMode: "directed", batch: 8, tickMs: 60000 };

test("reoffering keeps one FIFO position; a stale close cannot remove the replacement", async (t) => {
  const router = await openWakeRouter("127.0.0.1", "/work", assert.fail);
  t.after(() => router.close());
  router.configure(stage);
  async function join(id) {
    const socket = new WebSocket("ws://127.0.0.1:" + router.port + "/work?stage=unit&id=" + id);
    assert.equal(String((await once(socket, "message"))[0]), "registered");
    return socket;
  }
  const old = await join("A");
  old.send("ready");
  old.send("ready");
  await waitFor(() => router.snapshot().offers === 2);
  assert.equal(router.snapshot().waiting, 1);
  const closed = once(old, "close");
  const replacement = await join("A");
  await closed;
  replacement.send("ready");
  const second = await join("B");
  second.send("ready");
  await waitFor(() => router.snapshot().waiting === 2);
  for (const expected of [replacement, second]) {
    const response = once(expected, "message", { signal: AbortSignal.timeout(2000) });
    router.notify(stage.label, 1);
    assert.equal(String((await response)[0]), "arrival");
  }
  assert.equal(router.snapshot().waiting, 0);
  assert.equal(router.snapshot().online, 2);
});

test("a claim already issued before drain settles its committed rows within the slot budget", async (t) => {
  const router = await openWakeRouter("127.0.0.1", "/work", assert.fail);
  t.after(() => router.close());
  router.configure(stage);
  const answer = Promise.withResolvers(),
    asked = Promise.withResolvers();
  const executed = [],
    errors = [];
  const worker = connectClaimWorker({
    url: "ws://127.0.0.1:" + router.port + "/work",
    id: "A",
    stage,
    slots: 3,
    claim: (limit) => {
      asked.resolve(limit);
      return answer.promise;
    },
    execute: async ({ id }) => {
      executed.push(id);
    },
    fail: (error) => errors.push(error),
  });
  t.after(() => worker.close());
  assert.equal(await asked.promise, 3);
  worker.close();
  answer.resolve([{ id: 1 }, { id: 2 }, { id: 3 }]);
  await waitFor(() => !worker.busy);
  assert.deepEqual(executed, [1, 2, 3]);
  assert.equal(worker.stats.queries, 1);
  assert.deepEqual(errors, []);
});
