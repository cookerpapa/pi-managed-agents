import { answer, extraText, claimSql, claimBatchSql } from "./fixture.mjs";
import { connectClaimWorker } from "./wake-channel.mjs";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { performance } from "node:perf_hooks";
import { setTimeout as delay } from "node:timers/promises";
import {
  Harness,
  MemoryStorage,
  createRegistry,
  defineExtension,
  defineTool,
  InboxDoc,
} from "@earendil-works/pi-durable";
import { BACKGROUND_CONTEXT as ctx } from "@earendil-works/chord/context";
import { createModels } from "@earendil-works/pi-ai/models";
import {
  fauxAssistantMessage,
  fauxProvider,
  fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import { Type } from "@earendil-works/pi-ai";
import { KafkaWorkerProducer } from "@pi-cloud/event-log";
import {
  encodeSessionLogFact,
  kafkaProducerLane,
} from "@pi-cloud/runtime-core/kafka-accepted-fact";
import { brokers, topic, actor, send, fail, listen, openPool } from "./actor.mjs";

const pool = await openPool(5),
  notifications = await pool.connect(),
  producer = new KafkaWorkerProducer({ brokers, topic, clientId: topic + "-" + actor });
await producer.start();
const active = new Map();
let cpuAt = process.cpuUsage(),
  eluAt = performance.eventLoopUtilization();
let capacity = 0,
  claims = 0,
  running = 0,
  awake = 0,
  sleeping = true,
  closed = false,
  stage,
  directed,
  directedConfig;
function wake() {
  awake++;
  sleeping = false;
  pump();
}
function pump() {
  while (!closed && !sleeping && running + claims < capacity && claims < 2) {
    const version = awake,
      t = performance.now();
    claims++;
    pool
      .query(claimSql)
      .then(({ rows }) => {
        if (!rows.length) {
          if (awake === version) sleeping = true;
          return;
        }
        running++;
        void execute(rows[0], performance.now() - t)
          .catch(fail)
          .finally(() => {
            running--;
            wake();
          });
      })
      .catch((e) => {
        sleeping = true;
        fail(e);
      })
      .finally(() => {
        claims--;
        pump();
      });
  }
}
notifications.on("notification", () => {
  if (stage?.wakeMode !== "relay") wake();
});
await notifications.query("listen session_bench_work");
const poll = setInterval(() => {
  if (!directedConfig) wake();
}, 250);
poll.unref();
async function execute(row, claimMs, claimReason) {
  const started = performance.now(),
    sc = stage,
    id = row.id;
  const scope = {
    tenantId: row.tenant_id,
    sessionId: row.session_id,
    piSessionId: row.session_id,
    runId: id,
    turnId: id,
    writerId: row.lease_id,
    fencingToken: 1,
  };
  const partition = kafkaProducerLane(row.session_id, 32),
    gate = Promise.withResolvers(),
    release = Promise.withResolvers(),
    inspectRelease = Promise.withResolvers();
  let ackMs = 0,
    finishAckMs = 0,
    seq = 0,
    records = 0,
    bytes = 0,
    version = 0,
    rechecks = 0,
    ended = false,
    modelCalls = 0,
    clientAckWaitMs = 0;
  const ackTimes = new Map(),
    waiters = new Map(),
    inputSeq = new Map(),
    issued = [];
  let inputLine = Promise.resolve();
  const captured = sc.label === "warmup" ? [] : null;
  let lastNative;
  function serial(op) {
    const next = inputLine.then(op);
    inputLine = next.catch(() => {});
    return next;
  }
  function confirm(seq, at) {
    const w = waiters.get(seq);
    if (w) {
      waiters.delete(seq);
      w.resolve(at);
    } else ackTimes.set(seq, at);
  }
  async function waitAck(sequence) {
    assert(Number.isSafeInteger(sequence));
    if (ackTimes.has(sequence)) return ackTimes.get(sequence);
    const waiter = Promise.withResolvers();
    waiters.set(sequence, waiter);
    const timeout = setTimeout(
      () => waiter.reject(new Error("Projected input confirmation timed out")),
      30000,
    );
    try {
      return await waiter.promise;
    } finally {
      clearTimeout(timeout);
      waiters.delete(sequence);
    }
  }
  active.set(id, { confirm, inspectRelease });
  send("claim", { id, actor, at: Date.now() });
  async function publish(fact) {
    if (fact.kind === "durable_commit") {
      lastNative = fact;
      if (captured) captured.push(fact);
    }
    const payload = encodeSessionLogFact(fact);
    bytes += Buffer.byteLength(payload);
    const start = performance.now();
    await producer.append(partition, {
      factId: fact.factId,
      sessionId: row.session_id,
      value: payload,
    });
    const elapsed = performance.now() - start;
    records++;
    if (fact.kind === "work_finished") finishAckMs = elapsed;
    else ackMs += elapsed;
  }
  async function finish() {
    if (sc.holdFinish) {
      send("finishHeld", { id, actor });
      await inspectRelease.promise;
    }
    const terminal = {
      kind: "work_finished",
      factId: randomUUID(),
      scope,
      agentId: "root",
      baseSequence: 0,
      nativeSequence: seq,
      terminal: { type: "turn.completed", payload: { stopReason: "stop" } },
      occurredAt: new Date().toISOString(),
    };
    if (sc.redeliver) {
      await publish(lastNative);
      await publish({
        ...terminal,
        factId: randomUUID(),
        scope: { ...scope, writerId: randomUUID() },
      });
    }
    await publish(terminal);
    if (sc.redeliver) {
      await publish(terminal);
      await publish(lastNative);
    }
    if (captured) send("fixture", { source: id, batches: captured.map((f) => f.commit) });
    send("workerDone", {
      id,
      actor,
      claimMs,
      claimReason,
      workMs: performance.now() - started,
      ackMs,
      finishAckMs,
      seq,
      records,
      bytes,
      modelCalls,
      rechecks,
      clientAckWaitMs,
      inputAcks: issued.map((i) => i.ackMs),
      inputs: issued.map(({ id, text, withdrawn }) => ({ id, text, withdrawn: !!withdrawn })),
    });
  }
  if (sc.replay) {
    try {
      const batches = JSON.parse(
        JSON.stringify(sc.fixture.batches).replaceAll(sc.fixture.source, id),
      );
      const outstanding = [];
      for (const b of batches) {
        seq = b.seq;
        const sent = publish({
          kind: "durable_commit",
          scope,
          factId: randomUUID(),
          occurredAt: new Date().toISOString(),
          commit: b,
          origins: b.writes
            .filter((w) => w.type === "entry" || w.type === "submission" || w.type === "task")
            .map((w) => ({
              recordId: w.value.id,
              sessionId: row.session_id,
              runId: id,
              turnId: id,
            })),
          tools: [],
          cancellations: [],
          display: [],
        });
        if (sc.replay === "pipelined") outstanding.push(sent);
        else await sent;
      }
      await Promise.all(outstanding);
      await finish();
      return;
    } finally {
      active.delete(id);
    }
  }
  class PublishedMemory extends MemoryStorage {
    async commit(writes) {
      const ready = this.prepareCommit(writes);
      const batch = { seq: ready.seq, nextId: this.nextId, writes: ready.writes };
      await publish({
        kind: "durable_commit",
        scope,
        factId: randomUUID(),
        occurredAt: new Date().toISOString(),
        commit: batch,
        origins: ready.writes
          .filter((w) => w.type === "entry" || w.type === "submission" || w.type === "task")
          .map((w) => ({ recordId: w.value.id, sessionId: row.session_id, runId: id, turnId: id })),
        display: [],
        tools: [],
        cancellations: [],
      });
      seq = ready.apply();
      for (const w of writes)
        if (w.type === "submission" && w.value.requestId && !inputSeq.has(w.value.requestId))
          inputSeq.set(w.value.requestId, seq);
      return seq;
    }
  }
  const store = new PublishedMemory(),
    models = createModels(),
    fake = fauxProvider({
      tokensPerSecond: 0,
      tokenSize: { min: 256, max: 256 },
      models: [{ id: "local-load" }],
    });
  models.setProvider(fake.provider);
  const registry = createRegistry();
  if (sc.scene === "steer")
    registry.install(
      defineExtension({
        name: "bench",
        tools: [
          defineTool({
            name: "check",
            description: "Local synthetic observation",
            parameters: Type.Object({}),
            execute: async () => {
              gate.resolve();
              await release.promise;
              return { content: [{ type: "text", text: "checked" }] };
            },
          }),
        ],
      }),
    );
  const mixed = sc.scene !== "plain",
    late = sc.scene === "late",
    steer = sc.scene === "steer";
  const first = async () => {
    modelCalls++;
    if (sc.modelDelayMs) await delay(sc.modelDelayMs);
    if (steer) return fauxAssistantMessage(fauxToolCall("check", {}), { stopReason: "toolUse" });
    if (mixed && !late) {
      gate.resolve();
      await release.promise;
    }
    return fauxAssistantMessage(answer(id, 1));
  };
  const second = async (context) => {
    modelCalls++;
    const texts = context.messages
      .filter((m) => m.role === "user")
      .flatMap((m) =>
        (Array.isArray(m.content) ? m.content : [{ text: m.content }]).map((b) => b.text),
      );
    for (const extra of issued.filter((i) => !i.withdrawn))
      assert(texts.includes(extra.text), "Placed input missing from model context");
    for (const extra of issued.filter((i) => i.withdrawn))
      assert(!texts.includes(extra.text), "Withdrawn input reached the model");
    if (sc.inspect) {
      send("secondModel", { id, actor });
      await inspectRelease.promise;
    }
    return fauxAssistantMessage(answer(id, 2));
  };
  fake.setResponses(mixed ? [first, second] : [first]);
  const harness = await Harness.open(
    store,
    {
      models,
      registry,
      settings: {
        followUpMode: "all",
        steeringMode: "all",
        retry: { maxRetries: 0 },
        compaction: { enabled: false },
      },
    },
    ctx,
  );
  let harnessClosed = false;
  try {
    const model = fake.getModel(),
      root = await harness.root(ctx, {
        agent: { model: { provider: model.provider, modelId: model.id } },
      });
    const initial = await root.submit(
      { type: "input", content: [{ type: "text", text: row.initial_input }], requestId: id },
      ctx,
    );
    async function add(index) {
      const inputId = randomUUID(),
        text = extraText(id, index, sc.inputBytes),
        began = Date.now();
      const item = { id: inputId, text, index };
      issued.push(item);
      const result = await serial(async () => {
        assert(!ended);
        const sub = await root.submit(
          {
            type: "input",
            content: [{ type: "text", text }],
            requestId: inputId,
            whenBusy: steer ? "steer" : "followUp",
          },
          ctx,
        );
        version++;
        return sub;
      });
      item.nativeId = result.id;
      const start = performance.now();
      const at = await waitAck(inputSeq.get(inputId));
      clientAckWaitMs += performance.now() - start;
      item.ackMs = at - began;
      return result;
    }
    const inputs = (async () => {
      if (!mixed || late) return;
      await gate.promise;
      let sub;
      for (let i = 0; i < (sc.depth ?? 1); i++) sub = await add(i);
      if (sc.mutate) {
        const updated = issued[0].text + " edited";
        await serial(async () => {
          await root.commit(async (tx) => {
            const inbox = await tx.doc(InboxDoc, root.id);
            const item = inbox.items.find((i) => i.id === issued[0].nativeId);
            assert(item);
            item.content = [{ type: "text", text: updated }];
          }, ctx);
          version++;
        });
        issued[0].text = updated;
        await waitAck(seq);
        const last = issued.at(-1);
        last.withdrawn = true;
        await serial(async () => {
          assert.equal(await sub.abort(ctx), "aborted");
          version++;
        });
        await waitAck(seq);
      }
      release.resolve();
    })();
    // A rejected fixture input must also release the synthetic in-flight response.
    void inputs.catch((error) => {
      release.resolve();
      fail(error);
    });
    let lateInjected = false;
    for (;;) {
      const before = version;
      await root.waitForIdle(ctx);
      if (late && !lateInjected) {
        lateInjected = true;
        await add(0);
      }
      if (
        await serial(async () => {
          if (before !== version) {
            rechecks++;
            return false;
          }
          ended = true;
          return true;
        })
      )
        break;
    }
    await inputs;
    assert.equal((await initial.wait(ctx)).status, "done");
    assert.equal(modelCalls, mixed ? 2 : 1);
    assert.equal((await harness.snapshot(InboxDoc, root.id, ctx)).items.length, 0);
    if (late) assert(rechecks >= 1);
    await harness.close(ctx);
    harnessClosed = true;
    await finish();
  } finally {
    active.delete(id);
    if (!harnessClosed) await harness.close(ctx);
  }
}
send("ready", { actor });
function joinDirected() {
  assert(directedConfig && !directed);
  directed = connectClaimWorker({
    url: directedConfig.wakeUrl,
    id: actor,
    stage: directedConfig,
    slots: directedConfig.slots,
    claim: async (limit) => (await pool.query(claimBatchSql, [limit])).rows,
    execute,
    fail,
  });
}
listen(
  {
    stage: async (msg) => {
      assert.equal(active.size, 0);
      directed?.close();
      capacity = 0;
      while (claims || directed?.busy) await delay(2);
      directed = undefined;
      const previous = directedConfig;
      directedConfig = msg.wakeUrl ? msg : undefined;
      stage = msg;
      cpuAt = process.cpuUsage();
      eluAt = performance.eventLoopUtilization();
      if (directedConfig) {
        if (!previous) await notifications.query("unlisten session_bench_work");
        if (!msg.offline?.includes(actor)) joinDirected();
      } else {
        if (previous) await notifications.query("listen session_bench_work");
        capacity = msg.slots;
        wake();
      }
      send("configured", { stage: msg.stage });
    },
    join: () => joinDirected(),
    drain: (m) => {
      directed?.close();
      send("draining", { n: m.n });
    },
    metrics: (msg) =>
      send("metrics", {
        n: msg.n,
        claimStats: directed?.stats,
        cpu: process.cpuUsage(cpuAt),
        loop: performance.eventLoopUtilization(eluAt),
      }),
    confirmed: (msg) => active.get(msg.id)?.confirm(msg.seq, msg.at),
    wake,
    checked: (msg) => active.get(msg.id)?.inspectRelease.resolve(),
  },
  async () => {
    closed = true;
    directed?.close();
    clearInterval(poll);
    for (const state of active.values()) state.inspectRelease.resolve();
    await producer.close();
    notifications.release();
    await pool.end();
  },
);
