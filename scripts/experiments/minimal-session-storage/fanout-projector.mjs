import assert from "node:assert/strict";
import { FanoutSender } from "./fanout-network.mjs";
import { fanoutFixture, offered, targets } from "./fanout-fixture.mjs";
import { operationsFor } from "./workspace-operations.mjs";
import { prepareToolGrants } from "./tool-grant-prefix.mjs";
import { pace, wall } from "./pg-path-replay.mjs";

function phase() {
  const buckets = new Uint32Array(240);
  let count = 0,
    sum = 0;
  return {
    add(ms) {
      count++;
      sum += ms;
      buckets[Math.min(239, Math.ceil(Math.log1p(ms) / Math.log(1.05)))]++;
    },
    summary() {
      let n = 0;
      const i = buckets.findIndex((v) => (n += v) >= count * 0.95);
      return { count, mean: count ? sum / count : null, p95UpperMs: count ? 1.05 ** i - 1 : null };
    },
  };
}

export class FanoutProjector {
  constructor(token, lag, readerStats) {
    this.fixture = fanoutFixture(token);
    this.lag = lag;
    this.readerStats = readerStats;
    this.sender = new FanoutSender();
    this.bySession = new Map(this.fixture.owners.map((o, i) => [o.scope.sessionId, i]));
    this.byBinding = new Map(this.fixture.owners.map((o, i) => [o.binding, i]));
  }
  // The disposable actor observes librdkafka commit submissions and callbacks for
  // this group only. Production offset batching and its guard remain intact.
  observeCommits(native, group) {
    const Original = native.KafkaConsumer,
      bench = this;
    native.KafkaConsumer = class extends Original {
      constructor(config, ...rest) {
        super(config, ...rest);
        this.owned = config["group.id"] === group;
        if (this.owned)
          this.on("offset.commit", (err, offsets) => {
            const p = this.observed;
            if (
              !p ||
              err ||
              !p.offsets.every((x) =>
                offsets.some(
                  (y) =>
                    x.partition === y.partition && x.topic === y.topic && x.offset === y.offset,
                ),
              )
            )
              return;
            p.stats.callback.add(wall() - p.at);
            this.observed = undefined;
          });
      }
      commit(offsets) {
        const stats = bench.commitStats;
        if (this.owned && bench.spec && stats && Array.isArray(offsets)) {
          stats.calls++;
          stats.positions += offsets.length;
          this.observed = { at: wall(), offsets, stats };
        }
        return super.commit(offsets);
      }
    };
    this.restoreObserver = () => {
      native.KafkaConsumer = Original;
    };
  }
  async warm(router, topic) {
    router.spans.reset();
    for (let i = 0; i < targets; i += 32)
      await Promise.all(
        this.fixture.owners.slice(i, i + 32).map(async (o) => {
          const span = await router.spans.at(o.scope, 1, {
            topic,
            partition: o.partition,
            offset: 0n,
          });
          assert.equal(span.bindingId, o.binding);
        }),
      );
  }
  begin(spec) {
    this.sender.slots = spec.cap ?? 3072;
    this.sender.reset();
    this.commitStats = { calls: 0, positions: 0, callback: phase() };
    this.spec = spec;
    this.handled = 0;
    this.prepared = new WeakSet();
    this.grantSizes = {};
    this.lastAck = 0;
    this.waited = Promise.withResolvers();
    this.gate = Promise.withResolvers();
    if (!spec.preload) this.release(spec.start);
    this.phases = { reserve: phase(), route: phase(), pg: phase(), grant: phase(), ack: phase() };
    this.samples = [];
    this.memoryBase = process.memoryUsage();
    this.cpuBase = process.cpuUsage();
    this.atOfferEnd = 0;
    this.peakRss = this.memoryBase.rss;
    this.sender.onAck = (t) => {
      this.lastAck = wall();
      this.phases.ack.add(this.lastAck - t.at);
      if (!spec.preload && this.lastAck <= this.start + (spec.count * 1000) / offered)
        this.atOfferEnd++;
    };
    this.timer = setInterval(() => {
      if (!this.sampling)
        this.sampling = this.sample()
          .catch((e) => this.sender.abort(e))
          .finally(() => {
            this.sampling = undefined;
          });
    }, 500);
  }
  release(start) {
    this.start = start;
    this.gate.resolve();
  }
  async sample() {
    const high = await this.lag(),
      now = wall(),
      m = process.memoryUsage(),
      reader = this.readerStats();
    this.peakRss = Math.max(this.peakRss, m.rss);
    this.samples.push({
      at: now,
      lag: high.lag,
      endSum: high.endSum,
      processed: this.handled,
      partitionLag: high.partitionLagMax,
      acks: this.sender.acks,
      active: this.sender.count,
      reservedBytes: this.sender.bytes,
      heap: m.heapUsed,
      rss: m.rss,
      readerBytes: reader.estimatedBytes,
    });
  }
  async prepare(ready, current, grant, router) {
    if (!(this.spec?.grantWidth > 1) || !router.canGroupOperation(ready[0])) return 1;
    await this.gate.promise;
    let prior = wall();
    const count = await prepareToolGrants(ready, current, grant, {
      limit: this.spec.grantWidth,
      batchProjection: this.spec.batchProjection,
      sender: this.sender,
      router,
      targetFor: (r) => {
        const i = this.bySession.get(r.fact.scope.piSessionId);
        assert(i !== undefined);
        return i;
      },
      markProjection: (key) => {
        const now = wall();
        this.phases[key === "routeReady" ? "route" : "pg"].add(now - prior);
        prior = now;
      },
      observeGrant: (n, ms) => {
        this.grantSizes[n] = (this.grantSizes[n] ?? 0) + 1;
        this.phases.grant.add(ms);
      },
      prepareSend: (r, { route, command, op }, slot) => {
        assert.equal(this.byBinding.get(route.bindingId), slot.target);
        const packet = JSON.stringify({
          position: { topic: r.topic, partition: r.partition, offset: String(r.offset) },
          command,
          reply: op.reply,
        });
        assert(Buffer.byteLength(packet) <= this.sender.packetLimit);
        return () => this.sender.start(slot, packet, command.request.operationId);
      },
    });
    if (current()) for (const r of ready.slice(0, count)) this.prepared.add(r.fact);
    return count || 1;
  }
  processed() {
    if (++this.handled === this.spec.count) {
      this.processedAt = wall();
      this.waited.resolve();
    }
  }
  async handle(record, current, grant, router, nextOffsets) {
    if (this.prepared.delete(record.fact)) {
      this.processed();
      return;
    }
    await this.gate.promise;
    const index = this.bySession.get(record.fact.scope.piSessionId);
    assert(index !== undefined && operationsFor(record.fact).length === 1);
    const begin = wall(),
      t = await this.sender.reserve(index, current);
    if (!t) return;
    let prev = wall(),
      granted = false;
    this.phases.reserve.add(prev - begin);
    const mark = (key) => {
      const now = wall();
      this.phases[{ routeReady: "route", projected: "pg", granted: "grant" }[key]].add(now - prev);
      prev = now;
    };
    try {
      if (this.spec.mode === "offset-control") {
        // Read and decode the identical record. Commit one real offset but issue no command.
        assert(current() && (await grant()));
        mark("granted");
        nextOffsets.set(record.partition, record.offset + 1n);
      } else {
        await router.consume(
          record,
          current,
          async () => {
            if (this.spec.mode === "pg-control") return false;
            granted = await grant();
            if (granted) this.grantSizes[1] = (this.grantSizes[1] ?? 0) + 1;
            return granted;
          },
          mark,
        );
        if (current() && granted) assert(t.op, "Granted operation did not reach its target socket");
      }
    } finally {
      if (!t.op) this.sender.release(t);
    }
    this.processed();
  }
  resolve(binding) {
    const i = this.byBinding.get(binding);
    assert(i !== undefined);
    return {
      submit: (r, c, reply) =>
        this.submit(
          i,
          { topic: r.topic, partition: r.partition, offset: String(r.offset) },
          c,
          reply,
        ),
    };
  }
  submit(i, position, command, reply) {
    const t = this.sender.active.get(i);
    assert(t && !t.op);
    this.sender.start(t, JSON.stringify({ position, command, reply }), command.request.operationId);
    if (this.spec.mode === "serial") return t.done.promise;
  }
  async direct(spec) {
    this.begin(spec);
    const indices = Array.from({ length: spec.count }, (_, i) => i);
    await pace(indices, offered, spec.start, async (n) => {
      const i = n % targets,
        o = this.fixture.owners[i],
        op = this.fixture.op(spec.arm, n),
        call = "call-" + n;
      const t = await this.sender.reserve(i, () => true);
      assert(t);
      this.submit(
        i,
        { topic: spec.topic, partition: o.partition, offset: String(n) },
        {
          scope: o.scope,
          toolCallId: "task:" + (9 + Math.floor(n / targets)),
          request: {
            operation: "tool.execute",
            type: "tool_sandbox.operation",
            toolBrokerProtocolVersion: 1,
            toolName: "bash",
            toolCallId: call,
            operationId: op,
            activationId: o.binding,
            args: { command: "true" },
            timeoutMs: 10000,
            maximumOutputBytes: 51200,
          },
        },
        {
          topic: this.fixture.replyTopic,
          partition: 0,
          offset: "0",
          deadlineAt: Date.now() + 3600000,
          toolName: "bash",
          toolCallId: call,
        },
      );
    });
    this.waited.resolve();
    return this.finish();
  }
  async finish() {
    await this.waited.promise;
    await this.sender.drain();
    clearInterval(this.timer);
    await this.sampling;
    await this.sample();
    const internal = this.spec.mode.endsWith("-control");
    assert.equal(this.sender.acks, internal ? 0 : this.spec.count);
    const dt = (internal ? this.processedAt : this.lastAck) - this.start,
      samples = this.samples;
    const result = {
      rps: (this.spec.count * 1000) / dt,
      elapsedMs: dt,
      startAt: this.start,
      lastAckAt: this.lastAck,
      ackedByOfferEnd: this.spec.preload ? null : this.atOfferEnd,
      cpu: process.cpuUsage(this.cpuBase),
      memoryEnd: process.memoryUsage(),
      acks: this.sender.acks,
      cap: this.sender.slots,
      grantSizes: this.grantSizes,
      nativeCommits: {
        calls: this.commitStats.calls,
        positions: this.commitStats.positions,
        callback: this.commitStats.callback.summary(),
      },
      peakActive: this.sender.peak,
      peakReservedBytes: this.sender.peakBytes,
      capacityWaits: this.sender.waits,
      peakWaiting: this.sender.peakWaiting,
      rssBase: this.memoryBase.rss,
      peakRss: this.peakRss,
      heapEnd: process.memoryUsage().heapUsed,
      peakLag: Math.max(...samples.map((s) => s.lag)),
      peakReaderBytes: Math.max(...samples.map((s) => s.readerBytes)),
      phases: Object.fromEntries(Object.entries(this.phases).map(([k, v]) => [k, v.summary()])),
      samples,
    };
    this.spec = undefined;
    return result;
  }
  async close() {
    clearInterval(this.timer);
    this.gate?.resolve();
    await this.sender.close();
    this.restoreObserver?.();
  }
}
