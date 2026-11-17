import { afterEach, expect, it, vi } from "vitest";
const state = vi.hoisted(() => ({
  producers: [] as any[],
  connect: undefined as ((done: () => void) => void) | undefined,
}));
vi.mock("@confluentinc/kafka-javascript", () => ({
  default: {
    CODES: { ERRORS: { ERR__QUEUE_FULL: -184 } },
    Producer: class {
      events = new Map<string, Function>();
      constructor(readonly config: Record<string, unknown>) {
        state.producers.push(this);
      }
      on(name: string, callback: Function) {
        this.events.set(name, callback);
      }
      connect = vi.fn((_options, done) => (state.connect ? state.connect(done) : done()));
      setPollInterval = vi.fn();
      produce = vi.fn();
      flush = vi.fn((_timeout, done) => done());
      disconnect = vi.fn((done) => done());
      deliver(index: number, error?: Error) {
        this.events.get("delivery-report")!(error, { opaque: this.produce.mock.calls[index][5] });
      }
    },
  },
}));
import { KafkaWorkerProducer } from "../src/kafka-worker-producer.ts";
const owners: KafkaWorkerProducer[] = [];
afterEach(async () => {
  await Promise.allSettled(owners.splice(0).map((p) => p.close()));
  state.producers.length = 0;
  state.connect = undefined;
});
async function fixture() {
  const failed = vi.fn();
  const producer = new KafkaWorkerProducer({
    brokers: ["fixture"],
    clientId: "boot",
    topic: "log",
    onFailure: failed,
  });
  owners.push(producer);
  await producer.start();
  return { producer, failed, native: state.producers[0]! };
}
const record = (sessionId = "one") => ({ factId: crypto.randomUUID(), sessionId, value: "{}" });

it("closing during startup disposes the transport before any Session can publish", async () => {
  let connected!: () => void;
  state.connect = (done) => {
    connected = done;
  };
  const producer = new KafkaWorkerProducer({
    brokers: ["fixture"],
    clientId: "boot",
    topic: "log",
  });
  owners.push(producer);
  const opening = producer.start();
  const refused = expect(opening).rejects.toThrow("closed during startup");
  let closed = false;
  const closing = producer.close().then(() => {
    closed = true;
  });
  await Promise.resolve();
  expect(closed).toBe(false);
  connected();
  await refused;
  await closing;
  await expect(producer.append(0, record())).rejects.toBeDefined();
  expect(state.producers[0].produce).not.toHaveBeenCalled();
  expect(state.producers[0].disconnect).toHaveBeenCalledOnce();
});

it("connects once and multiplexes Sessions without a transactional Session identity", async () => {
  const f = await fixture();
  await Promise.all([f.producer.start(), f.producer.start()]);
  const first = record(),
    second = record("two");
  const a = f.producer.append(0, first),
    b = f.producer.append(1, second);
  f.native.deliver(1);
  f.native.deliver(0);
  await expect(a).resolves.toEqual({ factId: first.factId, durable: true });
  await expect(b).resolves.toEqual({ factId: second.factId, durable: true });
  expect(state.producers).toHaveLength(1);
  expect(f.native.config).toMatchObject({ "enable.idempotence": true, acks: -1, dr_cb: true });
  expect(f.native.config["transactional.id"]).toBeUndefined();
  expect(f.native.connect).toHaveBeenCalledOnce();
});

it("does not acknowledge memory enqueue before the delivery report", async () => {
  const f = await fixture();
  let acknowledged = false;
  const result = f.producer.append(0, record()).then(() => {
    acknowledged = true;
  });
  await Promise.resolve();
  expect(acknowledged).toBe(false);
  f.native.deliver(0);
  await result;
  expect(acknowledged).toBe(true);
});

it("definite queue rejection leaves the shared producer usable", async () => {
  const f = await fixture();
  f.native.produce.mockImplementationOnce(() => {
    throw Object.assign(Error("queue full"), { code: -184 });
  });
  await expect(f.producer.append(0, record())).rejects.toThrow("not enqueued");
  expect(f.failed).not.toHaveBeenCalled();
  const next = f.producer.append(0, record());
  f.native.deliver(1);
  await expect(next).resolves.toMatchObject({ durable: true });
});

it("an uncertain delivery fails all pending transport writes without replay or reinitialization", async () => {
  const f = await fixture();
  const a = f.producer.append(0, record()),
    b = f.producer.append(1, record("two"));
  const checks = [expect(a).rejects.toThrow("unknown"), expect(b).rejects.toThrow("unknown")];
  f.native.deliver(0, Error("delivery unknown"));
  await Promise.all(checks);
  expect(f.failed).toHaveBeenCalledOnce();
  expect(f.producer.failureSignal.aborted).toBe(true);
  await expect(f.producer.append(0, record())).rejects.toThrow("unknown");
  await f.producer.start();
  expect(f.native.connect).toHaveBeenCalledOnce();
  expect(f.native.produce).toHaveBeenCalledTimes(2);
  f.native.deliver(1); // A late success cannot revive the boot.
  expect(f.producer.failureSignal.aborted).toBe(true);
});

it("ordinary broker observations do not invent failed delivery", async () => {
  const f = await fixture();
  f.native.events.get("event.error")!(Error("temporary disconnection"));
  expect(f.producer.failureSignal.aborted).toBe(false);
});
