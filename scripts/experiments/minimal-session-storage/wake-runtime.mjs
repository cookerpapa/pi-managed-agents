import { topic, send, fail, listen, openPool } from "./actor.mjs";
import { openWakeRouter } from "./wake-channel.mjs";
import { runWakePopulation } from "./wake-population.mjs";

const db = await openPool(12);
const listener = await db.connect();
const router = await openWakeRouter(
  process.argv[4] === "wake-fleet" ? "127.0.0.1" : "0.0.0.0",
  "/" + topic,
  fail,
  Number(process.argv[5] ?? 0),
);
listener.on("notification", ({ payload }) => {
  try {
    const { label, count } = JSON.parse(payload);
    router.notify(label, count);
  } catch (error) {
    fail(error);
  }
});
await listener.query("listen session_bench_directed");
let publisher;
listen(
  {
    stage: (m) => {
      router.configure(m);
      send("configured", { stage: m.stage });
    },
    status: (m) => send("status", { n: m.n, ...router.snapshot() }),
    armTimer: (m) => {
      router.startTimer();
      send("armed", { n: m.n });
    },
    population: async (m) => {
      const result = await runWakePopulation(router, db, topic, m.stage, async (request) => {
        publisher = Promise.withResolvers();
        send("publishBursts", request);
        try {
          return await publisher.promise;
        } finally {
          publisher = undefined;
        }
      });
      send("population", { stage: m.stage, result });
    },
    published: (m) => publisher.resolve(m.measurement),
  },
  async () => {
    await router.close();
    listener.release();
    await db.end();
  },
);
send("ready", { port: router.port });
