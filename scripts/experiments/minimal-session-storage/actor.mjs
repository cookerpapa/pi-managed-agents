// Private benchmark control channel. Native data moves over Kafka.
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { createInterface } from "node:readline";
import pg from "pg";
export const name = process.argv[1],
  topic = process.argv[2],
  actor = process.argv[3];
assert.match(name, /^pma_sessbench_[0-9a-f]{16}$/);
assert.equal(topic, "pma-session-bench-" + name.slice(-16));
export const brokers = process.env.PI_CLOUD_KAFKA_BROKERS.split(",");
export const send = (kind, body = {}) =>
  process.stdout.write("SESSION_BENCH " + JSON.stringify({ kind, ...body }) + "\n");
export const fail = (error) =>
  send("failure", { message: String(error.stack ?? error).slice(0, 2000) });
export async function openPool(max, { idleTimeoutMillis } = {}) {
  const url = new URL((await readFile(process.env.DATABASE_URL_FILE, "utf8")).trim());
  url.pathname = "/" + name;
  const pool = new pg.Pool({
    connectionString: url.toString(),
    max,
    application_name: "session-bench-" + actor,
    connectionTimeoutMillis: 15000,
    statement_timeout: 20000,
    ...(idleTimeoutMillis === undefined ? {} : { idleTimeoutMillis }),
  });
  pool.on("error", fail);
  assert.equal((await pool.query("select current_database() as n")).rows[0].n, name);
  return pool;
}
export function listen(commands, cleanup) {
  let stopping;
  const lines = createInterface({ input: process.stdin });
  const stop = () =>
    (stopping ??= Promise.resolve()
      .then(async () => {
        lines.close();
        process.stdin.pause();
        await cleanup();
        process.off("SIGTERM", stop);
      })
      .then(
        () => send("stopped", { actor }),
        (error) =>
          process.stdout.write(
            "SESSION_BENCH " +
              JSON.stringify({ kind: "failure", message: String(error.message) }) +
              "\n",
            () => process.exit(1),
          ),
      ));
  lines.on("line", (line) => {
    let msg;
    try {
      msg = JSON.parse(line);
    } catch (e) {
      fail(e);
      return;
    }
    if (msg.kind === "stop") {
      void stop();
      return;
    }
    Promise.resolve()
      .then(() => commands[msg.kind](msg))
      .catch(fail);
  });
  lines.on("close", () => void stop());
  process.on("SIGTERM", () => void stop());
  process.on("unhandledRejection", fail);
}
