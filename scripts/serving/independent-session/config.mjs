// These processes run against a dedicated installation. The serving metadata
// and the segmented history use distinct search paths on that database.
import { readFile } from "node:fs/promises";
import pg from "pg";
export const schema = "pma_segments",
  readSchema = "pma_session_reads";
export function required(name) {
  const value = process.env[name];
  if (!value) throw Error("Missing " + name);
  return value;
}
export function integer(name, fallback, minimum = 1, maximum = 65535) {
  const n = Number(process.env[name] ?? fallback);
  if (!Number.isSafeInteger(n) || n < minimum || n > maximum) throw Error("Invalid " + name);
  return n;
}
export async function secret(name) {
  const value = (await readFile(required(name + "_FILE"), "utf8")).trim();
  if (!value) throw Error("Empty " + name);
  return value;
}
export function paths(url) {
  const make = (path) => {
    const u = new URL(url);
    u.searchParams.set("options", "-c search_path=" + path + ",public");
    return u.href;
  };
  return { write: make(schema), read: make(readSchema + "," + schema) };
}
export async function installation() {
  const url = await secret("DATABASE_URL"),
    topic = required("PI_CLOUD_SEGMENT_TOPIC"),
    partitions = integer("PI_CLOUD_SEGMENT_PARTITIONS", 32, 1, 1024),
    brokers = required("PI_CLOUD_KAFKA_BROKERS").split(",");
  if (!/^pma[.-]segment[.-][a-zA-Z0-9.-]+$/.test(topic))
    throw Error("Use a dedicated segmented Session topic");
  return { url, ...paths(url), topic, partitions, brokers };
}
export async function openStore(config) {
  const pool = new pg.Pool({ connectionString: config.write, max: 12 });
  try {
    const {
      rows: [v],
    } = await pool.query("select version,topic,partitions from pma_segments.installation");
    if (v?.version !== 2 || v.topic !== config.topic || v.partitions !== config.partitions)
      throw Error("Segmented installation does not match the transport");
    return pool;
  } catch (e) {
    await pool.end();
    throw e;
  }
}
