// PG notifications are advisory. Only this component subscribes; a socket
// conveys permission to check the queue, never permission to execute a Turn.
import { PostgresNotificationWake } from "@pi-cloud/database";
import { openWakeRouter, connectClaimWorker } from "./wake-channel.mjs";

const path = "/internal/segment-work";
const stage = { label: "segments", wakeMode: "directed", perTurn: true, batch: 1 };
function checkToken(token) {
  if (typeof token !== "string" || token.length < 32) throw Error("Invalid management credential");
}

// Coalesce acknowledgements for the same writer. Unconfirmed PG rows are
// discovered at renewal even if the socket, listener or an ACK write fails.
function steerReceipts(pool) {
  const pending = new Map(),
    active = new Map();
  let closed = false;
  function drain() {
    for (const [writer, { worker, boot, revision }] of pending) {
      if (closed || active.size >= 4) break;
      if (active.has(writer)) continue;
      pending.delete(writer);
      const work = pool
        .query("select acknowledge_segment_steer($1,$2,$3,$4)", [worker, boot, writer, revision])
        .catch(() => console.warn("segment.steer_ack_unconfirmed"))
        .finally(() => {
          active.delete(writer);
          drain();
        });
      active.set(writer, { work, revision });
    }
  }
  return {
    receive(id, writer, revision) {
      if (closed || !/^[0-9a-f-]{36}\/[0-9a-f-]{36}$/.test(id)) return;
      const [worker, boot] = id.split("/"),
        old = pending.get(writer);
      if (active.has(writer) && BigInt(active.get(writer).revision) >= BigInt(revision)) return;
      if ((!old && pending.size >= 1024) || (old && BigInt(old.revision) >= BigInt(revision)))
        return;
      pending.set(writer, { worker, boot, revision });
      drain();
    },
    async close() {
      closed = true;
      pending.clear();
      await Promise.all([...active.values()].map((a) => a.work));
    },
  };
}

export async function serveSegmentScheduler(
  pool,
  { connectionString, token, host = "127.0.0.1", port = 0, tickMs = 1000 },
) {
  checkToken(token);
  if (!Number.isSafeInteger(tickMs) || tickMs < 1) throw Error("Invalid reconciliation interval");
  const {
    rows: [{ channel }],
  } = await pool.query(
    "select 'pma_segment_ready_' || md5(n.nspname::text) as channel from pg_class c join pg_namespace n on n.oid=c.relnamespace where c.oid='runs'::regclass",
  );
  const abort = new AbortController();
  const receipts = steerReceipts(pool);
  const router = await openWakeRouter(
    host,
    path,
    (err) => abort.abort(err),
    port,
    token,
    receipts.receive,
  );
  const wake = new PostgresNotificationWake(connectionString, channel, (payload) => {
    if (!payload.startsWith("steer:")) return false;
    const [, id, writer, revision] = payload.split(":");
    router.notifySteer(id, writer, revision);
    return true;
  });
  router.configure({ ...stage, tickMs });
  const listen = (async () => {
    let observed = wake.generation;
    try {
      while (!abort.signal.aborted) {
        wake.refresh();
        await wake.wait(observed, 1000, abort.signal);
        const latest = wake.generation;
        if (!abort.signal.aborted && latest > observed)
          router.notify(stage.label, latest - observed);
        observed = latest;
      }
    } finally {
      await Promise.all([wake.close(), router.close(), receipts.close()]);
    }
  })();
  return {
    url: `ws://${host.includes(":") ? "[" + host + "]" : host}:${router.port}${path}`,
    snapshot: router.snapshot,
    async close() {
      abort.abort();
      await listen;
    },
  };
}

export function connectSegmentWorker({
  scheduler,
  id,
  slots,
  claim,
  execute,
  fail,
  onSteer,
  onRegistered,
}) {
  checkToken(scheduler.token);
  return connectClaimWorker({
    url: scheduler.url,
    token: scheduler.token,
    id,
    stage,
    slots,
    claim: async () => {
      const job = await claim();
      return job ? [job] : [];
    },
    execute,
    fail,
    onSteer,
    onRegistered,
  });
}
