// PG notifications are advisory. Only this component subscribes; a socket
// conveys permission to check the queue, never permission to execute a Turn.
import { PostgresNotificationWake } from "@pi-cloud/database";
import { openWakeRouter, connectClaimWorker } from "./wake-channel.mjs";

const path = "/internal/segment-work";
const stage = { label: "segments", wakeMode: "directed", perTurn: true, batch: 1 };
function checkToken(token) {
  if (typeof token !== "string" || token.length < 32) throw Error("Invalid management credential");
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
  const wake = new PostgresNotificationWake(connectionString, channel);
  const abort = new AbortController();
  const router = await openWakeRouter(host, path, (err) => abort.abort(err), port, token);
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
      await Promise.all([wake.close(), router.close()]);
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

export function connectSegmentWorker({ scheduler, id, slots, claim, execute, fail }) {
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
  });
}
