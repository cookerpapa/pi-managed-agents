// A single fixed standby in a disposable cluster. Check replay before opening
// the page snapshot. Never reuse a watermark from another page or timeline.
import { setTimeout as delay } from "node:timers/promises";
import pg from "pg";
import {
  createDatabase,
  databaseReadSignal,
  isTransientDatabaseReadError,
} from "@pi-cloud/database";
import { ConversationReader } from "../../../packages/control-plane/src/conversation-reader.ts";

export function guardedOlderPages(primaryUrl, standbyUrl) {
  const appName = "history-freshness";
  const pool = (connectionString) =>
    new pg.Pool({
      connectionString,
      application_name: appName,
      max: 8,
      connectionTimeoutMillis: 200,
      statement_timeout: 200,
      query_timeout: 250,
    });
  const primary = pool(primaryUrl),
    standby = pool(standbyUrl);
  primary.on("error", () => {});
  standby.on("error", () => {});
  const pages = createDatabase({
    connectionString: standbyUrl,
    maxConnections: 8,
    readTimeoutMs: 5000,
  });
  const stats = { replica: 0, lagFallback: 0, unavailableFallback: 0, replayChecks: 0 };
  async function read(tenant, id, query) {
    const signal = databaseReadSignal();
    let lsn;
    try {
      lsn = (await primary.query("select pg_current_wal_insert_lsn() lsn")).rows[0].lsn;
    } catch {
      signal?.throwIfAborted();
      stats.unavailableFallback++;
      return;
    }
    const until = performance.now() + 25;
    try {
      for (;;) {
        signal?.throwIfAborted();
        stats.replayChecks++;
        const {
          rows: [{ ready }],
        } = await standby.query(
          "select pg_is_in_recovery() and pg_last_wal_replay_lsn()>=$1::pg_lsn ready",
          [lsn],
        );
        if (ready) break;
        if (performance.now() >= until) {
          stats.lagFallback++;
          return;
        }
        await delay(4, undefined, signal ? { signal } : undefined);
      }
    } catch {
      signal?.throwIfAborted();
      stats.unavailableFallback++;
      return;
    }
    try {
      const p = await new ConversationReader(pages, tenant, true, true).getConversation(id, query);
      stats.replica++;
      return p;
    } catch (e) {
      signal?.throwIfAborted();
      if (!isTransientDatabaseReadError(e) && e.code !== "40001") throw e;
      stats.unavailableFallback++;
    }
  }
  return {
    read,
    counts: () => ({ ...stats }),
    close: () => Promise.all([primary.end(), standby.end(), pages.destroy()]),
  };
}
