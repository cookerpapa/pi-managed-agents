// A real Session API process with separate control and candidate read pools.
import assert from "node:assert/strict";
import { once } from "node:events";
import { fork } from "node:child_process";
import { fileURLToPath } from "node:url";
import { createDatabase } from "@pi-cloud/database";
import {
  createControlPlaneApplication,
  PostgresTenantApiAuthenticator,
  ProductionHttpGateway,
} from "../../../packages/control-plane/src/index.ts";
import { PiCloudApi } from "../../../packages/web-ui/src/api.ts";
import { emptyEventRuntime } from "../../../packages/control-plane/test/fixtures/event-runtime.ts";
import { profilePage } from "./flow-page-profile.mjs";

export async function openPageApi(url, readUrl) {
  const proc = fork(fileURLToPath(import.meta.url), [], {
    execArgv: ["--import", "tsx"],
    stdio: ["ignore", "inherit", "inherit", "ipc"],
  });
  const exited = once(proc, "exit");
  async function ask(message) {
    const abort = new AbortController();
    try {
      const response = once(proc, "message", { signal: abort.signal });
      proc.send(message);
      const reply = await Promise.race([response, exited]).then((args) => args[0]);
      assert(reply && !reply.error, reply?.error ?? "Session API fixture exited");
      return reply;
    } finally {
      abort.abort();
    }
  }
  async function close() {
    if (proc.connected) proc.disconnect();
    await exited;
  }
  try {
    const { origin } = await ask({ url, readUrl });
    return { origin, profile: (token, id, before) => ask({ token, id, before }), close };
  } catch (e) {
    await close();
    throw e;
  }
}
if (import.meta.main) {
  const [{ url, readUrl }] = await once(process, "message");
  assert(new URL(url).hostname === "127.0.0.1" && new URL(readUrl).hostname === "127.0.0.1");
  const database = createDatabase({ connectionString: url, maxConnections: 5 });
  const reads = createDatabase({
    connectionString: readUrl,
    maxConnections: 8,
    readTimeoutMs: 10000,
  });
  let app;
  try {
    app = await createControlPlaneApplication({
      database,
      conversationReadDatabase: reads,
      rootMessageHistory: true,
      flowRootHistory: true,
      eventRuntime: emptyEventRuntime(),
      productionHttpGateway: new ProductionHttpGateway({
        authenticator: new PostgresTenantApiAuthenticator({ database }),
        readiness: () => true,
      }),
    });
    await app.listen(0, "127.0.0.1");
    const origin = await app.getUrl();
    process.on("message", async ({ id, token, before }) => {
      try {
        const api = new PiCloudApi((path, opts) => fetch(new URL(path, origin), opts), token);
        for (const query of [{}, { before }])
          await profilePage(readUrl, (q) => api.getConversation(id, q), query);
        process.send({ ok: true });
      } catch (e) {
        process.send({ error: e.message });
      }
    });
    process.send({ origin });
    await once(process, "disconnect");
  } finally {
    await app?.close();
    await Promise.all([reads.destroy(), database.destroy()]);
  }
}
