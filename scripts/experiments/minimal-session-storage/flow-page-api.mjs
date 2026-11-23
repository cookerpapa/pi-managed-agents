// A real Session API process with separate control and candidate read pools.
import assert from "node:assert/strict";
import { once } from "node:events";
import { fork } from "node:child_process";
import { fileURLToPath, pathToFileURL } from "node:url";
import { resolve } from "node:path";
import { PiCloudApi } from "../../../packages/web-ui/src/api.ts";
import { capturePageQueries, profilePage } from "./flow-page-profile.mjs";

export async function openPageApi(url, readUrl, flowRootHistory = true, referenceRoot) {
  const proc = fork(fileURLToPath(import.meta.url), [], {
    execArgv: ["--import", "tsx"],
    ...(referenceRoot
      ? {
          env: {
            ...process.env,
            TSX_TSCONFIG_PATH: resolve(referenceRoot, "packages/control-plane/tsconfig.json"),
          },
        }
      : {}),
    stdio: ["ignore", "inherit", "inherit", "ipc"],
  });
  const exited = once(proc, "exit");
  async function ask(message) {
    const abort = new AbortController();
    try {
      const response = once(proc, "message", { signal: abort.signal });
      proc.send(message);
      const reply = await Promise.race([response, exited]).then((args) => args[0]);
      assert(
        reply && typeof reply === "object" && !reply.error,
        reply?.error ?? "Session API fixture exited",
      );
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
    const { origin } = await ask({ url, readUrl, flowRootHistory, referenceRoot });
    return {
      origin,
      capture: (token, id, query) => ask({ token, id, query }),
      profile: (token, id, before, cpuLabel) => ask({ token, id, before, cpuLabel }),
      close,
    };
  } catch (e) {
    await close();
    throw e;
  }
}
if (import.meta.main) {
  const [{ url, readUrl, flowRootHistory, referenceRoot }] = await once(process, "message");
  assert(new URL(url).hostname === "127.0.0.1" && new URL(readUrl).hostname === "127.0.0.1");
  const controlDsn = new URL(url);
  controlDsn.searchParams.set("application_name", "flow-control");
  const root = referenceRoot ?? fileURLToPath(new URL("../../../", import.meta.url));
  const moduleUrl = (path) => pathToFileURL(resolve(root, path)).href;
  const { createDatabase } = await import(moduleUrl("packages/database/src/index.ts"));
  const { emptyEventRuntime } = await import(
    moduleUrl("packages/control-plane/test/fixtures/event-runtime.ts")
  );
  const database = createDatabase({ connectionString: controlDsn.href, maxConnections: 5 });
  const reads = createDatabase({
    connectionString: readUrl,
    maxConnections: 8,
    readTimeoutMs: 10000,
  });
  let app;
  try {
    const { createControlPlaneApplication, ProductionHttpGateway, PostgresTenantApiAuthenticator } =
      await import(moduleUrl("packages/control-plane/src/index.ts"));
    app = await createControlPlaneApplication({
      database,
      conversationReadDatabase: reads,
      rootMessageHistory: true,
      flowRootHistory,
      eventRuntime: emptyEventRuntime(),
      productionHttpGateway: new ProductionHttpGateway({
        authenticator: new PostgresTenantApiAuthenticator({ database }),
        readiness: () => true,
      }),
    });
    await app.listen(0, "127.0.0.1");
    const origin = await app.getUrl();
    process.on("message", async ({ id, token, before, cpuLabel, query }) => {
      try {
        const api = new PiCloudApi((path, opts) => fetch(new URL(path, origin), opts), token);
        if (query) {
          const { statements } = await capturePageQueries((q) => api.getConversation(id, q), query);
          process.send({ statements: statements.map(({ text, values }) => ({ text, values })) });
          return;
        }
        for (const query of [{}, { before }])
          await profilePage(readUrl, (q) => api.getConversation(id, q), query, cpuLabel);
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
