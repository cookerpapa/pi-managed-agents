// A real Session API process with separate control and candidate read pools.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import { fork } from "node:child_process";
import { fileURLToPath, pathToFileURL } from "node:url";
import { resolve } from "node:path";
import { PiCloudApi } from "../../../packages/web-ui/src/api.ts";
import { capturePageQueries, profilePage } from "./flow-page-profile.mjs";

export async function openPageApi(url, readUrl, flowRootHistory = true, referenceRoot, standbyUrl) {
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
    const { origin } = await ask({ url, readUrl, flowRootHistory, referenceRoot, standbyUrl });
    return {
      origin,
      counts: () => ask({ op: "counts" }),
      liveEvent: (tenant, event) => ask({ op: "event", tenant, event }),
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
  const [{ url, readUrl, flowRootHistory, referenceRoot, standbyUrl }] = await once(
    process,
    "message",
  );
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
  const guard = standbyUrl
    ? (await import("./replica-history-reader.mjs")).guardedOlderPages(readUrl, standbyUrl)
    : undefined;
  const live = guard
    ? new (await import("../../../packages/runtime-core/src/session-live-view.ts")).SessionLiveView(
        async () => {},
      )
    : undefined;
  let app, liveServer, remote;

  try {
    const { createControlPlaneApplication, ProductionHttpGateway, PostgresTenantApiAuthenticator } =
      await import(moduleUrl("packages/control-plane/src/index.ts"));
    if (live) {
      const { default: fastify } = await import("fastify");
      const { SessionLiveService } = await import(
        moduleUrl("packages/control-plane/src/session-live-service.ts")
      );
      const { SessionEventStream } = await import("@pi-cloud/runtime-core/session-event-stream");
      liveServer = fastify({ forceCloseConnections: true });
      const token = randomUUID() + randomUUID();
      new SessionLiveService({
        database: reads,
        stream: new SessionEventStream(live, live.eventHub),
        rootMessageHistory: true,
        flowRootHistory,
        token,
      }).install(liveServer);
      remote = { baseUrl: await liveServer.listen({ port: 0, host: "127.0.0.1" }), token };
    }
    app = await createControlPlaneApplication({
      database,
      conversationReadDatabase: reads,
      rootMessageHistory: true,
      flowRootHistory,
      ...(guard ? { olderHistoryReader: guard.read } : {}),
      eventRuntime: emptyEventRuntime(),
      ...(remote ? { sessionEventStreamOptions: { remote } } : {}),
      productionHttpGateway: new ProductionHttpGateway({
        authenticator: new PostgresTenantApiAuthenticator({ database }),
        readiness: () => true,
      }),
    });
    await app.listen(0, "127.0.0.1");
    const origin = await app.getUrl();
    process.on("message", async ({ id, token, before, cpuLabel, query, op, tenant, event }) => {
      try {
        if (op === "counts") {
          process.send(guard.counts());
          return;
        }
        if (op === "event") {
          live.accept(tenant, event);
          process.send({ ok: true });
          return;
        }
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
    if (liveServer) {
      // All test clients are done. Release their loopback proxy keep-alives.
      app?.getHttpServer().closeAllConnections();
      liveServer.server.closeAllConnections();
    }
    await app?.close();
    await liveServer?.close();
    live?.close();
    await guard?.close();
    await Promise.all([reads.destroy(), database.destroy()]);
  }
}
