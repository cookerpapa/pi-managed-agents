import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { createServer } from "vite";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { createDatabase } from "@pi-cloud/database";
import { createControlPlaneApplication } from "../../../packages/control-plane/src/application.ts";
import { emptyEventRuntime } from "../../../packages/control-plane/test/fixtures/event-runtime.ts";
import { withChromePage } from "../../lib/chrome-cdp.mjs";

export async function verifyDelegatedHistory({
  url,
  schema,
  tenantId,
  sessionId,
  agentId,
  secondRunId,
}) {
  const dsn = new URL(url);
  dsn.searchParams.set("options", "-c search_path=" + schema);
  const database = createDatabase({ connectionString: dsn.href, maxConnections: 3 });
  const dir = await mkdtemp(join(tmpdir(), "pma-history-vite-"));
  let app, server;
  try {
    app = await createControlPlaneApplication({
      database,
      tenantId,
      defaultModelProfileId: randomUUID(),
      delegatedRunHistory: true,
      eventRuntime: emptyEventRuntime(),
    });
    await app.listen(0, "127.0.0.1");
    const origin = await app.getUrl();
    const path = "/v1/conversations/" + sessionId + "/agents";
    const list = await (await fetch(origin + path)).json();
    assert(list.agents.some((a) => a.agentId === agentId && a.task === "ALPHA_JOB"));
    const response = await fetch(origin + path + "/" + agentId + "/history");
    assert.equal(response.status, 200, await response.clone().text());
    const history = await response.json();
    assert.deepEqual(new Set(history.entries.map((e) => e.runId)), new Set([agentId, secondRunId]));
    assert.equal(
      (await fetch(origin + path.replace(sessionId, randomUUID()) + "/" + agentId + "/history"))
        .status,
      404,
    );
    assert.equal(
      (await fetch(origin + path + "/" + agentId + "/history?before=invalid")).status,
      400,
    );
    server = await createServer({
      configFile: false,
      root: resolve("packages/web-ui"),
      cacheDir: dir,
      logLevel: "silent",
      esbuild: { jsx: "automatic" },
      server: { port: 0, host: "127.0.0.1", proxy: { "/v1": origin } },
      plugins: [
        {
          name: "candidate-history-config",
          configureServer(v) {
            v.middlewares.use((req, res, next) => {
              if (req.url !== "/ui-config.json") return next();
              const host = "http://127.0.0.1:" + v.httpServer.address().port;
              res.setHeader("content-type", "application/json");
              res.end(
                JSON.stringify({
                  productUrl: host,
                  adminUrl: host,
                  delegatedRunHistory: true,
                  managementUrls: {
                    providerGateway: "",
                    grafana: "",
                    prometheus: "",
                    alertmanager: "",
                    jaeger: "",
                  },
                }),
              );
            });
          },
        },
      ],
    });
    await server.listen();
    const pageUrl =
      "http://127.0.0.1:" +
      server.httpServer.address().port +
      "/?session=" +
      sessionId +
      "&agents=";
    await withChromePage({ profilePrefix: "pma-child-history-" }, async (page) => {
      await page.navigate(pageUrl);
      const link =
        'Array.from(document.querySelectorAll("main li a")).find(e=>e.textContent==="ALPHA_JOB")';
      await page.waitFor(link);
      await page.evaluate(link + ".click()");
      await page.waitFor('document.body.innerText.includes("ALPHA_CONTINUED")');
      const body = await page.evaluate("document.body.innerText");
      assert(
        body.includes("ALPHA_PRIVATE") &&
          !body.includes("BETA_PRIVATE") &&
          !body.includes("ROOT_THREE"),
      );
      await page.navigate(pageUrl + agentId);
      await page.waitFor('document.body.innerText.includes("ALPHA_PRIVATE")');
    });
  } finally {
    try {
      await server?.close();
      await app?.close();
    } finally {
      await database.destroy();
      await rm(dir, { recursive: true, force: true });
    }
  }
}
