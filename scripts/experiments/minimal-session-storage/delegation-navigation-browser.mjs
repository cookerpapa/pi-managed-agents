import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { createServer } from "vite";
import { withChromePage } from "../../lib/chrome-cdp.mjs";

export async function verifyDelegationNavigationBrowser(origin, token, root) {
  const cache = await mkdtemp(join(tmpdir(), "pma-nav-"));
  const vite = await createServer({
    configFile: false,
    root: resolve("packages/web-ui"),
    cacheDir: cache,
    logLevel: "silent",
    esbuild: { jsx: "automatic" },
    server: {
      host: "127.0.0.1",
      port: 0,
      proxy: { "/v1": { target: origin, headers: { authorization: "Bearer " + token } } },
    },
    plugins: [
      {
        name: "navigation-config",
        configureServer(server) {
          server.middlewares.use((req, res, next) => {
            if (req.url !== "/ui-config.json") return next();
            const base = "http://127.0.0.1:" + server.httpServer.address().port;
            res.setHeader("content-type", "application/json");
            res.end(
              JSON.stringify({
                productUrl: base,
                adminUrl: base,
                managementUrls: {
                  providerGateway: "",
                  grafana: "",
                  prometheus: "",
                  jaeger: "",
                  alertmanager: "",
                },
              }),
            );
          });
        },
      },
    ],
  });
  try {
    await vite.listen();
    await withChromePage({ profilePrefix: "pma-nav-" }, async (page) => {
      const failures = [];
      page.onNetworkEvent((method, m) => {
        if (method === "Network.responseReceived" && m.response.status >= 400)
          failures.push([m.response.status, new URL(m.response.url).pathname]);
      });
      const group = ".product-tree-child-links";
      const select = (text) =>
        "[...document.querySelectorAll(" +
        JSON.stringify(group + " .product-tree-entry") +
        ")].find(b=>b.lastElementChild.textContent===" +
        JSON.stringify(text) +
        ")";
      async function step(text) {
        await page.waitFor(select(text));
        await page.evaluate(select(text) + ".click()");
      }
      async function inputs(expected) {
        await page.waitFor(
          "JSON.stringify([...document.querySelectorAll('.product-tree-entry-wrap > .product-tree-entry > span:last-child')].map(e=>e.textContent))===" +
            JSON.stringify(JSON.stringify(expected)),
        );
      }
      async function back() {
        await page.evaluate("document.querySelector('.product-tree-header button').click()");
      }
      try {
        await page.navigate(
          "http://127.0.0.1:" + vite.httpServer.address().port + "/?session=" + root,
        );
        const rootButton =
          "[...document.querySelectorAll('.product-conversation-row button')].find(b=>b.querySelector('strong')?.textContent==='Root')";
        await page.waitFor(rootButton);
        await page.evaluate(rootButton + ".click()");
        await inputs(["A", "B", "P"]);
        await step("M");
        await inputs(["A", "B", "P", "M"]);
        await step("N");
        await inputs(["A", "B", "P", "M", "N"]);
        await back();
        await inputs(["A", "B", "P", "M"]);
        await back();
        await inputs(["A", "B", "P"]);
        await page.evaluate(
          "document.querySelector('.product-tree-child-links .product-tree-page').click()",
        );
        await step("Y");
        await inputs(["Y"]);
        await back();
        await inputs(["A", "B"]);
      } catch (err) {
        throw new Error(
          "Navigation browser failed " +
            JSON.stringify(failures) +
            ": " +
            (await page.evaluate("document.body.innerText.slice(0,1100)")),
          { cause: err },
        );
      }
    });
  } finally {
    await vite.close();
    await rm(cache, { recursive: true, force: true });
  }
}
