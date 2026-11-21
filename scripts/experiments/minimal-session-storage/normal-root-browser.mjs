import assert from "node:assert/strict";
import { join, resolve } from "node:path";
import { createServer } from "vite";
import { withChromePage } from "../../lib/chrome-cdp.mjs";
import { browserNote, browserMessage } from "./browser-signals.mjs";

export async function verifyRootBrowser(dir) {
  let server;
  try {
    const { origin, id, token, first, second } = await browserMessage(dir, "ready");
    assert(new URL(origin).hostname === "127.0.0.1");
    server = await createServer({
      configFile: false,
      root: resolve("packages/web-ui"),
      cacheDir: join(dir, "vite-cache"),
      logLevel: "silent",
      esbuild: { jsx: "automatic" },
      server: {
        host: "127.0.0.1",
        port: 0,
        proxy: { "/v1": { target: origin, headers: { authorization: "Bearer " + token } } },
      },
      plugins: [
        {
          name: "isolated-web-config",
          configureServer(v) {
            v.middlewares.use((req, res, next) => {
              if (req.url !== "/ui-config.json") return next();
              const base = "http://127.0.0.1:" + v.httpServer.address().port;
              res.setHeader("content-type", "application/json");
              res.end(
                JSON.stringify({
                  productUrl: base,
                  adminUrl: base,
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
    const url = "http://127.0.0.1:" + server.httpServer.address().port + "/?session=" + id;
    await withChromePage({ profilePrefix: "pma-root-browser-" }, async (page) => {
      const failures = [];
      page.onNetworkEvent((method, m) => {
        if (method === "Network.responseReceived" && m.response.status >= 500) {
          failures.push(new URL(m.response.url).pathname);
          console.error("Browser HTTP", m.response.status, failures.at(-1));
        }
      });
      const has = (text) => "document.body.innerText.includes(" + JSON.stringify(text) + ")";
      async function submit(text) {
        await page.waitFor(
          "document.querySelector('.product-composer textarea') && !document.querySelector('.product-composer textarea').disabled",
        );
        await page.evaluate(
          `{const a=document.querySelector('.product-composer textarea');Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype,'value').set.call(a,${JSON.stringify(text)});a.dispatchEvent(new Event('input',{bubbles:true}));}`,
        );
        await page.waitFor(
          "document.querySelector('.product-send-button') && !document.querySelector('.product-send-button').disabled",
        );
        await page.evaluate("document.querySelector('.product-send-button').click()");
      }
      try {
        await page.navigate(url);
        await page.waitFor(
          "document.querySelector('.product-conversation-heading strong')?.textContent==='normal root'",
        );
        await submit(first);
        await page.waitFor("document.querySelector('.product-tool-running')", 60000);
        await page.navigate(url);
        await page.waitFor("document.querySelector('.product-tool-running')");
        await page.waitFor(has(first));
        await browserNote(dir, "active");
        await page.waitFor(has("First answer: GUEST-"), 60000);
        await page.navigate("about:blank");
        await browserNote(dir, "detached");
        const { marker } = await browserMessage(dir, "restarted");
        await page.navigate(url);
        await page.waitFor(has("First answer: " + marker));
        await submit(second);
        await page.waitFor(has("Second answer: " + marker), 60000);
        await page.navigate(url);
        await page.waitFor(has("Second answer: " + marker));
        await page.waitFor(has("First answer: " + marker));
        await page.waitFor(has(second));
        assert.equal(
          await page.evaluate("document.querySelectorAll('.product-tool-completed').length"),
          1,
        );
        assert.deepEqual(failures, []);
      } catch (e) {
        console.error(
          "Browser evidence",
          JSON.stringify({
            failures,
            runningTools: await page.evaluate(
              "document.querySelectorAll('.product-tool-running').length",
            ),
          }),
        );
        throw e;
      }
    });
    await browserNote(dir, "done", {
      browser: "Chrome",
      activeReload: true,
      settledReload: true,
      composerSubmissions: 2,
      http5xx: 0,
    });
  } catch (e) {
    await browserNote(dir, "failure", { message: String(e.message).slice(0, 500) });
    throw e;
  } finally {
    await server?.close();
  }
}
