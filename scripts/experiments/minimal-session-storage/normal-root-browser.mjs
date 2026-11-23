import assert from "node:assert/strict";
import { join, resolve } from "node:path";
import { createServer } from "vite";
import { until } from "./family-process.mjs";
import { withChromePage } from "../../lib/chrome-cdp.mjs";
import { browserNote, browserMessage } from "./browser-signals.mjs";

export async function verifyRootBrowser(dir) {
  let server;
  try {
    const {
      origin,
      id,
      token,
      first,
      second,
      inputMode,
      preferenceKey,
      stopInputs,
      stopHandoff,
      third,
      edited,
      directoryRecovery,
      base: guestBase,
      fresh,
    } = await browserMessage(dir, "ready");
    assert(new URL(origin).hostname === "127.0.0.1");
    let holdAck = stopInputs,
      blockedAcks = 0;
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
              if (
                holdAck &&
                req.method === "POST" &&
                req.url === `/v1/sessions/${id}/returned-inputs/acknowledgements`
              ) {
                blockedAcks++;
                return;
              }
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
      async function typeDraft(text) {
        await page.waitFor(
          "document.querySelector('.product-composer textarea') && !document.querySelector('.product-composer textarea').disabled",
        );
        await page.evaluate(
          `{const a=document.querySelector('.product-composer textarea');Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype,'value').set.call(a,${JSON.stringify(text)});a.dispatchEvent(new Event('input',{bubbles:true}));}`,
        );
      }
      async function submit(text) {
        await typeDraft(text);
        await page.waitFor(
          "document.querySelector('.product-send-button') && !document.querySelector('.product-send-button').disabled",
        );
        await page.evaluate("document.querySelector('.product-send-button').click()");
      }
      async function openBeforeSubmission() {
        const ready =
          "document.querySelector('.product-conversation-heading strong')?.textContent==='normal root'";

        await page.navigate(url);
        try {
          await page.waitFor(ready, 10000);
        } catch {
          // A cold Vite dependency rebuild can replace the initial document.
          // Retrying this navigation happens before any prompt is submitted.
          await page.navigate(url);
          await page.waitFor(ready);
        }
      }
      try {
        if (directoryRecovery) {
          await page.navigate(url);
          await page.waitFor("document.querySelector('.product-rebind-modal')", 45000);
          await page.waitFor(
            "document.querySelector('.product-composer textarea')?.value.includes('return steer') && document.querySelector('.product-composer textarea').value.includes('return follow_up')",
            30000,
          );
          // The machine row contains the selected working directory and its picker.
          const choose =
            "document.querySelector('.product-rebind-modal code')?.parentElement?.parentElement?.querySelector('button')";
          await page.waitFor(choose + " && !(" + choose + ").disabled", 30000);
          await page.evaluate("(" + choose + ").click()");
          const option = (name) =>
            "[...document.querySelectorAll('[role=option]')].find(e=>e.querySelector('strong')?.textContent===" +
            JSON.stringify(name) +
            ")";
          await page.waitFor(option(guestBase.split("/").at(-1)));
          await page.evaluate(
            "(" +
              option(guestBase.split("/").at(-1)) +
              ").dispatchEvent(new MouseEvent('dblclick',{bubbles:true}))",
          );
          await page.waitFor(option("new"));
          await page.evaluate("(" + option("new") + ").click()");
          await page.evaluate(
            "document.querySelector('[role=dialog] footer .product-primary-button').click()",
          );
          await page.waitFor(
            "document.querySelector('.product-rebind-modal code')?.textContent===" +
              JSON.stringify(fresh),
          );
          await page.evaluate(
            "document.querySelector('.product-rebind-modal button[type=submit]').click()",
          );
          await page.waitFor("!document.querySelector('.product-rebind-modal')");
          await submit(second);
          await browserNote(dir, "directorySubmitted");
          await browserMessage(dir, "directoryFinished");
          await page.waitFor(has("Cancellation hold unexpectedly completed."));
          assert.deepEqual(failures, []);
          return;
        }
        await openBeforeSubmission();
        if (inputMode) {
          await page.evaluate(
            `localStorage.setItem(${JSON.stringify(preferenceKey)},${JSON.stringify(inputMode)});window.dispatchEvent(new StorageEvent("storage",{key:${JSON.stringify(preferenceKey)}}));`,
          );
        }
        await submit(first);
        await page.waitFor("document.querySelector('.product-tool-running')", 60000);
        if (inputMode) {
          let requestId,
            key,
            status,
            body,
            finished = false;
          page.onNetworkEvent((method, m) => {
            if (
              method === "Network.requestWillBeSent" &&
              m.request.method === "POST" &&
              new URL(m.request.url).pathname === "/v1/sessions/" + id + "/turns"
            ) {
              requestId = m.requestId;
              key = Object.entries(m.request.headers).find(
                ([k]) => k.toLowerCase() === "idempotency-key",
              )?.[1];
              body = JSON.parse(m.request.postData);
            }
            if (requestId && m.requestId === requestId) {
              if (method === "Network.responseReceived") status = m.response.status;
              if (method === "Network.loadingFinished") finished = true;
            }
          });
          await submit(second);
          await browserMessage(dir, "held");
          assert(requestId && key && body.prompt === second && body.whenBusy === inputMode);
          assert.equal(status, undefined);
          await browserNote(dir, "unconfirmed");
          await until(() => finished, "browser accepted busy input");
          assert(status >= 200 && status < 300);
          const response = await page.send("Network.getResponseBody", { requestId });
          const receipt = JSON.parse(
            response.base64Encoded
              ? Buffer.from(response.body, "base64").toString()
              : response.body,
          );
          const queued = `[...document.querySelectorAll('.pending-input-row')].some(e=>e.dataset.inputId===${JSON.stringify(receipt.inputId)} && e.innerText.includes(${JSON.stringify(second)}))`;
          await page.waitFor(queued);
          await browserNote(dir, "accepted", { receipt, key, thinkingLevel: body.thinkingLevel });
          if (stopInputs) {
            await page.evaluate(
              `localStorage.setItem(${JSON.stringify(preferenceKey)},"steer");window.dispatchEvent(new StorageEvent("storage",{key:${JSON.stringify(preferenceKey)}}))`,
            );
            finished = false;
            await submit(third);
            await until(() => finished && body.prompt === third, "second busy input response");
            assert(body.whenBusy === "steer" && status >= 200 && status < 300);
            const raw = await page.send("Network.getResponseBody", { requestId });
            const another = JSON.parse(
              raw.base64Encoded ? Buffer.from(raw.body, "base64").toString() : raw.body,
            );
            await page.waitFor(
              "document.querySelectorAll('.pending-input-row').length===2 && document.querySelector('.product-stop-button:not(:disabled)')",
            );
            if (stopHandoff) {
              await browserNote(dir, "bothAccepted", { another });
              await browserMessage(dir, "workerLost");
            }
            await page.evaluate("document.querySelector('.product-stop-button').click()");
            await browserNote(dir, "stopClicked", { another });
            await until(() => blockedAcks > 0, "return acknowledgement held", 45000);
            const composer = "document.querySelector('.product-composer textarea')";
            await page.waitFor(`${composer}?.value===${JSON.stringify(second + "\n\n" + third)}`);
            await typeDraft(edited);
            await page.navigate("about:blank");
            holdAck = false;
            await page.navigate(url);
            await until(async () => {
              const pageState = await page.evaluate(
                `(async()=>{const q=await (await fetch("/v1/sessions/"+${JSON.stringify(id)}+"/queued-inputs")).json();return {pending:q.hasReturnedInputs,draft:${composer}?.value};})()`,
              );
              if (pageState.pending !== false || pageState.draft === undefined) return false;
              assert.equal(
                pageState.draft,
                edited,
                "Edited draft changed while retrying a Stop acknowledgement",
              );
              return true;
            }, "restored draft survived refresh before acknowledgement");
            await page.navigate(url);
            await page.waitFor(`${composer}?.value===${JSON.stringify(edited)}`);
            await browserNote(dir, "editedReload");
            await browserMessage(dir, "sendEdited");
            await page.evaluate("document.querySelector('.product-send-button').click()");
            await page.waitFor(has("Received edited draft."), 60000);
            await page.navigate(url);
            await page.waitFor(has("Received edited draft."));
            assert.deepEqual(failures, []);
            return;
          }
          await browserMessage(dir, "between");
          await page.navigate(url);
          await page.waitFor(queued);
          await page.waitFor(has(first));
          await page.waitFor("document.querySelector('.product-tool-running')");
          await browserNote(dir, "queueWhileUnclaimed");
          await browserMessage(dir, "reclaimed");
          await page.navigate(url);
          await page.waitFor(queued);
          await browserNote(dir, "queueReload");
          const { answer } = await browserMessage(dir, "settled");
          await page.waitFor(has(answer));
          await page.waitFor("document.querySelectorAll('.pending-input-row').length===0");
          await page.navigate(url);
          await page.waitFor(has(answer));
          assert.equal(
            await page.evaluate(
              `[...document.querySelectorAll('.product-user-bubble')].filter(e=>e.textContent===${JSON.stringify(second)}).length`,
            ),
            1,
          );
          assert.equal(
            await page.evaluate("document.querySelectorAll('.pending-input-row').length"),
            0,
          );
          assert.equal(
            await page.evaluate("document.querySelectorAll('.product-tool-completed').length"),
            1,
          );
          assert.deepEqual(failures, []);
          return;
        }
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
    if (directoryRecovery) {
      await browserNote(dir, "done", { ChromeReselection: true });
      return;
    }
    await browserNote(dir, "done", {
      browser: "Chrome",
      ...(stopInputs
        ? {
            originalTextRestoredInOrder: true,
            refreshBeforeReturnAck: true,
            editedDraftPersisted: true,
          }
        : { activeReload: true }),
      settledReload: true,
      composerSubmissions: stopInputs ? 4 : 2,
      http5xx: 0,
    });
  } catch (e) {
    await browserNote(dir, "failure", { message: String(e.message).slice(0, 500) });
    throw e;
  } finally {
    await server?.close();
  }
}
