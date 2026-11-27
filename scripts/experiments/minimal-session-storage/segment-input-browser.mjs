import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { createServer } from "vite";
import { withChromePage } from "../../lib/chrome-cdp.mjs";

export async function verifyBrowserTakeback(origin, session, token, repairSteerReceipt) {
  const cacheDir = await mkdtemp(join(tmpdir(), "segment-input-ui-"));
  const code = `
import React from "react";import {createRoot} from "react-dom/client";
import {InputQueueProvider,PendingInputQueue} from "/src/PendingInputQueue.tsx";
import {I18nProvider} from "/src/i18n.tsx";import {PiCloudApi} from "/src/api.ts";
import {ComposerSubmission} from "@pi-cloud/session-client";
import {ComposerDelivery} from "/src/ComposerDelivery.tsx";
import {inputReturnRecovery} from "/src/returned-inputs.ts";
const h=React.createElement,session=${JSON.stringify(session)},api=new PiCloudApi((path,opts)=>fetch(path,opts),${JSON.stringify(token)}), recovery=inputReturnRecovery();
const composer=new ComposerSubmission();window.paintTimes=[];
function Editor(){
 const view=React.useSyncExternalStore(composer.subscribe,composer.snapshot);
 React.useLayoutEffect(()=>{if(view?.state==="sending" && document.querySelector(".composer-delivery.sending"))window.paintTimes.push(performance.now()-window.clickedAt);},[view]);
 function send(){
   window.clickedAt=performance.now();
   const input={sessionId:session,kind:"turn",text:"UI sample",uploadIds:[],whenBusy:"follow_up"};
   window.lastSend=composer.send(input,key=>api.acceptTurn(session,input.text,key,undefined,undefined,input.whenBusy));
 }
 const [draft,setDraft]=React.useState("existing draft"),[rev,setRev]=React.useState(0);
 return h(I18nProvider,{},h(InputQueueProvider,{api,sessionId:session,canMutate:true,refreshSignal:String(rev),streamQueue:null,
  onSnapshot(){},returnRecovery:recovery,onRestoreDraft(){},onRestoreReturned(items){setDraft(s=>s+"\\n"+items.map(i=>i.text).join("\\n"));return true;},onReturnAcknowledged(){setRev(n=>n+1);}},
  h("textarea",{value:draft,readOnly:true}),h("button",{id:"send-sample",onClick:send},"Send"),h(ComposerDelivery,{value:view}),h(PendingInputQueue)));
}
createRoot(document.getElementById("root")).render(h(Editor));
`;
  const server = await createServer({
    root: fileURLToPath(new URL("../../../packages/web-ui", import.meta.url)),
    cacheDir,
    configFile: false,
    logLevel: "error",
    server: { host: "127.0.0.1", port: 0, proxy: { "/v1": origin } },
    plugins: [
      {
        name: "segment-input-check",
        configureServer(vite) {
          vite.middlewares.use((req, res, next) => {
            if (req.url === "/mail") {
              res.setHeader("content-type", "text/html");
              res.end(
                '<meta charset="utf-8"><div id="root"></div><script type="module" src="/mail-client.js"></script>',
              );
            } else next();
          });
        },
        resolveId(id) {
          if (id === "/mail-client.js") return "\\0mail-client";
        },
        load(id) {
          if (id === "\\0mail-client") return code;
        },
      },
    ],
  });
  try {
    await server.listen();
    return await withChromePage({}, async (page) => {
      await page.navigate("http://127.0.0.1:" + server.httpServer.address().port + "/mail");
      const until = async (fn) => {
        for (let i = 0; i < 150; i++) {
          const r = await page.evaluate(fn);
          if (r) return r;
          await new Promise((resolve) => setTimeout(resolve, 100));
        }
        throw Error("Browser did not reach " + fn);
      };
      await until("Boolean(document.querySelector('.pending-input-row'))");
      await until(
        '["已送达","Delivered"].includes(document.querySelector(".pending-input-row [data-receipt=delivered]")?.textContent)',
      );
      await repairSteerReceipt();
      await until(
        '["待引导","Steer queued"].includes(document.querySelector(".pending-input-status")?.textContent)',
      );
      await page.evaluate("document.querySelector('.pending-input-more').click()");
      await until(
        "Array.from(document.querySelectorAll('[role=menuitem]')).some(b=>b.textContent.includes('取回重写')||b.textContent.includes('Take back'))",
      );
      await page.evaluate(
        "Array.from(document.querySelectorAll('[role=menuitem]')).find(b=>b.textContent.includes('取回重写')||b.textContent.includes('Take back')).click()",
      );
      await until("document.querySelector('textarea')?.value.includes('browser input')");
      assert.equal(
        await page.evaluate("document.querySelector('textarea').value"),
        "existing draft\nbrowser input",
      );
      const ids = await page.evaluate(
        `(async()=>{const ids=[];for(let n=0;n<32;n++){document.getElementById('send-sample').click();ids.push((await window.lastSend).inputId);}return ids;})()`,
      );
      const samples = await page.evaluate("window.paintTimes.sort((a,b)=>a-b)");
      assert.equal(samples.length, 32);
      process.stdout.write(
        "segment-browser-click-to-sending-dom " +
          JSON.stringify({ p95: samples[30], p99: samples[31] }) +
          "\n",
      );
      return ids;
    });
  } finally {
    await server.close();
    await rm(cacheDir, { recursive: true, force: true });
  }
}
