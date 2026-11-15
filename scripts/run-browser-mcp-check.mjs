import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { createServer } from "vite";
import { withChromePage } from "./lib/chrome-cdp.mjs";

const cacheDir = await mkdtemp(join(tmpdir(), "pma-mcp-ui-"));
const fixture = `
import React from 'react';
import { createRoot } from 'react-dom/client';
import { McpConnections } from '/src/McpConnections.tsx';
import { I18nProvider } from '/src/i18n.tsx';
import '/src/product.css';
localStorage.setItem('pi-cloud:ui-language','en-US');
window.errors=[];window.saved=[];window.removed=[];window.rows=[];
window.addEventListener('error',e=>errors.push(String(e.message)));
window.addEventListener('unhandledrejection',e=>errors.push(String(e.reason)));
const api={
  listMcpConnections:async()=>({connections:structuredClone(rows),next:null}),
  getMcpConnection:async id=>({connection:structuredClone(rows.find(r=>r.id===id)),tools:[{name:'echo',platformName:'mcp_echo_test',description:'Echo',parameters:{type:'object',properties:{}}}]}),
  saveMcpConnection:async(id,input)=>{saved.push(structuredClone(input));const prior=rows.find(r=>r.id===id);
    const row={id,name:input.name,endpoint:input.endpoint,visibility:input.visibility,enabled:input.enabled,
      revision:String(Number(prior?.revision??0)+1),authentication:input.credential?.kind??prior?.authentication??'none',
      status:input.enabled?'ready':'disabled',error:null,checkedAt:new Date().toISOString(),toolCount:1,
      allowedTools:input.allowedTools,canEdit:true,ownerUserId:'owner'};
    window.rows=[...rows.filter(r=>r.id!==id),row];return {connection:row,tools:[]};},
  removeMcpConnection:async id=>{removed.push(id);window.rows=rows.filter(r=>r.id!==id);},
  refreshMcpConnection:async()=>{},
};
const root=createRoot(document.getElementById('root'));
window.renderMcp=owner=>root.render(React.createElement(I18nProvider,null,React.createElement(McpConnections,{api,owner,onClose:()=>{window.closed=true;}})));
window.clickText=text=>[...document.querySelectorAll('button')].find(b=>b.textContent===text)?.click();
window.setField=(selector,value)=>{const e=document.querySelector(selector);Object.getOwnPropertyDescriptor(e.tagName==='SELECT'?HTMLSelectElement.prototype:HTMLInputElement.prototype,'value').set.call(e,value);e.dispatchEvent(new Event(e.tagName==='SELECT'?'change':'input',{bubbles:true}));};
renderMcp(true);window.ready=true;
`;
const server = await createServer({
  configFile: false,
  root: fileURLToPath(new URL("../packages/web-ui", import.meta.url)),
  cacheDir,
  logLevel: "error",
  esbuild: { jsx: "automatic" },
  server: { host: "127.0.0.1", port: 0 },
  plugins: [
    {
      name: "mcp-ui-fixture",
      resolveId: (id) => (id === "/mcp-fixture.js" ? "\0mcp-fixture" : undefined),
      load: (id) => (id === "\0mcp-fixture" ? fixture : undefined),
      configureServer(server) {
        server.middlewares.use((req, res, next) => {
          if (req.url !== "/mcp-check.html") return next();
          res.setHeader("content-type", "text/html");
          res.end(
            '<!doctype html><meta charset="utf-8"><div id="root"></div><script type="module" src="/mcp-fixture.js"></script>',
          );
        });
      },
    },
  ],
});
try {
  await server.listen();
  await withChromePage({}, async (page) => {
    await page.navigate(`http://127.0.0.1:${server.httpServer.address().port}/mcp-check.html`);
    await page.waitFor("window.ready && document.body.textContent.includes('Add service')");
    await page.evaluate("clickText('Add service')");
    await page.waitFor("document.querySelector('form')");
    await page.evaluate(
      "setField('form input:not([type])','Owned fixture');setField('form input[type=url]','https://mcp.example/mcp');setField('form select:nth-of-type(1)','personal')",
    );
    await page.evaluate("setField('form .mcp-field-pair label:nth-child(2) select','bearer')");
    await page.waitFor("document.querySelector('input[type=password]')");
    assert.equal(await page.evaluate("document.querySelector('form').checkValidity()"), false);
    await page.evaluate("setField('input[type=password]','private-fixture-token')");
    await page.evaluate("document.querySelector('form').requestSubmit()");
    await page.waitFor(
      "saved.length===1 && !document.querySelector('form') && document.querySelector('.mcp-row')",
    );
    assert.deepEqual(
      await page.evaluate("({visibility:saved[0].visibility,auth:saved[0].credential.kind})"),
      { visibility: "personal", auth: "bearer" },
    );
    assert.equal(
      await page.evaluate(
        "document.body.textContent.includes('private-fixture-token') || JSON.stringify(localStorage).includes('private-fixture-token')",
      ),
      false,
    );
    await page.evaluate("clickText('Edit')");
    await page.waitFor("document.querySelector('input[type=password]')");
    assert.equal(await page.evaluate("document.querySelector('input[type=password]').value"), "");
    await page.evaluate("document.querySelector('form').requestSubmit()");
    await page.waitFor("saved.length===2 && !document.querySelector('form')");
    assert.equal(await page.evaluate("Object.hasOwn(saved[1],'credential')"), false);
    await page.evaluate("clickText('Disable')");
    await page.waitFor("saved.length===3 && document.body.textContent.includes('Disabled')");
    await page.evaluate("clickText('Remove')");
    assert.equal(await page.evaluate("removed.length"), 0);
    await page.evaluate("clickText('Confirm removal')");
    await page.waitFor("removed.length===1 && !document.querySelector('.mcp-row')");
    await page.evaluate("renderMcp(false)");
    await page.waitFor(
      "![...document.querySelectorAll('button')].some(b=>b.textContent==='Add service')",
    );
    assert.deepEqual(await page.evaluate("errors"), []);
    console.log(
      JSON.stringify({
        accepted: true,
        checks: [
          "personal default",
          "required Bearer",
          "masked edit",
          "credential preservation",
          "disable",
          "confirmed removal",
          "member view",
        ],
      }),
    );
  });
} finally {
  await server.close();
  await rm(cacheDir, { recursive: true, force: true });
}
