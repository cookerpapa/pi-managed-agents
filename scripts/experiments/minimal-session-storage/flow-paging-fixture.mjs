// The public application reads control metadata and candidate history in the same snapshot.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { preview } from "vite";
import pg from "pg";
import { createDatabase, runMigrations } from "@pi-cloud/database";
import {
  createPrivateTenant,
  ControlPlaneStore,
} from "../../../packages/control-plane/src/index.ts";
import { openPageApi } from "./flow-page-api.mjs";
import { PiCloudApi } from "../../../packages/web-ui/src/api.ts";
import { withChromePage } from "../../lib/chrome-cdp.mjs";
import { installFlowHistory } from "./flow-history.mjs";
import { normalRootProjection } from "./normal-root-projection.mjs";

export async function installPagingProjection(db) {
  await db.query(await readFile(new URL("./schema.sql", import.meta.url), "utf8"));
  await db.query(`alter table runs add output_first_topic text,add output_first_partition int,add output_first_offset bigint,add output_display_seq bigint default 0,add output_display_native_seq bigint default 0,
   add turn_id uuid generated always as(id) stored,add input_text text generated always as(initial_input) stored,add input_uploads jsonb default '[]',
   add pruned_at timestamptz,add operation_kind text default 'chat',add stop_reason text,add scheduled_origin jsonb,add output_seal_id uuid,add output_sealed_at timestamptz;
   create function fixture_run(f uuid,n integer) returns uuid language sql immutable as $$
      select overlay(overlay(md5(f::text||n) placing '4' from 13 for 1) placing '8' from 17 for 1)::uuid
   $$;
`);
  const topic = "flow-pages",
    partitions = 32;
  const projector = normalRootProjection(
    db,
    topic,
    () => assert.fail(),
    () => assert.fail(),
    undefined,
    { partitions },
  );
  await projector.view.install();
  return projector;
}

export async function withPagingFixture(url, tenantId, work) {
  const meta = createDatabase({ connectionString: url, maxConnections: 5 });
  const db = new pg.Pool({
    application_name: "flow-projector",
    connectionString: url,
    options: "-c search_path=flow_study",
    max: 12,
  });
  const readDsn = new URL(url);
  readDsn.searchParams.set("application_name", "flow-page");
  readDsn.searchParams.set("options", "-c search_path=flow_web,flow_study,public");
  let app, web;
  try {
    await runMigrations(meta, "up");
    let first = true;
    const tenant = await createPrivateTenant(meta, {
      slug: "flow-page",
      ownerDisplayName: "Fixture",
      idGenerator: () => {
        if (first) {
          first = false;
          return tenantId;
        }
        return randomUUID();
      },
      initialModel: { provider: "openai-codex", modelId: "gpt-6-luna" },
    });
    const store = new ControlPlaneStore({ database: meta, ...tenant });
    const proj = await store.createProject({ name: "Flow paging", source: { kind: "empty" } });
    const windows = [];
    for (const title of ["Origin", "Left", "Right", "Empty", "Live"]) {
      windows.push(
        (await store.createSession(proj.projectId, proj.workspaceId, title, "elastic")).sessionId,
      );
    }
    const [root, left, right, empty, live] = windows;
    await db.query("create schema flow_study; create schema flow_web");
    const projector = await installPagingProjection(db);
    const topic = "flow-pages";
    await db.query("create view flow_web.sessions as select * from public.sessions");
    await installFlowHistory(db, true, false, "base");
    const f = randomUUID(),
      lf = randomUUID(),
      rf = randomUUID();
    await db.query("insert into sessions(id,tenant_id,flow_id) values($1,$2,$3)", [
      root,
      tenantId,
      f,
    ]);
    async function seed(flow, length, base, label) {
      await db.query(
        `insert into runs(id,tenant_id,session_id,prev_run_id,mailbox_position,initial_input,model_ref,idempotency_key,state,history_head,output_display_seq)
    select fixture_run($1::uuid,n),$2::uuid,$1::uuid,case when n=1 then $4::uuid else fixture_run($1::uuid,n-1) end,n,$5::text||':'||n,'fixture',fixture_run($1::uuid,n),'completed','{"floor":1,"nextId":2}',n
    from generate_series(1,$3::int)n`,
        [flow, tenantId, length, base, label],
      );
      await db.query(
        `insert into messages(tenant_id,pi_session_id,entry_id,conversation_id,run_id,role,content,commit_seq,ordinal)
    select $1::uuid,$2::uuid,n,1,fixture_run($2::uuid,n),'assistant',
    json_build_object('id',n,'kind','pi.assistant','model',json_build_array(json_build_object('role','assistant','content',json_build_array(json_build_object('type','text','text',$4::text||':'||n||' '||repeat('abcd',256))),'stopReason','stop','timestamp',1)))::text,n,0
    from generate_series(1,$3::int)n`,
        [tenantId, flow, length, label],
      );
    }
    async function at(flow, n) {
      return (await db.query("select fixture_run($1::uuid,$2::int) id", [flow, n])).rows[0].id;
    }
    async function fork(s, at, dest, flow) {
      await db.query("select flow_fork($1,$2,$3,$4,$5)", [tenantId, s, at, dest, flow]);
    }
    await seed(f, 100000, null, "root");
    const b = await at(f, 99980);
    await fork(root, b, left, lf);
    await seed(lf, 12, b, "left");
    const c = await at(lf, 4);
    await fork(left, c, right, rf);
    await seed(rf, 4, c, "right");
    await fork(left, b, empty, randomUUID());
    const job = (await db.query("select flow_detach($1,$2,$3,null) job", [tenantId, root, f]))
      .rows[0].job;
    assert.equal((await db.query("select flow_collect($1,256) n", [job])).rows[0].n, 20);
    await meta
      .updateTable("sessions")
      .set({ pruned_at: new Date() })
      .where("id", "=", root)
      .execute();
    await db.query("analyze runs;analyze sessions;analyze messages");
    app = await openPageApi(url, readDsn.href);
    const { origin } = app;
    const token = tenant.credential.token;
    const api = new PiCloudApi((path, opts) => fetch(new URL(path, origin), opts), token);
    async function page(id, query = {}) {
      return api.getConversation(id, query);
    }
    const latest = await page(right);
    assert.equal(latest.turns.at(-1)?.prompt, "right:4");
    assert.equal((await page(empty)).turns.at(-1)?.prompt, "root:99980");
    const expectedRight = Array.from({ length: 4 }, (_, n) => "right:" + (4 - n)).concat(
      Array.from({ length: 4 }, (_, n) => "left:" + (4 - n)),
      Array.from({ length: 240 }, (_, n) => "root:" + (99980 - n)),
    );
    let cursor,
      collected = [];
    for (let i = 0; i < 3; i++) {
      const p = i ? await page(right, { before: cursor }) : latest;
      assert(p.history.hasBefore);
      cursor = p.history.before;
      for (const turn of [...p.turns].reverse()) {
        if (turn.prompt) collected.push(turn.prompt);
        const marker = JSON.stringify(turn.transcript);
        if (turn.transcript?.items.length && turn.prompt)
          assert(marker.includes(turn.prompt + " "), "Missing associated body for " + turn.prompt);
      }
    }
    assert.deepEqual(collected, expectedRight.slice(0, collected.length));
    assert(collected.length >= 80);
    const status = async (id, params = {}) =>
      (
        await fetch(origin + "/v1/conversations/" + id + "?" + new URLSearchParams(params), {
          headers: { authorization: "Bearer " + token },
        })
      ).status;
    assert.equal(await status(left, { before: cursor }), 400);
    assert.equal(await status(root), 404);
    assert.equal(await status(right, { turnId: await at(lf, 12) }), 404);
    const forged = JSON.parse(Buffer.from(cursor, "base64url").toString());
    forged.storageId = randomUUID();
    assert.equal(
      await status(right, { before: Buffer.from(JSON.stringify(forged)).toString("base64url") }),
      404,
    );
    // Follow the frontend's forward cursor back toward the newest message.
    await app.profile(token, right, latest.history.before);
    const older = await page(right, { before: latest.history.before });
    const forward = await page(right, { after: older.history.after });
    assert.deepEqual(
      forward.turns.map((t) => t.turnId),
      latest.turns.map((t) => t.turnId),
    );
    const beforeCut = latest.history.after,
      nextBase = await at(f, 99970);
    await db.query("select flow_detach($1,$2,$3,$4)", [tenantId, right, rf, nextBase]);
    assert.equal(await status(right, { before: beforeCut }), 404);
    assert.equal((await page(right)).turns.at(-1).prompt, "root:99970");
    console.log(
      JSON.stringify({
        phase: "http-lineage",
        checkedHeaders: collected.length,
        sourceDeleted: true,
        cutAcrossFlows: true,
        forwardCursor: true,
      }),
    );
    web = await preview({
      configFile: false,
      root: "packages/web-ui",
      preview: {
        port: 0,
        host: "127.0.0.1",
        proxy: { "/v1": { target: origin, headers: { authorization: "Bearer " + token } } },
      },
      plugins: [
        {
          name: "flow-page-ui-config",
          configurePreviewServer(v) {
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
    await withChromePage({ profilePrefix: "pma-flow-pages-" }, async (chrome) => {
      const address = "http://127.0.0.1:" + web.httpServer.address().port + "/?session=" + left;
      let olderResponses = 0,
        errors = [];
      chrome.onNetworkEvent((method, msg) => {
        if (
          method === "Network.responseReceived" &&
          msg.response.url.includes("/v1/conversations/" + left + "?before=")
        ) {
          if (msg.response.status === 200) olderResponses++;
          else errors.push(msg.response.status);
        }
      });
      async function scroll() {
        await chrome.evaluate(
          "(()=>{const p=document.querySelector('.product-chat-scroll');p.dispatchEvent(new WheelEvent('wheel',{deltaY:-8000,bubbles:true}));p.scrollTop=0;p.dispatchEvent(new Event('scroll'));})()",
        );
      }
      await chrome.navigate(address);
      await chrome.waitFor("document.body.innerText.includes('left:12')", 30000);
      await scroll();
      await chrome.waitFor(
        "Number(document.querySelector('.product-chat-scroll')?.dataset.historyPages)>1",
      );
      assert(olderResponses > 0);
      await work({
        db,
        projector,
        topic,
        windows: [left, right, empty],
        live,
        page,
        scroll,
        browserReads: () => olderResponses,
        errors,
      });
      assert.deepEqual(errors, []);
    });
  } finally {
    try {
      await web?.close();
      await app?.close();
    } finally {
      await Promise.all([meta.destroy(), db.end()]);
    }
  }
}
if (import.meta.main) {
  const url = process.env.PI_CLOUD_POSTGRES_INTEGRATION_URL;
  assert(url && new URL(url).hostname === "127.0.0.1");
  await withPagingFixture(url, randomUUID(), async () => {});
}
