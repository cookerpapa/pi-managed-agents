// Compare the ordinary candidate reader and the Flow reader at equal public content.
import assert from "node:assert/strict";
import { randomUUID, createHash } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import pg from "pg";
import { createDatabase, runMigrations } from "@pi-cloud/database";
import {
  createPrivateTenant,
  ControlPlaneStore,
} from "../../../packages/control-plane/src/index.ts";
import { PiCloudApi } from "../../../packages/web-ui/src/api.ts";
import { installPagingProjection } from "./flow-paging-fixture.mjs";
import { openPageApi } from "./flow-page-api.mjs";
import { installFlowHistory } from "./flow-history.mjs";
import { pageCpuSnapshot, pageCpuDelta } from "./page-backend-cpu.mjs";

const names = ["direct", "root", "branch"];
const directions = ["latest", "older", "newer"];
const rounded = (x) => +x.toFixed(3);
const topic = "flow-pages";

async function seed(db, tenant, physical, n, start, base, flow) {
  const args = [physical, tenant, n, start];
  await db.query(
    `insert into runs(id,tenant_id,session_id,mailbox_position,initial_input,model_ref,idempotency_key,state,history_head,output_display_seq,created_at${flow ? ",prev_run_id" : ""})
 select fixture_run($1::uuid,i),$2::uuid,$1::uuid,i,'prompt:'||(i+$4::int),'fixture',fixture_run($1::uuid,i),'completed','{"floor":1,"nextId":2}',i+$4::int,'2026-01-01'::timestamptz
 ${flow ? ",case when i=1 then $5::uuid else fixture_run($1::uuid,i-1) end" : ""} from generate_series(1,$3::int)i`,
    flow ? [...args, base] : args,
  );
  await db.query(
    `insert into messages(tenant_id,pi_session_id,entry_id,conversation_id,run_id,role,content,commit_seq,ordinal,created_at)
 select $2::uuid,$1::uuid,i+$4::int,1,fixture_run($1::uuid,i),'assistant',json_build_object('id',i+$4::int,'kind','pi.assistant',
 'model',json_build_array(json_build_object('role','assistant','content',json_build_array(json_build_object('type','text','text','answer:'||(i+$4::int)||' '||repeat('abcd',256))),'stopReason','stop','timestamp',1)))::text,i+$4::int,0,'2026-01-01'::timestamptz
 from generate_series(1,$3::int)i`,
    args,
  );
}
function visibleContent(page) {
  return page.turns.map((t) => [
    t.mailboxPosition,
    t.prompt,
    t.transcript?.items.map((i) => [i.kind, i.text]),
  ]);
}
export async function withMatchedPageFixture(url, tenant, work, referenceRoot) {
  const meta = createDatabase({ connectionString: url, maxConnections: 3 });
  const pools = [],
    apps = [],
    arms = {};
  try {
    await runMigrations(meta, "up");
    let first = true;
    const auth = await createPrivateTenant(meta, {
      slug: "page-layouts",
      ownerDisplayName: "Fixture",
      idGenerator: () => {
        if (first) {
          first = false;
          return tenant;
        }
        return randomUUID();
      },
      initialModel: { provider: "openai-codex", modelId: "gpt-6-luna" },
    });
    const store = new ControlPlaneStore({ database: meta, ...auth });
    const project = await store.createProject({
      name: "Matched histories",
      source: { kind: "empty" },
    });
    const windows = [];
    for (let i = 0; i < 3; i++)
      windows.push(
        (await store.createSession(project.projectId, project.workspaceId, "History", "elastic"))
          .sessionId,
      );
    const [root, middle, branch] = windows;
    for (const flow of [false, true]) {
      const schema = flow ? "flow_study" : "flat_study";
      const db = new pg.Pool({
        connectionString: url,
        options: "-c search_path=" + schema,
        application_name: "flow-projector",
        max: 12,
      });
      pools.push(db);
      await db.query("create schema " + schema);
      if (!flow)
        await db.query(
          "create schema flow_web; create view flow_web.sessions as select * from public.sessions",
        );
      const projector = await installPagingProjection(db);
      // The original reader resolves turn_id. Keep its serving lookup indexes
      // in both arms; avoid comparing an unindexed control to the primary key.
      await db.query(
        "create unique index runs_turn_identity_unique on runs(turn_id);create unique index runs_tenant_session_turn_unique on runs(tenant_id,session_id,turn_id)",
      );
      if (flow) {
        await installFlowHistory(db, true, false, "base");
        await db.query("insert into sessions(id,tenant_id,flow_id) values($1,$2,$1)", [
          root,
          tenant,
        ]);
      }
      await seed(db, tenant, root, 100000, 0, null, flow);
      async function segment(parent, physical, cut, length, start) {
        const {
          rows: [{ base }],
        } = await db.query("select fixture_run($1::uuid,$2::int) base", [parent, cut]);
        if (flow)
          await db.query("select flow_fork($1,$2,$3,$4,$4)", [tenant, parent, base, physical]);
        await seed(db, tenant, physical, length, start, base, flow);
      }
      await segment(root, middle, 99988, 8, 99988);
      await segment(middle, branch, 6, 6, 99994);
      const check = await db.query(`select count(*)::int n,
        to_regclass(current_schema()||'.flow_session_tips') is not null as overlay,
        exists(select from information_schema.columns where table_schema=current_schema() and table_name='runs' and column_name='prev_run_id') as predecessor from runs`);
      assert.deepEqual(check.rows[0], { n: 100014, overlay: flow, predecessor: flow });
      if (flow) {
        for (const [id, segments] of [
          [root, 1],
          [branch, 3],
        ]) {
          const r = await db.query(
            "select count(*)::int n,sum(through_position)::int total from flow_read_ranges($1,$2)",
            [tenant, id],
          );
          assert.deepEqual(r.rows[0], { n: segments, total: 100000 });
        }
      }
      await db.query("vacuum (analyze) runs");
      await db.query("vacuum (analyze) messages");
      if (flow) await db.query("analyze sessions");
      const dsn = new URL(url);
      dsn.searchParams.set("options", "-c search_path=flow_web," + schema + ",public");
      dsn.searchParams.set("application_name", "flow-page");
      const app = await openPageApi(url, dsn.href, flow);
      apps.push(app);
      const client = new PiCloudApi(
        (path, opts) => fetch(new URL(path, app.origin), opts),
        auth.credential.token,
      );
      let old;
      if (referenceRoot && flow) {
        const reference = await openPageApi(url, dsn.href, true, referenceRoot);
        apps.push(reference);
        old = {
          app: reference,
          client: new PiCloudApi(
            (path, opts) => fetch(new URL(path, reference.origin), opts),
            auth.credential.token,
          ),
        };
      }
      for (const name of flow ? ["root", "branch"] : ["direct"]) {
        const id = name === "branch" ? branch : root,
          page = (q, window = id) => client.getConversation(window, q);
        const latest = await page({}),
          older = await page({ before: latest.history.before });
        const queries = [{}, { before: latest.history.before }, { after: older.history.after }];
        arms[name] = {
          db,
          projector,
          topic,
          page,
          queries,
          id,
          capture: (q, window = id) => app.capture(auth.credential.token, window, q),
          ...(old
            ? {
                reference: {
                  page: (q, window = id) => old.client.getConversation(window, q),
                  capture: (q, window = id) => old.app.capture(auth.credential.token, window, q),
                },
              }
            : {}),
          profile: () => app.profile(auth.credential.token, id, latest.history.before, name),
        };
      }
    }
    for (let d = 0; d < 3; d++) {
      let expected;
      for (const name of names) {
        const arm = arms[name],
          page = await arm.page(arm.queries[d]),
          content = visibleContent(page);
        assert.equal(content.length, 32);
        for (const [position, prompt, items] of content) {
          assert.equal(prompt, "prompt:" + position);
          assert.equal(items.length, 1);
          assert(JSON.stringify(items).includes("answer:" + position + " "), "Missing answer text");
        }
        if (expected) assert.deepEqual(content, expected);
        else expected = content;
      }
      console.log(
        JSON.stringify({
          mode: "matched-content",
          direction: directions[d],
          turns: expected.length,
          bytes: Buffer.byteLength(JSON.stringify(expected)),
          sha256: createHash("sha256").update(JSON.stringify(expected)).digest("hex"),
        }),
      );
    }
    await work({
      db: arms.root.db,
      topic,
      arms,
      createWindow: async (title) =>
        (await store.createSession(project.projectId, project.workspaceId, title, "elastic"))
          .sessionId,
    });
  } finally {
    for (const app of apps) await app.close();
    for (const pool of pools) await pool.end();
    await meta.destroy();
  }
}

export async function measurePageCpu(arms) {
  for (const name of names) await arms[name].profile();
  const n = 96;
  for (const name of names)
    for (let i = 0; i < 24; i++) await arms[name].page(arms[name].queries[i % 3]);
  // Rotate which layout runs first. Each block uses full authenticated HTTP
  // requests; read and authentication CPU are sampled from PG backend processes.
  for (let pass = 0; pass < 3; pass++)
    for (let j = 0; j < 3; j++) {
      const name = names[(pass + j) % 3],
        arm = arms[name];
      for (let k = 0; k < 3; k++) {
        const d = (k + pass) % 3,
          query = arm.queries[d];
        await Promise.all(Array.from({ length: 8 }, () => arm.page(query)));
        const before = await pageCpuSnapshot(arm.db),
          start = performance.now();
        await Promise.all(
          Array.from({ length: 8 }, async () => {
            for (let i = 0; i < n / 8; i++) await arm.page(query);
          }),
        );
        const wallMs = performance.now() - start,
          delta = pageCpuDelta(before, await pageCpuSnapshot(arm.db));
        console.log(
          JSON.stringify({
            mode: "page-cpu",
            pass,
            name,
            direction: directions[d],
            pages: n,
            historyMs: rounded(delta.history / n),
            authMs: rounded(delta.control / n),
            containerMs: rounded(delta.container / n),
            wallMs: rounded(wallMs),
          }),
        );
      }
    }
}

export function pageCostTrials(suite) {
  if (suite === "selector") {
    const order = [
      "reference",
      "candidate",
      "candidate",
      "reference",
      "candidate",
      "reference",
      "reference",
      "candidate",
    ];
    return [["candidate", "writer"], ...order.map((arm) => [arm, 32]), ["candidate", "writer"]];
  }
  if (suite === "fixed32") {
    // Alternate a writer-only control and a fixed 32 pages/s for each layout,
    // then reverse both layout order and the order of each paired comparison.
    // The root and branch controls have the same physical Flow writer.
    return [
      ...names.flatMap((n) => [
        [n, "writer"],
        [n, 32],
      ]),
      ...names.toReversed().flatMap((n) => [
        [n, 32],
        [n, "writer"],
      ]),
      ...[names[1], names[2], names[0]].flatMap((n) => [
        [n, "writer"],
        [n, 32],
      ]),
    ];
  }
  const conditions = [
    ["direct", "writer"],
    ["direct", 8],
    ["root", 8],
    ["branch", 8],
    ["root", "writer"],
    ["branch", "saturated"],
    ["root", "saturated"],
    ["direct", "saturated"],
  ];
  return [
    conditions,
    conditions.toReversed(),
    [...conditions.slice(3), ...conditions.slice(0, 3)],
  ].flat();
}
export function loadMatchedPages(arm, rate) {
  const active = new Set(),
    samples = [];
  let offered = 0,
    skipped = 0,
    ended = false,
    error;
  const start = performance.now();
  const one = async (i) => {
    const t = performance.now();
    const p = await arm.page(arm.queries[i % 3]);
    assert.equal(p.turns.length, 32);
    samples.push({ start: t, end: performance.now() });
  };
  const tasks =
    rate === "saturated"
      ? Array.from({ length: 8 }, async () => {
          while (!ended && !error) {
            try {
              await one(offered++);
            } catch (e) {
              error = e;
            }
          }
        })
      : [
          (async () => {
            while (!ended && !error) {
              const due = start + (offered * 1000) / rate;
              if (performance.now() < due) {
                await delay(Math.max(1, due - performance.now()));
                continue;
              }
              const i = offered++;
              if (active.size >= 8) {
                skipped++;
                continue;
              }
              const p = one(i)
                .catch((e) => {
                  error = e;
                })
                .finally(() => active.delete(p));
              active.add(p);
            }
          })(),
        ];
  return {
    async stop(begin, end) {
      ended = true;
      await Promise.all(tasks);
      await Promise.all([...active]);
      if (error) throw error;
      const times = samples
        .filter((s) => s.start >= begin && s.end <= end)
        .map((s) => s.end - s.start)
        .sort((a, b) => a - b);
      assert(times.length > 0);
      return {
        completed: times.length,
        rate: rounded((times.length * 1000) / (end - begin)),
        p95Ms: rounded(times[Math.ceil(times.length * 0.95) - 1]),
        skipped,
      };
    },
  };
}

if (import.meta.main) {
  const url = process.env.PI_CLOUD_POSTGRES_INTEGRATION_URL;
  assert(url && new URL(url).hostname === "127.0.0.1");
  await withMatchedPageFixture(url, randomUUID(), async ({ arms }) => {
    for (const n of names) await arms[n].profile();
  });
}
