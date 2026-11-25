// Compare the public snapshot at actual native boundaries, then interrupt the
// input between them. No model provider, live user data or Guest is involved.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { testPostgres } from "../../lib/test-postgres.mjs";
import { withPagingFixture } from "./flow-paging-fixture.mjs";
import { nativeBashTrace, nativeBashFact } from "./native-bash-trace.mjs";
import { FamilyPresentation } from "./family-presentation.mjs";
import { normalRootProjection } from "./normal-root-projection.mjs";
import { installWorkspaceRoutes } from "./workspace-route-spans.mjs";
import { WorkspaceOperationRouter } from "./workspace-operation-router.mjs";
import { encodeWorkspaceFact, decodeWorkspaceFact } from "./workspace-operations.mjs";
import { kafkaProducerLane } from "@pi-cloud/runtime-core/kafka-accepted-fact";
import { measureBashHistoryRead } from "./bash-intent-read-cost.mjs";
import { setBashLogReader, setBashLogIndexes } from "./bash-intent-log.mjs";

const tenant = randomUUID(),
  database = await testPostgres({});
try {
  await withPagingFixture(
    database.url,
    tenant,
    async (api) => {
      const { db } = api;
      await installWorkspaceRoutes(db);
      for (const stop of [false, true]) {
        const expected = [];
        for (const mode of [false, "folded"]) {
          await setBashLogIndexes(db, mode);
          await setBashLogReader(db, mode);
          await api.resetTail();
          const session = await api.createScrollWindow("Bash intent read check"),
            run = randomUUID(),
            writer = randomUUID();
          const scope = {
            tenantId: tenant,
            sessionId: session,
            piSessionId: session,
            runId: run,
            turnId: run,
            writerId: writer,
            fencingToken: 1,
          };
          const part = kafkaProducerLane(session, 32),
            topic = api.topic,
            replyTopic = "pi-cloud.tool-replies.v4." + randomUUID(),
            operation = randomUUID(),
            binding = randomUUID();
          await db.query("insert into sessions(id,tenant_id,flow_id) values($1,$2,$1)", [
            session,
            tenant,
          ]);
          await db.query(
            "insert into runs(id,tenant_id,session_id,lease_id,mailbox_position,idempotency_key,initial_input,model_ref,state) values($1,$2,$3,$4,1,$1,'check intent','fixture','running')",
            [run, tenant, session, writer],
          );
          const sent = [],
            cancelled = [];
          const p = normalRootProjection(
            db,
            topic,
            () => ({ bindingId: binding, allocationId: binding }),
            (id) => ({
              submit() {
                sent.push(id);
              },
              cancel() {
                cancelled.push(id);
              },
            }),
            undefined,
            { partitions: 32, bashLog: mode },
          );
          await p.view.reset(new Map([[part, 0n]]), 16, "recorded-history");
          const locator = new WorkspaceOperationRouter({ pool: db, view: p.view });
          const presenter = new FamilyPresentation();
          presenter.bind(1, scope);
          const frames = await nativeBashTrace(scope, undefined, { callId: "original" });
          const checkpoints = [];
          let task,
            original,
            lastThrough = 0;
          for (const [i, raw] of frames.entries()) {
            const commit = structuredClone(raw);
            const result = commit.writes.find(
              (w) => w.type === "entry" && w.value.kind === "pi.tool-result",
            );
            if (result && stop) {
              const t = commit.writes.find((w) => w.type === "task" && w.value.kind === "pi.tool");
              t.value.abortRequested = true;
              t.value.state.outcome.status = "aborted";
              result.value.model[0].isError = true;
            }
            const display = presenter.prepare(commit);
            const f = nativeBashFact(scope, commit, display.display, operation, replyTopic);
            if (result && stop)
              f.workspaceOperations = [{ action: "cancel", operationId: operation, taskId: task }];
            const record = {
              topic,
              partition: part,
              offset: BigInt(i),
              fact: decodeWorkspaceFact(Buffer.from(encodeWorkspaceFact(f))).fact,
            };
            const view = await p.nativeStore.projection.project(record.fact, record, () => true);
            assert(view.admitted);
            display.confirmed();
            await p.dispatch(
              record,
              () => true,
              async () => true,
            );
            if (view.events.length || view.covers.length)
              await api.publishFrame(tenant, view.events, view.covers);
            lastThrough = Math.max(
              lastThrough,
              ...view.events.map((e) => e.seq),
              ...view.covers.map((c) => c.through),
            );
            const proposal = commit.writes.some(
              (w) => w.type === "entry" && w.value.model?.[0]?.stopReason === "toolUse",
            );
            const intent = f.workspaceOperations.find((op) => op.action === "execute");
            if (intent) {
              task = intent.taskId;
              original = intent;
            }
            if (proposal || intent || result || raw === frames.at(-1)) {
              const page = await api.page(session, {});
              const tx = page.turns.at(-1).transcript,
                tools = tx.items.filter((x) => x.kind === "tool");
              checkpoints.push({
                coverage: tx.throughSequence,
                tools: tools.map((x) => [x.toolCallId, x.status, x.input, x.output?.content]),
              });
              assert.equal(tx.throughSequence, lastThrough);
              if (proposal) {
                assert.equal(tools.length, 0);
              }
              if (intent) {
                assert.equal(tools.length, 1);
                assert.equal(tools[0].status, "running");
                assert.deepEqual(tools[0].input, { command: "true", timeout: 10 });
                const where = await locator.original(scope, intent, record);
                assert.equal(where.bindingId, binding);
                assert.equal(
                  await locator.original(scope, { ...intent, operationId: randomUUID() }, record),
                  undefined,
                );
                if (mode) {
                  const physical = (
                    await db.query(
                      "select projection_tools,output_display_seq from runs where id=$1",
                      [run],
                    )
                  ).rows[0];
                  assert.deepEqual(physical.projection_tools, {});
                  const point = await db.query(
                    "select flow_web.flow_pending_tool($1,$2,$3,1,$4) own,flow_web.flow_pending_tool($1,$2,$3,2,$4) sibling",
                    [tenant, session, run, task],
                  );
                  assert.equal(point.rows[0].own.reply.toolCallId, "original");
                  assert.equal(point.rows[0].sibling, null);
                  assert(Number(physical.output_display_seq) < lastThrough);
                  const log = (
                    await db.query(
                      "select content::jsonb as value from pending_bash_intents where run_id=$1",
                      [run],
                    )
                  ).rows[0].value;
                  assert.deepEqual(log.args, { timeout: 10 });
                  assert(!JSON.stringify(log).includes("deadlineAt"));
                }
                const u = new URL(api.address);
                u.searchParams.set("session", session);
                await api.chrome.navigate(u.href);
                await api.chrome.waitFor(
                  "document.querySelector('[data-tool-name=bash]')?.innerText.includes('true')",
                );
              }
              if (result) {
                assert.equal(tools.length, 1);
                assert.equal(tools[0].status, stop ? "failed" : "completed");
                assert.equal(await locator.original(scope, original, record), undefined);
                if (stop) {
                  assert.deepEqual(cancelled, [binding]);
                  const delayed = await locator.original(
                    scope,
                    { action: "cancel", operationId: operation, taskId: task },
                    record,
                  );
                  assert.equal(delayed.bindingId, binding);
                }
                await api.chrome.waitFor(
                  "document.querySelector('[data-tool-name=bash]')?.innerText.includes('fixture')",
                );
                await api.chrome.navigate(new URL("/?session=" + session, api.address).href);
                await api.chrome.waitFor(
                  "document.querySelector('[data-tool-name=bash]')?.innerText.includes('fixture')",
                );
                assert.equal(
                  await api.chrome.evaluate(
                    "document.querySelectorAll('[data-tool-name=bash]').length",
                  ),
                  1,
                );
                if (stop) break;
              }
            }
          }
          if (!stop) {
            // Fixture terminal boundary for an immutable fork of this exact branch.
            await db.query(
              "update runs set state='completed',history_head='{}',output_sealed_at=now() where id=$1",
              [run],
            );
            const branch = await api.createScrollWindow("Inherited Bash result");
            await db.query("select flow_fork($1,$2,$3,$4,$5)", [
              tenant,
              session,
              run,
              branch,
              randomUUID(),
            ]);
            const fork = await api.page(branch, {});
            const tools = fork.turns.at(-1).transcript.items.filter((i) => i.kind === "tool");
            assert.equal(tools.length, 1);
            assert.equal(tools[0].status, "completed");
            assert.equal(tools[0].input.command, "true");
            assert(tools[0].output.content[0].text.includes("fixture"));
          }
          assert.deepEqual(sent, [binding]);
          if (!mode) expected.push(...checkpoints);
          else assert.deepEqual(checkpoints, expected);
          console.log(
            JSON.stringify({
              kind: "append-read-fault",
              layout: mode || "combined",
              stop,
              checkpoints: checkpoints.length,
              oneEffect: true,
              publicPage: true,
              originalRouteVerified: true,
              reopen: true,
            }),
          );
        }
      }
      await measureBashHistoryRead(api, tenant);
    },
    { liveStream: true, bashLogStudy: true },
  );
} finally {
  await database.close();
}
