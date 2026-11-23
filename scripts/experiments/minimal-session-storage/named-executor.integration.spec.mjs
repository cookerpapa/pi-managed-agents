import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { fork } from "node:child_process";
import { once } from "node:events";
import { setTimeout as pause } from "node:timers/promises";
import { it } from "vitest";
import Fastify from "fastify";
import pg from "pg";
import { MemoryStorage } from "@earendil-works/pi-durable";
import { BACKGROUND_CONTEXT as ctx } from "@earendil-works/chord/context";
import { createModels } from "@earendil-works/pi-ai/models";
import {
  fauxAssistantMessage,
  fauxProvider,
  fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import {
  createExecutionReference,
  infrastructureToolDefinitions,
  SUBAGENT_HOST_PATH,
} from "@pi-cloud/protocol";
import { namedAgentId } from "@pi-cloud/protocol/delegation-identity";
import { withToolInvocation } from "@pi-cloud/sandbox-supervisor";
import { DurableAgentSession } from "../../../packages/sandbox-supervisor/src/durable/agent-session.ts";
import { WorkerToolRuntime } from "../../../packages/supervisor-host/src/tools/worker-tool-runtime.ts";
import { ControlToolRegistry } from "../../../packages/control-plane/src/control-tool-registry.ts";
import { ControlToolDispatcher } from "../../../packages/control-plane/src/control-tool-dispatcher.ts";
import {
  NamedSubagentClient,
  namedExecutorPath,
} from "../../../packages/control-plane/src/named-subagent-client.ts";
import { bootMailbox } from "./direct-delegation-worker.mjs";
import { normalRootProjection } from "./normal-root-projection.mjs";
import { sendToActiveConversation } from "./delegated-native.mjs";
import { openDelegatedTransport } from "./delegated-test-transport.mjs";

const url = process.env.PI_CLOUD_POSTGRES_INTEGRATION_URL,
  brokers = process.env.PI_CLOUD_KAFKA_INTEGRATION_BROKERS;
const until = async (read) => {
  for (let i = 0; i < 700; i++) {
    const r = await read();
    if (r) return r;
    await pause(50);
  }
  throw Error("Expected executor transition did not arrive");
};
async function within(p, label, ms = 20000) {
  let timer;
  try {
    return await Promise.race([
      p,
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(Error(label + " timed out")), ms);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}
it.skipIf(!url || !brokers)(
  "hands named intents to a restartable process, keeps mailbox ownership and returns a real child answer",
  async () => {
    const tenantId = randomUUID(),
      sessionId = randomUUID(),
      runId = randomUUID(),
      writerId = randomUUID(),
      scope = {
        tenantId,
        piSessionId: sessionId,
        sessionId,
        turnId: runId,
        runId,
        writerId,
        fencingToken: 1,
      };
    const topic = "named-session-" + randomUUID(),
      schema = "named_" + randomUUID().replaceAll("-", ""),
      token = randomUUID() + randomUUID();
    const dsn = new URL(url);
    dsn.searchParams.set("options", "-c search_path=" + schema);
    const pool = new pg.Pool({ connectionString: dsn.href, max: 6 }),
      management = Fastify(),
      relay = Fastify();
    let runtime,
      dispatcher,
      transport,
      executor,
      executorUrl,
      exited,
      boot,
      otherBoot,
      stopping = false;
    const holdFirst = Promise.withResolvers(),
      holdPlacement = Promise.withResolvers(),
      gotPlacement = Promise.withResolvers(),
      gotDuplicate = Promise.withResolvers(),
      finishChild = Promise.withResolvers();
    const childID = namedAgentId(tenantId, sessionId, sessionId, "payment-tests"),
      decisionId = namedAgentId(tenantId, sessionId, sessionId, "format-review");
    const offers = [],
      placed = new Map(),
      positions = [];
    const args = { name: "payment-tests", task: "Inspect the payment tests", workspace: null };
    const defs = infrastructureToolDefinitions(true);
    const models = createModels(),
      model = fauxProvider({ provider: "named-executor-test", tokensPerSecond: 0 });
    models.setProvider(model.provider);
    const config = (lane) => ({
      runId,
      conversationId: lane,
      model: model.getModel(),
      agent: {},
      context: {},
      remoteTools: () => defs.map((d) => d.name),
      tools: [],
      assertCurrent: async () => {},
      openModel: async (r) => ({ model: model.getModel(), options: r.options, release() {} }),
      prepareRemote: async (v) => {
        const bindings = await workerTools.create({
          command: {
            payload: {
              ...scope,
              executionReference: createExecutionReference(writerId, runId, 1),
              piSession: { conversationId: lane },
              sessionKind: lane === 1 ? "conversation" : "subagent",
            },
          },
          environment: () => assert.fail(),
        });
        const tool = bindings.current().find((x) => x.tool.name === v.toolName).tool;
        return withToolInvocation({ id: "task:" + v.taskId, nativeToolCallId: v.callId }, () =>
          tool.prepareExecution(v.callId, v.args, v.context.abortSignal),
        );
      },
    });
    let workerTools;
    async function launch() {
      executor = fork(
        new URL("../../../packages/control-plane/src/subagent-executor-main.ts", import.meta.url),
        [],
        {
          execArgv: ["--import", "tsx"],
          stdio: ["ignore", "inherit", "inherit", "ipc"],
          env: {
            ...process.env,
            DATABASE_URL_FILE: "",
            PI_CLOUD_SUPERVISOR_MANAGEMENT_TOKEN_FILE: "",
            DATABASE_URL: dsn.href,
            PI_CLOUD_SUPERVISOR_MANAGEMENT_TOKEN: token,
            PI_CLOUD_KAFKA_BROKERS: brokers,
            PI_CLOUD_SESSION_LOG_TOPIC: topic,
            HOST: "127.0.0.1",
            PORT: "0",
          },
        },
      );
      exited = once(executor, "exit");
      const [msg] = await Promise.race([
        once(executor, "message"),
        exited.then(() => {
          throw Error("Executor exited before ready");
        }),
      ]);
      executorUrl = msg.ready;
    }
    async function kill() {
      if (!executor) return;
      executor.kill("SIGKILL");
      await exited;
      executor = undefined;
    }
    try {
      await pool.query("create schema " + schema);
      for (const file of ["schema.sql", "delegated-runs.sql"])
        await pool.query(await readFile(new URL(file, import.meta.url), "utf8"));
      await pool.query(`alter table runs add worker_id uuid, add fencing_token bigint,add output_first_topic text,add output_first_partition int,add output_first_offset bigint,add output_display_seq bigint default 0,add output_display_native_seq bigint default 0;
     create table workers(id uuid,supervisor_id uuid);create table supervisor_hosts(supervisor_id uuid,management_base_url text);`);
      management.post(SUBAGENT_HOST_PATH, async (req, res) => {
        assert.equal(req.headers.authorization, "Bearer " + token);
        const cmd = req.body;
        if (cmd.action === "input") {
          const result = await sendToActiveConversation(
            runtime.harness,
            { lane: cmd.conversationId },
            cmd.message,
            cmd.delivery,
            cmd.requestId,
            ctx,
          );
          return { accepted: true, result: { accepted: result === "accepted" } };
        }
        if (cmd.action === "stop_conversation") {
          await runtime.stopConversation(cmd.requestId);
          return { accepted: true, result: {} };
        }
        assert.equal(cmd.action, "start_conversation");
        gotPlacement.resolve();
        await holdPlacement.promise;
        if (stopping || !executor) return res.code(503).send();
        assert(
          [childID, decisionId, namedAgentId(tenantId, sessionId, sessionId, "cancel-me")].includes(
            cmd.view.id,
          ),
        );
        let work = placed.get(cmd.view.id);
        if (!work) {
          work = (async () => {
            const view = cmd.view;
            const lane = await runtime.createConversation({
              requestId: view.id,
              parentConversationId: view.parentConversationId,
              ownerTaskId: view.ownerTaskId,
              ...(view.contextAnchor ? { at: view.contextAnchor } : {}),
              view,
            });
            const child = await runtime.bind(config(lane));
            await child.submit({ type: "input", requestId: view.id, content: cmd.prompt }, ctx);
            return lane;
          })();
          placed.set(cmd.view.id, work);
        }
        return { accepted: true, result: await work };
      });
      const workerUrl = await management.listen({ host: "127.0.0.1", port: 0 });
      await pool.query("insert into sessions values($1,$2)", [sessionId, tenantId]);
      await pool.query("insert into workers values($1,$1)", [writerId]);
      await pool.query("insert into supervisor_hosts values($1,$2)", [writerId, workerUrl]);
      await pool.query(
        "insert into runs(id,tenant_id,session_id,initial_input,model_ref,idempotency_key,mailbox_position,state,lease_id,worker_id,fencing_token) values($1,$2,$3,'Delegate','fixture',$1,1,'running',$4,$4,1)",
        [runId, tenantId, sessionId, writerId],
      );
      boot = await bootMailbox();
      await launch();
      relay.post(namedExecutorPath, async (req, res) => {
        const o = req.body;
        offers.push(o);
        if (o.input.call === "first") await holdFirst.promise;
        if (stopping) return res.code(503).send();
        try {
          const r = await fetch(new URL(namedExecutorPath, executorUrl), {
            method: "POST",
            headers: { authorization: "Bearer " + token, "content-type": "application/json" },
            body: JSON.stringify(o),
          });
          await r.body?.cancel();
          if (o.input.call === "first" && r.ok) gotDuplicate.resolve();
          return res.code(r.status).send();
        } catch {
          return res.code(503).send();
        }
      });
      const relayUrl = await relay.listen({ host: "127.0.0.1", port: 0 });
      dispatcher = new ControlToolDispatcher({
        database: undefined,
        partitions: 1,
        ownsPartition: () => true,
        namedExecutor: new NamedSubagentClient(relayUrl, token),
        execute: () => assert.fail("Projector executed a control tool"),
        publish: () => assert.fail("Projector published a tool reply"),
      });
      const projection = normalRootProjection(
        pool,
        topic,
        () => assert.fail(),
        () => assert.fail(),
        undefined,
        { delegatedRuns: true, namedExecutor: true },
      );
      await projection.view.install();
      await projection.view.reset(new Map([[0, 0n]]), 16, "recorded-history");
      transport = await openDelegatedTransport(brokers.split(","), topic, async (r, current) => {
        if (r.fact.tools.length) positions.push(r.offset);
        const p = await projection.nativeStore.projection.project(r.fact, r, current);
        assert(p.admitted);
        await dispatcher.consume(r, current, async () => true);
      });
      workerTools = new WorkerToolRuntime({
        namedSubagents: true,
        catalog: new ControlToolRegistry(),
        maximumDepth: null,
        implementation: () => undefined,
        nativeConversations: { childAnchor: async () => null, conversationDepth: async () => 1 },
        wait: (id, call, name, signal) => boot.mailbox.wait(id, call, name, signal),
      });
      class Store extends MemoryStorage {
        async commit(writes) {
          const p = this.prepareCommit(writes),
            commit = { seq: p.seq, nextId: this.nextId, writes: p.writes },
            prepared = runtime?.preparePublication(commit);
          const tools = (prepared?.tools ?? []).map(({ taskId, command: c }) => ({
            scope,
            target: "control",
            taskId,
            conversationId: c.conversationId,
            toolName: c.toolName,
            operationId: c.operationId,
            replyTopic: boot.replyTopic,
            contextAnchor: c.contextAnchor,
          }));
          await transport.publish({
            kind: "durable_commit",
            scope,
            commit,
            tools,
            display: [],
            origins: [],
            cancellations: [],
            factId: randomUUID(),
            occurredAt: new Date().toISOString(),
          });
          const seq = p.apply();
          prepared?.confirmed();
          return seq;
        }
      }
      model.setResponses(
        Array.from({ length: 18 }, () => async (transcript) => {
          const msgs = transcript.messages;
          if (
            msgs.some(
              (m) =>
                m.role === "user" && JSON.stringify(m.content).includes("Wait for cancellation"),
            )
          )
            return fauxAssistantMessage(
              fauxToolCall(
                "contact_supervisor",
                { reason: "need_decision", message: "Continue?" },
                { id: "cancel-question" },
              ),
              { stopReason: "toolUse" },
            );
          const cancelTurn = msgs.findLastIndex(
            (m) => m.role === "user" && JSON.stringify(m.content).includes("Cancel round"),
          );
          if (cancelTurn >= 0) {
            const last = msgs
              .slice(cancelTurn + 1)
              .filter((m) => m.role === "toolResult")
              .at(-1);
            if (!last)
              return fauxAssistantMessage(
                fauxToolCall(
                  "subagent",
                  { name: "cancel-me", task: "Wait for cancellation", workspace: null },
                  { id: "cancel-child" },
                ),
                { stopReason: "toolUse" },
              );
            if (last.toolName === "subagent")
              return fauxAssistantMessage(
                fauxToolCall(
                  "subagent_control",
                  { action: "cancel", target: "cancel-me" },
                  { id: "cancel-by-name" },
                ),
                { stopReason: "toolUse" },
              );
            return fauxAssistantMessage("Cancellation delivered");
          }
          if (
            msgs.some(
              (m) => m.role === "user" && JSON.stringify(m.content).includes("Ask which separator"),
            )
          ) {
            if (msgs.at(-1).role === "toolResult") {
              assert(JSON.stringify(msgs.at(-1)).includes("use tabs"));
              return fauxAssistantMessage("Child chose tabs");
            }
            return fauxAssistantMessage(
              fauxToolCall(
                "contact_supervisor",
                { reason: "need_decision", message: "Which separator?" },
                { id: "question" },
              ),
              { stopReason: "toolUse" },
            );
          }
          const decisionTurn = msgs.findLastIndex(
            (m) => m.role === "user" && JSON.stringify(m.content).includes("Decision round"),
          );
          if (decisionTurn >= 0) {
            const reply = msgs
              .slice(decisionTurn + 1)
              .filter((m) => m.role === "toolResult")
              .at(-1);
            if (!reply)
              return fauxAssistantMessage(
                fauxToolCall(
                  "subagent",
                  { name: "format-review", task: "Ask which separator", workspace: null },
                  { id: "decision-child" },
                ),
                { stopReason: "toolUse" },
              );
            if (reply.toolName === "subagent") {
              const v = JSON.parse(reply.content[0].text);
              assert.equal(v.state, "blocked");
              assert.equal(v.executionId, decisionId);
              return fauxAssistantMessage(
                fauxToolCall(
                  "subagent_supervisor",
                  { action: "reply", replyTo: v.replyTo, message: "use tabs" },
                  { id: "parent-decision" },
                ),
                { stopReason: "toolUse" },
              );
            }
            assert(JSON.stringify(reply).includes("Child chose tabs"));
            return fauxAssistantMessage("Decision delivered");
          }
          if (
            msgs.some((m) => m.role === "user" && JSON.stringify(m.content).includes(args.task))
          ) {
            await finishChild.promise;
            return fauxAssistantMessage("Checked in the named child");
          }
          const results = msgs.filter((m) => m.role === "toolResult");
          if (!results.length)
            return fauxAssistantMessage(
              ["first", "second"].map((id) => fauxToolCall("subagent", args, { id })),
              { stopReason: "toolUse" },
            );

          return fauxAssistantMessage("Parent received both replies");
        }),
      );
      runtime = await DurableAgentSession.open(new Store(), {
        models,
        remoteTools: defs,
        settings: { compaction: { enabled: false } },
      });
      const root = await runtime.bind(config(1));
      const turn = await root.submit({ type: "input", requestId: runId, content: "Delegate" }, ctx);
      await within(gotPlacement.promise, "first placement");
      assert.deepEqual(
        (
          await pool.query("select id,source_task_id from runs where root_run_id=$1", [runId])
        ).rows.map((r) => [r.id, Number(r.source_task_id)]),
        [[childID, offers.find((o) => o.input.call === "second").task]],
      );
      // The first remote request is still held; an ordinary native entry advances the same partition.
      await root.commit(
        (tx) =>
          tx.appendEntry(1, {
            kind: "study.interleaved",
            data: { message: "independent projection marker" },
          }),
        ctx,
      );
      assert.equal(
        (
          await pool.query(
            "select count(*)::int n from messages where content like '%independent projection marker%'",
          )
        ).rows[0].n,
        1,
      );
      const checkpoint = (
        await pool.query(
          "select projection_first,projection_pin,projection_state from runs where id=$1",
          [runId],
        )
      ).rows[0];
      assert(
        Number(checkpoint.projection_first) > Number(positions.at(-1)),
        JSON.stringify({
          pin: checkpoint.projection_pin,
          first: checkpoint.projection_first,
          last: String(positions.at(-1)),
          state: projection.view.sessions.get(runId)?.liveDocs,
        }),
      );
      const head = BigInt(
        (
          await pool.query(
            "select next_offset from accepted_fact_projection_offsets where topic=$1",
            [topic],
          )
        ).rows[0].next_offset,
      );
      const cold = normalRootProjection(
        pool,
        topic,
        () => assert.fail(),
        () => assert.fail(),
        undefined,
        { delegatedRuns: true, namedExecutor: true },
      );
      const { starts } = await cold.nativeStore.replayPlan(undefined, topic, [
        { partition: 0, low: positions.at(-1) + 1n, high: head },
      ]);
      assert(typeof starts.get(0) === "bigint" && starts.get(0) > positions.at(-1));
      holdFirst.resolve();
      await within(gotDuplicate.promise, "duplicate admission");
      const count = () =>
        pool
          .query("select count(*)::int n from subagent_deliveries where child_id=$1", [childID])
          .then((r) => r.rows[0].n);
      assert.equal(await count(), 2);
      await until(
        async () =>
          (await pool.query("select sent from subagent_deliveries where call_id='first'")).rows[0]
            ?.sent,
      );
      await kill();
      holdPlacement.resolve();
      // Advance the dead process's visibility deadline rather than sleep 30 seconds.
      await pool.query("update runs set placement_due=now() where id=$1", [childID]);
      await launch();
      let nextTask = 1000;
      await until(
        async () =>
          (await pool.query("select native_lane from runs where id=$1", [childID])).rows[0]
            ?.native_lane,
      );
      const msg = await request("subagent_control", {
        action: "send",
        target: args.name,
        delivery: "steer",
        message: "Check both cases",
      });
      assert(JSON.stringify(msg).includes("accepted"));
      finishChild.resolve();
      assert.equal((await within(turn.wait(ctx), "parent result", 35000)).status, "done");
      assert.equal(placed.size, 1);
      const canonical = (await root.context(ctx)).messages.filter((m) => m.role === "toolResult");
      const toolResults = new Map(
        canonical.map((m) => {
          assert.match(m.content[0].text, /^\{/, JSON.stringify(m));
          return [m.toolCallId, JSON.parse(m.content[0].text)];
        }),
      );
      assert.equal(toolResults.get("second").output, "Checked in the named child");
      assert.equal(toolResults.get("first").executionId, childID);
      assert.equal(toolResults.get("first").state, "preparing");
      assert.equal(
        (await pool.query("select count(*)::int n from runs where root_run_id=$1", [runId])).rows[0]
          .n,
        1,
      );

      async function request(tool, args, scopeOverride = {}, mail = boot) {
        const id = randomUUID(),
          call = "extra-" + id,
          signal = AbortSignal.timeout(8000);
        const wait = mail.mailbox.wait(id, call, tool, signal);
        const o = {
          ...offers[0],
          ...scopeOverride,
          destination: mail.replyTopic,
          operation: id,
          tool,
          task: nextTask++,
          input: { seq: 99, call, args },
        };
        const r = await fetch(new URL(namedExecutorPath, executorUrl), {
          method: "POST",
          headers: { authorization: "Bearer " + token, "content-type": "application/json" },
          body: JSON.stringify(o),
        });
        assert.equal(r.status, 202);
        return await wait.result;
      }
      const found = await request("subagent_control", { action: "status", target: args.name });
      assert(JSON.stringify(found).includes(childID));
      const decisionTurn = await root.submit(
        { type: "input", requestId: randomUUID(), content: "Decision round" },
        ctx,
      );
      assert.equal(
        (await within(decisionTurn.wait(ctx), "supervisor round", 35000)).status,
        "done",
      );
      assert(
        JSON.stringify((await root.context(ctx)).messages.at(-1)).includes("Decision delivered"),
      );
      const cancelTurn = await root.submit(
        { type: "input", requestId: randomUUID(), content: "Cancel round" },
        ctx,
      );
      assert.equal((await within(cancelTurn.wait(ctx), "cancel round", 35000)).status, "done");
      await until(
        async () =>
          (await pool.query("select state from runs where agent_name='cancel-me'")).rows[0]
            ?.state === "cancelled",
      );
      // Lose the Kafka acknowledgement at the durable delivery boundary.
      // Observe the first real packet, kill the process while PG is blocked
      // recording its acknowledgement, and receive its identical redelivery.
      const unacked = {
        ...offers.find((o) => o.input.call === "second"),
        operation: randomUUID(),
        task: nextTask++,
      };
      const lock = await pool.connect();
      const lockKey = 731;
      try {
        await lock.query("select pg_advisory_lock($1)", [lockKey]);
        await pool.query(`create function pause_named_ack() returns trigger language plpgsql as $body$ begin perform pg_advisory_xact_lock(731); raise exception 'injected delivery acknowledgement rollback'; end $body$;
       create trigger pause_named_ack before update of sent on subagent_deliveries for each row when(new.sent and new.operation_id='${unacked.operation}'::uuid) execute function pause_named_ack()`);
        const recv = () =>
          boot.mailbox.wait(
            unacked.operation,
            unacked.input.call,
            "subagent",
            AbortSignal.timeout(12000),
          ).result;
        const first = recv();
        const accepted = await fetch(new URL(namedExecutorPath, executorUrl), {
          method: "POST",
          headers: { authorization: "Bearer " + token, "content-type": "application/json" },
          body: JSON.stringify(unacked),
        });
        assert.equal(accepted.status, 202);
        const payload = await first;
        await kill();
        assert.equal(
          (
            await pool.query("select sent from subagent_deliveries where operation_id=$1", [
              unacked.operation,
            ])
          ).rows[0].sent,
          false,
        );
        await lock.query("select pg_advisory_unlock($1)", [lockKey]);
        await pool.query("drop trigger pause_named_ack on subagent_deliveries");
        await pool.query("update subagent_deliveries set due_at=now() where operation_id=$1", [
          unacked.operation,
        ]);
        const repeat = recv();
        await launch();
        assert.deepEqual(await repeat, payload);
        assert.equal(placed.size, 3);
      } finally {
        await lock.query("select pg_advisory_unlock_all()");
        lock.release();
      }
      const lost = {
        ...offers.find((o) => o.input.call === "second"),
        task: nextTask++,
        operation: randomUUID(),
      };
      let receipt = await fetch(new URL(namedExecutorPath, executorUrl), {
        method: "POST",
        headers: { authorization: "Bearer " + token, "content-type": "application/json" },
        body: JSON.stringify(lost),
      });
      assert.equal(receipt.status, 202);
      await until(
        async () =>
          (
            await pool.query("select sent from subagent_deliveries where operation_id=$1", [
              lost.operation,
            ])
          ).rows[0]?.sent,
      );
      // No wait was registered for that returned operation. Retry by name on a new Run.
      const nextRoot = randomUUID();
      await pool.query("update runs set state='completed' where id=$1", [runId]);
      await pool.query(
        "insert into runs(id,tenant_id,session_id,initial_input,model_ref,idempotency_key,mailbox_position,state,lease_id,worker_id,fencing_token) values($1,$2,$3,'retry','fixture',$1,2,'running',$4,$4,1)",
        [nextRoot, tenantId, sessionId, writerId],
      );
      otherBoot = await bootMailbox();
      const duplicate = await request(
        "subagent",
        { ...args, context: "fresh" },
        { root: nextRoot },
        otherBoot,
      );
      assert(JSON.stringify(duplicate).includes("Checked in the named child"));
      const conflict = await request(
        "subagent",
        { ...args, task: "Change the project" },
        { root: nextRoot },
      );
      assert(conflict.isError && JSON.stringify(conflict).includes(childID));
      const missing = await request(
        "subagent_control",
        { action: "status", target: "missing-child" },
        { root: nextRoot },
      );
      assert(missing.isError && JSON.stringify(missing).includes("暂时查不到"));
      // Forks start a new name namespace. The same name under a different
      // parent also produces an independent address.
      assert.notEqual(namedAgentId(tenantId, sessionId, decisionId, args.name), childID);
      const forkSession = randomUUID(),
        forkRoot = randomUUID();
      await pool.query("insert into sessions values($1,$2)", [forkSession, tenantId]);
      await pool.query(
        "insert into runs(id,tenant_id,session_id,initial_input,model_ref,idempotency_key,mailbox_position,state,lease_id,worker_id,fencing_token) values($1,$2,$3,'fork','fixture',$1,1,'running',$4,$4,1)",
        [forkRoot, tenantId, forkSession, writerId],
      );
      assert(
        (
          await request(
            "subagent_control",
            { action: "status", target: args.name },
            { session: forkSession, root: forkRoot },
          )
        ).isError,
      );
      const forkName = {
        ...lost,
        operation: randomUUID(),
        session: forkSession,
        root: forkRoot,
        task: nextTask++,
      };
      await pool.query("update runs set worker_id=null where id=$1", [forkRoot]);
      const forkAccepted = await fetch(new URL(namedExecutorPath, executorUrl), {
        method: "POST",
        headers: { authorization: "Bearer " + token, "content-type": "application/json" },
        body: JSON.stringify(forkName),
      });
      assert.equal(forkAccepted.status, 202);
      await forkAccepted.body?.cancel();
      const forkChild = (await pool.query("select id from runs where root_run_id=$1", [forkRoot]))
        .rows[0].id;
      assert.equal(forkChild, namedAgentId(tenantId, forkSession, forkSession, args.name));
      assert.notEqual(forkChild, childID);
      const races = ["Review A", "Review B"].map((task) => ({
        ...forkName,
        operation: randomUUID(),
        task: nextTask++,
        input: { seq: 101, call: randomUUID(), args: { ...args, name: "design-review", task } },
      }));
      const receiving = new AbortController();
      try {
        const receipts = races.map(
          (o) => boot.mailbox.wait(o.operation, o.input.call, o.tool, receiving.signal).result,
        );
        const firstReply = Promise.race(receipts);
        const statuses = await Promise.all(
          races.map(async (o) => {
            const r = await fetch(new URL(namedExecutorPath, executorUrl), {
              method: "POST",
              headers: { authorization: "Bearer " + token, "content-type": "application/json" },
              body: JSON.stringify(o),
            });
            await r.body?.cancel();
            return r.status;
          }),
        );
        assert.deepEqual(statuses, [202, 202]);
        const conflict = await within(firstReply, "concurrent name conflict");
        const uuid = namedAgentId(tenantId, forkSession, forkSession, "design-review");
        assert(conflict.isError && JSON.stringify(conflict).includes(uuid));
        assert.deepEqual(
          (
            await pool.query(
              "select id from runs where root_run_id=$1 and agent_name='design-review'",
              [forkRoot],
            )
          ).rows,
          [{ id: uuid }],
        );
      } finally {
        receiving.abort();
      }
    } catch (e) {
      const children = (
        await pool.query(
          "select agent_name,state,native_lane,source_task_id,placement_due from runs",
        )
      ).rows;
      const deliveries = (
        await pool.query(
          "select tool,kind,caller_task,call_id,sent,due_at from subagent_deliveries order by created_at",
        )
      ).rows;
      console.error("named failure boundary", JSON.stringify({ children, deliveries }));
      throw e;
    } finally {
      stopping = true;
      holdFirst.resolve();
      holdPlacement.resolve();
      finishChild.resolve();
      await kill();
      try {
        await runtime?.harness.abort?.(ctx);
        await runtime?.close();
      } finally {
        await dispatcher?.close();
        await transport?.close();
        await boot?.close();
        await otherBoot?.close();
        await management.close();
        await relay.close();
        try {
          await pool.query("drop schema if exists " + schema + " cascade");
        } finally {
          await pool.end();
        }
      }
    }
  },
  120000,
);
