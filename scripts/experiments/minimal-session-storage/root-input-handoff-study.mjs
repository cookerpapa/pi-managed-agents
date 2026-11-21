import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import { NATIVE_HUMAN_INPUT_DOCUMENT } from "@pi-cloud/protocol";
import Fastify from "fastify";
import { SessionLiveService } from "../../../packages/control-plane/src/session-live-service.ts";
import { SessionExpiryReconciler } from "../../../packages/control-plane/src/session-expiry-reconciler.ts";
import { HttpInputConfirmations } from "../../../packages/control-plane/src/input-confirmation-client.ts";
import { InputQueueView } from "../../../packages/control-plane/src/input-queue-view.ts";
import { readInputQueue } from "../../../packages/control-plane/src/input-queue-snapshot.ts";
import { browserNote, browserMessage } from "./browser-signals.mjs";
import { until } from "./family-process.mjs";

export function rootInputStudy(db, pool, topic, mode) {
  const identities = new WeakMap(),
    byWorker = new Map(),
    rpc = new Map(),
    gate = Promise.withResolvers();
  const failed = Promise.withResolvers();
  void failed.promise.catch(() => {});
  let actor,
    backendReturned = false,
    blocked = false,
    confirmed = 0,
    restored,
    service,
    queueView;
  function attach(child) {
    actor = child;
    child.on("message", (m) => {
      if (m.workerId) {
        byWorker.set(m.workerId, child);
        identities.set(child, { writer: m.writerId, boot: m.bootId });
      }
      if (m.restoredInput) restored = m;
      if (!m.rpc) return;
      const waiter = rpc.get(m.rpc);
      if (!waiter) return;
      rpc.delete(m.rpc);
      if (m.error) waiter.reject(Error(m.error));
      else {
        backendReturned = true;
        waiter.resolve(m.value);
      }
    });
    child.once("exit", () => {
      for (const waiter of rpc.values()) waiter.reject(Error("Input Worker exited"));
      rpc.clear();
    });
  }
  return {
    attach,
    failed: failed.promise,
    async ports(projector) {
      queueView = new InputQueueView({
        read: (tenant, id) => readInputQueue(db, tenant, id, {}, true),
        watched: (tenant, id) => projector.eventHub.hasSubscribers(tenant, id),
        publish: (tenant, value) => projector.eventHub.publishInputQueue(tenant, value),
        unavailable: (tenant, id) => projector.eventHub.resyncSession(tenant, id),
      });
      const token = randomUUID(),
        server = Fastify();
      new SessionLiveService({
        database: db,
        stream: projector.eventStore,
        token,
        inputConfirmations: projector.eventHub,
        rootMessageHistory: true,
      }).install(server);
      const client = new HttpInputConfirmations({
        baseUrl: await server.listen({ host: "127.0.0.1", port: 0 }),
        token,
      });
      service = server;
      return {
        inputConfirmations: {
          async open(expected) {
            const w = await client.open(expected);
            return {
              ...w,
              result: w.result.then((ok) => {
                if (ok) confirmed++;
                return ok;
              }),
            };
          },
        },
        nativeInputBackendFactory: async (workerId) => ({
          async humanInput(command) {
            const target = byWorker.get(workerId);
            assert(target?.connected);
            const id = randomUUID(),
              waiter = Promise.withResolvers();
            rpc.set(id, waiter);
            target.send({ rpc: id, command });
            return waiter.promise;
          },
        }),
      };
    },
    nativeChanges: {
      async consume({ fact }, current) {
        if (current?.() === false) return;
        const ids =
          fact.kind === "durable_commit" ? (fact.inputQueueSessions ?? []) : [fact.scope.sessionId];
        for (const id of ids)
          queueView?.notify(fact.scope.tenantId, id, () => current?.() !== false);
      },
      close: async () => queueView?.close(),
    },
    async close() {
      gate.resolve();
      await service?.close();
    },
    guard(composed) {
      const project = composed.nativeStore.projection.project;
      composed.nativeStore.projection.project = async (f, ...args) => {
        if (
          !blocked &&
          f.commit.writes.some(
            (w) => w.type === "document.create" && w.record.kind === NATIVE_HUMAN_INPUT_DOCUMENT,
          )
        ) {
          blocked = true;
          await Promise.race([
            gate.promise,
            new Promise((_, reject) => {
              const t = setTimeout(() => reject(Error("Input PG test barrier timed out")), 20000);
              t.unref();
            }),
          ]);
        }
        try {
          return await project(f, ...args);
        } catch (error) {
          failed.reject(error);
          throw error;
        }
      };
      return {
        tracks: (scope) => scope.fencingToken === 1,
        async advance(_tx, f, pos, current, seq) {
          assert(current() && composed.view.sessions.get(f.scope.runId)?.seq === seq);
          assert.equal(pos.topic, topic);
          // The enclosing seal transaction saves the family, requeues the Run
          // and writes the same partition cursor as the candidate native writer.
        },
      };
    },
    async exercise({ first, second, id, runId, api, endpoint, work, dispatchCount }) {
      const queue = () => api.getInputQueue(id),
        active = actor;
      await until(() => blocked && backendReturned, "Worker published busy input before PG");
      const {
        rows: [before],
      } = await pool.query("select root_input_receipts is null as empty from runs where id=$1", [
        runId,
      ]);
      assert(before.empty && confirmed === 0);
      assert.equal((await queue()).items.length, 0);
      await browserNote("/study", "held");
      await browserMessage("/study", "unconfirmed");
      gate.resolve();
      const { receipt, key, thinkingLevel } = await browserMessage("/study", "accepted");
      assert(
        receipt.kind === "input" &&
          receipt.runId === runId &&
          receipt.state === "accepted" &&
          receipt.delivery === mode &&
          confirmed === 1,
      );
      const exited = once(active, "exit");
      active.kill("SIGKILL");
      assert.equal((await exited)[1], "SIGKILL");
      const expected = receipt.inputId;
      async function checkQueue() {
        const current = await queue();
        assert.deepEqual(
          current.items.map((i) => [i.inputId, i.text, i.status]),
          [[expected, second, mode === "steer" ? "steering" : "queued"]],
        );
      }
      await checkQueue();
      const reaper = new SessionExpiryReconciler(db);
      await until(
        async () => (await reaper.sweep()).expiredAssignments === 1,
        "expiry of killed input Worker",
      );
      await until(
        async () => (await api.getRun(runId)).state === "queued",
        "ordered requeue after active input",
      );
      await checkQueue();
      const gap = await api.getConversation(id);
      assert.equal(gap.turns[0]?.prompt, first);
      assert(
        gap.turns[0].transcript.items.some((i) => i.kind === "tool" && i.status === "running"),
      );
      await browserNote("/study", "between");
      await browserMessage("/study", "queueWhileUnclaimed");
      const retried = await api.acceptTurn(id, second, key, thinkingLevel, undefined, mode);
      assert.equal(retried.inputId, expected);
      assert(retried.replayed);
      const b = work(runId, true);
      await Promise.race([
        until(() => restored, "replacement reopened original waiter"),
        b.then(() => {
          throw Error("Worker completed before original tool release");
        }),
      ]);
      assert.equal(restored.restoredInput, expected);
      assert.equal(
        restored.originalTopic,
        "pi-cloud.tool-replies.v4." + identities.get(active).boot,
      );
      assert.equal(restored.oldWriter, identities.get(active).writer);
      assert.notEqual(identities.get(actor).writer, restored.oldWriter);
      await browserNote("/study", "reclaimed");
      await browserMessage("/study", "queueReload");
      await endpoint.release();
      const result = await b;
      assert.deepEqual(result.restoredUserPrompts, [first, second]);
      assert.equal(result.modelCalls, mode === "steer" ? 1 : 2);
      await until(
        async () => (await api.getRun(runId)).state === "completed",
        "recovered root settlement",
      );
      assert.equal((await queue()).items.length, 0);
      assert.equal(dispatchCount(), 1);
      assert.equal(await endpoint.marker(), "once");
      const {
        rows: [counts],
      } = await pool.query(
        `select count(*)::int runs,
        (select count(*)::int from messages where run_id=$1 and source_input_id=$3) placements,
        (select count(*)::int from messages where run_id=$1 and role='toolResult') results
        from runs where session_id=$2`,
        [runId, id, expected],
      );
      assert.deepEqual(counts, { runs: 1, placements: 1, results: 1 });
      const { turns } = await api.getConversation(id),
        items = turns[0].transcript.items;
      assert.equal(turns.length, 1);
      assert.equal(
        items.filter((i) => i.kind === "user" && i.inputId === expected && i.text === second)
          .length,
        1,
      );
      await browserNote("/study", "settled", {
        answer: "Recovered input: " + second + " / " + (await endpoint.result()),
      });
      const web = await browserMessage("/study", "done");
      return {
        ...web,
        mode,
        confirmedAfterProjection: true,
        workerKilledAfterReceipt: true,
        retrySameInput: true,
        queueVisibleBeforeReclaim: true,
        restoredQueuedId: true,
        originalMailboxRestored: true,
        modelCallsAfterHandoff: result.modelCalls,
        physicalExecutions: 1,
        ...counts,
        legacyNativeRowsAdded: 0,
      };
    },
  };
}
