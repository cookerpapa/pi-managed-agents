// Real HTTP admission, Pi runner, RF3 request/reply, projected PG and fixed Guest.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { KafkaWorkerProducer } from "../../../packages/event-log/src/kafka-worker-producer.ts";
import { KafkaToolReplyMailbox } from "../../../packages/event-log/src/tool-replies.ts";
import { createExecutionReference, NATIVE_HUMAN_INPUT_DOCUMENT } from "@pi-cloud/protocol";
import { fixture } from "../../../packages/sandbox-supervisor/test/fixtures/native-runner.ts";
import { command as testCommand } from "../../../packages/sandbox-supervisor/test/fixtures/durable-command.ts";
import { loadPriorRoot } from "./retained-family-read.mjs";
import { claimRun } from "./serving-family-claim.mjs";
import { encodeWorkspaceFact } from "./workspace-operations.mjs";
import { browserMessage, browserNote } from "./browser-signals.mjs";
import { until } from "./family-process.mjs";

export async function directoryAcceptance({
  guest,
  api,
  pool,
  database,
  tenant,
  dsn,
  topic,
  brokers,
  replyBrokers,
  replyBoots,
  browser,
  origin,
  setEndpoint,
  binding,
}) {
  const project = await api.createProject("Directory recovery");
  // Register the owned physical fixture as a dedicated environment in the isolated control DB.
  const ws = await database
    .updateTable("workspaces")
    .set({ workspace_kind: "development_environment" })
    .where("id", "=", project.workspaceId)
    .returning("sandbox_domain_id")
    .executeTakeFirstOrThrow();
  const instance = randomUUID(),
    machine = randomUUID(),
    url = "http://fixture.invalid";
  await database
    .insertInto("sandbox_control_instances")
    .values({
      instance_id: instance,
      sandbox_domain_id: ws.sandbox_domain_id,
      owner_base_url: url,
      state: "ready",
      lease_expires_at: new Date(Date.now() + 600000),
      last_heartbeat_at: new Date(),
    })
    .execute();
  await database
    .insertInto("development_environments")
    .values({
      id: machine,
      tenant_id: tenant.tenantId,
      owner_user_id: tenant.ownerUserId,
      project_id: project.projectId,
      workspace_id: project.workspaceId,
      sandbox_domain_id: ws.sandbox_domain_id,
      environment_version_id: project.environment.environmentVersionId,
      owner_instance_id: instance,
      owner_base_url: url,
      runtime_id: guest.allocationId,
      runtime_name: guest.allocationId,
      state: "ready",
      desired_state: "running",
      idempotency_key: machine,
      request_sha256: "a".repeat(64),
      profile_key: "starter",
      cpu_count: 1,
      memory_mib: 2048,
      system_disk_gib: 8,
    })
    .execute();
  const base = "/home/user/directory-" + randomUUID(),
    old = base + "/old",
    fresh = base + "/new",
    marker = base + "/executions";
  await guest.installToolWorker("/study/envd-tool-exec.mjs");
  await guest.command("install -d -o 1000 -g 1000 " + base + " " + old + " " + fresh);
  const session = (
    await api.createSession(
      project.projectId,
      project.workspaceId,
      "Directory recovery",
      "development_environment",
      "starter",
      old,
    )
  ).sessionId;
  const dispatch = Promise.withResolvers(),
    release = Promise.withResolvers();
  let sends = 0;
  setEndpoint({
    async directory(identity, id, path) {
      assert(
        identity.tenantId === tenant.tenantId &&
          identity.userId === tenant.ownerUserId &&
          id === machine,
      );
      const next = path === "/home/user" ? base : path === base ? fresh : null;
      assert(["/home/user", base, fresh].includes(path));
      if (next) await guest.command("test -d " + next);
      return {
        environmentId: id,
        path,
        entries: next
          ? [{ path: next, name: next.split("/").at(-1), kind: "directory", sizeBytes: 0 }]
          : [],
      };
    },
    async submit(r, c, ref) {
      const {
        rows: [row],
      } = await pool.query("select working_directory from runs where id=$1", [r.fact.scope.runId]);
      if (++sends === 1) {
        dispatch.resolve();
        await release.promise;
      }
      await guest.submitAt({ binding, cwd: row.working_directory }, r, c, ref);
    },
    async cancel() {
      assert.fail("The completed guest reply was cancelled");
    },
  });
  async function run(prompt, isFirst, alreadyAccepted) {
    const { runId } = alreadyAccepted ?? (await api.acceptTurn(session, prompt, randomUUID()));
    const claim = await claimRun(dsn, runId),
      scope = claim.scope;
    replyBoots.add(claim.bootId);
    const box = new KafkaToolReplyMailbox({
      brokers: replyBrokers,
      bootId: claim.bootId,
      replicas: 3,
    });
    const sender = new KafkaWorkerProducer({ brokers, topic, clientId: "directory-" + runId });
    const lifetime = new AbortController(),
      docs = new Set();
    let host;
    try {
      await box.start();
      await sender.start();
      const command = structuredClone(testCommand);
      Object.assign(command.payload, {
        tenantId: scope.tenantId,
        sessionId: session,
        turnId: scope.turnId,
        runId,
        projectId: project.projectId,
        workspaceId: project.workspaceId,
        workingDirectory: claim.request.workingDirectory,
        input: { kind: "prompt", text: prompt },
        piSession: { conversationId: 1, id: scope.piSessionId, writerId: scope.writerId },
        executionReference: createExecutionReference(scope.writerId, runId, scope.fencingToken),
      });
      assert.equal(command.payload.workingDirectory, isFirst ? old : fresh);
      const send = (f) =>
        sender.append(0, { factId: f.factId, sessionId: session, value: encodeWorkspaceFact(f) });
      const bridge = {
        command,
        bash: "pwd -P >> " + marker + " && pwd -P",
        async publish(commit, effects, receive) {
          const workspaceOperations = [];
          for (const { taskId, command: c } of effects) {
            const q = c.request,
              reply = {
                ...(await box.replyPosition()),
                deadlineAt: Date.now() + 60000,
                toolName: q.toolName,
                toolCallId: q.toolCallId,
              };
            const wait = box.wait(
              q.operationId,
              q.toolCallId,
              q.toolName,
              AbortSignal.any([lifetime.signal, AbortSignal.timeout(60000)]),
            );
            receive(
              q.operationId,
              wait.result.then((r) => ({
                type: "tool_execution_end",
                toolName: q.toolName,
                toolCallId: q.toolCallId,
                result: r.result,
                isError: r.isError,
              })),
            );
            workspaceOperations.push({
              action: "execute",
              operationId: q.operationId,
              taskId,
              reply,
              timeoutMs: q.timeoutMs,
              maximumOutputBytes: q.maximumOutputBytes,
            });
          }
          let changed = false;
          for (const w of commit.writes) {
            if (
              w.type === "document.create" &&
              [NATIVE_HUMAN_INPUT_DOCUMENT, "pi.inbox"].includes(w.record.kind)
            ) {
              docs.add(w.record.id);
              changed = true;
            }
            if (w.type === "document.change" && docs.has(w.id)) changed = true;
          }
          await send({
            kind: "durable_commit",
            scope,
            commit,
            tools: [],
            cancellations: [],
            display: [],
            workspaceOperations,
            ...(changed ? { inputQueueSessions: [session] } : {}),
            origins: commit.writes
              .filter((w) => ["entry", "task", "submission"].includes(w.type))
              .map((w) => ({
                recordId: w.value.id,
                sessionId: session,
                runId,
                turnId: scope.turnId,
              })),
            occurredAt: new Date().toISOString(),
            factId: randomUUID(),
          });
        },
      };
      host = await fixture("tool_hold", true, {}, bridge);
      const prior = await loadPriorRoot(pool, scope, host.storage);
      const work = host.run();
      // Observe rejection immediately while waiting for committed input or a dispatch.
      void work.catch(() => {});
      if (isFirst) {
        await Promise.race([
          dispatch.promise,
          work.then(() => assert.fail("Run ended before dispatch")),
        ]);
        try {
          for (const [i, mode] of ["steer", "follow_up"].entries()) {
            const key = (i ? "b" : "a").repeat(64);
            await host.runner.humanInput({
              action: "submit",
              tenantId: scope.tenantId,
              sessionId: session,
              runId,
              executionReference: command.payload.executionReference,
              input: {
                inputId: "input-" + key,
                idempotencyKey: key,
                fingerprint: key,
                text: "return " + mode,
                uploads: [],
                mode,
                acceptedAt: new Date().toISOString(),
              },
            });
          }
          await guest.command("rmdir " + old);
        } finally {
          release.resolve();
        }
      }
      let failure;
      try {
        await work;
      } catch (e) {
        failure = e;
      }
      if (isFirst) assert.equal(failure?.code, "workspace_directory_missing", failure?.message);
      else if (failure) throw failure;
      await claim.finish(
        (terminal) =>
          send({
            kind: "work_finished",
            scope,
            factId: randomUUID(),
            occurredAt: new Date().toISOString(),
            agentId: "root",
            baseSequence: claim.request.nextEventSeq - 1,
            nativeSequence: host.storage.committed,
            terminal,
          }),
        false,
        failure,
      );
      const row = await until(
        async () => {
          const {
            rows: [r],
          } = await pool.query(
            "select state,output_sealed_at,working_directory,history_head from runs where id=$1",
            [runId],
          );
          return r?.output_sealed_at && r;
        },
        "sealed directory Run",
        30000,
      );
      assert.equal(row.state, isFirst ? "failed" : "completed");
      assert(row.history_head);
      assert.equal(host.server.observations.length, isFirst ? 1 : 2);
      return {
        runId,
        modelRequests: host.server.observations.length,
        priorEntries: prior.priorPgEntries,
      };
    } finally {
      release.resolve();
      lifetime.abort();
      try {
        await host?.close();
      } finally {
        await box.shutdown();
        await sender.close();
        await claim.close();
      }
    }
  }
  const first = await run("Run pwd in the selected directory.", true);
  assert.equal(await guest.command("test ! -e " + marker + " && echo zero"), "zero");
  const original = await api.getConversation(session);
  assert(original.session.workingDirectoryMissing);
  assert.equal(original.turns[0].transcript.failure?.code, "workspace_directory_missing");
  const returned = await api.getReturnedInputs(session);
  assert.deepEqual(
    returned.items.map((i) => i.text),
    ["return steer", "return follow_up"],
  );
  let accepted;
  const nextPrompt = "Continue in the new directory.";
  if (browser) {
    await browserNote("/study", "ready", {
      origin,
      id: session,
      token: tenant.credential.token,
      directoryRecovery: true,
      base,
      fresh,
      second: nextPrompt,
    });
    await browserMessage("/study", "directorySubmitted");
    accepted = await until(
      async () => {
        const r = await pool.query(
          "select id from runs where tenant_id=$1 and session_id=$2 and mailbox_position=2",
          [tenant.tenantId, session],
        );
        return r.rowCount ? { runId: r.rows[0].id } : null;
      },
      "new browser Run",
      15000,
    );
  } else {
    await api.acknowledgeReturnedInputs(session, returned.requestId);
    await api.rebindConversationWorkspace(session, project.workspaceId, randomUUID(), fresh);
  }
  const rebound = await api.getConversation(session);
  assert.equal(rebound.session.workingDirectory, fresh);
  assert(!rebound.session.workingDirectoryMissing);
  const second = await run(nextPrompt, false, accepted);
  if (browser) {
    await browserNote("/study", "directoryFinished", { fresh });
    await browserMessage("/study", "done");
  }
  assert.equal(await guest.command("cat " + marker), fresh);
  assert.equal(sends, 2);
  return {
    firstModelRequests: first.modelRequests,
    resumedModelRequests: second.modelRequests,
    missingCommandStarts: 0,
    restoredCommandStarts: 1,
    returnedDrafts: returned.items.length,
    originalRunSealed: true,
    coldPriorEntries: second.priorEntries,
    reboundThroughSessionApi: true,
    ChromeReselection: !!browser,
  };
}
