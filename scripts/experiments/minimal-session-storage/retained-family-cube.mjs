import assert from "node:assert/strict";
import { setTimeout as delay } from "node:timers/promises";
import { randomUUID } from "node:crypto";
import { OfficialCubeSandboxRuntimeClient } from "../../../packages/sandbox-control/src/cubesandbox-runtime-client.ts";
import { executionTargetId } from "../../../packages/sandbox-control/src/sandbox-execution.ts";
import { ToolCompletionIssuer } from "../../../packages/sandbox-control/src/tool-completion-issuer.ts";

// Fixed Guest and maintained HTTPS completion, using one owned VM with no Volume.
export async function withFamilyGuest(config, run) {
  const client = new OfficialCubeSandboxRuntimeClient(config.client),
    issuer = new ToolCompletionIssuer(config.issuer);
  const fixture = randomUUID();
  let created;
  try {
    await client.checkHealth();
    created = await client.create({
      templateId: config.templateId,
      timeoutSeconds: 600,
      metadata: { "picloud.workload": "retained-family-study", "study-id": fixture },
      allowPublicTraffic: false,
    });
    const current = await client.read(created.sandboxId);
    assert(current?.nodeId && current.metadata["study-id"] === fixture);
    const vm = { ...created, nodeId: current.nodeId };
    const target = await client.executionTarget(vm.sandboxId, vm.nodeId, vm);
    async function command(script) {
      const r = await client.runCommand(vm, {
        command: script,
        cwd: "/",
        user: "root",
        timeoutMs: 15000,
        maximumOutputBytes: 4096,
      });
      assert.equal(r.exitCode, 0, "Owned VM command failed");
      return r.stdout.trim();
    }
    return await run({
      async directory(id) {
        assert(/^[a-f\d-]{36}$/.test(id));
        const root = "/tmp/pma-family-" + id;
        await command(
          "install -d -m 0700 -o 1000 -g 1000 " +
            root +
            " && printf 'GUEST-%s' \"$(cat /proc/sys/kernel/random/uuid)\" > " +
            root +
            "/result && chmod 0444 " +
            root +
            "/result",
        );
        return {
          async submit(r, cmd, ref, deliveryExpiry = ref.deadlineAt) {
            await client.submitTool(target, {
              source: { topic: r.topic, partition: 0, offset: String(r.offset) },
              operationId: cmd.request.operationId,
              executionScopeId: cmd.request.activationId,
              writerId: cmd.scope.writerId,
              timeoutMs: 105000,
              payload: {
                toolWorkerProtocolVersion: 7,
                type: "worker.operation",
                attachment: { activationId: cmd.request.activationId, toolRoot: root },
                request: cmd.request,
                delivery: await issuer.issue({
                  operationId: cmd.request.operationId,
                  expiresAt: deliveryExpiry,
                  context: {
                    scope: cmd.scope,
                    replyTopic: ref.topic,
                    toolCallId: cmd.toolCallId,
                    nativeToolCallId: cmd.request.toolCallId,
                    toolName: "bash",
                    partition: 0,
                  },
                }),
              },
            });
          },
          async probe(f) {
            const sentAt = Date.parse(f.occurredAt);
            await client.submitProbe(target, {
              probeId: f.factId,
              delivery: await issuer.issue({
                operationId: f.factId,
                expiresAt: sentAt + 10000,
                context: {
                  kind: "execution_probe",
                  sentAt,
                  replyTopic: f.execution.replyTopic,
                  bindingIds: [id],
                  execution: {
                    bindingId: id,
                    targetId: executionTargetId(target),
                    bootId: target.bootId,
                  },
                },
              }),
            });
          },
          cancel: (r, request) =>
            client.cancelTool(target, {
              source: { topic: r.topic, partition: r.partition, offset: String(r.offset) },
              executionScopeId: request.activationId,
              operationId: request.operationId,
              writerId: request.originalWriterId,
            }),
          marker: () =>
            command("if test -f " + root + "/executions; then cat " + root + "/executions; fi"),
          release: () => command("touch " + root + "/release"),
          result: () => command("cat " + root + "/result"),
        };
      },
    });
  } finally {
    try {
      // A lost create response is reconciled by the fixture's unique metadata;
      // never send a second create to guess the outcome.
      const owned = created
        ? [created]
        : (await client.list()).filter((s) => s.metadata["study-id"] === fixture);
      for (const vm of owned) {
        await client.destroy(vm.sandboxId);
        const end = Date.now() + 45000;
        while ((await client.observe(vm.sandboxId)).presence !== "absent") {
          assert(Date.now() < end, "Physical VM deletion unconfirmed");
          await delay(250);
        }
      }
    } finally {
      await client.close();
    }
  }
}
