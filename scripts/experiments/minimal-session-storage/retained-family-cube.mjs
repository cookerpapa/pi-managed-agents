import assert from "node:assert/strict";
import { setTimeout as delay } from "node:timers/promises";
import { createReadStream } from "node:fs";
import { stat, readFile } from "node:fs/promises";
import { createHash, randomUUID } from "node:crypto";
import { OfficialCubeSandboxRuntimeClient } from "../../../packages/sandbox-control/src/cubesandbox-runtime-client.ts";
import { executionTargetId } from "../../../packages/sandbox-control/src/sandbox-execution.ts";
import { FixedGuestToolExecutor } from "../../../packages/sandbox-control/src/fixed-guest-tool-executor.ts";
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
    // Ordinary results in the candidate carry only logical Tool correlation.
    // The execution-owned probe below still receives its physical evidence.
    const executor = new FixedGuestToolExecutor(client, {
      issue({ context, ...grant }) {
        const { execution, ...reply } = context;
        assert(execution);
        return issuer.issue({ ...grant, context: reply });
      },
    });
    function submitAt(attachment, r, cmd, ref, deliveryExpiry = ref.deadlineAt) {
      assert.equal(attachment.binding, cmd.request.activationId);
      return executor.submit(
        {
          bindingId: attachment.binding,
          descriptor: {
            target,
            attachment: { activationId: attachment.binding, toolRoot: attachment.cwd },
          },
          snapshot: { physicalActivationId: fixture },
        },
        { ...cmd, replyTopic: ref.topic, replyRecovery: { ...ref, deadlineAt: deliveryExpiry } },
        { topic: r.topic, partition: r.partition, offset: String(r.offset) },
      );
    }
    return await run({
      allocationId: fixture,
      async installToolWorker(path) {
        const dest = "/tmp/pma-worker-" + fixture + ".mjs";
        await client.writeGuestFileStream(
          vm,
          dest,
          createReadStream(path),
          (await stat(path)).size,
        );
        assert.equal(
          (await command("sha256sum " + dest)).split(" ")[0],
          createHash("sha256")
            .update(await readFile(path))
            .digest("hex"),
        );
        await command("install -m 0555 " + dest + " /opt/pi-cloud/bin/envd-tool-exec.mjs");
      },
      command,
      submitAt,
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
          submit: (r, cmd, ref, expiry) =>
            submitAt({ binding: id, cwd: root }, r, cmd, ref, expiry),
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
