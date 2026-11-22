// Operator fixture: one private Workspace, two sequential stock Cube VMs. The
// callbacks receive verified observations, never a Cube credential or address.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { createDatabase } from "@pi-cloud/database";
import { createPrivateTenant } from "../../../packages/control-plane/src/tenant-administration.ts";
import { PiCloudApi } from "../../../packages/web-ui/src/api.ts";
import { OfficialCubeSandboxRuntimeClient } from "../../../packages/sandbox-control/src/cubesandbox-runtime-client.ts";
import {
  HttpWorkspaceVolumeGateway,
  workspaceVolumeId,
} from "../../../packages/sandbox-control/src/workspace-volume-gateway.ts";
import { purgeOwnedAcceptanceTenant } from "../../lib/owned-acceptance-cleanup.mjs";
import {
  runtimeDirectory,
  runtimeEnvironment,
  serviceAddress,
} from "../../lib/production-kubernetes.mjs";
import { studyCubeConfig } from "./study-cube-config.mjs";
import { until } from "./family-process.mjs";

export async function withForkWorkspace(study) {
  assert.equal(process.env.PI_CLOUD_RUNTIME_LOAD_CHECK, "1");
  const secret = async (name) =>
    (await readFile(runtimeDirectory + "/secrets/" + name, "utf8")).trim();
  const config = await studyCubeConfig();
  const cube = new OfficialCubeSandboxRuntimeClient(config.client);
  const storage = new HttpWorkspaceVolumeGateway({
    baseUrl: "http://" + serviceAddress("workspace-volume-gateway") + ":4500",
    serviceToken: await secret("workspace-volume-gateway-token"),
  });
  const url = new URL(await secret("database-url"));
  url.hostname = serviceAddress("postgres");
  const db = createDatabase({ connectionString: url.href, maxConnections: 2 });
  const marker = randomUUID(),
    label = randomUUID();
  let tenant, api, project, volume, vm, pending, generation;
  const endpoint = "http://127.0.0.1:18341/fork-marker";
  async function command(command) {
    const r = await cube.runCommand(vm, {
      command,
      cwd: "/home/user",
      user: "user",
      timeoutMs: 10000,
      maximumOutputBytes: 2048,
    });
    assert.equal(r.exitCode, 0, "Owned Guest command failed");
    return r.stdout.trim();
  }
  async function start() {
    assert(!vm && !pending);
    generation = randomUUID();
    pending = true;
    vm = await cube.create({
      templateId: config.templateId,
      timeoutSeconds: 600,
      metadata: {
        "picloud.workload": "fork-world-study",
        "study-id": label,
        "study-generation": generation,
      },
      allowPublicTraffic: false,
      volumeMounts: [{ name: volume, path: "/home/user" }],
    });
    const observed = await cube.read(vm.sandboxId);
    assert.equal(observed?.metadata["study-generation"], generation);
    vm = { ...vm, nodeId: observed.nodeId };
    const boot = await until(async () => {
      try {
        return await command("cat /proc/sys/kernel/random/boot_id");
      } catch {
        return false;
      }
    }, "Owned Guest readiness");
    assert.match(boot, /^[a-f0-9-]{36}$/);
    return boot;
  }
  async function retire() {
    if (!pending) return;
    // A create with a lost reply may already own a physical VM.
    const found =
      vm ??
      (await cube.list()).find(
        (x) => x.metadata["study-id"] === label && x.metadata["study-generation"] === generation,
      );
    assert(found, "Cube create unconfirmed; keep the owned Workspace");
    await cube.destroy(found.sandboxId);
    await until(
      async () => (await cube.observe(found.sandboxId, found.nodeId)).presence === "absent",
      "Physical VM deletion",
      90000,
    );
    vm = undefined;
    pending = false;
  }
  async function startSite() {
    await command(
      "setsid python3 -m http.server 18341 --bind 127.0.0.1 --directory /home/user </dev/null >/tmp/fork-site.log 2>&1 &",
    );
    await until(async () => {
      try {
        return (await command("curl -fsS --max-time 2 " + endpoint)) === marker;
      } catch {
        return false;
      }
    }, "Original workspace site");
  }
  try {
    tenant = await createPrivateTenant(db, {
      slug: "release-acceptance-fork-" + label,
      ownerDisplayName: "Fork environment acceptance",
    });
    const base = "http://127.0.0.1:" + runtimeEnvironment().PI_CLOUD_HTTP_PORT;
    api = new PiCloudApi((p, o) => fetch(new URL(p, base), o), tenant.credential.token);
    project = await api.createProject("Fork environment acceptance");
    const identity = { tenantId: tenant.tenantId, workspaceId: project.workspaceId };
    volume = workspaceVolumeId(identity);
    await storage.persistence.ensure({ ...identity, sessionId: randomUUID(), volumeId: volume });
    await cube.ensureVolume(volume, "picloud-posix");
    const firstBoot = await start(),
      first = generation;
    await command("printf '%s' '" + marker + "' > /home/user/fork-marker");
    await startSite();
    let replaced = false;
    const result = await study({
      ...identity,
      first,
      async replace() {
        assert(!replaced);
        assert.equal(await command("curl -fsS " + endpoint), marker);
        await retire();
        assert.notEqual(await start(), firstBoot);
        assert.notEqual(generation, first);
        assert.equal(await command("cat /home/user/fork-marker"), marker);
        // Only ECONNREFUSED demonstrates a missing listener on this loopback port.
        assert.equal(
          await command(
            `python3 -c 'import errno,socket; s=socket.socket(); assert s.connect_ex(("127.0.0.1",18341))==errno.ECONNREFUSED; print("absent")'`,
          ),
          "absent",
        );
        replaced = true;
        return generation;
      },
      async startControl() {
        assert(replaced);
        await startSite();
        return generation;
      },
      async confirmControl() {
        assert.equal(await command("curl -fsS " + endpoint), marker);
      },
    });
    assert(replaced);
    return result;
  } finally {
    try {
      await retire();
      assert(!(await cube.list()).some((x) => x.metadata["study-id"] === label));
      if (volume) {
        await cube.deleteVolume(volume);
        await until(async () => !(await cube.hasVolume(volume)), "Native registration retirement");
      }
      if (project) {
        await api.deleteWorkspace(project.workspaceId, randomUUID());
        await until(
          async () => {
            const row = await db
              .selectFrom("workspaces")
              .select("storage_purged_at")
              .where("id", "=", project.workspaceId)
              .executeTakeFirst();
            return !row || row.storage_purged_at;
          },
          "Owned Workspace byte cleanup",
          180000,
        );
      }
      if (tenant)
        await purgeOwnedAcceptanceTenant(db, tenant, {
          delete: async () => assert.fail("Unexpected attachment"),
        });
    } finally {
      await cube.close();
      await storage.close();
      await db.destroy();
    }
  }
}
