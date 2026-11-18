import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { kubectl, kubectlAsync, podFor, workload } from "./production-kubernetes.mjs";

/** Match the Worker's flush contract during the owned Projector experiment.
 * The parent owns the operator lock, private restoration receipt and ConfigMap. */
export function workerProjectionDiagnostic(name, revision) {
  const path = "packages/pi-session-postgres/src/durable/published-storage.ts";
  const target = workload("pi-workers");
  const { template: original } = JSON.parse(kubectl(["get", target, "-o", "json"])).spec;
  const expected = execFileSync("git", ["show", revision + ":" + path]);
  const serving = kubectl([
    "exec",
    podFor("pi-workers"),
    "-c",
    "pi-worker",
    "--",
    "node",
    "-e",
    "console.log(require('crypto').createHash('sha256').update(require('fs').readFileSync(" +
      JSON.stringify("/app/" + path) +
      ")).digest('hex'))",
  ]).trim();
  assert.equal(
    serving,
    createHash("sha256").update(expected).digest("hex"),
    "Worker source differs from its serving revision",
  );
  const modified = structuredClone(original);
  modified.spec.volumes.push({ name, configMap: { name, defaultMode: 420 } });
  const container = modified.spec.containers.find((c) => c.name === "pi-worker");
  assert(container, "Worker container missing");
  container.volumeMounts.push({
    name,
    mountPath: "/app/" + path,
    subPath: "published-storage.ts",
    readOnly: true,
  });
  let attempted = false;
  const change = async (from, to) => {
    kubectl([
      "patch",
      target,
      "--type=json",
      "-p",
      JSON.stringify([
        { op: "test", path: "/spec/template", value: from },
        { op: "replace", path: "/spec/template", value: to },
      ]),
    ]);
    await kubectlAsync(["rollout", "status", target, "--timeout=240s"]);
  };
  return {
    receipt: { target, original, modified },
    async apply() {
      attempted = true;
      await change(original, modified);
    },
    async restore() {
      if (!attempted) return;
      const live = JSON.parse(kubectl(["get", target, "-o", "json"])).spec.template;
      if (!live.spec.volumes.some((v) => v.name === name)) {
        assert.deepEqual(live, original);
        return;
      }
      const stripped = structuredClone(live);
      stripped.spec.volumes = stripped.spec.volumes.filter((v) => v.name !== name);
      const c = stripped.spec.containers.find((v) => v.name === "pi-worker");
      c.volumeMounts = c.volumeMounts.filter((v) => v.name !== name);
      assert.deepEqual(stripped, original, "Unrelated Worker change prevents restoration");
      await change(live, original);
    },
  };
}
