import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { kubectl, kubectlAsync, namespace, podFor, workload } from "./production-kubernetes.mjs";

export const nativeProjectionSources = [
  "packages/pi-session-postgres/src/durable/current-state.ts",
  "packages/pi-session-postgres/src/durable/live-state.ts",
  "packages/pi-session-postgres/src/durable/postgres-projection.ts",
  "packages/pi-session-postgres/src/durable/shared-entry-bodies.ts",
  "packages/pi-session-postgres/src/durable/published-storage.ts",
  "packages/pi-session-postgres/src/durable/conversation-usage.ts",
  "packages/pi-session-postgres/src/durable/index.ts",
];
export const historyPresentationSources = [
  "packages/runtime-core/src/canonical-history-window.ts",
  "packages/runtime-core/src/native-transcript-presentation.ts",
  "packages/runtime-core/src/frozen-history.ts",
];
export const workerStartupSources = [
  "packages/sandbox-supervisor/src/pi-cloud-turn-runner.ts",
  "packages/sandbox-supervisor/src/pi-sandbox-continuity.ts",
  "packages/sandbox-supervisor/src/durable/agent-session.ts",
];

function templateWithSources(service, containerName, name, revision, paths) {
  const target = workload(service);
  const { template: original } = JSON.parse(kubectl(["get", target, "-o", "json"])).spec;
  const actual = JSON.parse(
    kubectl([
      "exec",
      podFor(service),
      "-c",
      containerName,
      "--",
      "node",
      "-e",
      'const fs=require("node:fs"),crypto=require("node:crypto");console.log(JSON.stringify(process.argv.slice(1).map(p=>crypto.createHash("sha256").update(fs.readFileSync("/app/"+p)).digest("hex"))))',
      ...paths,
    ]),
  );
  const modified = structuredClone(original);
  modified.spec.volumes.push({ name, configMap: { name, defaultMode: 420 } });
  const c = modified.spec.containers.find((c) => c.name === containerName);
  assert(c, "Missing diagnostic role " + containerName);
  paths.forEach((path, i) => {
    const expected = execFileSync("git", ["show", revision + ":" + path]);
    assert.equal(
      actual[i],
      createHash("sha256").update(expected).digest("hex"),
      "Serving " + service + " differs at " + path,
    );
    c.volumeMounts.push({
      name,
      mountPath: "/app/" + path,
      subPath: path.split("/").at(-1),
      readOnly: true,
    });
  });
  return { target, original, modified };
}

/** Match the Worker's storage/flush contract under the operator lock. */
export function workerProjectionDiagnostic(name, revision, extraSources = []) {
  const { target, original, modified } = templateWithSources(
    "pi-workers",
    "pi-worker",
    name,
    revision,
    [...nativeProjectionSources, ...extraSources],
  );
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

/** The test tenant uses a private copy of the normal Session API. Existing
 * users keep their original reader throughout both sides of the comparison. */
export function sessionApiProjectionDiagnostic(name, revision) {
  const paths = [...nativeProjectionSources, ...historyPresentationSources];
  const { target, modified } = templateWithSources(
    "session-api",
    "session-api",
    name,
    revision,
    paths,
  );
  const pod = name + "-api",
    labels = { "pi-cloud.io/projection-diagnostic": name };
  const policy = JSON.parse(kubectl(["get", "networkpolicy", target.split("/")[1], "-o", "json"]));
  const create = (resource) => kubectl(["create", "-f", "-"], { input: JSON.stringify(resource) });
  let attempted = false;
  return {
    name: pod,
    async start() {
      attempted = true;
      create({
        apiVersion: "networking.k8s.io/v1",
        kind: "NetworkPolicy",
        metadata: { name: pod, namespace, labels },
        spec: { ...policy.spec, podSelector: { matchLabels: labels } },
      });
      create({
        apiVersion: "v1",
        kind: "Pod",
        metadata: { name: pod, namespace, labels },
        spec: { ...modified.spec, restartPolicy: "Never" },
      });
      await kubectlAsync(["wait", "--for=condition=Ready", "pod/" + pod, "--timeout=180s"]);
      const instance = JSON.parse(kubectl(["get", "pod", pod, "-o", "json"]));
      const port = instance.spec.containers
        .find((c) => c.name === "session-api")
        .ports.find((p) => p.name === "http").containerPort;
      const address = instance.status.podIP;
      return `http://${address.includes(":") ? `[${address}]` : address}:${port}`;
    },
    async close() {
      if (!attempted) return;
      await kubectlAsync(["delete", "pod/" + pod, "--ignore-not-found", "--timeout=120s"]);
      kubectl(["delete", "networkpolicy/" + pod, "--ignore-not-found"]);
    },
  };
}
