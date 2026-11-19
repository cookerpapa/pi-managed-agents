import assert from "node:assert/strict";
import { setTimeout as delay } from "node:timers/promises";
import { kubectl } from "../../lib/production-kubernetes.mjs";

// Temporary additive allow between the two selected managed roles. Admit
// only the experiment listener's kernel-assigned port; no service/Pod changes.
export function wakeNetwork(routerPod, workerPods, port, suffix) {
  const read = (kind, name) =>
    JSON.parse(kubectl(["get", kind, ...(name ? [name] : []), "-o", "json"]));
  const server = read("pod", routerPod);
  const policies = read("networkpolicy").items;
  for (const [pod, type] of [
    [server, "Ingress"],
    ...workerPods.map((pod) => [read("pod", pod), "Egress"]),
  ]) {
    assert(
      policies.some(
        ({ spec }) =>
          spec.policyTypes.includes(type) &&
          !spec.podSelector.matchExpressions?.length &&
          Object.entries(spec.podSelector.matchLabels ?? {}).every(
            ([k, v]) => pod.metadata.labels[k] === v,
          ),
      ),
      "Refusing to introduce isolation to a previously unisolated workload",
    );
  }
  const serverSelector = {
    matchLabels: {
      "app.kubernetes.io/component": server.metadata.labels["app.kubernetes.io/component"],
      "pod-template-hash": server.metadata.labels["pod-template-hash"],
    },
  };
  const clientSelector = {
    matchExpressions: [
      { key: "statefulset.kubernetes.io/pod-name", operator: "In", values: workerPods },
    ],
  };
  const owned = [];
  function close() {
    for (const name of owned) {
      kubectl(["delete", "networkpolicy", name, "--ignore-not-found", "--wait=true"]);
      assert.equal(
        kubectl(["get", "networkpolicy", name, "--ignore-not-found", "-o", "name"]).trim(),
        "",
      );
    }
  }
  try {
    for (const [type, own, peer] of [
      ["Ingress", serverSelector, clientSelector],
      ["Egress", clientSelector, serverSelector],
    ]) {
      const ingress = type === "Ingress",
        name = "pma-wake-" + suffix + (ingress ? "-in" : "-out");
      const spec = {
        podSelector: own,
        policyTypes: [type],
        [type.toLowerCase()]: [
          {
            [ingress ? "from" : "to"]: [{ podSelector: peer }],
            ports: [{ protocol: "TCP", port }],
          },
        ],
      };
      kubectl(["create", "-f", "-"], {
        input: JSON.stringify({
          apiVersion: "networking.k8s.io/v1",
          kind: "NetworkPolicy",
          metadata: {
            name,
            ownerReferences: [
              {
                apiVersion: "v1",
                kind: "Pod",
                name: server.metadata.name,
                uid: server.metadata.uid,
              },
            ],
          },
          spec,
        }),
      });
      owned.push(name);
    }
    return { address: "ws://" + server.status.podIP + ":" + port, close };
  } catch (error) {
    close();
    throw error;
  }
}

export async function runWakeTrials(wave) {
  const base = {
    slots: 32,
    omitNotify: true,
    batch: 8,
    tickMs: 200,
    wakeMode: "directed",
    probeUnlisted: true,
  };
  const run = (label, options) => wave({ ...base, label, ...options });
  for (const fault of ["lost-notification", "lost-wake", "lost-ready", "disconnect"]) {
    const r = await run("fault-" + fault, { fault, offline: ["w1"], concurrency: 1, count: 1 });
    assert.equal(r.completed, 1);
    assert.equal(r.router.faults, 1);
    if (fault === "lost-notification" || fault === "lost-wake")
      assert.equal(r.claimReasons.tick, 1);
    if (fault === "lost-ready") {
      assert.equal(r.claimReasons.tick, 1);
      assert(r.router.unlistedTicks >= 1);
    }
    if (fault === "disconnect") {
      assert(r.workerResources[0].claimStats.registrations >= 2);
      assert.equal(r.claimReasons.bootstrap, 1);
    }
  }
  for (const fault of ["lost-wake", "lost-ready"]) {
    const r = await run("listed-only-" + fault, {
      fault,
      probeUnlisted: false,
      offline: ["w1"],
      concurrency: 1,
      count: 1,
    });
    assert.equal(r.router.faults, 1);
    assert(r.router.offers >= 2);
  }
  const full = await run("no-check-while-full", {
    slots: 1,
    offline: ["w1"],
    count: 1,
    concurrency: 1,
    modelDelayMs: 850,
  });
  assert(full.workerResources[0].claimStats.fullHints >= 2);
  const joined = await run("scale-after-enqueue", {
    offline: ["w0", "w1"],
    joinAfterAdmission: true,
    concurrency: 8,
    count: 8,
  });
  assert.equal(joined.claimReasons.bootstrap, 8);
  const restarted = await run("runtime-restart", { restartRouter: true, concurrency: 8, count: 8 });
  assert.equal(restarted.claimReasons.bootstrap, 8);
  assert(restarted.wakeCount > 0 && restarted.router.notifications === 0);
  const draining = await run("scale-in-busy", {
    offline: ["w1"],
    scaleWhenClaimed: true,
    slots: 2,
    concurrency: 16,
    count: 16,
    modelDelayMs: 200,
  });
  assert(draining.claimDistribution.w0 >= 1 && draining.claimDistribution.w0 <= 2);
  assert(draining.claimDistribution.w1 >= 14);
  // Matched arms share the wake channel, execution, pool, coalescing window and
  // data. A return to the first arm checks temporal drift on the development host.
  for (const [i, [wakeMode, batch]] of [
    ["broadcast", 1],
    ["directed", 1],
    ["directed", 8],
    ["broadcast", 8],
    ["broadcast", 8],
    ["directed", 8],
    ["directed", 1],
    ["broadcast", 1],
  ].entries())
    await run("burst-" + i, { wakeMode, batch, concurrency: 64, count: 384 });
  for (const [i, wakeMode] of ["broadcast", "directed", "directed", "broadcast"].entries())
    await run("steady-" + i, { wakeMode, rate: 90, concurrency: 192, count: 900 });
  for (const [i, windowMs] of [5, 20, 20, 5].entries()) {
    await run("sparse-window-" + i, { windowMs, rate: 20, concurrency: 32, count: 100 });
    await run("burst-window-" + i, { windowMs, concurrency: 64, count: 384 });
  }
}

export async function waitForWakeState(get, test) {
  const end = Date.now() + 30000;
  let state;
  while (!test((state = await get()))) {
    assert(Date.now() < end, "Timed out waiting for wake connections: " + JSON.stringify(state));
    await delay(12);
  }
}
