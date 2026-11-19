import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { randomBytes } from "node:crypto";
import { readFileSync } from "node:fs";
import { kubectl, namespace, podFor } from "../../lib/production-kubernetes.mjs";

const label = "pi-cloud.io/pg-cpu-study";
const roleLabel = "pi-cloud.io/pg-cpu-role";
const get = (kind, name) => JSON.parse(kubectl(["get", kind, name, "-o", "json"]));
const owned = (prefix) => {
  assert.match(prefix, /^pma-cpu-[0-9a-f]{16}$/);
  return { [label]: prefix };
};
const address = "unix:///run/k3s/containerd/containerd.sock";
const cri = (args) =>
  execFileSync("crictl", ["--config", "/dev/null", "--runtime-endpoint", address, ...args], {
    encoding: "utf8",
    env: { ...process.env, CRI_CONFIG_FILE: "/dev/null", CONTAINER_RUNTIME_ENDPOINT: address },
    timeout: 15000,
    stdio: ["ignore", "pipe", "pipe"],
  });
export function cpus(list) {
  assert.match(list, /^\d+(?:-\d+)?(?:,\d+(?:-\d+)?)*$/);
  const values = new Set();
  for (const part of list.split(",")) {
    const [first, last = first] = part.split("-").map(Number);
    assert(first <= last && last < 65536);
    for (let n = first; n <= last; n++) values.add(n);
  }
  return [...values].sort((a, b) => a - b);
}
export function listCpus(values) {
  const sorted = [...new Set(values)].sort((a, b) => a - b),
    ranges = [];
  for (let i = 0; i < sorted.length;) {
    const start = sorted[i];
    let end = start;
    while (sorted[++i] === end + 1) end++;
    ranges.push(start === end ? String(start) : start + "-" + end);
  }
  return ranges.join(",");
}
// Select adjacent whole sibling groups for each half. Keep CPU 0's core and
// at least two other complete cores available to the generator and Workers.
export function placementPlan(siblings) {
  const groups = [...new Map(siblings.map((g) => [listCpus(g), g])).values()].sort(
    (a, b) => a[0] - b[0],
  );
  const width = groups[0]?.length;
  assert(
    groups.length >= 11 && groups.every((g) => g.length === width),
    "Need eight comparable guest cores plus support cores",
  );
  assert.equal(new Set(groups.flat()).size, groups.flat().length, "CPU siblings overlap");
  const start = Math.floor((groups.length - 8) / 2);
  const selected = groups.slice(start, start + 8);
  const a = listCpus(selected.filter((_, i) => i % 2 === 0).flat());
  const b = listCpus(selected.filter((_, i) => i % 2 === 1).flat());
  const shared = listCpus(selected.flat());
  const support = listCpus(groups.filter((g) => !selected.includes(g)).flat());
  return { a, b, shared, support, siblings: groups };
}
export function localPlan() {
  const allowed = cpus(readFileSync("/sys/fs/cgroup/cpuset.cpus.effective", "utf8").trim());
  return placementPlan(
    allowed.map((n) => {
      const group = cpus(
        readFileSync(
          "/sys/devices/system/cpu/cpu" + n + "/topology/thread_siblings_list",
          "utf8",
        ).trim(),
      );
      assert(group.every((c) => allowed.includes(c)));
      return group;
    }),
  );
}

export function assertUnusedFixture(prefix) {
  owned(prefix);
  assert.equal(
    kubectl(["get", "pods,services,secrets", "-l", label + "=" + prefix, "-o", "name"]).trim(),
    "",
  );
}
export function createPods(prefix) {
  const labels = owned(prefix),
    meta = (name) => ({ name, labels });
  const pg = get("pod", podFor("postgres")),
    runtime = get("pod", podFor("session-runtime")),
    worker = get("pod", podFor("pi-cloud-pi-worker-primary-v1-0"));
  assert(
    [runtime, worker].every((p) => p.spec.nodeName === pg.spec.nodeName),
    "Study requires one development node",
  );
  const password = randomBytes(32).toString("hex"),
    pgName = prefix + "-pg",
    secretName = prefix + "-auth";
  const url = "postgresql://bench:" + password + "@" + pgName + ":5432/postgres";
  const volume = (name, medium) => ({ name, emptyDir: medium ? { medium } : {} });
  const auth = (key) => ({ name: "auth", secret: { secretName, items: [{ key, path: key }] } });
  const mount = (name, mountPath, readOnly = false) => ({ name, mountPath, readOnly });
  const pod = (role, base, container, volumes) => ({
    apiVersion: "v1",
    kind: "Pod",
    metadata: { name: prefix + "-" + role, labels: { ...labels, [roleLabel]: role } },
    spec: {
      nodeName: pg.spec.nodeName,
      restartPolicy: "Never",
      activeDeadlineSeconds: 5400,
      automountServiceAccountToken: false,
      enableServiceLinks: false,
      terminationGracePeriodSeconds: 30,
      securityContext: base.spec.securityContext,
      containers: [container],
      volumes,
    },
  });
  const standby = (role, base) => {
    const c = base.spec.containers[0],
      brokers = c.env.find((x) => x.name === "PI_CLOUD_KAFKA_BROKERS");
    assert(typeof brokers?.value === "string");
    return pod(
      role,
      base,
      {
        name: role,
        image: c.image,
        imagePullPolicy: "IfNotPresent",
        command: ["node", "-e", "setInterval(()=>{},60000)"],
        securityContext: c.securityContext,
        resources: c.resources,
        env: [brokers, { name: "DATABASE_URL_FILE", value: "/run/pg-study/url" }],
        volumeMounts: [mount("auth", "/run/pg-study", true), mount("tmp", "/tmp")],
      },
      [auth("url"), volume("tmp")],
    );
  };
  const c = pg.spec.containers[0];
  const items = [
    {
      apiVersion: "v1",
      kind: "Secret",
      metadata: meta(secretName),
      type: "Opaque",
      stringData: { password, url },
    },
    {
      apiVersion: "v1",
      kind: "Service",
      metadata: meta(pgName),
      spec: {
        selector: { ...labels, [roleLabel]: "pg" },
        ports: [{ port: 5432, targetPort: 5432 }],
      },
    },
    pod(
      "pg",
      pg,
      {
        name: "pg",
        image: c.image,
        imagePullPolicy: "IfNotPresent",
        resources: c.resources,
        env: [
          { name: "POSTGRES_USER", value: "bench" },
          { name: "POSTGRES_DB", value: "postgres" },
          { name: "POSTGRES_PASSWORD_FILE", value: "/run/pg-study/password" },
        ],
        readinessProbe: {
          exec: { command: ["pg_isready", "-h", "127.0.0.1", "-U", "bench", "-d", "postgres"] },
          periodSeconds: 1,
        },
        volumeMounts: [
          mount("data", "/var/lib/postgresql/data"),
          mount("shm", "/dev/shm"),
          mount("auth", "/run/pg-study", true),
        ],
      },
      [volume("data"), volume("shm", "Memory"), auth("password")],
    ),
    standby("projector", runtime),
    standby("support", worker),
  ];
  kubectl(["create", "-f", "-", "-o", "name"], {
    input: JSON.stringify({ apiVersion: "v1", kind: "List", items }),
  });
  kubectl(["wait", "--for=condition=Ready", "pods", "-l", label + "=" + prefix, "--timeout=100s"], {
    timeout: 110000,
  });
}
export function removePods(prefix) {
  owned(prefix);
  const types = "pod,service,secret",
    select = ["-l", label + "=" + prefix];
  kubectl(["delete", types, ...select, "--ignore-not-found", "--wait=true", "--timeout=90s"], {
    timeout: 100000,
  });
  assert.equal(
    kubectl(["get", types, ...select, "-o", "name"]).trim(),
    "",
    "Owned fixture still exists",
  );
}
export function attachFixture(prefix) {
  owned(prefix);
  const roles = Object.fromEntries(
    ["pg", "projector", "support"].map((role) => {
      const name = prefix + "-" + role,
        p = get("pod", name),
        status = p.status.containerStatuses[0];
      assert(
        p.metadata.labels[label] === prefix &&
          p.metadata.labels[roleLabel] === role &&
          p.spec.restartPolicy === "Never" &&
          p.spec.containers.length === 1 &&
          status.name === role &&
          status.ready,
      );
      const id = status.containerID.replace(/^containerd:\/\//, "");
      assert.match(id, /^[0-9a-f]{64}$/);
      const info = JSON.parse(cri(["inspect", id])).status;
      assert(
        info.id === id &&
          info.labels["io.kubernetes.pod.uid"] === p.metadata.uid &&
          info.labels["io.kubernetes.pod.name"] === name &&
          info.labels["io.kubernetes.pod.namespace"] === namespace,
      );
      return [role, { name, id, image: status.imageID }];
    }),
  );
  const svc = get("service", roles.pg.name);
  assert.equal(svc.spec.selector[label], prefix);
  const secret = get("secret", prefix + "-auth");
  assert.equal(secret.metadata.labels[label], prefix);
  const url = new URL(Buffer.from(secret.data.url, "base64").toString());
  assert.equal(url.hostname, roles.pg.name);
  url.hostname = svc.spec.clusterIP;
  const plan = localPlan();
  const driverMask = readFileSync("/proc/self/status", "utf8").match(
    /^Cpus_allowed_list:\s*(.+)$/m,
  )?.[1];
  assert.equal(driverMask, plan.support, "Load driver must run outside the eight study cores");
  const fingerprint = (pod) =>
    kubectl(["exec", pod, "--", "sh", "-c", 'stat -f -c "%T %i" "$PGDATA"; postgres --version'])
      .trim()
      .split("\n");
  const sourceStorage = fingerprint(podFor("postgres")),
    tempStorage = fingerprint(roles.pg.name);
  assert.deepEqual(tempStorage, sourceStorage, "Expected the same guest filesystem and PG binary");
  const environment = {
    sameGuestFilesystem: true,
    filesystem: tempStorage[0].split(" ")[0],
    postgres: tempStorage[1],
  };
  const set = (role, mask) => cri(["update", "--cpuset-cpus", mask, roles[role].id]);
  function verify(role, mask) {
    const script = `set -eu
      cat /sys/fs/cgroup/cpuset.cpus.effective /sys/fs/cgroup/cpu.max /sys/fs/cgroup/cpu.weight /sys/fs/cgroup/memory.max
      awk '/^Cpus_allowed_list:/{print $2}' /proc/[0-9]*/task/[0-9]*/status 2>/dev/null | sort -u`;
    const stdout = kubectl(["exec", roles[role].name, "--", "sh", "-c", script], {
      timeout: 6000,
    });
    const [effective, quota, weight, memory, ...threadMasks] = stdout.trim().split("\n");
    assert.equal(listCpus(cpus(effective)), mask, "Wrong effective cpuset for " + role);
    const allowed = cpus(mask);
    assert(
      threadMasks.length && threadMasks.every((m) => cpus(m).every((n) => allowed.includes(n))),
      "Thread escaped " + role + " affinity",
    );
    return { effective, quota, weight, memory, threadMasks };
  }
  const initial = new Map();
  function pin(layout) {
    const masks = {
      open: [listCpus(plan.siblings.flat()), listCpus(plan.siblings.flat())],
      shared: [plan.shared, plan.shared],
      ab: [plan.a, plan.b],
      ba: [plan.b, plan.a],
      overlapA: [plan.a, plan.a],
      overlapB: [plan.b, plan.b],
    }[layout];
    assert(masks, "Unknown CPU layout");
    set("pg", masks[0]);
    set("projector", masks[1]);
    set("support", plan.support);
    const checked = Object.keys(roles).map((role, i) => verify(role, [...masks, plan.support][i]));
    for (const [i, r] of checked.entries()) {
      const other = [r.quota, r.weight, r.memory];
      if (initial.has(i)) assert.deepEqual(other, initial.get(i));
      else initial.set(i, other);
    }
    return { layout, pg: checked[0], projector: checked[1], support: checked[2] };
  }
  return {
    url,
    pg: roles.pg.name,
    projector: roles.projector.name,
    support: roles.support.name,
    plan,
    environment,
    pin,
    images: Object.fromEntries(Object.entries(roles).map(([k, v]) => [k, v.image])),
  };
}
