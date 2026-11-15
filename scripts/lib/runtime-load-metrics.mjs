import { readFile, statfs } from "node:fs/promises";
import { sql } from "kysely";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import {
  kubectl,
  runtimeDirectory,
  repositoryRoot,
  kubeconfig,
  namespace,
} from "./production-kubernetes.mjs";
import { ACCEPTED_FACT_TOPIC } from "@pi-cloud/event-log";

const exec = promisify(execFile);
async function readKubectl(args) {
  const binary = process.env.PI_CLOUD_KUBECTL_BIN ?? "/usr/local/bin/k3s";
  const r = await exec(
    binary,
    [
      ...(process.env.PI_CLOUD_KUBECTL_BIN ? [] : ["kubectl"]),
      "--kubeconfig",
      kubeconfig,
      "-n",
      namespace,
      ...args,
    ],
    { timeout: 7000, maxBuffer: 8 * 1024 ** 2 },
  );
  return r.stdout;
}
async function kafkaLag() {
  // Admin offset reads only: no Session consumer, assignment or offset mutation.
  const code = `import native from '@confluentinc/kafka-javascript';const a=new native.KafkaJS.Kafka({kafkaJS:{brokers:process.env.PI_CLOUD_KAFKA_BROKERS.split(','),clientId:'capacity-offset-observer',logLevel:native.KafkaJS.logLevel.NOTHING}}).admin();try{await a.connect();const t=${JSON.stringify(ACCEPTED_FACT_TOPIC)};const [heads,groups]=await Promise.all([a.fetchTopicOffsets(t),a.fetchOffsets({groupId:'pi-cloud-session-projector-v2',topics:[t]})]);const offsets=new Map(groups[0].partitions.map(p=>[p.partition,p.offset]));const partitions=heads.map(p=>({partition:p.partition,head:p.offset,committed:offsets.get(p.partition)??'-1',lag:Number(BigInt(p.offset)-BigInt(offsets.get(p.partition)??p.offset))}));console.log(JSON.stringify({topic:t,observedAt:Date.now(),partitions,lag:partitions.reduce((n,p)=>n+Math.max(0,p.lag),0)}));}finally{await a.disconnect();}`;
  return JSON.parse(
    (
      await readKubectl([
        "exec",
        "deployment/pi-cloud-tool-host",
        "--",
        "node",
        "--input-type=module",
        "-e",
        code,
      ])
    ).trim(),
  );
}

export function parseMetricText(body) {
  return body.split("\n").flatMap((line) => {
    const m = /^([a-zA-Z_:][\w:]*)(\{[^\n]*\})?\s+([-+\d.eE]+)(?:\s+\d+)?$/.exec(line);
    return m && Number.isFinite(Number(m[3]))
      ? [{ name: m[1], labels: m[2] ?? "", value: Number(m[3]) }]
      : [];
  });
}
export async function hostResources() {
  const mem = await readFile("/proc/meminfo", "utf8"),
    read = (name) => Number(new RegExp(`^${name}:\\s+(\\d+)`, "m").exec(mem)?.[1] ?? 0) * 1024;
  const disk = await statfs(repositoryRoot);
  return {
    memoryTotalBytes: read("MemTotal"),
    memoryAvailableBytes: read("MemAvailable"),
    swapUsedBytes: read("SwapTotal") - read("SwapFree"),
    diskAvailableBytes: disk.bavail * disk.bsize,
  };
}
export function runtimeInventory() {
  const resources = JSON.parse(kubectl(["get", "deployments,statefulsets", "-o", "json"])).items;
  return resources.map((r) => ({
    name: r.metadata.name,
    kind: r.kind,
    replicas: r.spec.replicas,
    containers: r.spec.template.spec.containers.map((c) => ({
      name: c.name,
      image: c.image,
      resources: c.resources,
      configuration: (c.env ?? [])
        .filter(
          (e) =>
            e.value !== undefined &&
            /CONCURRENCY|CAPACITY|KAFKA_PARTITIONS|MAXIMUM_ACTIVE_COMMANDS/.test(e.name),
        )
        .map((e) => ({ name: e.name, value: e.value })),
    })),
  }));
}

export class RuntimeLoadMetrics {
  constructor({ database, tenantId, fixture, minimumAvailableMiB = 3072 }) {
    this.database = database;
    this.tenantId = tenantId;
    this.fixture = fixture;
    this.minimumAvailableMiB = minimumAvailableMiB;
    this.samples = [];
    this.errors = [];
    this.abort = new AbortController();
  }
  async start() {
    this.key = (await readFile(runtimeDirectory + "/secrets/metrics-token", "utf8")).trim();
    this.nodeName = JSON.parse(kubectl(["get", "nodes", "-o", "json"])).items[0].metadata.name;
    await this.refreshTargets();
    await this.sample();
    this.timer = setInterval(() => {
      if (this.pending) return;
      this.pending = this.sample()
        .catch((e) => this.errors.push(String(e.message)))
        .finally(() => {
          this.pending = undefined;
        });
    }, 2000);
    this.timer.unref();
  }
  async refreshTargets() {
    const pods = JSON.parse(kubectl(["get", "pods", "-o", "json"])).items;
    this.targets = pods
      .flatMap((p) =>
        p.metadata.deletionTimestamp
          ? []
          : p.spec.containers.flatMap((c) => {
              const port =
                c.ports?.find((p) => p.name === "metrics")?.containerPort ??
                Number(c.env?.find((e) => e.name === "PI_CLOUD_METRICS_PORT")?.value);
              return port && p.status.podIP
                ? [{ name: p.metadata.name, uid: p.metadata.uid, port, ip: p.status.podIP }]
                : [];
            }),
      )
      .filter((t) => t.name.startsWith("pi-cloud-"));
  }
  async sample() {
    const at = Date.now(),
      host = await hostResources();
    if (
      host.memoryAvailableBytes < this.minimumAvailableMiB * 1024 ** 2 ||
      host.diskAvailableBytes < 30 * 1024 ** 3
    )
      this.abort.abort(new Error("Host headroom budget reached"));
    const extended = !this.lastExtendedAt || at - this.lastExtendedAt >= 5000;
    const [pg, processes, provider, extendedSample] = await Promise.all([
      sql`select
        (select count(*)::int from runs where tenant_id=${this.tenantId}::uuid and state='queued') as queued,
        (select count(*)::int from runs where tenant_id=${this.tenantId}::uuid and state='running') as running,
        (select count(*)::int from runs where tenant_id=${this.tenantId}::uuid and state='settling') as settling,
        (select count(*)::int from session_leases where tenant_id=${this.tenantId}::uuid and released_at is null) as owners,
        (select count(*)::int from pg_stat_activity where datname=current_database() and wait_event_type='Lock') as lock_waiters,
        (select xact_commit::text from pg_stat_database where datname=current_database()) as transactions,
        (select wal_bytes::text from pg_stat_wal) as wal_bytes,
        (select temp_bytes::text from pg_stat_database where datname=current_database()) as temp_bytes`
        .execute(this.database)
        .then((r) => r.rows[0]),
      Promise.all(
        this.targets.map(async (target) => {
          try {
            const r = await fetch(`http://${target.ip}:${target.port}/metrics`, {
              headers: { authorization: `Bearer ${this.key}` },
              signal: AbortSignal.timeout(1500),
            });
            if (!r.ok) throw new Error("metrics HTTP " + r.status);
            const metrics = parseMetricText(await r.text()).filter((m) =>
              /^(process_cpu_seconds_total|process_resident_memory_bytes|process_start_time_seconds|nodejs_eventloop_lag_p99_seconds)$|^pi_cloud_(active_|model_permits_|kafka_producer_pending|kafka_producer_rejected|tool_transport_rejected)|^pi_cloud_(session_projection|session_mutation_wait|run_claim|run_preparation).*_(sum|count)$/.test(
                m.name,
              ),
            );
            return { name: target.name, uid: target.uid, metrics };
          } catch {
            return { name: target.name, uid: target.uid, unavailable: true };
          }
        }),
      ),
      this.fixture.metrics(true).catch(() => ({ unavailable: true })),
      extended
        ? Promise.all([
            kafkaLag().catch(() => ({ unavailable: true })),
            readKubectl([
              "get",
              "--raw",
              "/api/v1/nodes/" + encodeURIComponent(this.nodeName) + "/proxy/stats/summary",
            ])
              .then((raw) => {
                const s = JSON.parse(raw);
                return s.pods
                  .filter((p) => p.podRef.namespace === namespace)
                  .map((p) => ({
                    name: p.podRef.name,
                    uid: p.podRef.uid,
                    cpu: p.cpu,
                    memory: p.memory,
                    io: p.io,
                  }));
              })
              .catch(() => null),
          ])
        : undefined,
    ]);
    if (extendedSample) {
      this.lastExtendedAt = at;
      this.extended = { kafka: extendedSample[0], containers: extendedSample[1] };
    }
    this.samples.push({
      at,
      finishedAt: Date.now(),
      host,
      pg,
      processes,
      provider,
      ...this.extended,
    });
  }
  async stop() {
    clearInterval(this.timer);
    await this.pending;
    await this.sample().catch((e) => this.errors.push(String(e.message)));
  }
  summary() {
    const s = this.samples;
    if (!s.length) return { samples: 0 };
    return {
      samples: s.length,
      peakActiveRootRuns: Math.max(...s.map((s) => s.pg.running)),
      peakQueuedRuns: Math.max(...s.map((s) => s.pg.queued)),
      minimumAvailableMiB: Math.min(...s.map((s) => s.host.memoryAvailableBytes)) / 1024 ** 2,
      maximumSwapUsedMiB: Math.max(...s.map((s) => s.host.swapUsedBytes)) / 1024 ** 2,
      walBytes: Number(BigInt(s.at(-1).pg.wal_bytes) - BigInt(s[0].pg.wal_bytes)),
      transactions: Number(BigInt(s.at(-1).pg.transactions) - BigInt(s[0].pg.transactions)),
      maximumLockWaiters: Math.max(...s.map((s) => s.pg.lock_waiters)),
      maximumObservedKafkaLag: Math.max(0, ...s.map((s) => s.kafka?.lag ?? 0)),
      kafkaSamples: new Set(
        s.filter((s) => s.kafka && !s.kafka.unavailable).map((s) => s.kafka.observedAt),
      ).size,
      scrapeMs: s.map((s) => s.finishedAt - s.at),
      headroomStopped: this.abort.signal.aborted,
      collectionErrors: this.errors,
    };
  }
}
