// Trusted operator container supplies Kubernetes broker DNS aliases. Only the
// disposable PG URL and owned Session identity are given to experimental Workers.
import assert from "node:assert/strict";
import { mkdtemp, readFile, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve, join, dirname } from "node:path";
import { spawn } from "node:child_process";
import { testPostgres } from "../../lib/test-postgres.mjs";
import {
  kubectl,
  runtimeDirectory,
  serviceAddress,
  repositoryRoot,
} from "../../lib/production-kubernetes.mjs";
import { nativeCubeLifecycleOptions } from "../../lib/cube-native-lifecycle-options.mjs";
import { realpath } from "node:fs/promises";
import { testKafka } from "../../lib/test-kafka.mjs";
const output = process.argv[2],
  bashFields = process.argv[3] === "--bash-fields",
  pgHandoff = bashFields || process.argv[3] === "--pg-handoff",
  serving = pgHandoff || process.argv[3] === "--serving";
let isolatedKafka;
assert(output && process.env.PI_CLOUD_RUNTIME_LOAD_CHECK === "1");
process.umask(0o077);
const cfg = JSON.parse(await readFile(join(runtimeDirectory, "cubesandbox/cluster.json"), "utf8"));
const template = JSON.parse(
  await readFile(join(runtimeDirectory, "cubesandbox/template.json"), "utf8"),
).agent.templateId;
const runtime = JSON.parse(kubectl(["get", "deployment/pi-cloud-session-runtime", "-o", "json"]));
const completionUrl = runtime.spec.template.spec.containers[0].env.find(
  (e) => e.name === "PI_CLOUD_TOOL_COMPLETION_URL",
).value;
const completionIp = serviceAddress("tool-completion");
const secret = async (name) =>
  (await readFile(join(runtimeDirectory, "secrets", name), "utf8")).trim();
const cube = {
  templateId: template,
  client: {
    ...nativeCubeLifecycleOptions(),
    apiUrl: "http://" + cfg.api.host + ":" + cfg.api.port,
    apiKey: await secret("cubesandbox-api-key"),
    proxyNodeIp: cfg.proxy.host,
    proxyPort: cfg.proxy.port,
    proxyScheme: "http",
    sandboxDomain: cfg.sandboxDomain,
    egressProxyIp: "10.255.255.254",
    completionIp,
  },
  issuer: {
    url: completionUrl,
    address: completionIp,
    secret: await secret("supervisor-management-token"),
    ca: Buffer.from(
      kubectl([
        "get",
        "secret",
        "pi-cloud-tool-completion-tls",
        "-o",
        "jsonpath={.data.ca\\.crt}",
      ]).trim(),
      "base64",
    ).toString(),
  },
};
const tmp = await mkdtemp(join(tmpdir(), "pma-family-")),
  pg = await testPostgres({});
try {
  if (serving) isolatedKafka = await testKafka({});
  const names = ["kafka-1", "kafka-2", "kafka-3"];
  await writeFile(
    join(tmp, "config"),
    JSON.stringify({
      brokers: names.map((n) => n + ":9092"),
      dbUrl: pg.url,
      cube,
      servingBrokers: isolatedKafka?.brokers,
      pgHandoff,
      bashFields,
      scene: process.argv[4],
    }),
    { mode: 0o600 },
  );
  const modules = dirname(await realpath(join(repositoryRoot, "node_modules", "tsx")));
  const image = JSON.parse(
    await readFile(join(repositoryRoot, "deploy/cubesandbox/test-images.json"), "utf8"),
  ).operator;
  const args = [
    "run",
    "--rm",
    "--network",
    "host",
    "--user",
    process.getuid() + ":" + process.getgid(),
    ...names.flatMap((n) => ["--add-host", n + ":" + serviceAddress(n)]),
    "--mount",
    "type=bind,src=" + repositoryRoot + ",dst=" + repositoryRoot + ",readonly",
    ...(modules === join(repositoryRoot, "node_modules")
      ? []
      : ["--mount", "type=bind,src=" + modules + ",dst=" + modules + ",readonly"]),
    "--mount",
    "type=bind,src=" + resolve(dirname(process.execPath), "..") + ",dst=/opt/node,readonly",
    "--mount",
    "type=bind,src=" + tmp + ",dst=/study",
    "--workdir",
    repositoryRoot,
    "--env",
    "PATH=/opt/node/bin:/usr/bin:/bin",
    "--env",
    "TSX_TSCONFIG_PATH=" + join(repositoryRoot, "packages/control-plane/tsconfig.json"),
    image,
    "/opt/node/bin/node",
    "--import",
    "tsx",
    "scripts/experiments/minimal-session-storage/" +
      (serving ? "serving-family-study" : "retained-family-study") +
      ".mjs",
    "/study/config",
    "/study/report.json",
  ];
  const p = spawn("docker", args, { stdio: "inherit" });
  const code = await new Promise((r, j) => {
    p.once("error", j);
    p.once("exit", r);
  });
  assert.equal(code, 0, "Isolated family handoff failed");
  await writeFile(output, await readFile(join(tmp, "report.json")), { mode: 0o600 });
} finally {
  try {
    try {
      await isolatedKafka?.close();
    } finally {
      await pg.close();
    }
  } finally {
    await rm(tmp, { recursive: true, force: true });
  }
}
