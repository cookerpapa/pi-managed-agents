import assert from "node:assert/strict";
import { randomUUID, createHash } from "node:crypto";
import { mkdir, readFile, writeFile, rm } from "node:fs/promises";
import { resolve } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { build } from "esbuild";
import { parseDocument } from "yaml";
import { parseModelCatalogConfiguration } from "../../packages/protocol/src/model-catalog.ts";
import { createDatabase } from "../../packages/database/src/index.ts";
import {
  kubectl,
  namespace,
  runtimeDirectory,
  serviceAddress,
  repositoryRoot,
} from "./production-kubernetes.mjs";
import { applyProviderConfiguration, providerConfigDigest } from "./provider-config-release.mjs";

const hash = (value) => createHash("sha256").update(value).digest("hex");
const clone = (value) => JSON.parse(JSON.stringify(value));
export function benchmarkCatalog(original, model) {
  const configuration = parseModelCatalogConfiguration(original);
  assert(!configuration.models.some((m) => m.modelId === model), "Benchmark model already exists");
  return parseModelCatalogConfiguration({
    ...configuration,
    models: [
      ...configuration.models,
      {
        provider: "openai-codex",
        modelId: model,
        displayName: "Capacity benchmark (synthetic)",
        visible: true,
        api: "openai-codex-responses",
        thinkingLevels: ["off", "low", "max", "ultra"],
        defaultThinkingLevel: "low",
        fastModeAvailable: false,
        contextWindow: 1000000,
        autoCompactTokenLimit: 850000,
        maxTokens: 8192,
        inputModalities: ["text", "image"],
        hostedTools: [],
      },
    ],
  });
}
export function addBenchmarkProvider(source, { name, url, key, model }) {
  const document = parseDocument(source);
  assert.equal(document.errors.length, 0, "Provider config is invalid");
  assert.equal(document.get("config-version"), 8, "Use the current provider layout");
  const values = document.toJS(),
    groups = values["api-keys"]?.codex ?? [];
  assert(
    Array.isArray(groups) && !groups.some((g) => g.name === name),
    "Owned provider group conflicts",
  );
  document.setIn(
    ["api-keys", "codex"],
    [
      ...groups,
      {
        name,
        "base-url": url,
        "proxy-url": "direct",
        "disable-cooling": true,
        "request-retry": 0,
        models: [{ name: model, alias: model }],
        keys: [{ "api-key": key, websockets: true }],
      },
    ],
  );
  return document.toString();
}
async function wait(read, label, ms = 90000) {
  const end = Date.now() + ms;
  let last;
  while (Date.now() < end) {
    try {
      const value = await read();
      if (value) return value;
    } catch (e) {
      last = e;
    }
    await delay(500);
  }
  throw new Error(`${label} did not become ready`, { cause: last });
}

/** A private receipt exists before configuration changes. Resume cleanup with
 * restoreRuntimeLoadFixture after interruption; never overwrite a foreign edit. */
export async function createRuntimeLoadFixture(directory) {
  await mkdir(directory, { recursive: true, mode: 0o700 });
  const receiptPath = resolve(directory, "fixture-private.json");
  const id = randomUUID(),
    name = "pma-load-" + id,
    model = "pma-benchmark-" + id,
    key = randomUUID() + randomUUID();
  const managementKey = (
    await readFile(resolve(runtimeDirectory, "secrets/cli-proxy-management-key"), "utf8")
  ).trim();
  const baseUrl = `http://${serviceAddress("cli-proxy-api")}:8317`;
  const beforeResponse = await fetch(baseUrl + "/v8/management/config.yaml", {
    headers: { authorization: `Bearer ${managementKey}` },
    signal: AbortSignal.timeout(10000),
  });
  assert(beforeResponse.ok, "Provider configuration read failed");
  const beforeProvider = await beforeResponse.text(),
    beforeEtag = beforeResponse.headers.get("etag");
  assert.equal(
    beforeEtag,
    `"${providerConfigDigest(beforeProvider)}"`,
    "Provider config identity missing",
  );
  const originalMap = JSON.parse(
    kubectl(["get", "configmap/pi-cloud-model-catalog", "-o", "json"]),
  );
  const beforeCatalog = originalMap.data["model-catalog.json"];
  const desiredCatalog = benchmarkCatalog(JSON.parse(beforeCatalog), model);
  const fixture = {
    version: 1,
    name,
    id,
    model,
    key,
    beforeProvider,
    beforeCatalog,
    desiredCatalog,
    sourceCatalogHash: hash(JSON.stringify(JSON.parse(beforeCatalog))),
    phase: "preparing",
    createdAt: new Date().toISOString(),
  };
  const save = () =>
    writeFile(receiptPath, JSON.stringify(fixture, null, 2) + "\n", { mode: 0o600 });
  await writeFile(receiptPath, JSON.stringify(fixture, null, 2) + "\n", {
    mode: 0o600,
    flag: "wx",
  });
  try {
    const outputs = [];
    for (const [entry, label] of [
      ["packages/fake-model-server/src/responses-server.ts", "provider.cjs"],
      ["packages/control-plane/test/mcp/http-fixture.ts", "mcp.cjs"],
    ]) {
      const result = await build({
        entryPoints: [resolve(repositoryRoot, entry)],
        bundle: true,
        write: false,
        platform: "node",
        format: "cjs",
        target: "node24",
        logLevel: "silent",
      });
      outputs.push([label, result.outputFiles[0].text]);
    }
    const main = `const {FakeResponsesServer}=require('./provider.cjs');const {httpMcpFixture}=require('./mcp.cjs');
const server=new FakeResponsesServer({host:'0.0.0.0',port:8089,allowNetwork:true,model:process.env.BENCH_MODEL,apiKey:process.env.BENCH_KEY});
(async()=>{const {deflateSync,crc32}=require('node:zlib');const pixels=Buffer.alloc(512*(1+512*3));let seed=42;for(let y=0;y<512;y++)for(let x=1;x<=512*3;x++){seed^=seed<<13;seed^=seed>>>17;seed^=seed<<5;pixels[y*(1+512*3)+x]=seed&255;}const chunk=(type,data)=>{const body=Buffer.concat([Buffer.from(type),data]);const size=Buffer.alloc(4);size.writeUInt32BE(data.length);const crc=Buffer.alloc(4);crc.writeUInt32BE(crc32(body));return Buffer.concat([size,body,crc]);};const header=Buffer.alloc(13);header.writeUInt32BE(512,0);header.writeUInt32BE(512,4);header[8]=8;header[9]=2;const image=Buffer.concat([Buffer.from([137,80,78,71,13,10,26,10]),chunk('IHDR',header),chunk('IDAT',deflateSync(pixels)),chunk('IEND',Buffer.alloc(0))]);
await server.start();await httpMcpFixture({host:'0.0.0.0',port:8090,control:true,token:process.env.BENCH_KEY,imageData:image.toString('base64')});})().catch(e=>{console.error(e.message);process.exit(1);});`;
    const image = JSON.parse(
      kubectl(["get", "deployment/pi-cloud-tool-host", "-o", "json"]),
    ).spec.template.spec.containers.find((c) => c.name === "tool-host").image;
    const metadata = { name, namespace, labels: { "pma-capacity-fixture": name } };
    const items = [
      {
        apiVersion: "v1",
        kind: "ConfigMap",
        metadata,
        data: Object.fromEntries([...outputs, ["main.cjs", main]]),
      },
      { apiVersion: "v1", kind: "Secret", metadata, stringData: { key } },
      {
        apiVersion: "v1",
        kind: "Service",
        metadata,
        spec: {
          selector: { "pma-capacity-fixture": name },
          ports: [
            { name: "model", port: 8089, targetPort: 8089 },
            { name: "mcp", port: 8090, targetPort: 8090 },
          ],
        },
      },
      {
        apiVersion: "v1",
        kind: "Pod",
        metadata,
        spec: {
          restartPolicy: "Never",
          automountServiceAccountToken: false,
          securityContext: {
            runAsNonRoot: true,
            runAsUser: 10001,
            runAsGroup: 10001,
            seccompProfile: { type: "RuntimeDefault" },
          },
          containers: [
            {
              name: "fixture",
              image,
              imagePullPolicy: "IfNotPresent",
              command: ["node", "/fixture/main.cjs"],
              env: [
                { name: "BENCH_MODEL", value: model },
                { name: "BENCH_KEY", valueFrom: { secretKeyRef: { name, key: "key" } } },
              ],
              resources: {
                requests: { cpu: "100m", memory: "64Mi" },
                limits: { cpu: "2", memory: "512Mi" },
              },
              securityContext: {
                readOnlyRootFilesystem: true,
                allowPrivilegeEscalation: false,
                capabilities: { drop: ["ALL"] },
              },
              readinessProbe: { tcpSocket: { port: 8089 }, periodSeconds: 1 },
              volumeMounts: [{ name: "code", mountPath: "/fixture", readOnly: true }],
            },
          ],
          volumes: [{ name: "code", configMap: { name } }],
        },
      },
    ];
    kubectl(["apply", "-f", "-"], {
      input: JSON.stringify({ apiVersion: "v1", kind: "List", items }),
    });
    kubectl(["wait", "pod/" + name, "--for=condition=Ready", "--timeout=90s"], { timeout: 95000 });
    const ip = JSON.parse(kubectl(["get", "service", name, "-o", "json"])).spec.clusterIP;
    fixture.serviceIp = ip;
    fixture.modelUrl = `http://${name}.${namespace}.svc.cluster.local:8089`;
    fixture.mcpUrl = `http://${name}.${namespace}.svc.cluster.local:8090/mcp`;
    await save();
    await wait(async () => {
      const r = await fetch(`http://${ip}:8089/_fixture/metrics`, {
        headers: { authorization: `Bearer ${key}` },
        signal: AbortSignal.timeout(3000),
      });
      return r.ok;
    }, "Fixture route");
    const desiredSource = addBenchmarkProvider(beforeProvider, {
      name,
      url: fixture.modelUrl,
      key,
      model,
    });
    let desiredProvider;
    try {
      desiredProvider = kubectl(
        ["exec", "-i", "statefulset/cli-proxy-api", "--", "./pi-cloud-config"],
        { input: desiredSource, timeout: 15000 },
      );
    } catch {
      throw new Error("Pinned provider rejected benchmark configuration");
    }
    fixture.desiredProviderHash = providerConfigDigest(desiredProvider);
    fixture.phase = "provider-prepared";
    await save();
    await applyProviderConfiguration({
      baseUrl,
      managementKey,
      plan: {
        version: 1,
        sourceDigest: providerConfigDigest(beforeProvider),
        desiredDigest: fixture.desiredProviderHash,
      },
      body: desiredProvider,
    });
    fixture.phase = "provider-configured";
    await save();
    const map = clone(originalMap);
    map.data["model-catalog.json"] = JSON.stringify(desiredCatalog);
    kubectl(["replace", "-f", "-"], { input: JSON.stringify(map) });
    fixture.phase = "catalog-configured";
    await save();
    const localCatalog = resolve(directory, "catalog-private.json");
    await writeFile(localCatalog, JSON.stringify(desiredCatalog), { mode: 0o600 });
    process.env.PI_CLOUD_MODEL_CATALOG_FILE = localCatalog;
    return runtimeLoadFixtureClient(fixture, directory);
  } catch (error) {
    // The receipt remains available if removal cannot be verified.
    try {
      await restoreRuntimeLoadFixture(directory);
    } catch {}
    throw error;
  }
}

export function runtimeLoadFixtureClient(fixture, directory) {
  const call = async (port, path, body) => {
    const r = await fetch(`http://${fixture.serviceIp}:${port}${path}`, {
      method: body ? "POST" : "GET",
      headers: { authorization: `Bearer ${fixture.key}`, "content-type": "application/json" },
      ...(body ? { body: JSON.stringify(body) } : {}),
      signal: AbortSignal.timeout(10000),
    });
    if (!r.ok) throw new Error("Fixture control request failed");
    return r.json();
  };
  return {
    name: fixture.name,
    model: fixture.model,
    key: fixture.key,
    mcpUrl: fixture.mcpUrl,
    metrics: (summary = false) => call(8089, "/_fixture/metrics" + (summary ? "?summary=1" : "")),
    releaseModel: (id) => call(8089, "/_fixture/release", { id }),
    transport: (body) => call(8089, "/_fixture/transport", body),
    mcpState: () => call(8090, "/fixture/state"),
    mcpTools: (tools) => call(8090, "/fixture/tools", { tools }),
    releaseTools: (text) => call(8090, "/fixture/release", { text }),
    close: () => restoreRuntimeLoadFixture(directory),
  };
}

export async function restoreRuntimeLoadFixture(directory) {
  const path = resolve(directory, "fixture-private.json"),
    f = JSON.parse(await readFile(path, "utf8"));
  assert.equal(f.version, 1);
  assert(/^pma-load-[a-f0-9-]{36}$/.test(f.name));
  if (f.phase === "restored") return { restored: true };
  const u = new URL((await readFile(runtimeDirectory + "/secrets/database-url", "utf8")).trim());
  u.hostname = serviceAddress("postgres");
  const db = createDatabase({ connectionString: u.toString(), maxConnections: 1 });
  try {
    const active = await db
      .selectFrom("runs")
      .select("id")
      .where("model_id", "=", f.model)
      .where("state", "not in", ["completed", "failed", "cancelled", "timed_out"])
      .execute();
    assert.equal(
      active.length,
      0,
      "Settle accepted benchmark Runs before removing their provider route",
    );
  } finally {
    await db.destroy();
  }
  const map = JSON.parse(kubectl(["get", "configmap/pi-cloud-model-catalog", "-o", "json"]));
  const current = JSON.parse(map.data["model-catalog.json"]);
  if (current.models.some((m) => m.modelId === f.model)) {
    current.models = current.models.filter((m) => m.modelId !== f.model);
    assert.equal(
      hash(JSON.stringify(current)),
      f.sourceCatalogHash,
      "Catalog changed outside this fixture; preserve the restoration receipt",
    );
    map.data["model-catalog.json"] = f.beforeCatalog;
    kubectl(["replace", "-f", "-"], { input: JSON.stringify(map) });
  } else
    assert.equal(
      hash(JSON.stringify(current)),
      f.sourceCatalogHash,
      "Catalog no longer matches the fixture receipt",
    );
  const key = (
    await readFile(resolve(runtimeDirectory, "secrets/cli-proxy-management-key"), "utf8")
  ).trim();
  const baseUrl = `http://${serviceAddress("cli-proxy-api")}:8317`;
  const response = await fetch(baseUrl + "/v8/management/config.yaml", {
    headers: { authorization: `Bearer ${key}` },
    signal: AbortSignal.timeout(10000),
  });
  assert(response.ok, "Provider config unavailable during cleanup");
  const body = await response.text(),
    digest = providerConfigDigest(body);
  if (digest !== providerConfigDigest(f.beforeProvider)) {
    assert.equal(
      digest,
      f.desiredProviderHash,
      "Provider changed outside this fixture; preserve the restoration receipt",
    );
    await applyProviderConfiguration({
      baseUrl,
      managementKey: key,
      plan: {
        version: 1,
        sourceDigest: digest,
        desiredDigest: providerConfigDigest(f.beforeProvider),
      },
      body: f.beforeProvider,
    });
  }
  kubectl(
    [
      "delete",
      "pod,service,configmap,secret",
      f.name,
      "--ignore-not-found",
      "--wait=true",
      "--timeout=45s",
    ],
    { timeout: 50000 },
  );
  // Keep only content-free restoration evidence, not an old credential backup.
  await writeFile(
    path,
    JSON.stringify({
      version: 1,
      name: f.name,
      phase: "restored",
      model: f.model,
      restoredAt: new Date().toISOString(),
      sourceCatalogHash: f.sourceCatalogHash,
      providerHash: providerConfigDigest(f.beforeProvider),
    }) + "\n",
    { mode: 0o600 },
  );
  await rm(resolve(directory, "catalog-private.json"), { force: true });
  return { restored: true, modelRemoved: true, providerRestored: true };
}
