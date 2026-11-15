import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { transform } from "esbuild";
import { setTimeout as delay } from "node:timers/promises";
import { kubectl, namespace } from "./production-kubernetes.mjs";

/** Owned integration-test server inside the existing cluster, with no platform
 * credentials or mounted user data. This is not production MCP server hosting. */
export async function createMcpLiveFixture(token) {
  const name = "pma-mcp-fixture-" + randomUUID();
  const code = await transform(
    await readFile(
      new URL("../../packages/control-plane/test/mcp/http-fixture.ts", import.meta.url),
      "utf8",
    ),
    { loader: "ts", format: "esm", target: "node24" },
  );
  const image = JSON.parse(
    kubectl(["get", "deployment/pi-cloud-tool-host", "-o", "json"]),
  ).spec.template.spec.containers.find((c) => c.name === "tool-host").image;
  const metadata = { name, namespace, labels: { "pma-acceptance": name } };
  const resources = [
    {
      apiVersion: "v1",
      kind: "ConfigMap",
      metadata,
      data: {
        "server.mjs": code.code,
        "main.mjs":
          "import {httpMcpFixture} from './server.mjs'; await httpMcpFixture({host:'0.0.0.0',port:8089,control:true,token:process.env.MCP_FIXTURE_TOKEN});",
      },
    },
    { apiVersion: "v1", kind: "Secret", metadata, stringData: { token } },
    {
      apiVersion: "v1",
      kind: "Service",
      metadata,
      spec: { selector: { "pma-acceptance": name }, ports: [{ port: 8089, targetPort: 8089 }] },
    },
    {
      apiVersion: "v1",
      kind: "Pod",
      metadata,
      spec: {
        restartPolicy: "Never",
        automountServiceAccountToken: false,
        securityContext: {
          runAsUser: 10001,
          runAsGroup: 10001,
          runAsNonRoot: true,
          seccompProfile: { type: "RuntimeDefault" },
        },
        containers: [
          {
            name: "fixture",
            image,
            imagePullPolicy: "IfNotPresent",
            command: ["node", "/fixture/main.mjs"],
            env: [
              { name: "MCP_FIXTURE_TOKEN", valueFrom: { secretKeyRef: { name, key: "token" } } },
            ],
            resources: {
              requests: { cpu: "10m", memory: "32Mi" },
              limits: { cpu: "1", memory: "256Mi" },
            },
            securityContext: {
              allowPrivilegeEscalation: false,
              readOnlyRootFilesystem: true,
              capabilities: { drop: ["ALL"] },
            },
            readinessProbe: { tcpSocket: { port: 8089 }, periodSeconds: 1 },
            volumeMounts: [{ name: "fixture", mountPath: "/fixture", readOnly: true }],
          },
        ],
        volumes: [{ name: "fixture", configMap: { name } }],
      },
    },
  ];
  const close = async () => {
    kubectl(
      [
        "delete",
        "pod,service,configmap,secret",
        name,
        "--ignore-not-found",
        "--wait=true",
        "--timeout=45s",
      ],
      { timeout: 50000 },
    );
  };
  try {
    kubectl(["apply", "-f", "-"], {
      input: JSON.stringify({ apiVersion: "v1", kind: "List", items: resources }),
    });
    kubectl(["wait", "pod/" + name, "--for=condition=Ready", "--timeout=90s"], { timeout: 95000 });
    const ip = JSON.parse(kubectl(["get", "service", name, "-o", "json"])).spec.clusterIP;
    const request = async (path, body) => {
      const response = await fetch(`http://${ip}:8089/fixture/${path}`, {
        method: body ? "POST" : "GET",
        headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
        ...(body ? { body: JSON.stringify(body) } : {}),
        signal: AbortSignal.timeout(5000),
      });
      if (!response.ok) throw new Error("Owned MCP fixture request failed");
      return response.json();
    };
    // Pod readiness can precede EndpointSlice/kube-proxy propagation. Only the
    // read-only fixture health request is retried; never an MCP tools/call.
    const deadline = Date.now() + 45000;
    for (;;) {
      try {
        await request("state");
        break;
      } catch (error) {
        if (Date.now() >= deadline) throw error;
        await delay(500);
      }
    }
    return {
      name,
      url: `http://${name}.${namespace}.svc.cluster.local:8089/mcp`,
      state: () => request("state"),
      change: (tools) => request("tools", { tools }),
      release: (text = "ORIGINAL_MCP_REPLY") => request("release", { text }),
      close,
    };
  } catch (error) {
    await close().catch(() => {});
    throw error;
  }
}
