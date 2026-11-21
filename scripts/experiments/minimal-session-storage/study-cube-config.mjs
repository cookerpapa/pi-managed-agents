import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { kubectl, runtimeDirectory, serviceAddress } from "../../lib/production-kubernetes.mjs";
import { nativeCubeLifecycleOptions } from "../../lib/cube-native-lifecycle-options.mjs";

export async function studyCubeConfig() {
  const cfg = JSON.parse(
    await readFile(join(runtimeDirectory, "cubesandbox/cluster.json"), "utf8"),
  );
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
  return {
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
}
