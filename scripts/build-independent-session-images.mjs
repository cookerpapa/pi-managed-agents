// Copy the runtime import closure over the maintained dependency images. No
// host node_modules, deployment files, credentials or test programs enter the context.
import { mkdtemp, cp, rm, mkdir, writeFile, readFile } from "node:fs/promises";
import { join, dirname, resolve } from "node:path";
import { tmpdir } from "node:os";
import { createHash } from "node:crypto";
import { createRequire } from "node:module";
import { execFileSync } from "node:child_process";
import { build } from "esbuild";
const root = process.cwd(),
  require = createRequire(import.meta.url);
const tag = process.argv[2];
if (!/^[a-z0-9][a-z0-9_.-]{0,90}$/.test(tag ?? "")) throw Error("Specify an image tag");
const control = process.env.PI_CLOUD_SEGMENT_CONTROL_BASE_IMAGE,
  worker = process.env.PI_CLOUD_SEGMENT_WORKER_BASE_IMAGE;
if (!control || !worker) throw Error("Specify the maintained control-plane and Worker base images");
const segment = "scripts/experiments/minimal-session-storage/";
const sqlAssets = [
  "schema",
  "flow-segments",
  "fork-layout",
  "flow-transcript-points",
  "flow-page-turns",
  "fork-input-assets",
  "subagent-deliveries",
  "independent-session-leases",
  "turn-snapshots",
  "session-inputs",
  "turn-ready",
].map((n) => segment + n + ".sql");
const commit = execFileSync("git", ["rev-parse", "HEAD"], { encoding: "utf8" }).trim();
const dirty = !!execFileSync("git", ["status", "--porcelain"], { encoding: "utf8" }).trim();
const revision = commit + (dirty ? "-dirty" : "");
const lock = JSON.parse(await readFile("package-lock.json", "utf8"));
const verified = new Set();
for (const [role, base, entry, assets] of [
  [
    "migrate",
    control,
    "scripts/serving/independent-session/migrate.mjs",
    [...sqlAssets, "scripts/serving/independent-session/turn-references.sql"],
  ],
  ["subagents", control, "packages/control-plane/src/independent-session-subagents-main.mjs", []],
  ["tool-host", control, "packages/control-plane/src/independent-session-tool-host-main.mjs", []],
  ["api", control, "packages/control-plane/src/independent-session-api-main.mjs", []],
  ["projector", control, "packages/control-plane/src/independent-session-projector-main.mjs", []],
  [
    "worker",
    worker,
    "packages/supervisor-host/src/independent-session-worker-main.mjs",
    [segment + "segment-history-sqlite.mjs"],
  ],
]) {
  const out = await mkdtemp(join(tmpdir(), "pma-role-"));
  try {
    const result = await build({
      entryPoints: [entry],
      bundle: true,
      platform: "node",
      format: "esm",
      packages: "external",
      write: false,
      metafile: true,
      logLevel: "silent",
      plugins: [
        {
          name: "workspace-imports",
          setup(b) {
            b.onResolve({ filter: /^@pi-cloud\// }, (args) => ({
              path: require.resolve(args.path, { paths: [dirname(args.importer)] }),
            }));
          },
        },
      ],
    });
    if (!verified.has(base)) {
      const deps = [
        "@earendil-works/pi-durable",
        "@earendil-works/pi-ai",
        "@confluentinc/kafka-javascript",
        "pg",
        "tsx",
      ];
      const installed = JSON.parse(
        execFileSync(
          "docker",
          [
            "run",
            "--rm",
            "--entrypoint",
            "node",
            base,
            "-e",
            "console.log(JSON.stringify(process.argv.slice(1).map(n=>require('/app/node_modules/'+n+'/package.json').version)))",
            ...deps,
          ],
          { encoding: "utf8" },
        ),
      );
      for (const [i, n] of deps.entries())
        if (installed[i] !== lock.packages["node_modules/" + n]?.version)
          throw Error("Base image dependency differs from lockfile: " + n);
      verified.add(base);
    }
    const files = new Set([
      ...Object.keys(result.metafile.inputs),
      ...assets,
      "package.json",
      "tsconfig.base.json",
      "deploy/helm/pi-cloud-platform/files/model-catalog.json",
    ]);
    for (const path of [...files]) {
      if (path.startsWith("..") || path.startsWith("/") || path.includes("node_modules/"))
        throw Error("Non-workspace role source: " + path);
      if (path.startsWith("packages/"))
        files.add(path.split("/").slice(0, 2).join("/") + "/package.json");
      if (
        path.includes("/test/") ||
        path.includes("faux") ||
        path.endsWith("/fixture.mjs") ||
        path.endsWith(".spec.mjs")
      )
        throw Error("Test dependency in role: " + path);
      if (role === "worker" && path.startsWith("packages/control-plane/"))
        throw Error("Worker imported the control plane: " + path);
    }
    const digest = createHash("sha256");
    for (const path of [...files].sort()) {
      digest
        .update(path + "\0")
        .update(await readFile(path))
        .update("\0");
      const dest = join(out, "src", path);
      await mkdir(dirname(dest), { recursive: true });
      await cp(resolve(root, path), dest);
    }
    const version = execFileSync("docker", ["image", "inspect", base, "--format", "{{.Id}}"], {
      encoding: "utf8",
    }).trim();
    await writeFile(
      join(out, "Dockerfile"),
      [
        "ARG BASE",
        "FROM ${BASE}",
        "WORKDIR /app",
        "COPY src /app",
        "LABEL org.opencontainers.image.revision=" +
          revision +
          " org.opencontainers.image.base.digest=" +
          version +
          " io.pi-managed-agents.source-sha256=" +
          digest.digest("hex"),
        "CMD " + JSON.stringify(["/app/" + entry]),
        "",
      ].join("\n"),
    );
    execFileSync(
      "docker",
      [
        "build",
        "--build-arg",
        "BASE=" + base,
        "-t",
        "pi-managed-agents/segment-" + role + ":" + tag,
        out,
      ],
      { stdio: "inherit" },
    );
  } finally {
    await rm(out, { recursive: true, force: true });
  }
}

const webBase = process.env.PI_CLOUD_SEGMENT_WEB_BASE_IMAGE;
if (webBase) {
  execFileSync("npm", ["run", "build", "--workspace", "@pi-cloud/web-ui"], { stdio: "inherit" });
  const out = await mkdtemp(join(tmpdir(), "pma-web-"));
  try {
    await cp("packages/web-ui/dist", join(out, "dist"), { recursive: true });
    await cp("packages/web-ui/Caddyfile", join(out, "Caddyfile"));
    await writeFile(
      join(out, "Dockerfile"),
      "ARG BASE\nFROM ${BASE}\nCOPY Caddyfile /etc/caddy/Caddyfile\nCOPY dist /srv\nLABEL org.opencontainers.image.revision=" +
        revision +
        "\n",
    );
    execFileSync(
      "docker",
      [
        "build",
        "--build-arg",
        "BASE=" + webBase,
        "-t",
        "pi-managed-agents/segment-web:" + tag,
        out,
      ],
      { stdio: "inherit" },
    );
  } finally {
    await rm(out, { recursive: true, force: true });
  }
}
