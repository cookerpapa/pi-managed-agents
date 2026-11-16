import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtemp, mkdir, readFile, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

const execute = promisify(execFile);
const turbo = fileURLToPath(new URL("../../node_modules/turbo/bin/turbo", import.meta.url));
export const dependencyContextName = "pma-dependencies";
export const dependencyRecipe = "scripts/lib/workload-dependencies.mjs";
const cache = new Map();
const stable = (value) =>
  Array.isArray(value)
    ? value.map(stable)
    : value && typeof value === "object"
      ? Object.fromEntries(
          Object.keys(value)
            .sort()
            .map((k) => [k, stable(value[k])]),
        )
      : value;
const json = (value) => JSON.stringify(stable(value), null, 2) + "\n";

/** Docker's named input is generated from the authoritative npm lockfile.
 * Turbo owns graph pruning; this adapter selects roots/build dependencies only. */
export async function workloadDependencyInputs(read, dockerfile) {
  const source = await read(dockerfile);
  const declarations = source
    .replace(/\\\r?\n\s*/g, " ")
    .split("\n")
    .filter((line) => line.startsWith(`COPY --from=${dependencyContextName} `));
  if (!declarations.length) return undefined;
  const paths = [
    ...new Set(declarations.flatMap((line) => line.trim().split(/\s+/).slice(2, -1))),
  ].sort();
  assert(
    paths.includes("package.json") && paths.includes("package-lock.json"),
    "Declare both generated npm inputs",
  );
  const workspaces = paths.filter((p) => /^packages\/[a-z0-9-]+\/package\.json$/.test(p));
  assert.equal(paths.length, workspaces.length + 2, "Unexpected dependency-context COPY");
  assert(workspaces.length, "Declare workload workspace manifests");
  const root = JSON.parse(await read("package.json"));
  const lock = JSON.parse(await read("package-lock.json"));
  assert.deepEqual(
    stable(root.dependencies ?? {}),
    stable(lock.packages[""].dependencies ?? {}),
    "Root production lock drift",
  );
  const version = root.devDependencies?.turbo;
  assert(/^\d+\.\d+\.\d+$/.test(version), "Pin the pruning tool in the source manifest");
  assert.equal(
    lock.packages["node_modules/turbo"]?.version,
    version,
    "Pruning tool lock does not match its pin",
  );
  root.pmaBuildToolchain = {
    turbo: { version, integrity: lock.packages["node_modules/turbo"].integrity },
  };
  const buildWorkspaces = source.match(/^# pma-build-workspaces: (.+)$/m)?.[1].split(/\s+/) ?? [];
  const buildRoot = source.match(/^# pma-build-root: (.+)$/m)?.[1].split(/\s+/) ?? [];
  const manifests = new Map();
  for (const path of workspaces) {
    const manifest = JSON.parse(await read(path));
    for (const field of [
      "version",
      "dependencies",
      "optionalDependencies",
      "peerDependencies",
      "devDependencies",
    ])
      assert.deepEqual(
        stable(manifest[field]),
        stable(lock.packages[dirname(path)]?.[field]),
        `Source lock drift: ${path} ${field}`,
      );
    if (!buildWorkspaces.includes(manifest.name)) delete manifest.devDependencies;
    manifests.set(path, manifest);
  }
  for (const name of buildWorkspaces)
    assert(
      [...manifests.values()].some((m) => m.name === name),
      "Unknown build workspace",
    );
  const roots = [...manifests.values()].map((p) => p.name);
  for (const manifest of manifests.values())
    for (const name of Object.keys({
      ...manifest.dependencies,
      ...manifest.optionalDependencies,
      ...manifest.devDependencies,
    }))
      if (name.startsWith("@pi-cloud/"))
        assert(roots.includes(name), `Missing workspace COPY: ${name}`);
  delete root.scripts;
  root.devDependencies = Object.fromEntries(
    buildRoot.map((name) => {
      const pin = root.devDependencies?.[name];
      assert(pin && /^\d+\.\d+\.\d+$/.test(pin), `Pin build dependency ${name}`);
      return [name, pin];
    }),
  );
  root.workspaces = workspaces.map((p) => dirname(p));
  const snapshot = structuredClone(lock);
  snapshot.packages[""].workspaces = root.workspaces;
  snapshot.packages[""].devDependencies = root.devDependencies;
  delete snapshot.packages[""].hasInstallScript;
  for (const [path, manifest] of manifests) {
    const entry = snapshot.packages[dirname(path)];
    assert(
      entry?.name === undefined || entry.name === manifest.name,
      "Workspace lock identity changed",
    );
    if (manifest.devDependencies) entry.devDependencies = manifest.devDependencies;
    else delete entry.devDependencies;
  }
  const key = createHash("sha256")
    .update(json({ root, snapshot, manifests: [...manifests], version }))
    .digest("hex");
  if (!cache.has(key)) cache.set(key, prune());
  return new Map(await cache.get(key));

  async function prune() {
    const installed = (await execute(process.execPath, [turbo, "--version"])).stdout.trim();
    assert.equal(installed, version, "Install the source-pinned Turborepo before building");
    const directory = await mkdtemp(join(tmpdir(), "pma-workload-deps-"));
    try {
      const input = join(directory, "input"),
        output = join(directory, "output");
      await mkdir(input);
      await writeFile(join(input, "package.json"), json(root));
      await writeFile(join(input, "package-lock.json"), json(snapshot));
      for (const [path, manifest] of manifests) {
        await mkdir(dirname(join(input, path)), { recursive: true });
        await writeFile(join(input, path), json(manifest));
      }
      await execute(
        process.execPath,
        [turbo, "prune", ...roots, "--docker", `--cwd=${input}`, `--out-dir=${output}`],
        {
          env: { ...process.env, TURBO_TELEMETRY_DISABLED: "1" },
          timeout: 60_000,
          maxBuffer: 1024 * 1024,
        },
      );
      const pruned = JSON.parse(await readFile(join(output, "json/package-lock.json"), "utf8"));
      assert.equal(pruned.lockfileVersion, lock.lockfileVersion);
      const retainedWorkspaces = Object.keys(pruned.packages)
        .filter((p) => /^packages\/[^/]+$/.test(p))
        .sort();
      assert.deepEqual(
        retainedWorkspaces,
        root.workspaces.toSorted(),
        "Pruning changed declared workspace closure",
      );
      const packageName = (path) =>
        path.slice(path.lastIndexOf("node_modules/") + "node_modules/".length);
      for (const [path, pkg] of Object.entries(pruned.packages)) {
        if (!path.includes("node_modules/") || pkg.link) continue;
        // Pruning may hoist a formerly nested dependency into the smaller tree.
        // Its package bytes must still be an exact version from the source lock.
        assert(
          Object.entries(lock.packages).some(
            ([originalPath, original]) =>
              originalPath.includes("node_modules/") &&
              packageName(originalPath) === packageName(path) &&
              ["version", "resolved", "integrity"].every((field) => pkg[field] === original[field]),
          ),
          `Pruning introduced an unpinned dependency: ${path}`,
        );
      }
      const result = new Map([["package-lock.json", json(pruned)]]);
      for (const path of paths.filter((p) => p !== "package-lock.json"))
        result.set(path, json(JSON.parse(await readFile(join(output, "json", path), "utf8"))));
      return result;
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  }
}

export async function withWorkloadDependencies(root, dockerfile, run) {
  const read = (path) => readFile(join(root, path), "utf8");
  const inputs = await workloadDependencyInputs(read, dockerfile);
  if (!inputs) return run([]);
  const directory = await mkdtemp(join(tmpdir(), "pma-docker-deps-"));
  try {
    for (const [path, content] of inputs) {
      await mkdir(dirname(join(directory, path)), { recursive: true });
      await writeFile(join(directory, path), content, { mode: 0o644 });
    }
    return await run(["--build-context", `${dependencyContextName}=${directory}`]);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}
