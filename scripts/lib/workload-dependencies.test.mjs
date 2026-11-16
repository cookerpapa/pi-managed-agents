import { expect, it } from "vitest";
import { readFile, mkdtemp, mkdir, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { execFileSync } from "node:child_process";
import { workloadDependencyInputs, withWorkloadDependencies } from "./workload-dependencies.mjs";
import { dockerSourceInputs } from "./component-release.mjs";
import { artifactInputIdentity } from "./release-plan.mjs";

const read = (path) => readFile(path, "utf8");
const jsonRead = (changes) => async (path) => {
  const text = await read(path);
  return changes[path] ? JSON.stringify(changes[path](JSON.parse(text))) : text;
};
const workloads = [
  "packages/control-plane/Dockerfile",
  "packages/supervisor-host/Dockerfile",
  "packages/sandbox-control/Dockerfile.volume-gateway",
  "packages/ssh-gateway/Dockerfile",
  "packages/web-ui/Dockerfile",
  "packages/device-link/Dockerfile",
  "deploy/cubesandbox/Dockerfile.tool",
];

it("keeps unrelated fake-provider and root test dependencies out of every workload input", async () => {
  const changed = jsonRead({
    "package.json": (p) => {
      p.devDependencies.vitest = "4.1.12";
      return p;
    },
    "package-lock.json": (p) => {
      p.packages[""].devDependencies.vitest = "4.1.12";
      p.packages["packages/fake-model-server"].devDependencies.ws = "8.21.2";
      return p;
    },
  });
  for (const dockerfile of workloads) {
    const original = await workloadDependencyInputs(read, dockerfile);
    expect(await workloadDependencyInputs(changed, dockerfile), dockerfile).toEqual(original);
    const lock = JSON.parse(original.get("package-lock.json"));
    expect(lock.packages["packages/fake-model-server"]).toBeUndefined();
    expect(lock.packages["node_modules/vitest"]).toBeUndefined();
    expect(lock.packages["node_modules/turbo"]).toBeUndefined();
  }
}, 30_000);

it("retains a relevant optional/native dependency change and refuses source/lock drift", async () => {
  const dockerfile = "deploy/cubesandbox/Dockerfile.tool";
  const before = await workloadDependencyInputs(read, dockerfile);
  const changed = jsonRead({
    "package-lock.json": (p) => {
      p.packages["node_modules/@img/sharp-linux-x64"].integrity = "sha512-fixture-native-change";
      return p;
    },
  });
  const after = await workloadDependencyInputs(changed, dockerfile);
  expect(after).not.toEqual(before);
  expect(after.get("package-lock.json")).toContain("sha512-fixture-native-change");
  const drift = jsonRead({
    "packages/tool-sandbox/package.json": (p) => {
      p.dependencies.sharp = "0.35.6";
      return p;
    },
  });
  await expect(workloadDependencyInputs(drift, dockerfile)).rejects.toThrow("Source lock drift");
});

it("supplies exactly the hashed inputs to Docker and removes them even after build failure", async () => {
  const dockerfile = "packages/supervisor-host/Dockerfile";
  const expected = await workloadDependencyInputs(read, dockerfile);
  let directory;
  await expect(
    withWorkloadDependencies(process.cwd(), dockerfile, async (args) => {
      expect(args[0]).toBe("--build-context");
      directory = args[1].slice("pma-dependencies=".length);
      for (const [path, bytes] of expected)
        expect(await readFile(directory + "/" + path, "utf8")).toBe(bytes);
      await expect(
        readFile(directory + "/deploy/production/runtime/secrets/database-url"),
      ).rejects.toMatchObject({ code: "ENOENT" });
      throw new Error("fixture build failure");
    }),
  ).rejects.toThrow("fixture build failure");
  await expect(readFile(directory + "/package.json")).rejects.toMatchObject({ code: "ENOENT" });
});

it("keeps Projector history source outside the Worker Docker inputs", async () => {
  const file = "packages/supervisor-host/Dockerfile";
  const tracked = execFileSync("git", ["ls-files"], { encoding: "utf8" }).trim().split("\n");
  const inputs = dockerSourceInputs(file, tracked, await read(file));
  expect(inputs).toContain("packages/runtime-core/src/direct-session-kafka.ts");
  for (const path of [
    "frozen-history",
    "frozen-history-window",
    "postgres-durable-commit-projector",
    "session-projector",
  ])
    expect(inputs).not.toContain(`packages/runtime-core/src/${path}.ts`);
  expect(inputs).not.toContain("package-lock.json");
});

it("uses generated dependency bytes when comparing committed release inputs", async () => {
  const root = await mkdtemp(join(tmpdir(), "pma-dependency-identity-"));
  const git = (...args) =>
    execFileSync(
      "git",
      [
        "-C",
        root,
        "-c",
        "core.hooksPath=/dev/null",
        "-c",
        "commit.gpgsign=false",
        "-c",
        "user.name=Fixture",
        "-c",
        "user.email=fixture@example.invalid",
        ...args,
      ],
      { encoding: "utf8" },
    ).trim();
  const commit = () => {
    git("add", ".");
    git("commit", "-m", "fixture");
    return git("rev-parse", "HEAD");
  };
  try {
    git("init", "--initial-branch=main");
    for (const path of [
      "package.json",
      "package-lock.json",
      "scripts/lib/workload-dependencies.mjs",
      "packages/protocol/package.json",
      "packages/tool-sandbox/package.json",
      "packages/workspace-runtime/package.json",
    ]) {
      await mkdir(dirname(join(root, path)), { recursive: true });
      await writeFile(join(root, path), await read(path));
    }
    await writeFile(join(root, ".dockerignore"), "**\n");
    await writeFile(
      join(root, "Dockerfile"),
      "FROM fixture\nCOPY --from=pma-dependencies package.json package-lock.json ./\n" +
        ["protocol", "tool-sandbox", "workspace-runtime"]
          .map(
            (p) =>
              `COPY --from=pma-dependencies packages/${p}/package.json packages/${p}/package.json\n`,
          )
          .join(""),
    );
    const before = await artifactInputIdentity(root, commit(), "Dockerfile");
    const lock = JSON.parse(await readFile(join(root, "package-lock.json"), "utf8"));
    lock.packages["packages/fake-model-server"].devDependencies.ws = "8.21.2";
    await writeFile(join(root, "package-lock.json"), JSON.stringify(lock));
    expect((await artifactInputIdentity(root, commit(), "Dockerfile")).identity).toBe(
      before.identity,
    );
    lock.packages["node_modules/@img/sharp-linux-x64"].integrity = "sha512-native-fixture-change";
    await writeFile(join(root, "package-lock.json"), JSON.stringify(lock));
    expect((await artifactInputIdentity(root, commit(), "Dockerfile")).identity).not.toBe(
      before.identity,
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
