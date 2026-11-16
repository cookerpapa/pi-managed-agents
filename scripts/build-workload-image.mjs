import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { withWorkloadDependencies } from "./lib/workload-dependencies.mjs";

// All remaining arguments are ordinary Docker build options, never shell text.
const args = process.argv.slice(2);
const at = args.findIndex((x) => x === "--file" || x === "-f");
assert(at >= 0 && args[at + 1], "Provide an explicit Dockerfile");
assert(!args.includes("--build-context"), "Named dependency inputs belong to the build adapter");
await withWorkloadDependencies(process.cwd(), args[at + 1], async (inputs) => {
  execFileSync("docker", ["build", ...inputs, ...args], { stdio: "inherit" });
});
