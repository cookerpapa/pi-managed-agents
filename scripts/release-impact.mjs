import { execFileSync } from "node:child_process";
import { appendFileSync } from "node:fs";
import { releaseImpact } from "./lib/release-impact.mjs";
let files;
const base = process.env.CI_BASE_SHA,
  head = process.env.CI_HEAD_SHA ?? "HEAD";
if (
  /^[a-f0-9]{40}$/.test(base ?? "") &&
  !/^0+$/.test(base) &&
  (/^[a-f0-9]{40}$/.test(head) || head === "HEAD")
) {
  try {
    files = execFileSync(
      "git",
      ["diff", "--name-only", "--no-renames", "-z", `${base}...${head}`],
      { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] },
    )
      .split("\0")
      .filter(Boolean);
  } catch {
    /* Missing history must select the full gate. */
  }
}
const result = releaseImpact(files);
// Source policy selects the required gates; actual build inputs select images.
// Missing history or generation evidence keeps the conservative full matrix.
if (files && result.buildImages) {
  try {
    const { artifactInputIdentity } = await import("./lib/release-plan.mjs");
    const before = execFileSync("git", ["merge-base", base, head], { encoding: "utf8" }).trim();
    const after = execFileSync("git", ["rev-parse", head], { encoding: "utf8" }).trim();
    const changed = [];
    for (const artifact of result.matrix.include) {
      const a = await artifactInputIdentity(process.cwd(), before, artifact.dockerfile);
      const b = await artifactInputIdentity(process.cwd(), after, artifact.dockerfile);
      if (a.identity !== b.identity) changed.push(artifact);
    }
    result.matrix = { include: changed };
    result.buildImages = changed.length > 0;
  } catch (error) {
    console.error("Keeping full image verification: " + error.message);
  }
}
if (process.env.GITHUB_OUTPUT)
  for (const [key, value] of Object.entries(result))
    appendFileSync(
      process.env.GITHUB_OUTPUT,
      `${key}=${typeof value === "string" ? value : JSON.stringify(value)}\n`,
    );
console.log(JSON.stringify(result));
