import { expect, it } from "vitest";
import { reviewDevelopmentAudit } from "./development-audit-review.mjs";

function fixture() {
  const leaf = {
    name: "picomatch",
    dependency: "picomatch",
    url: "https://github.com/advisories/GHSA-fc5q-39xc-vjpf",
    severity: "high",
  };
  return {
    audit: {
      vulnerabilities: {
        picomatch: {
          name: "picomatch",
          severity: "high",
          via: [leaf],
          nodes: ["node_modules/picomatch"],
        },
        vite: { name: "vite", severity: "high", via: ["picomatch"], nodes: ["node_modules/vite"] },
      },
    },
    lock: {
      packages: {
        "node_modules/picomatch": { version: "4.0.5", dev: true },
        "node_modules/vite": { version: "7.3.7", dev: true },
      },
    },
  };
}
it("reviews only the exact development leaf and its fully explained derived findings", () => {
  const f = fixture(),
    before = JSON.stringify(f.audit),
    r = reviewDevelopmentAudit(f.audit, f.lock);
  expect(r.blocking).toEqual([]);
  expect(r.reviewed.map((v) => v.name)).toEqual(["picomatch", "vite"]);
  expect(JSON.stringify(f.audit)).toBe(before);
});
it.each(["picomatch", "vite"])("does not review runtime reachability through %s", (name) => {
  const f = fixture();
  f.lock.packages["node_modules/" + name].dev = false;
  expect(reviewDevelopmentAudit(f.audit, f.lock).blocking.some((v) => v.name === name)).toBe(true);
});
it("rejects another version or advisory", () => {
  const f = fixture();
  f.lock.packages["node_modules/picomatch"].version = "4.0.7";
  expect(reviewDevelopmentAudit(f.audit, f.lock).blocking).toHaveLength(2);
  f.lock.packages["node_modules/picomatch"].version = "4.0.5";
  f.audit.vulnerabilities.picomatch.via[0].url = "https://github.com/advisories/another";
  expect(reviewDevelopmentAudit(f.audit, f.lock).blocking).toHaveLength(2);
});

it("does not treat mixed optional/runtime reachability as exclusively development", () => {
  const f = fixture();
  f.lock.packages["node_modules/picomatch"].devOptional = true;
  expect(reviewDevelopmentAudit(f.audit, f.lock).blocking).toHaveLength(2);
});
it("keeps an unrelated high finding and unresolved evidence blocking", () => {
  const f = fixture();
  f.audit.vulnerabilities.vite.via.push({
    name: "vite",
    dependency: "vite",
    severity: "high",
    url: "another",
  });
  expect(reviewDevelopmentAudit(f.audit, f.lock).blocking.map((v) => v.name)).toEqual(["vite"]);
  f.audit.vulnerabilities.vite.via = ["missing"];
  expect(reviewDevelopmentAudit(f.audit, f.lock).blocking.map((v) => v.name)).toEqual(["vite"]);
  f.audit.vulnerabilities.vite.via = ["vite"];
  expect(reviewDevelopmentAudit(f.audit, f.lock).blocking.map((v) => v.name)).toEqual(["vite"]);
});
