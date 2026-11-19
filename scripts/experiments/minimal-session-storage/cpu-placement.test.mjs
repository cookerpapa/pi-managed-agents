import assert from "node:assert/strict";
import test from "node:test";
import { cpus, listCpus, placementPlan } from "./cpu-placement-fixture.mjs";
import { cpuReplayCases } from "./pg-path-trials.mjs";

test("CPU masks preserve whole SMT cores and the same combined budget", () => {
  const groups = Array.from({ length: 16 }, (_, i) => [2 * i, 2 * i + 1]);
  const p = placementPlan(groups.flatMap((g) => [g, g]));
  assert.deepEqual(cpus(p.shared), cpus("8-23"));
  assert.equal(cpus(p.a).length, 8);
  assert.equal(cpus(p.b).length, 8);
  assert(cpus(p.a).every((n) => !cpus(p.b).includes(n)));
  assert(cpus(p.support).every((n) => !cpus(p.shared).includes(n)));
  assert.equal(listCpus([...cpus(p.a), ...cpus(p.b)]), p.shared);
  for (const g of groups)
    assert([p.a, p.b, p.support].some((list) => g.every((n) => cpus(list).includes(n))));
  assert.throws(() => cpus("4-1"));
  assert.throws(() => placementPlan(groups.slice(0, 6)));
});
test("the independent primary series counterbalance sharing and swap both split placements", () => {
  for (const reverse of [false, true]) {
    const trials = cpuReplayCases(false, reverse);
    const main = reverse ? trials.filter((t) => t[3] !== "open") : trials.slice(0, 8);
    assert(main.every((t) => t[0] === 260 && t[1] === "projector" && t[2]));
    const layouts = main.map((t) => t[3]);
    assert.equal(layouts.filter((v) => v === "shared").length, 4);
    for (const side of ["ab", "ba"]) assert.equal(layouts.filter((v) => v === side).length, 2);
    assert.equal(trials.filter((t) => t[3] === "open").length, reverse ? 2 : 0);
  }
});
