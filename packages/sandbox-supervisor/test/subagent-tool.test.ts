import { infrastructureToolDefinitions, prepareSubagentArguments } from "@pi-cloud/protocol";
import { expect, it } from "vitest";
import { Value } from "typebox/value";

it.each(["x".repeat(100001), "汉".repeat(100000), "\u0001".repeat(50000)])(
  "rejects oversized delegation input before native intent",
  (task) => {
    expect(() => prepareSubagentArguments({ task })).toThrow();
  },
);
it("keeps the role-free public tool schema and rejects retired invocation forms", () => {
  const schema = infrastructureToolDefinitions().find(
    (tool) => tool.name === "subagent",
  )!.parameters;
  for (const args of [
    { action: "run", task: "inspect" },
    { action: "workflow", script: "return 1" },
    { agent: "researcher", task: "inspect" },
    { workflowScript: "return 1" },
    { task: "inspect", workspace: "isolated" },
    { task: "inspect", sandbox: "isolated" },
    { task: "inspect", cwd: "relative/path" },
    { task: "inspect", context: "fork" },
    { task: "inspect", sandbox: "shared" },
    { task: "inspect", cwd: "/workspace" },
    { task: "inspect", tools: [] },
    { task: "inspect", workspace: { tools: [] } },
    { task: "inspect", workspace: { tools: ["read", "read"] } },
    { task: "inspect", workspace: { tools: ["android_tap"] } },
    { task: "inspect", workspace: { cwd: "relative" } },
    { task: "inspect", workspace: { compute: "phone" } },
  ])
    expect(Value.Check(schema, args)).toBe(false);
  expect(
    Value.Check(schema, { task: "inspect", context: "fresh", workspace: { compute: "shared" } }),
  ).toBe(true);
});

it("separates context inheritance from optional Linux workspace access", () => {
  for (const workspace of [
    undefined,
    null,
    {},
    { compute: "ephemeral", cwd: "/workspace/code", tools: ["read"] },
  ]) {
    const args = {
      task: "Analyze phone screenshots without operating the phone",
      context: "branch",
      ...(workspace === undefined ? {} : { workspace }),
    };
    expect(prepareSubagentArguments(args)).toEqual(args);
  }
});
