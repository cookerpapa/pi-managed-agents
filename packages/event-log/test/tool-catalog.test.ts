import { createHash } from "node:crypto";
import { expect, it } from "vitest";
import { ToolCatalogView } from "../src/tool-catalog.ts";

function snapshot(generation: number, names: string[] = []) {
  const tools = names.map((name) => ({
    name,
    description: name,
    parameters: { type: "object", properties: {} },
    environment: { kind: "none" as const },
  }));
  return {
    version: 2 as const,
    scope: "platform" as const,
    generation,
    releaseId: `code-${generation}`,
    replayable: names,
    catalog: {
      version: 4 as const,
      tools,
      revision: createHash("sha256").update(JSON.stringify(tools)).digest("hex"),
    },
  };
}

it("restores a complete catalog and never regresses after stale publisher delivery", () => {
  const view = new ToolCatalogView();
  expect(view.ready).toBe(false);
  expect(() => view.read()).toThrow();
  view.accept(snapshot(8, ["new_tool"]));
  view.accept(snapshot(7, ["withdrawn_tool"]));
  expect(view.snapshot?.generation).toBe(8);
  expect(view.read().tools.map((t) => t.name)).toEqual(["new_tool"]);
  expect(view.replayable("withdrawn_tool")).toBe(false);
});

it("deduplicates a snapshot but rejects an equal generation with different contents", () => {
  const view = new ToolCatalogView();
  let updates = 0;
  view.subscribe(() => {
    updates++;
  });
  view.accept(snapshot(1, ["first"]));
  view.accept(snapshot(1, ["first"]));
  expect(updates).toBe(1);
  expect(() => view.accept(snapshot(1, ["second"]))).toThrow("generation");
  expect(view.read().tools[0]?.name).toBe("first");
});

it("an empty new snapshot withdraws tools and malformed data preserves the prior value", () => {
  const view = new ToolCatalogView();
  view.accept(snapshot(1, ["old_tool"]));
  const broken = snapshot(2, ["new_tool"]);
  broken.catalog.tools[0]!.description = "changed after digest";
  expect(() => view.accept(broken)).toThrow("digest");
  expect(view.read().tools[0]?.name).toBe("old_tool");
  view.accept(snapshot(2));
  expect(view.read().tools).toEqual([]);
  expect(view.replayable("old_tool")).toBe(false);
});

it("does not mistake visibility for a replay grant", () => {
  const value = snapshot(1, ["write_external"]);
  value.replayable = [];
  const view = new ToolCatalogView();
  view.accept(value);
  expect(view.read().tools).toHaveLength(1);
  expect(view.replayable("write_external")).toBe(false);
  expect(() => view.accept({ ...snapshot(2), replayable: ["missing"] })).toThrow("absent");
});

const tenant = "10000000-0000-4000-8000-000000000001";
const alice = "20000000-0000-4000-8000-000000000001";
const bob = "20000000-0000-4000-8000-000000000002";
const connection = "30000000-0000-4000-8000-000000000001";
function mcp(
  generation: number,
  options: { enabled?: boolean; visibility?: "personal" | "organization"; releaseId?: string } = {},
) {
  const base = snapshot(generation, ["mcp_lookup_fixture"]);
  const tools = base.catalog.tools.map((tool) => ({ ...tool, availability: "root" as const }));
  return {
    ...base,
    scope: `mcp:${connection}`,
    releaseId: options.releaseId ?? "code-3",
    replayable: [],
    mcp: {
      tenantId: tenant,
      ownerUserId: alice,
      visibility: options.visibility ?? "personal",
      revision: generation,
      enabled: options.enabled ?? true,
    },
    catalog: {
      ...base.catalog,
      tools,
      revision: createHash("sha256").update(JSON.stringify(tools)).digest("hex"),
    },
  };
}

it("exposes connection tools only to their admitted tenant/actor roots", () => {
  const view = new ToolCatalogView();
  view.accept(snapshot(3, ["platform_tool"]));
  const owner = view.forActor({ tenantId: tenant, userId: alice, root: true });
  const colleague = view.forActor({ tenantId: tenant, userId: bob, root: true });
  view.accept(mcp(1));
  expect(owner.read().tools.map((tool) => tool.name)).toContain("mcp_lookup_fixture");
  expect(colleague.read().tools.map((tool) => tool.name)).not.toContain("mcp_lookup_fixture");
  expect(
    view
      .forActor({ tenantId: "40000000-0000-4000-8000-000000000001", userId: alice, root: true })
      .read().tools,
  ).toHaveLength(1);
  expect(view.forActor({ tenantId: tenant, userId: alice, root: false }).read().tools).toHaveLength(
    1,
  );
  expect(view.forActor({ tenantId: tenant, root: true }).read().tools).toHaveLength(1);
  expect(view.read().tools).toHaveLength(1);
  view.accept(mcp(2, { visibility: "organization" }));
  expect(colleague.read().tools).toHaveLength(2);
  expect(view.snapshot?.generation).toBe(3);
  expect(view.implementation("mcp_lookup_fixture")).toEqual({
    releaseId: "code-3",
    generation: 2,
    replay: "never",
    catalogScope: `mcp:${connection}`,
  });
});

it("withdraws a connection without changing platform tools or resurrecting stale generations", () => {
  const view = new ToolCatalogView();
  view.accept(snapshot(3, ["platform_tool"]));
  const actor = view.forActor({ tenantId: tenant, userId: alice, root: true });
  view.accept(mcp(1));
  view.accept(mcp(2, { enabled: false }));
  view.accept(mcp(1));
  expect(actor.read().tools).toHaveLength(1);
  expect(view.implementation("mcp_lookup_fixture")).toBeUndefined();
  expect(view.implementation("platform_tool")?.generation).toBe(3);
  expect(() => view.accept(mcp(2))).toThrow("generation");
});

it("waits for a new code release's scoped selection instead of routing through an old host", () => {
  const view = new ToolCatalogView();
  view.accept(snapshot(3));
  view.accept(mcp(1));
  const actor = view.forActor({ tenantId: tenant, userId: alice, root: true });
  expect(actor.read().tools).toHaveLength(1);
  view.accept(snapshot(4));
  expect(actor.read().tools).toHaveLength(0);
  expect(view.implementation("mcp_lookup_fixture")).toBeUndefined();
  view.accept(mcp(2, { releaseId: "code-4" }));
  expect(actor.read().tools).toHaveLength(1);
});
