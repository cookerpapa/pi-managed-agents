import { expect, it } from "vitest";
import { ToolCatalogAuthority } from "../src/tool-catalog-authority.ts";
import { controlToolCatalog } from "../src/control-tool-registry.ts";
import { nativeSubagentFixture } from "./native-subagent-fixture.ts";

it("selects a deployment with CAS and rejects obsolete or conflicting catalog ownership", async () => {
  const f = await nativeSubagentFixture();
  try {
    const authority = new ToolCatalogAuthority(f.database),
      catalog = controlToolCatalog([]);
    const first = await authority.select({
      releaseId: "v1",
      catalog,
      replayable: [],
      expectedGeneration: 0,
    });
    expect(await authority.current(first)).toBe(true);
    await authority.confirmPublished(first);
    expect(await authority.read()).toEqual(first);
    const second = await authority.select({
      releaseId: "v2",
      catalog,
      replayable: [],
      expectedGeneration: 1,
    });
    expect(second.generation).toBe(2);
    expect(await authority.current(first)).toBe(false);
    await expect(authority.confirmPublished(first)).rejects.toThrow();
    await expect(
      authority.select({ releaseId: "v1", catalog, replayable: [], expectedGeneration: 1 }),
    ).rejects.toThrow("selection changed");
    expect(
      await authority.select({ releaseId: "v2", catalog, replayable: [], expectedGeneration: 1 }),
    ).toEqual(second);
    expect(await authority.read()).toEqual(second);
  } finally {
    await f.close();
  }
});

it("retains exact release endpoints and fences delayed policy changes across re-enable", async () => {
  const fixture = await nativeSubagentFixture();
  try {
    const authority = new ToolCatalogAuthority(fixture.database);
    const catalog = controlToolCatalog([
      {
        name: "export_report",
        description: "Export a report",
        environment: { kind: "none" },
        parameters: { type: "object", properties: {} },
      },
    ]);
    const first = {
      releaseId: "code-1",
      catalog,
      replayable: [],
      expectedGeneration: 0,
      versioned: { endpoint: "http://release-one:4310", graceMs: 60000 },
    };
    const v1 = await authority.select(first);
    expect(await authority.select(first)).toEqual(v1);
    await expect(
      authority.select({ ...first, releaseId: "code-2", expectedGeneration: 1 }),
    ).rejects.toThrow("address");
    const second = {
      ...first,
      versioned: { ...first.versioned, endpoint: "http://release-two:4310" },
      releaseId: "code-2",
      expectedGeneration: 1,
    };
    const v2 = await authority.select(second);
    expect(await authority.versionEndpoint(v1.generation, v1.releaseId)).toBe(
      first.versioned.endpoint,
    );
    expect(await authority.versionEndpoint(v2.generation, v1.releaseId)).toBeUndefined();
    const disable = {
      ...second,
      expectedGeneration: v2.generation,
      versioned: { ...second.versioned, availability: { name: "export_report", disabled: true } },
    };
    const hidden = await authority.select(disable);
    expect(await authority.select(disable)).toEqual(hidden);
    expect(hidden.catalog.tools).toEqual([]);
    expect(await authority.admitOrdinary(v1.generation, v1.releaseId, "export_report")).toBe(
      "disabled",
    );
    const enabled = await authority.select({
      ...disable,
      expectedGeneration: hidden.generation,
      versioned: { ...second.versioned, availability: { name: "export_report", disabled: false } },
    });
    await expect(authority.select(disable)).rejects.toThrow("generation changed");
    expect(await authority.admitOrdinary(v2.generation, v2.releaseId, "export_report")).toBe(
      "disabled",
    );
    expect(
      await authority.admitOrdinary(enabled.generation, enabled.releaseId, "export_report"),
    ).toBe("accepted");
    expect(await authority.admitOrdinary(enabled.generation, enabled.releaseId, "other")).toBe(
      "expired",
    );
  } finally {
    await fixture.close();
  }
});
