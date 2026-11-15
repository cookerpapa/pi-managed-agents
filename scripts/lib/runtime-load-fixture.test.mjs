import { expect, it } from "vitest";
import { parse } from "yaml";
import { benchmarkCatalog, addBenchmarkProvider } from "./runtime-load-fixture.mjs";
import { fixtureModelCatalog } from "./test-model-catalog.mjs";

it("adds one synthetic model without changing original capabilities or the default", () => {
  const original = structuredClone(fixtureModelCatalog),
    before = JSON.stringify(original);
  const result = benchmarkCatalog(original, "pma-benchmark-test");
  expect(JSON.stringify(original)).toBe(before);
  expect(result.defaultModel).toEqual(original.defaultModel);
  expect(result.models.slice(0, -1)).toEqual(original.models);
  expect(result.models.at(-1).modelId).toBe("pma-benchmark-test");
  expect(() => benchmarkCatalog(result, "pma-benchmark-test")).toThrow("exists");
});
it("keeps original provider accounts/routes and puts websocket support on the owned key", () => {
  const source =
    "config-version: 8\nserver: {port: 8317}\napi-keys:\n  codex:\n    - name: real-route\n      keys: [{api-key: preserved}]\n  openai-compatibility: [{name: preserved-compat}]\noauth: {auth-dir: /data/auth}\n";
  const result = parse(
      addBenchmarkProvider(source, {
        name: "owned",
        url: "http://owned.invalid",
        key: "synthetic",
        model: "pma-benchmark-test",
      }),
    ),
    before = parse(source);
  expect(result["api-keys"].codex[0]).toEqual(before["api-keys"].codex[0]);
  expect(result["api-keys"]["openai-compatibility"]).toEqual(
    before["api-keys"]["openai-compatibility"],
  );
  expect(result.oauth).toEqual(before.oauth);
  expect(result["api-keys"].codex[1]).toMatchObject({
    "base-url": "http://owned.invalid",
    "proxy-url": "direct",
    keys: [{ "api-key": "synthetic", websockets: true }],
    models: [{ name: "pma-benchmark-test", alias: "pma-benchmark-test" }],
  });
});
