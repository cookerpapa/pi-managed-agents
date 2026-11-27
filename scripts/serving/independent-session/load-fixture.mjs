// Test-only CLIProxyAPI-shaped route for a deterministic Responses upstream.
import { createServer } from "node:http";
import { zstdDecompressSync, gunzipSync } from "node:zlib";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import {
  FakeResponsesServer,
  benchmarkPrompt,
} from "../../../packages/fake-model-server/src/responses-server.ts";
export async function servingLoadFixture(key) {
  const model = "pma-benchmark-serving-text",
    provider = new FakeResponsesServer({ model, apiKey: key });
  await provider.start();
  const diagnostics = { requests: 0, errors: {} };
  const gateway = createServer(async (req, res) => {
    if (
      req.method !== "POST" ||
      req.url !== "/v1/picloud/responses" ||
      req.headers.authorization !== "Bearer " + key
    ) {
      res.writeHead(403).end();
      return;
    }
    diagnostics.requests++;
    try {
      const chunks = [];
      for await (const c of req) chunks.push(c);
      const bytes = Buffer.concat(chunks);
      const body = JSON.parse(
        (req.headers["content-encoding"] === "zstd"
          ? zstdDecompressSync(bytes)
          : req.headers["content-encoding"] === "gzip"
            ? gunzipSync(bytes)
            : bytes
        ).toString(),
      );
      body.model = model;
      const upstream = await fetch(provider.origin + "/v1/responses", {
        method: "POST",
        headers: { authorization: "Bearer " + key, "content-type": "application/json" },
        body: JSON.stringify(body),
      });
      if (!upstream.ok)
        diagnostics.errors[upstream.status] = (diagnostics.errors[upstream.status] ?? 0) + 1;
      res.writeHead(upstream.status, { "content-type": upstream.headers.get("content-type") });
      await pipeline(Readable.fromWeb(upstream.body), res);
    } catch (error) {
      diagnostics.errors[error.name] = (diagnostics.errors[error.name] ?? 0) + 1;
      res.destroy();
    }
  });
  await new Promise((r) => gateway.listen(0, "127.0.0.1", r));
  return {
    url: "http://127.0.0.1:" + gateway.address().port,
    prompt: (id, options = {}) =>
      benchmarkPrompt({ id, kind: "text", outputBytes: 256, chunkBytes: 64, ...options }),
    metrics: () => ({ ...provider.metrics(), diagnostics }),
    async close() {
      gateway.closeAllConnections();
      await new Promise((r) => gateway.close(r));
      await provider.stop();
    },
  };
}
