import { expect, it, vi } from "vitest";
import { HttpMcpConnection, mcpToolName, boundedMcpResponse } from "../../src/mcp/http-client.ts";
import { httpMcpFixture } from "./http-fixture.ts";

it("uses the pinned Pi HTTP transport for discovery, changes, results and stream resumption without replay", async () => {
  const server = await httpMcpFixture(),
    changed = vi.fn();
  const client = new HttpMcpConnection({
    id: crypto.randomUUID(),
    name: "Glossary",
    endpoint: server.url,
    token: "fixture-token",
    fetch,
    changed,
  });
  try {
    await client.connect();
    const tools = await client.discover();
    expect(tools[0]!.platformName).toMatch(/^mcp_echo_[a-f0-9]{32}$/);
    const result = await client.call(tools[0]!, { text: "hello" }, new AbortController().signal);
    expect(result.result.content).toEqual([{ type: "text", text: "hello" }]);
    expect(result.result.details).toEqual({ structuredContent: { fixture: true } });
    server.change();
    await vi.waitFor(() => expect(changed).toHaveBeenCalled());
    const resumed = { ...tools[0]!, name: "resumed_write" };
    expect(
      await client.call(resumed, { text: "resume" }, new AbortController().signal),
    ).toHaveProperty("result.content.0.text", "resumed original response");
    expect(server.state.writes).toBe(1);
    expect(server.state.calls.filter((x) => x === "resumed_write")).toHaveLength(1);
    await expect(
      client.call(
        { ...tools[0]!, name: "lost_write" },
        { text: "once" },
        new AbortController().signal,
      ),
    ).rejects.toThrow();
    expect(server.state.writes).toBe(2);
    expect(server.state.calls.filter((x) => x === "lost_write")).toHaveLength(1);
    expect(
      (
        await client.call(
          { ...tools[0]!, name: "error" },
          { text: "failure" },
          new AbortController().signal,
        )
      ).isError,
    ).toBe(true);
    const abort = new AbortController();
    const pending = client.call({ ...tools[0]!, name: "wait" }, { text: "pending" }, abort.signal);
    const rejected = expect(pending).rejects.toThrow();
    await vi.waitFor(() => expect(server.state.calls).toContain("wait"));
    abort.abort(new Error("owned cancellation"));
    await rejected;
    await vi.waitFor(() => expect(server.state.cancellations).toBeGreaterThan(0));
  } finally {
    await client.close();
    await server.close();
  }
});

it("keeps tool identity stable across labels and distinguishes colliding normalized names and connections", () => {
  const id = crypto.randomUUID();
  expect(mcpToolName(id, "read:file")).not.toBe(mcpToolName(id, "read_file"));
  expect(mcpToolName(id, "read_file")).not.toBe(mcpToolName(crypto.randomUUID(), "read_file"));
  expect(mcpToolName(id, "工具".repeat(100))).toMatch(/^[a-z][a-z0-9_]{0,63}$/);
});

it.each([200, 500])(
  "stops JSON/error response reading at the transport boundary (%s)",
  async (status) => {
    let read = 0,
      cancelled = false;
    const source = new ReadableStream<Uint8Array>(
      {
        pull(controller) {
          read++;
          controller.enqueue(new Uint8Array(8));
        },
        cancel() {
          cancelled = true;
        },
      },
      { highWaterMark: 0 },
    );
    const response = boundedMcpResponse(
      new Response(source, { status, headers: { "content-type": "application/json" } }),
      16,
    );
    await expect(response.text()).rejects.toThrow("transport budget");
    expect(cancelled).toBe(true);
    expect(read).toBe(3);
  },
);
