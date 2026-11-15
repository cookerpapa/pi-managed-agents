import { createServer, type ServerResponse } from "node:http";

/** Owned protocol fixture; never connects to an external account or filesystem. */
export async function httpMcpFixture(
  options: { host?: string; token?: string; port?: number; control?: boolean } = {},
) {
  const streams = new Set<ServerResponse>();
  const resumptions = new Map<string, unknown>();
  const held = new Set<(text: string) => void>();
  const release = (text = "ORIGINAL_MCP_REPLY") => {
    for (const send of held) send(text);
    held.clear();
  };
  const change = () => {
    for (const stream of streams)
      stream.write(
        `data: ${JSON.stringify({ jsonrpc: "2.0", method: "notifications/tools/list_changed" })}\n\n`,
      );
  };
  const state = {
    writes: 0,
    cancellations: 0,
    calls: [] as string[],
    tools: [
      {
        name: "echo",
        description: "Return the supplied text",
        inputSchema: {
          type: "object",
          properties: { text: { type: "string" } },
          required: ["text"],
        },
      },
    ],
  };
  const server = createServer(async (req, res) => {
    if (req.headers.authorization !== `Bearer ${options.token ?? "fixture-token"}`) {
      res.writeHead(401);
      res.end();
      return;
    }
    if (options.control && req.url?.startsWith("/fixture/")) {
      const chunks: Buffer[] = [];
      for await (const chunk of req) chunks.push(chunk);
      const body = chunks.length ? JSON.parse(Buffer.concat(chunks).toString()) : {};
      if (req.url === "/fixture/tools") {
        state.tools = body.tools;
        change();
      }
      if (req.url === "/fixture/release") release(body.text);
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify(state));
      return;
    }
    if (req.method === "DELETE") {
      res.writeHead(204);
      res.end();
      return;
    }
    if (req.method === "GET") {
      res.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-cache" });
      const id = req.headers["last-event-id"];
      if (typeof id === "string" && resumptions.has(id)) {
        res.end(`id: ${id}\ndata: ${JSON.stringify(resumptions.get(id))}\n\n`);
        return;
      }
      res.write(": ready\n\n");
      streams.add(res);
      res.once("close", () => streams.delete(res));
      return;
    }
    const chunks: Buffer[] = [];
    for await (const chunk of req) chunks.push(chunk);
    const message = JSON.parse(Buffer.concat(chunks).toString());
    if (message.method === "notifications/cancelled") state.cancellations++;
    if (message.id === undefined) {
      res.writeHead(202);
      res.end();
      return;
    }
    const send = (result: unknown) => {
      if (!res.destroyed) {
        res.writeHead(200, {
          "content-type": "application/json",
          "mcp-session-id": "owned-fixture",
        });
        res.end(JSON.stringify({ jsonrpc: "2.0", id: message.id, result }));
      }
    };
    if (message.method === "initialize") {
      send({
        protocolVersion: "2025-11-25",
        capabilities: { tools: { listChanged: true } },
        serverInfo: { name: "owned-fixture", version: "1" },
      });
      return;
    }
    if (message.method === "tools/list") {
      send({ tools: state.tools });
      return;
    }
    if (message.method === "tools/call") {
      const name = message.params.name;
      state.calls.push(name);
      if (name === "hold") {
        held.add((text) => send({ content: [{ type: "text", text }] }));
        return;
      }
      if (name === "image") {
        send({
          content: [
            {
              type: "image",
              mimeType: "image/png",
              data: "iVBORw0KGgoAAAANSUhEUgAAAEAAAABACAIAAAAlC+aJAAAACXBIWXMAAAPoAAAD6AG1e1JrAAAAn0lEQVRoge2SQQkAQRDDKqcST2zErIh7hIFCBKSh6cdpoht0A9ArdhfiLtENugHoFbsLcZfoBt0A9IrdhbhLdINuAHrF7kLcJbpBNwC9Ynch7hLdoBuAXrG7EHeJbtANQK/YXYi7RDfoBqBX7C7EXaIbdAPQK3YX4i7RDboB6BW7C3GX6AbdAPSK3YW4S3SDbgB6xe5C3CW6QTcAveIfHoQSwVoC6bimAAAAAElFTkSuQmCC",
            },
          ],
        });
        return;
      }
      if (name === "lost_write") {
        state.writes++;
        req.socket.destroy();
        return;
      }
      if (name === "resumed_write") {
        state.writes++;
        const id = `reply-${message.id}`;
        resumptions.set(id, {
          jsonrpc: "2.0",
          id: message.id,
          result: { content: [{ type: "text", text: "resumed original response" }] },
        });
        res.writeHead(200, { "content-type": "text/event-stream" });
        res.end(`retry: 1\nid: ${id}\n\n`);
        return;
      }
      if (name === "wait") {
        setTimeout(() => send({ content: [{ type: "text", text: "late" }] }), 250);
        return;
      }
      if (name === "large") {
        send({ content: [{ type: "text", text: "x".repeat(60 * 1024) }] });
        return;
      }
      send({
        content: [{ type: "text", text: message.params.arguments.text ?? "expected tool error" }],
        structuredContent: { fixture: true },
        isError: name === "error",
      });
      return;
    }
    send({});
  });
  await new Promise<void>((resolve) =>
    server.listen(options.port ?? 0, options.host ?? "127.0.0.1", resolve),
  );
  return {
    state,
    port: (server.address() as { port: number }).port,
    release,
    url: `http://127.0.0.1:${(server.address() as { port: number }).port}/mcp`,
    change,
    async close() {
      for (const stream of streams) stream.end();
      server.closeAllConnections();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}
