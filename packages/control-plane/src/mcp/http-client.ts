import { createHash } from "node:crypto";
import {
  McpClient,
  McpHttpError,
  StreamableHttpTransport,
  toLlmContent,
} from "@earendil-works/pi-mcp";
import { Value } from "typebox/value";
import type { TSchema } from "typebox";
import {
  MAX_TOOL_RESPONSE_BYTES,
  MCP_TOOL_TIMEOUT_MS,
  type McpConnectionTool,
} from "@pi-cloud/protocol";

export function mcpToolName(connectionId: string, remoteName: string): string {
  const label =
    remoteName
      .toLowerCase()
      .replace(/[^a-z0-9_]/g, "_")
      .slice(0, 24) || "tool";
  const hash = createHash("sha256")
    .update(connectionId + "\0" + remoteName)
    .digest("hex")
    .slice(0, 32);
  return `mcp_${label}_${hash}`;
}

export function mcpFailureCode(error: unknown): string {
  if (error instanceof McpHttpError) {
    if (error.status === 401 || error.status === 403) return "mcp_authentication_failed";
    if (error.status === 404) return "mcp_endpoint_unavailable";
    return "mcp_http_error";
  }
  return error instanceof McpDiscoveryError ? error.code : "mcp_connection_unavailable";
}
export class McpDiscoveryError extends Error {
  constructor(readonly code: string) {
    super(code);
  }
}

/** Pi parses JSON/error bodies after reading them. Bound that read at the public
 * fetch port; its own SSE parser separately bounds each streamed MCP message. */
export function boundedMcpResponse(response: Response, limit = MAX_TOOL_RESPONSE_BYTES): Response {
  if (
    !response.body ||
    (response.ok &&
      response.headers.get("content-type")?.toLowerCase().includes("text/event-stream"))
  )
    return response;
  const reader = response.body.getReader();
  let bytes = 0;
  const body = new ReadableStream<Uint8Array>({
    async pull(controller) {
      try {
        const item = await reader.read();
        if (item.done) {
          controller.close();
          return;
        }
        bytes += item.value.byteLength;
        if (bytes > limit) {
          await reader.cancel();
          controller.error(new Error("MCP response exceeds its transport budget"));
          return;
        }
        controller.enqueue(item.value);
      } catch (error) {
        controller.error(error);
      }
    },
    cancel: (reason) => reader.cancel(reason),
  });
  return new Response(body, {
    status: response.status,
    statusText: response.statusText,
    headers: response.headers,
  });
}

/** One protocol connection, never an external-effect retry queue. */
export class HttpMcpConnection {
  readonly client = new McpClient({
    name: "pi-managed-agents",
    version: "1",
    requestTimeoutMs: 15_000,
  });
  readonly #lifetime = new AbortController();
  #active = 0;
  #retired = false;
  #closed: Promise<void> | undefined;
  constructor(
    readonly options: {
      id: string;
      name: string;
      endpoint: string;
      token?: string;
      fetch: typeof globalThis.fetch;
      changed(): void;
    },
  ) {}

  get usable(): boolean {
    return !this.#retired && this.client.connectionState === "connected";
  }
  async connect(): Promise<void> {
    this.client.onError(() => {});
    this.client.onNotification("notifications/tools/list_changed", () => this.options.changed());
    this.client.onClose(() => {
      if (!this.#retired) this.options.changed();
    });
    await this.client.connect(
      new StreamableHttpTransport({
        url: this.options.endpoint,
        ...(this.options.token
          ? { headers: { Authorization: `Bearer ${this.options.token}` } }
          : {}),
        maxMessageBytes: MAX_TOOL_RESPONSE_BYTES,
        fetch: async (url, init) =>
          boundedMcpResponse(
            await this.options.fetch(url, {
              ...init,
              redirect: "error",
              signal: AbortSignal.any([
                this.#lifetime.signal,
                ...(init?.signal ? [init.signal] : []),
              ]),
            }),
          ),
      }),
    );
  }

  async discover(): Promise<McpConnectionTool[]> {
    const remote = await this.client.listTools({
      signal: AbortSignal.timeout(15_000),
      timeoutMs: 15_000,
    });
    const names = new Set<string>();
    const tools = remote
      .map((tool) => {
        if (
          typeof tool.name !== "string" ||
          !tool.name ||
          tool.name.length > 256 ||
          /[\x00-\x1f\x7f]/.test(tool.name) ||
          names.has(tool.name)
        )
          throw new McpDiscoveryError("mcp_tool_identity_invalid");
        names.add(tool.name);
        if (!tool.inputSchema || tool.inputSchema.type !== "object")
          throw new McpDiscoveryError("mcp_tool_schema_invalid");
        const parameters = JSON.parse(
          JSON.stringify({ ...tool.inputSchema, properties: tool.inputSchema.properties ?? {} }),
        ) as Record<string, unknown>;
        // Resolve supported validators now rather than accepting an unusable directory.
        try {
          Value.Check(parameters as TSchema, {});
        } catch {
          throw new McpDiscoveryError("mcp_tool_schema_unsupported");
        }
        return {
          name: tool.name,
          platformName: mcpToolName(this.options.id, tool.name),
          description: `${this.options.name}: ${tool.description || tool.name}`,
          parameters,
        };
      })
      .sort((a, b) => a.platformName.localeCompare(b.platformName));
    if (Buffer.byteLength(JSON.stringify(tools)) > 800_000)
      throw new McpDiscoveryError("mcp_tool_catalog_too_large");
    return tools;
  }

  async call(tool: McpConnectionTool, args: Record<string, unknown>, signal: AbortSignal) {
    if (!this.usable) throw new Error("MCP connection is not accepting calls");
    if (!Value.Check(tool.parameters as TSchema, args))
      throw new McpDiscoveryError("mcp_tool_arguments_invalid");
    this.#active++;
    try {
      const result = await this.client.callTool(tool.name, args, {
        signal,
        timeoutMs: MCP_TOOL_TIMEOUT_MS,
      });
      return {
        isError: result.isError === true,
        result: {
          content: toLlmContent(result),
          ...(result.structuredContent === undefined
            ? {}
            : { details: { structuredContent: result.structuredContent } }),
        },
      };
    } finally {
      this.#active--;
      if (this.#retired && !this.#active) void this.close().catch(() => {});
    }
  }

  retire(): void {
    this.#retired = true;
    if (!this.#active) void this.close().catch(() => {});
  }
  close(): Promise<void> {
    this.#retired = true;
    return (this.#closed ??= (async () => {
      this.#lifetime.abort(new Error("MCP connection closed"));
      await this.client.close().catch(() => {});
    })());
  }
}
