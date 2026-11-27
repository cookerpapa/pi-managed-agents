import { Agent, fetch as internalFetch } from "undici";
import {
  MAX_TOOL_RESPONSE_BYTES,
  parsePackedToolResultPayload,
  type ImagePacked,
  type ToolResultPayload,
  type ToolCatalogPublication,
} from "@pi-cloud/protocol";
import type { AcceptedControlToolCommand } from "@pi-cloud/runtime-core/accepted-fact";
import type { ControlToolOutcome } from "./control-tool-dispatcher.ts";

export const TOOL_HOST_EXECUTE_PATH = "/internal/v1/tools/execute";
export const TOOL_HOST_ACTIVATE_PATH = "/internal/v1/tools/activate";
export const TOOL_HOST_MANIFEST_PATH = "/internal/v1/tools/manifest";
export const TOOL_HOST_DRAIN_PATH = "/internal/v1/tools/drain";

/** One request, one selected implementation. A transport loss is not retried:
 * unlike a definite refusal it cannot establish that the handler did not run. */
export class ToolHostClient {
  readonly #dispatcher = new Agent({
    connect: { timeout: 5000 },
    headersTimeout: 0,
    bodyTimeout: 0,
  });
  constructor(
    readonly options: {
      baseUrl: string;
      token: string;
      revokedVersion?(generation: number, releaseId: string, name: string): Promise<boolean>;
      currentEndpoint?(): Promise<string | undefined>;
      resolveVersion?(releaseId: string, generation: number): Promise<string | undefined>;
      catalog(toolName: string): ToolCatalogPublication | undefined;
    },
  ) {}

  async execute(
    command: AcceptedControlToolCommand,
    current: () => boolean,
    signal?: AbortSignal,
  ): Promise<ControlToolOutcome> {
    const catalog = this.options.catalog(command.toolName);
    const selected = command.implementation;
    const failure = (message: string): ControlToolOutcome => ({
      isError: true,
      result: { content: [{ type: "text", text: message }] },
    });
    if (!current() || signal?.aborted || !selected)
      return failure("tool_unavailable: The tool catalog or execution owner is unavailable.");
    const pinned = !!this.options.resolveVersion && !selected.catalogScope;
    if (
      !pinned &&
      (catalog?.generation !== selected.generation ||
        catalog?.releaseId !== selected.releaseId ||
        catalog?.scope !== (selected.catalogScope ?? "platform"))
    )
      return failure(
        "tool_unavailable: The accepted tool implementation has been replaced. This invocation was not submitted.",
      );
    let url = this.options.baseUrl;
    if (!pinned && this.options.currentEndpoint) {
      const endpoint = await this.options.currentEndpoint();
      if (!endpoint)
        return failure(
          "tool_unavailable: The selected Tool host is not available. This invocation was not submitted.",
        );
      url = endpoint;
    }
    if (pinned) {
      const endpoint = await this.options.resolveVersion!(selected.releaseId, selected.generation);
      if (
        !endpoint &&
        (await this.options.revokedVersion?.(
          selected.generation,
          selected.releaseId,
          command.toolName,
        ))
      )
        return failure(
          "tool_disabled: An administrator disabled this tool. This call was not executed.",
        );
      if (!endpoint)
        return failure(
          "tool_version_expired: The requested implementation is retired. This call was not executed.",
        );
      url = endpoint;
    }
    try {
      const response = await internalFetch(new URL(TOOL_HOST_EXECUTE_PATH, url), {
        dispatcher: this.#dispatcher,
        method: "POST",
        redirect: "error",
        headers: {
          authorization: `Bearer ${this.options.token}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({ releaseId: selected.releaseId, command }),
        ...(signal ? { signal } : {}),
      });
      if (!response.ok) {
        await response.body?.cancel();
        if (response.status === 423)
          return failure(
            "tool_disabled: An administrator disabled this tool. This call was not executed.",
          );
        if (response.status === 410)
          return failure(
            "tool_version_expired: The requested implementation is retired. This call was not executed.",
          );
        return failure(
          [409, 412, 503].includes(response.status)
            ? "tool_unavailable: The selected tool implementation did not accept this invocation. Refresh the catalog before another call."
            : "tool_result_unknown: The tool host could not confirm this invocation. Inspect effects before repeating it.",
        );
      }
      const chunks: Uint8Array[] = [];
      let bytes = 0;
      if (response.body) {
        const reader = response.body.getReader();
        try {
          for (;;) {
            const item = await reader.read();
            if (item.done) break;
            bytes += item.value.byteLength;
            if (bytes > MAX_TOOL_RESPONSE_BYTES) throw new Error("Tool host reply is oversized");
            chunks.push(item.value);
          }
        } finally {
          await reader.cancel().catch(() => {});
        }
      }
      const body = JSON.parse(Buffer.concat(chunks).toString("utf8")) as {
        operationId?: string;
        releaseId?: string;
        result?: ImagePacked<ToolResultPayload>;
      };
      if (body.operationId !== command.operationId || body.releaseId !== selected.releaseId)
        throw new Error("Tool host response identity changed");
      const packed = parsePackedToolResultPayload(body.result);
      if (
        packed.toolCallId !== command.nativeCallId ||
        packed.toolName !==
          (command.replyMode === "hostcall" ? "subagent-control" : command.toolName)
      )
        throw new Error("Tool host response refers to another invocation");
      return { packed };
    } catch (error) {
      if (signal?.aborted) throw signal.reason;
      return failure(
        "tool_result_unknown: The connection to the tool host ended without a confirmed result. This invocation was not resent; inspect effects before repeating it.",
      );
    }
  }
  close(): Promise<void> {
    return this.#dispatcher.destroy();
  }
}
