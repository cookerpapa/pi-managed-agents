import { isControlDispatchToolName } from "./control-tool-catalog.ts";
import { parseToolEnvironmentRequest, type ToolEnvironmentRequest } from "./tool-environment.ts";
import { MCP_CATALOG_SCOPE_PATTERN } from "./tool-catalog-publication.ts";
import { parseToolReplyRecovery, type ToolReplyRecovery } from "./tool-recovery.ts";

export const MCP_TOOL_TIMEOUT_MS = 300_000;
export type ToolImplementation = Readonly<{
  releaseId: string;
  generation: number;
  replay: "safe" | "never";
  /** Absent for code-owned platform tools; MCP identifies its connection catalog. */
  catalogScope?: string;
}>;

/** The candidate uses the execution checkpoint and logical child Run ID here.
 * The enclosing dispatch operationId correlates the current mailbox delivery. */
export type ControlToolRecoverySource = Readonly<{
  sequence: number;
  operationId: string;
}>;
export type ControlToolRecovery = ControlToolRecoverySource & Readonly<{ documentId: number }>;
export type OriginalControlToolIntent = ControlToolRecoverySource &
  Readonly<{
    taskId: number;
    conversationId: number;
    toolName: string;
    nativeCallId: string;
    arguments: Record<string, unknown>;
    contextAnchor: number | null;
  }>;

/** Prepared by the trusted Worker; arguments are removed from the transport
 * descriptor and resolved from the same native execute checkpoint. */
export type CandidateControlToolCommand = Readonly<{
  target: "control";
  executionReference: string;
  operationId: string;
  toolCallId: string;
  nativeCallId: string;
  conversationId: number;
  toolName: string;
  arguments: Record<string, unknown>;
  contextAnchor: number | null;
  bindingId?: string;
  environment?: ToolEnvironmentRequest;
  implementation?: ToolImplementation;
  sourceDocumentId?: number;
  recovery?: ControlToolRecoverySource & { documentId?: number };
  /** Reply-only reattachment; never a request to invoke the external tool again. */
  replyRecovery?: ToolReplyRecovery;
  replyMode?: "hostcall";
  workflowKey?: string;
  occurredAt: string;
}>;

export type ControlToolDispatch = Readonly<{
  target: "control";
  operationId: string;
  taskId: number;
  conversationId: number;
  toolName: string;
  contextAnchor: number | null;
  bindingId?: string;
  environment?: ToolEnvironmentRequest;
  implementation?: ToolImplementation;
  sourceDocumentId?: number;
  recovery?: ControlToolRecovery;
  replyRecovery?: ToolReplyRecovery;
  replyMode?: "hostcall";
  workflowKey?: string;
  replyTopic: string;
}>;

export function parseControlToolDispatch(value: unknown): ControlToolDispatch {
  const v = value as ControlToolDispatch;
  if (
    v?.implementation &&
    (!v.implementation.releaseId ||
      v.implementation.releaseId.length > 256 ||
      !Number.isSafeInteger(v.implementation.generation) ||
      v.implementation.generation < 1 ||
      !["safe", "never"].includes(v.implementation.replay) ||
      (v.implementation.catalogScope !== undefined &&
        (!MCP_CATALOG_SCOPE_PATTERN.test(v.implementation.catalogScope) ||
          v.implementation.replay !== "never" ||
          !v.toolName.startsWith("mcp_"))))
  )
    throw new Error("Invalid Tool implementation identity");
  if (
    !v ||
    v.target !== "control" ||
    typeof v.operationId !== "string" ||
    !v.operationId ||
    !Number.isSafeInteger(v.taskId) ||
    v.taskId < 1 ||
    !Number.isSafeInteger(v.conversationId) ||
    v.conversationId < 1 ||
    !isControlDispatchToolName(v.toolName) ||
    (v.contextAnchor !== null && (!Number.isSafeInteger(v.contextAnchor) || v.contextAnchor < 1)) ||
    (v.bindingId !== undefined && (typeof v.bindingId !== "string" || !v.bindingId)) ||
    (v.sourceDocumentId !== undefined &&
      (!Number.isSafeInteger(v.sourceDocumentId) || v.sourceDocumentId < 1)) ||
    (v.replyMode !== undefined && v.replyMode !== "hostcall") ||
    (v.replyMode === "hostcall" && v.sourceDocumentId === undefined) ||
    (v.workflowKey !== undefined && (typeof v.workflowKey !== "string" || !v.workflowKey)) ||
    typeof v.replyTopic !== "string" ||
    !v.replyTopic ||
    "arguments" in v ||
    "args" in v
  )
    throw new Error("Invalid control Tool dispatch reference");
  const recovery = v.recovery;
  if (v.replyRecovery !== undefined) {
    const reply = parseToolReplyRecovery(v.replyRecovery);
    if (
      !v.implementation?.catalogScope ||
      v.recovery ||
      v.sourceDocumentId !== undefined ||
      v.replyMode !== undefined ||
      v.workflowKey !== undefined ||
      reply.topic !== v.replyTopic ||
      reply.toolName !== v.toolName
    )
      throw new Error("Invalid control Tool reply recovery");
  }
  if (v.implementation?.catalogScope && !v.replyRecovery)
    throw new Error("MCP Tool requires an original reply reference");
  if (v.environment !== undefined) {
    parseToolEnvironmentRequest(v.environment);
    if (v.bindingId !== v.environment.requestId)
      throw new Error("Tool environment does not match its logical binding");
  }
  if (
    recovery !== undefined &&
    (!recovery ||
      v.toolName !== "subagent" ||
      v.sourceDocumentId !== undefined ||
      v.replyMode !== undefined ||
      v.workflowKey !== undefined ||
      v.bindingId !== undefined ||
      !Number.isSafeInteger(recovery.documentId) ||
      recovery.documentId < 1 ||
      !Number.isSafeInteger(recovery.sequence) ||
      recovery.sequence < 1 ||
      typeof recovery.operationId !== "string" ||
      !recovery.operationId ||
      recovery.operationId === v.operationId ||
      Object.keys(recovery).sort().join(",") !== "documentId,operationId,sequence")
  )
    throw new Error("Invalid control Tool recovery reference");
  return v;
}
