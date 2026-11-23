import { randomUUID } from "node:crypto";
import {
  MODEL_METERING_HEADER,
  parseExecutionReference,
  type ExecuteTurnCommandMessage,
  type ModelMeteringContext,
} from "@pi-cloud/protocol";

/** Stamp each actual HTTP attempt at the Model Gateway boundary. */
export function attributedModelFetch(
  command: ExecuteTurnCommandMessage,
  options: {
    taskId: number;
    provider: string;
    model: string;
    purpose: "agent" | "context_maintenance";
    fetch?: typeof fetch;
  },
): typeof fetch {
  const p = command.payload,
    identity = parseExecutionReference(p.executionReference);
  return (url, init) => {
    const context: ModelMeteringContext = {
      version: 1,
      invocationId: randomUUID(),
      requestedAt: new Date().toISOString(),
      tenantId: p.tenantId,
      sessionId: p.sessionId,
      runId: p.runId,
      piSessionId: p.piSession.id,
      conversationId: p.piSession.conversationId,
      provider: options.provider,
      model: options.model,
      purpose: options.purpose,
      execution: {
        writerId: identity.leaseId,
        fencingToken: identity.fencingToken,
        taskId: options.taskId,
      },
    };
    const headers = new Headers(
      init?.headers ?? (url instanceof Request ? url.headers : undefined),
    );
    headers.set(MODEL_METERING_HEADER, Buffer.from(JSON.stringify(context)).toString("base64url"));
    return (options.fetch ?? globalThis.fetch)(url, { ...init, headers });
  };
}
