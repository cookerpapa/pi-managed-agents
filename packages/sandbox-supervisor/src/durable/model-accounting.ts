import { randomUUID } from "node:crypto";
import {
  MODEL_METERING_HEADER,
  parseExecutionReference,
  type ExecuteTurnCommandMessage,
  type ModelExecutionEvidence,
  type ModelMeteringContext,
  type DurableSessionCommit,
} from "@pi-cloud/protocol";

type Pending = {
  phase: string;
  outcome?: "completed" | "error" | "aborted" | "deferred";
  requests: ModelMeteringContext[];
};

/** Trusted Models/Storage adapter. Receipts settle with Pi's task transition,
 * never with a partial text update or the model HTTP response by itself. */
export class DurableModelAccounting {
  readonly #pending = new Map<number, Pending>();
  constructor(readonly command: ExecuteTurnCommandMessage) {}

  request(options: {
    taskId: number;
    phase: string;
    provider: string;
    model: string;
    purpose: "agent" | "context_maintenance";
    fetch?: typeof fetch;
  }) {
    const pending: Pending = { phase: options.phase, requests: [] };
    this.#pending.set(options.taskId, pending);
    const p = this.command.payload,
      identity = parseExecutionReference(p.executionReference);
    return {
      finish: (outcome: NonNullable<Pending["outcome"]>) => {
        pending.outcome = outcome;
      },
      fetch: (async (url: Parameters<typeof fetch>[0], init?: RequestInit) => {
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
        pending.requests.push(context);
        const headers = new Headers(
          init?.headers ?? (url instanceof Request ? url.headers : undefined),
        );
        headers.set(
          MODEL_METERING_HEADER,
          Buffer.from(JSON.stringify(context)).toString("base64url"),
        );
        return (options.fetch ?? globalThis.fetch)(url, { ...init, headers });
      }) as typeof fetch,
    };
  }

  prepare(commit: DurableSessionCommit): { evidence: ModelExecutionEvidence[]; confirmed(): void } {
    const evidence: ModelExecutionEvidence[] = [];
    const settled: [number, Pending][] = [];
    type Task = {
      id: number;
      state: { status: string; checkpoint?: { phase?: string }; outcome?: { status: string } };
    };
    const tasks = new Map<number, Task>();
    for (const write of commit.writes) {
      if (write.type !== "task") continue;
      const task = write.value as Task;
      tasks.set(task.id, task);
    }
    for (const task of tasks.values()) {
      const pending = this.#pending.get(task.id);
      if (
        !pending ||
        (task.state.status !== "terminal" && task.state.checkpoint?.phase === pending.phase)
      )
        continue;
      if (task.state.status !== "terminal" && !pending.outcome) continue;
      const outcome =
        task.state.outcome?.status === "aborted"
          ? "cancelled"
          : task.state.outcome?.status === "failed"
            ? "failed"
            : pending.outcome === "completed"
              ? "completed"
              : pending.outcome === "aborted"
                ? "cancelled"
                : "failed";
      for (const [index, context] of pending.requests.entries())
        evidence.push({
          context,
          outcome: index === pending.requests.length - 1 ? outcome : "superseded",
        });
      settled.push([task.id, pending]);
    }
    return {
      evidence,
      confirmed: () => {
        for (const [id, pending] of settled)
          if (this.#pending.get(id) === pending) this.#pending.delete(id);
      },
    };
  }
}
