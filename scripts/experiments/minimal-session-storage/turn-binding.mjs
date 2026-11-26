// Translate the admitted Turn into Pi's model and working context. The model
// registry supplies transports; only the Turn supplies the selected identity.
import { parseModelRuntimeSnapshot, resolveAgentProfile } from "@pi-cloud/protocol";
import {
  mergeProviderServiceTier,
  mergeProviderHostedTools,
} from "../../../packages/sandbox-supervisor/src/provider-hosted-tools.ts";
import { continuationPrompt } from "./continuation-environment.mjs";

export function turnBinding(job, models) {
  const { provider, modelId, thinkingLevel, serviceTier, runtime } = job.model_snapshot;
  const registered = models.getModel(provider, modelId);
  if (!registered || registered.provider !== provider || registered.id !== modelId)
    throw Error("Admitted Turn model is unavailable");
  const config = runtime && parseModelRuntimeSnapshot(runtime);
  if (config && registered.api !== config.api) throw Error("Turn model transport changed");
  const model = config
    ? {
        ...registered,
        contextWindow: config.contextWindow,
        maxTokens: config.maxTokens,
        input: config.inputModalities,
      }
    : registered;
  const current = job.workspace_grant,
    prior = job.previous_workspace;
  const world = (g) => g && { workspace: g.workspaceId, cwd: g.cwd };
  const instructions = [];
  if (current) {
    instructions.push(
      continuationPrompt(world(prior), {
        ...job.environment_snapshot,
        workspace: current.workspaceId,
        cwd: current.cwd,
      }),
    );
    if (
      prior?.workspaceId === current.workspaceId &&
      prior.computeSessionId !== current.computeSessionId
    )
      instructions.push(
        "The selected execution environment changed. Check any processes you need.",
      );
  } else if (prior) instructions.push("This Turn has no workspace access.");
  if (job.parent_session_id)
    instructions.push(
      "You are child Agent " +
        job.agent_name +
        ". Use send_message to contact parent when you have a result to share.",
    );
  return {
    model,
    autoCompactTokenLimit: config?.autoCompactTokenLimit,
    workspaceAvailable: !!current,
    turnEnvironment: instructions.join("\n") || undefined,
    agent: {
      cwd: current?.cwd ?? null,
      thinkingLevel: resolveAgentProfile(thinkingLevel).thinkingLevel,
    },
    context: {
      sessionId: job.logical_id,
      turnId: job.turnId,
      selectedThinkingLevel: thinkingLevel,
    },
    requestOptions(options, purpose) {
      return {
        ...options,
        onPayload: async (payload, descriptor) => {
          const previous = await options.onPayload?.(payload, descriptor);
          return mergeProviderServiceTier(
            mergeProviderHostedTools(
              previous ?? payload,
              purpose === "agent" ? (config?.hostedTools ?? []) : [],
            ),
            serviceTier,
          );
        },
      };
    },
  };
}
