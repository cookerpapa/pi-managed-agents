import { resolveAgentProfile, type AgentThinkingLevel } from "@pi-cloud/protocol";

export function delegationInstructions(selected: AgentThinkingLevel): string {
  const profile = resolveAgentProfile(selected);
  const policy =
    profile.delegation === "proactive"
      ? "You may proactively delegate concrete independent work to subagents when it materially helps the task. Avoid delegation for trivial work or extra review stages with no concrete benefit."
      : "Use subagents only when the user, or instructions explicitly adopted for this task, requests agent delegation or parallel agent work. Tool availability alone is not an instruction to delegate. Complete other work yourself.";
  return `${policy} Give each child a clear scope, deliverable and completion condition. Children inherit the same delegation policy. Inspect the child results. Answer paused children through subagent_supervisor. Complete necessary validation, return useful results promptly and disclose unavailable checks; expand the task only for a concrete benefit.`;
}
