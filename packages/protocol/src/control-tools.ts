import { Type, type Static, type TSchema } from "typebox";
import { Value } from "typebox/value";
import { MAX_AGENT_INPUT_CHARACTERS, assertSubagentTaskInputBudget } from "./agent-input-budget.ts";
import { CloudToolNameSchema } from "./tool-capabilities.ts";

const context = Type.Union([Type.Literal("fresh"), Type.Literal("branch")], {
  description:
    "fresh starts from this task; branch inherits a frozen parent conversation. Context does not grant workspace or device access.",
});
const cwd = Type.String({
  minLength: 1,
  maxLength: 4096,
  pattern: "^/[^\\u0000-\\u001f\\u007f]*$",
  description:
    "An existing directory on the Linux workspace Volume. Defaults to the parent's working directory.",
});
export const SubagentWorkspaceSchema = Type.Object(
  {
    compute: Type.Optional(
      Type.Union([Type.Literal("shared"), Type.Literal("ephemeral")], {
        description:
          "Linux compute only: shared (default) reuses parent compute; ephemeral starts independent temporary compute on the same persistent Volume. Files are shared. This never creates or clones a phone.",
      }),
    ),
    cwd: Type.Optional(cwd),
    tools: Type.Optional(
      Type.Array(CloudToolNameSchema, {
        minItems: 1,
        maxItems: 4,
        uniqueItems: true,
        description:
          "Nonempty subset of the parent's Linux file/shell tools. Omit to inherit; use workspace:null to disable workspace access.",
      }),
    ),
  },
  { additionalProperties: false },
);
export type SubagentWorkspace = Static<typeof SubagentWorkspaceSchema>;
export const SubagentToolSchema = Type.Object(
  {
    task: Type.String({ minLength: 1, maxLength: MAX_AGENT_INPUT_CHARACTERS }),
    context: Type.Optional(context),
    workspace: Type.Optional(
      Type.Union([SubagentWorkspaceSchema, Type.Null()], {
        description:
          "Omit to inherit existing Linux workspace access, or use null for no workspace-dependent tools or compute. This does not grant control of phones or other devices.",
      }),
    ),
  },
  { additionalProperties: false },
);

export const NamedSubagentToolSchema = Type.Object(
  {
    ...SubagentToolSchema.properties,
    name: Type.String({
      minLength: 1,
      maxLength: 64,
      description:
        "Stable sibling name, for example payment-tests. Use the same name to find or retry this child.",
    }),
  },
  { additionalProperties: false },
);
export function subagentName(raw: unknown): string {
  if (typeof raw !== "string") throw new Error("A subagent name is required");
  const name = raw.normalize("NFC");
  if (
    !/^[\p{L}\p{N}][\p{L}\p{N}_.-]{0,63}$/u.test(name) ||
    ["main", "parent"].includes(name) ||
    /^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/i.test(name)
  )
    throw new Error("Invalid or reserved subagent name");
  return name;
}
export function prepareNamedSubagentArguments(
  value: unknown,
): Static<typeof NamedSubagentToolSchema> {
  if (!Value.Check(NamedSubagentToolSchema, value))
    throw Error("Invalid named delegation arguments");
  assertSubagentTaskInputBudget(value.task);
  return { ...value, name: subagentName(value.name) };
}
/** Root coding Conversations own a Workspace; a child with no file/shell grant
 * has workspace:null. Its ordinary workspace tools must follow the same rule. */
export function hasConversationWorkspace(scope: {
  sessionKind?: "conversation" | "subagent";
  toolCapabilities?: readonly string[];
  executionTargets?: readonly { kind: string }[];
}): boolean {
  if (scope.executionTargets?.some((target) => target.kind === "linux")) return false;
  return (
    scope.sessionKind !== "subagent" ||
    scope.toolCapabilities === undefined ||
    scope.toolCapabilities.length > 0
  );
}
const controlActions = Type.Union([
  Type.Object(
    {
      action: Type.Literal("list"),
      cursor: Type.Optional(
        Type.String({
          pattern: "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
        }),
      ),
    },
    { additionalProperties: false },
  ),
  Type.Object(
    {
      action: Type.Union([Type.Literal("status"), Type.Literal("wait"), Type.Literal("cancel")]),
      target: Type.String({ minLength: 1 }),
    },
    { additionalProperties: false },
  ),
  Type.Object(
    {
      action: Type.Literal("send"),
      target: Type.String({ minLength: 1 }),
      message: Type.String({ minLength: 1 }),
      delivery: Type.Union([
        Type.Literal("notify"),
        Type.Literal("steer"),
        Type.Literal("follow_up"),
      ]),
    },
    { additionalProperties: false },
  ),
]);
const SubagentControlSchema = Type.Object(
  {
    action: Type.Union([
      Type.Literal("list"),
      Type.Literal("status"),
      Type.Literal("wait"),
      Type.Literal("cancel"),
      Type.Literal("send"),
    ]),
    target: Type.Optional(Type.String({ minLength: 1 })),
    cursor: Type.Optional(Type.String()),
    message: Type.Optional(Type.String({ minLength: 1 })),
    delivery: Type.Optional(
      Type.Union([Type.Literal("notify"), Type.Literal("steer"), Type.Literal("follow_up")]),
    ),
  },
  { additionalProperties: false },
);
export function prepareSubagentArguments(value: unknown): Static<typeof SubagentToolSchema> {
  if (!Value.Check(SubagentToolSchema, value)) throw new Error("Invalid delegation arguments");
  assertSubagentTaskInputBudget(value.task);
  return value;
}
export function prepareSubagentControlArguments(value: unknown): Static<typeof controlActions> {
  if (!Value.Check(controlActions, value)) throw new Error("Invalid Subagent control arguments");
  return value;
}
export type ControlToolName =
  "subagent" | "subagent_control" | "contact_supervisor" | "subagent_supervisor";
export type InfrastructureToolDefinition = {
  name: ControlToolName;
  description: string;
  parameters: TSchema;
  replay?: "safe" | "unsafe";
  prepareArguments?(value: unknown): unknown;
  availability?: "child" | "delegation" | "root";
  inheritedContext?(args: Record<string, unknown>): boolean;
};
const subagent: InfrastructureToolDefinition = {
  name: "subagent",
  replay: "safe",
  parameters: SubagentToolSchema,
  prepareArguments: prepareSubagentArguments,
  availability: "delegation",
  inheritedContext: (args) => args.context === "branch",
  description: [
    "Delegate one concrete task to another Pi Agent and wait for its result or supervisor question.",
    "Recovery continues this same delegation; a new Tool call creates distinct work. Use subagent_control to list, inspect or message existing children.",
    "Task text is limited to 100,000 characters and 256 KiB of JSON. fresh uses the task only; branch inherits a frozen parent context.",
    "workspace only configures Linux working files and compute. Omit it to inherit existing workspace access; null disables all workspace-dependent tools. workspace.compute=shared reuses parent compute; ephemeral lazily creates temporary Linux compute on the same Volume without copying files.",
    "workspace.cwd must already exist. Use a Git worktree for independent code changes and coordinate shared-file edits. Do not commit or discard user changes without permission.",
    "workspace.tools is a nonempty subset of inherited read/write/edit/bash capabilities. Hosted tools and delegation remain independently configured. Children do not inherit phone/device control; this Tool never creates or allocates a second phone.",
    "Results are limited to 50 KiB UTF-8 or 2000 lines. Return a summary/path for larger output; a result-size error does not undo effects.",
  ].join(" "),
};
const subagentControl: InfrastructureToolDefinition = {
  name: "subagent_control",
  replay: "unsafe",
  parameters: SubagentControlSchema,
  prepareArguments: prepareSubagentControlArguments,
  description: [
    "Inspect and manage children of this physical Session. list needs no target and returns execution IDs, task summaries and state; follow nextCursor for more.",
    "Use an executionId from list or delegation as target for status/wait/send/cancel. list includes ended children in the current root work.",
    "send uses notify, steer or follow_up. notify does not wake an idle Agent; steer reaches active work; follow_up queues input.",
    "send may target main or parent. cancel stops current child work. Interrupted management is not automatically replayed; inspect state before repeating it.",
  ].join(" "),
};
const contactSchema = Type.Object(
  {
    reason: Type.Union([
      Type.Literal("progress_update"),
      Type.Literal("need_decision"),
      Type.Literal("interview_request"),
    ]),
    message: Type.String({ minLength: 1 }),
    interview: Type.Optional(Type.Record(Type.String(), Type.Unknown())),
  },
  { additionalProperties: false },
);
const supervisorSchema = Type.Object(
  {
    action: Type.Union([Type.Literal("pending"), Type.Literal("reply"), Type.Literal("wait")]),
    replyTo: Type.Optional(Type.String()),
    message: Type.Optional(Type.String()),
  },
  { additionalProperties: false },
);

/** Frozen code-owned catalogue; handlers never import the Pi Runner. */
export function infrastructureToolDefinitions(
  named = false,
): readonly InfrastructureToolDefinition[] {
  return [
    named
      ? {
          ...subagent,
          parameters: NamedSubagentToolSchema,
          prepareArguments: prepareNamedSubagentArguments,
          description: subagent.description.replace(
            "Recovery continues this same delegation; a new Tool call creates distinct work.",
            "Choose a stable name unique among your children. Same name, task and options return the existing child or its original result. Conflicting options return a name conflict.",
          ),
        }
      : subagent,
    named
      ? {
          ...subagentControl,
          description:
            subagentControl.description +
            " A direct child can also be addressed by its name across Runs in this Session.",
        }
      : subagentControl,
    {
      name: "contact_supervisor",
      availability: "child",
      description:
        "Send a durable progress update or request a decision from the parent Agent. A decision request waits for its reply.",
      parameters: contactSchema,
    },
    {
      name: "subagent_supervisor",
      description:
        "Inspect pending Child requests, reply, and wait for the Child result. This does not start another Child.",
      parameters: supervisorSchema,
    },
  ];
}
export function isControlToolName(value: unknown): value is ControlToolName {
  return ["subagent", "subagent_control", "contact_supervisor", "subagent_supervisor"].includes(
    String(value),
  );
}
