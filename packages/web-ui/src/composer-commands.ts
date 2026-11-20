export type ReportCommand = "status" | "usage";
export type ComposerCommand =
  | { kind: "status" }
  | { kind: "usage" }
  | { kind: "compact"; instructions: string }
  | { kind: "invalid"; command: ReportCommand };
export function parseComposerCommand(text: string): ComposerCommand | undefined {
  const match = /^\/(status|usage|compact)(?:\s+([\s\S]*))?$/.exec(text.trim());
  if (!match) return undefined;
  const rest = (match[2] ?? "").trim();
  return match[1] === "compact"
    ? { kind: "compact", instructions: rest }
    : rest
      ? { kind: "invalid", command: match[1] === "status" ? "status" : "usage" }
      : { kind: match[1] === "status" ? "status" : "usage" };
}
export function composerCommandSuggestions(text: string): Array<ReportCommand | "compact"> {
  const value = text.trimStart();
  if (!/^\/[a-z]*$/.test(value)) return [];
  return (["status", "usage", "compact"] as const).filter((command) =>
    command.startsWith(value.slice(1)),
  );
}
export const compactionPending = (state: string) =>
  ["queued", "summarizing", "awaiting_placement"].includes(state);
