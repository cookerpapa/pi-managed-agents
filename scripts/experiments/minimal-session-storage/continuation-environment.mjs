// Both claims carry the admitted Run's model-facing environment. The comparison
// is with the previous normally completed Run, including on a cold Worker.
export function continuationPrompt(previous, current) {
  const lines = ["Current working directory: " + current.cwd];
  if (previous) {
    if (previous.workspace !== current.workspace) {
      lines.push("Workspace changed. Inspect the current files before reusing earlier paths.");
    } else {
      if (previous.cwd !== current.cwd)
        lines.push("Working directory changed from " + previous.cwd + " to " + current.cwd + ".");
      if (
        previous.processGeneration != null &&
        current.processGeneration != null &&
        previous.processGeneration !== current.processGeneration
      )
        lines.push(
          "Sandbox restarted. Workspace files are preserved; earlier processes may be gone.",
        );
    }
  }
  return lines.join("\n");
}

export const environmentSection = "workspace-environment";
