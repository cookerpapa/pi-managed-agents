// Both claims carry the admitted Run's model-facing environment. The comparison
// is with the previous normally completed Run, including on a cold Worker.
export function continuationPrompt(previous, current, now = Date.now()) {
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
  if (current.elastic) {
    const { state, until } = current.elastic;
    const descriptions = {
      retained:
        Date.parse(until) > now
          ? "The platform retained the existing elastic sandbox when this Run was accepted."
          : "The retention deadline recorded at admission has passed. Check the environment before relying on earlier processes.",
      closing:
        "The elastic sandbox was being reclaimed when this Run was accepted. Workspace files are preserved.",
      released:
        "The previous elastic sandbox had been released when this Run was accepted. Its processes ended. Workspace files are preserved.",
      unallocated:
        "No elastic sandbox was allocated when this Run was accepted. A workspace tool can start one on demand.",
      starting: "The elastic sandbox was being prepared when this Run was accepted.",
      unknown: "The elastic sandbox state was unconfirmed when this Run was accepted.",
    };
    lines.push(descriptions[state]);
  }
  return lines.join("\n");
}

export const environmentSection = "workspace-environment";
