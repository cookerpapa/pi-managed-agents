// Count completed model-facing entries once, independently of UI deltas or dispatch attempts.
export function visibleOutput(writes) {
  let texts = 0,
    calls = 0,
    results = 0;
  for (const w of writes) {
    if (w.type !== "entry" || !["pi.assistant", "pi.tool-result"].includes(w.value.kind)) continue;
    for (const m of w.value.model ?? []) {
      if (m.role === "toolResult") results++;
      if (m.role !== "assistant" || !Array.isArray(m.content)) continue;
      if (m.content.some((b) => b.type === "text" && b.text?.trim())) texts++;
      calls += m.content.filter((b) => b.type === "toolCall").length;
    }
  }
  return { texts, calls, results };
}
