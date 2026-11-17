const HIGH = new Set(["high", "critical"]);
const REVIEWED = "https://github.com/advisories/GHSA-fc5q-39xc-vjpf";

/** ADR-0333: one exact dev-only leaf, never a package-wide severity override. */
export function reviewDevelopmentAudit(audit, lock) {
  const findings = audit.vulnerabilities ?? {};
  const devOnly = (finding) =>
    Array.isArray(finding.nodes) &&
    finding.nodes.length > 0 &&
    finding.nodes.every(
      (node) => lock.packages?.[node]?.dev === true && lock.packages[node].devOptional !== true,
    );
  const leafReviewed = (advisory, finding) =>
    advisory.url === REVIEWED &&
    advisory.name === "picomatch" &&
    advisory.dependency === "picomatch" &&
    finding.name === "picomatch" &&
    Array.isArray(finding.nodes) &&
    finding.nodes.length === 1 &&
    finding.nodes[0] === "node_modules/picomatch" &&
    lock.packages?.[finding.nodes[0]]?.version === "4.0.5" &&
    devOnly(finding);

  const evidence = (name, visiting = new Set()) => {
    const finding = findings[name];
    if (!finding || visiting.has(name) || !Array.isArray(finding.via))
      return { reviewed: false, found: false };
    const next = new Set(visiting).add(name);
    let found = false;
    for (const via of finding.via) {
      if (typeof via === "string") {
        const child = evidence(via, next);
        if (!child.reviewed) return { reviewed: false, found: child.found };
        found ||= child.found;
      } else if (HIGH.has(via.severity)) {
        if (!leafReviewed(via, { ...finding, name })) return { reviewed: false, found: true };
        found = true;
      }
    }
    if (HIGH.has(finding.severity) && (!found || !devOnly(finding)))
      return { reviewed: false, found };
    return { reviewed: true, found };
  };
  const blocking = [],
    reviewed = [];
  for (const [name, finding] of Object.entries(findings)) {
    if (!HIGH.has(finding.severity)) continue;
    const result = evidence(name);
    if (result.reviewed && result.found)
      reviewed.push({ name, advisory: REVIEWED, nodes: finding.nodes });
    else blocking.push({ ...finding, name });
  }
  return { blocking, reviewed };
}
