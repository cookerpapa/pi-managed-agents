import { createHash } from "node:crypto";
import {
  parseControlToolCatalog,
  type ControlToolCatalogDefinition,
  type ControlToolCatalog,
} from "@pi-cloud/protocol";

export function controlToolCatalog(
  tools: readonly ControlToolCatalogDefinition[],
): ControlToolCatalog {
  const definitions = JSON.parse(
    JSON.stringify([...tools].sort((a, b) => a.name.localeCompare(b.name))),
  ) as ControlToolCatalogDefinition[];
  return parseControlToolCatalog({
    version: 4,
    revision: createHash("sha256").update(JSON.stringify(definitions)).digest("hex"),
    tools: definitions,
  });
}
