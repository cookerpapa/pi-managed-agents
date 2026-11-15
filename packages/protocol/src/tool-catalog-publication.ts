import { Value } from "typebox/value";
import { Type, type Static } from "typebox";
import { ControlToolCatalogSchema, parseControlToolCatalog } from "./control-tool-catalog.ts";
import { UuidSchema } from "./protocol-primitives.ts";

export const TOOL_CATALOG_TOPIC = "pi-cloud.tool-catalog.v2";
export const TOOL_CATALOG_KEY = "platform";
export const MAX_TOOL_CATALOG_BYTES = 1_048_576;
export const MCP_CATALOG_SCOPE_PATTERN =
  /^mcp:[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
export const ToolCatalogScopeSchema = Type.Union([
  Type.Literal(TOOL_CATALOG_KEY),
  Type.String({ pattern: MCP_CATALOG_SCOPE_PATTERN.source }),
]);

export const McpCatalogAccessSchema = Type.Object(
  {
    tenantId: UuidSchema,
    ownerUserId: UuidSchema,
    visibility: Type.Union([Type.Literal("personal"), Type.Literal("organization")]),
    revision: Type.Integer({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER }),
    enabled: Type.Boolean(),
  },
  { additionalProperties: false },
);

export type ToolCatalogWatermark = Readonly<{ scope: string; generation: number }>;

export const ToolCatalogPublicationSchema = Type.Object(
  {
    version: Type.Literal(2),
    scope: ToolCatalogScopeSchema,
    generation: Type.Integer({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER }),
    releaseId: Type.String({ minLength: 1, maxLength: 256 }),
    catalog: ControlToolCatalogSchema,
    mcp: Type.Optional(McpCatalogAccessSchema),
    replayable: Type.Array(Type.String({ pattern: "^[a-z][a-z0-9_-]{0,63}$" }), {
      uniqueItems: true,
    }),
  },
  { additionalProperties: false },
);

export type ToolCatalogPublication = Static<typeof ToolCatalogPublicationSchema>;

export function parseToolCatalogPublication(value: unknown): ToolCatalogPublication {
  if (!Value.Check(ToolCatalogPublicationSchema, value))
    throw new Error("Invalid tool catalog publication");
  parseControlToolCatalog(value.catalog);
  const names = new Set(value.catalog.tools.map((tool) => tool.name));
  if (value.replayable.some((name) => !names.has(name)))
    throw new Error("Catalog replay policy names an absent Tool");
  if (value.scope === TOOL_CATALOG_KEY) {
    if (value.mcp || value.catalog.tools.some((tool) => tool.name.startsWith("mcp_")))
      throw new Error("Platform catalog cannot advertise scoped MCP tools");
  } else if (
    !value.mcp ||
    value.replayable.length ||
    value.catalog.tools.some(
      (tool) =>
        !tool.name.startsWith("mcp_") ||
        tool.environment.kind !== "none" ||
        tool.availability !== "root",
    )
  )
    throw new Error("MCP catalog has invalid access, execution or replay scope");
  return value;
}
