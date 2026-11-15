import { Type, type Static } from "typebox";
import { Value } from "typebox/value";
import { UuidSchema, UtcTimestampSchema } from "./protocol-primitives.ts";

const Revision = Type.String({ pattern: "^[1-9][0-9]{0,14}$" });
const ToolName = Type.String({
  minLength: 1,
  maxLength: 256,
  pattern: "^[^\\u0000-\\u001f\\u007f]+$",
});
export const McpConnectionChangeSchema = Type.Object(
  {
    expectedRevision: Type.Union([Revision, Type.Null()]),
    name: Type.String({ minLength: 1, maxLength: 128 }),
    endpoint: Type.String({ minLength: 8, maxLength: 2048 }),
    visibility: Type.Union([Type.Literal("personal"), Type.Literal("organization")]),
    enabled: Type.Boolean(),
    allowedTools: Type.Union([Type.Null(), Type.Array(ToolName, { uniqueItems: true })]),
    credential: Type.Optional(
      Type.Union([
        Type.Object({ kind: Type.Literal("none") }, { additionalProperties: false }),
        Type.Object(
          {
            kind: Type.Literal("bearer"),
            token: Type.String({ minLength: 1, maxLength: 4096, pattern: "^[\\x21-\\x7e]+$" }),
          },
          { additionalProperties: false },
        ),
      ]),
    ),
  },
  { additionalProperties: false },
);
export type McpConnectionChange = Static<typeof McpConnectionChangeSchema>;

export const McpConnectionRevisionSchema = Type.Object(
  { expectedRevision: Revision },
  { additionalProperties: false },
);
export const McpConnectionResourceSchema = Type.Object(
  {
    id: UuidSchema,
    name: Type.String(),
    endpoint: Type.String(),
    ownerUserId: UuidSchema,
    visibility: Type.Union([Type.Literal("personal"), Type.Literal("organization")]),
    enabled: Type.Boolean(),
    revision: Revision,
    authentication: Type.Union([Type.Literal("none"), Type.Literal("bearer")]),
    status: Type.Union([
      Type.Literal("pending"),
      Type.Literal("ready"),
      Type.Literal("error"),
      Type.Literal("disabled"),
    ]),
    error: Type.Union([Type.String(), Type.Null()]),
    checkedAt: Type.Union([UtcTimestampSchema, Type.Null()]),
    toolCount: Type.Integer({ minimum: 0 }),
    allowedTools: Type.Union([Type.Null(), Type.Array(ToolName)]),
    canEdit: Type.Boolean(),
  },
  { additionalProperties: false },
);
export type McpConnectionResource = Static<typeof McpConnectionResourceSchema>;

export const McpConnectionToolSchema = Type.Object(
  {
    name: ToolName,
    platformName: Type.String(),
    description: Type.String(),
    parameters: Type.Record(Type.String(), Type.Unknown()),
  },
  { additionalProperties: false },
);
export type McpConnectionTool = Static<typeof McpConnectionToolSchema>;

export function parseMcpConnectionChange(value: unknown): McpConnectionChange {
  if (
    !Value.Check(McpConnectionChangeSchema, value) ||
    new TextEncoder().encode(JSON.stringify(value)).byteLength > 65536
  )
    throw new TypeError("Invalid MCP connection settings");
  let url: URL;
  try {
    url = new URL(value.endpoint);
  } catch {
    throw new TypeError("MCP endpoint must be an HTTP(S) URL");
  }
  if (!["http:", "https:"].includes(url.protocol) || url.username || url.password || url.hash)
    throw new TypeError(
      "MCP endpoint must be an HTTP(S) URL without embedded credentials or a fragment",
    );
  if (!value.name.trim()) throw new TypeError("MCP connection name is required");
  return { ...value, name: value.name.trim(), endpoint: url.toString() };
}

export function parseMcpConnectionRevision(value: unknown): string {
  if (!Value.Check(McpConnectionRevisionSchema, value))
    throw new TypeError("An MCP connection revision is required");
  return value.expectedRevision;
}
