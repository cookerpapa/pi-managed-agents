# HTTP MCP connections

Tenant owners open **MCP** in the sidebar and add a Streamable HTTP endpoint.
Choose anonymous access or a Bearer token, personal visibility (the default) or
explicit organization sharing, and all discovered tools or a selected subset.
The connection becomes ready after discovery and catalog publication. Members
can use organization tools but cannot administer another person's connection.
Only root Conversations receive MCP tools in this first integration.

Personal scope is resolved against the Session's recorded owner, as with personal
Skill installations. It is not a new private-Session access-control system:
existing tenant collaborators who can steer that Session act through its Agent.
Use organization sharing only when the connection is intended for all members'
own Sessions; per-message credential delegation is outside this slice.

Saving, refreshing, disabling and removing a connection do not restart Workers
or the Session API. A running conversation sees directory changes on its next
model request. An already submitted model request retains its original tool list;
if its eventual call addresses a withdrawn implementation, it receives a definite
refusal instead of being redirected to a different service. Accepted calls may
finish after withdrawal. Removing a connection clears its stored credential and
publishes an empty catalog; its identity tombstone prevents stale restoration.

## What runs where

1. The Session API stores current connection settings in PG. The Tool host uses
   the pinned Pi MCP 1.0 client for initialization, discovery and invocation.
2. Credential-free definitions travel through the existing compacted catalog
   topic. Each connection has its own generation and audience. Workers register
   ordinary Pi Tool proxies and receive neither endpoint credentials nor MCP code.
3. Pi validates the arguments. The Worker commits its complete native intent and
   original reply reference to the Session Kafka partition. Projector confirms
   dispatch before submitting the call to the Tool host.
4. The response follows the original Kafka reply topic. Images use the existing
   attachment claim check. Worker/Pi processing produces the canonical Tool Result.

The Tool host checks the current connection revision and Session business owner,
not Worker lease validity. Registering a server does not grant it access to a
Workspace. An MCP server that needs files must provide its own explicit file or
device access. Skills remain a separate workflow/document capability.

## Failure and recovery

- A replacement Worker restores pending original reply references and deadlines.
  Its internal reply proxy remains available even when that tool was withdrawn
  from new requests. Recovery never resubmits an MCP operation.
- A recoverable Worker seal leaves an already admitted Tool-host call in flight.
  Native Stop cancels it through the existing ordered task state; cancellation
  does not prove that external effects were undone.
- A lost Tool-host/Projector caller or missing reply can produce UNKNOWN. A remote
  read-only annotation does not make replay safe. Inspect external state before
  deliberately repeating an uncertain action.
- MCP execution has a five-minute budget plus the existing two-minute reply
  delivery margin, measured from the original intent. Handoff and progress do not
  reset it. A result already retained in Kafka can be recovered after that time.
- HTTP JSON/error bodies and individual SSE messages have the platform 5 MiB
  transport bound. Completed text/details use the existing 50 KiB Tool result
  bound; an oversized completed result is an error, not permission to repeat it.

The initial supported surface is Streamable HTTP, anonymous/Bearer authentication,
tool discovery, text/image/structured results, notifications and cancellation.
STDIO process hosting, interactive OAuth, sampling, MCP tasks, resources and prompts
are outside this release. Use an independently hosted HTTP service when a provider
only distributes a local server. Pi's HTTP response-stream resumption can recover
a stream where supported; it does not resend a lost `tools/call` POST.

## Operations

`mcp_connections` stores settings and discovered metadata, not Tool results.
Tokens are encrypted with a dedicated 32-byte base64url key. The maintained
installer/matched updater creates `secrets/mcp-credential-master-key` once;
distributed operators provision the same Secret key themselves. Only Session API
and Tool host mount it through `PI_CLOUD_MCP_CREDENTIAL_MASTER_KEY_FILE`. Preserve
it with the configuration: replacing it makes existing credentials unreadable.
The UI never returns the original token and requires an explicit replacement or
removal when changing an authenticated endpoint.

The current topic is `pi-cloud.tool-catalog.v2`, with `platform` and
`mcp:<connection UUID>` keys in one RF=3 partition. PG selects each scope's
generation. Consumers restore all selected watermarks before admission. New code
activation publishes stored connection snapshots before the new platform record
so consumers do not temporarily lose every connection during an upgrade.
Discovery uses notifications and a periodic refresh; errors remain visible in
the connection page. No endpoint body, credential or Tool argument is logged.

API routes are `GET /v1/mcp/connections`, `GET|PUT|DELETE /v1/mcp/connections/:id`
and `POST /v1/mcp/connections/:id/refresh`. PUT creates with a client UUID and
`expectedRevision: null`; updates/deletes/refreshes require the current revision.
Reusing the same acknowledged change is idempotent. See the owned schemas in
`packages/protocol/src/mcp-connections.ts` and [ADR-0325](adr/0325-http-mcp-tools.md).
