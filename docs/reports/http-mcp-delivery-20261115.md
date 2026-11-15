# HTTP MCP delivery and acceptance

Date: 2026-11-15. Issue: #40. Decision: [ADR-0325](../adr/0325-http-mcp-tools.md).
Status: implemented, deployed and live-verified.

## Result

Tenant owners can manage personal-Session or organization HTTP MCP connections in
the sidebar. The execution-side Tool host uses the pinned MIT-licensed
`@earendil-works/pi-mcp@1.0.0` client. Workers receive scoped definitions through
the existing Kafka catalog and register Pi-native proxies. Adding, refreshing or
withdrawing connections does not restart Workers or the Session API.

Tool calls use the existing Session intent, Projector admission and original
Kafka reply path. Replacement Workers restore original operations/deadlines; an
accepted MCP call survives recoverable Worker closure. A withdrawn tool keeps
only its internal reply proxy and is absent from new model requests. Pi core,
native task definitions and storage batches are not patched.

PG stores current connection settings, encrypted credentials and discovered
metadata. No separate operation ledger or result database was introduced. The
MCP encryption key is mounted only in Session API and Tool host. Execution checks
current connection/business ownership without checking Worker lease expiry.

## Published artifacts and preservation

- Application source: `015c413fe20c13d241d0e004eb6b5df9805db89c`.
- Maintained matched release: `http-mcp-20261115-r1`; schema
  `225_http_mcp_connections`, catalog publication 2 / topic v2. Session topic v21
  and reply topic v4 retain their identities.
- Guest identity: `5eedbc2462a8c30980612d2c86d6ec57eed6b780`. Shared dependency
  manifests changed its build inputs, so this initial release included native
  compute retirement and recovery with retained Workspaces.
- Six artifacts were built and eight reused. Fourteen image checks passed the
  maintained policy, including the existing exact stock-Cube exception.
- Original account/model/provider/credential records, two development-machine
  identities and all 393 checked project files were preserved. Both original
  machines returned to `ready` with `desired_state=running`.
- All four live catalog consumer groups were verified on v2 before deleting the
  obsolete `pi-cloud.tool-catalog.v1` topic. No compatibility reader remains.

Preparation encountered a direct-route vulnerability-database download failure;
the pinned scanner downloaded through the configured proxy. Old Docker build
cache was reclaimed to satisfy the existing disk guard. The application stayed
on its prior version until a complete candidate passed preflight.

## Verification

Source checks covered global types, scoped metadata/actor visibility, encrypted
credential CAS, rotation and revocation, HTTP result conversion, bounded bodies,
response-stream resumption, cancellation and lost-write behavior. The final
focused regression run passed 70 tests across 11 files. Native storage/rebuild
and cleanup-key checks passed 94 tests; the final HTTP/rotation suite passed six.
The three MCP recovery cases are part of the maintained fault-evaluation manifest.

The isolated RF=3 Kafka/PostgreSQL transport suite passed 11 tests across nine
files, including multi-scope catalog cold bootstrap/withdrawal and protected
original replies. Browser acceptance covered connection creation, personal
default, required Bearer input, masked edits, credential preservation, disabling,
confirmed removal and member visibility. Installer, Helm, runtime policy, image
closure/context, formatting, documentation, web build and dependency audit checks
also passed.

The live check used an owned HTTP server inside the existing cluster and the real
`gpt-6-luna` provider through the published services. It verified:

1. Text and a valid 64×64 PNG reached canonical native Tool Results; the test
   tenant allocated no VM for those MCP calls.
2. A tool added while another call was pending became callable within the same
   native submission, without replacing its Worker.
3. The owning Worker was killed while a call waited, and its connection was
   withdrawn. A replacement lease resumed the same logical Run, received the
   original result once, and the fixture recorded exactly one physical call.
4. Native Stop emitted upstream cancellation. A deliberately late reply did not
   replace the aborted native result.
5. A dropped response after a controlled write returned UNKNOWN; the fixture
   recorded one effect and no automatic repeat.

Temporary tenants, conversations, attachments, Workspaces, connections and
Kubernetes fixture resources were removed. Initial fixture failures (an endpoint
outside the configured egress ports and an invalid tiny PNG) were corrected in
the maintained acceptance harness; neither was counted as a successful check.
Private evidence remains under `.cache/mcp-20261114/`; transcripts and credentials
are not committed.

## Boundaries and remaining work

The supported slice is Streamable HTTP with anonymous/Bearer authentication,
root-Conversation tools, text/image/structured results and cancellation. OAuth
UI, STDIO hosting, MCP sampling/tasks and resources/prompts are not enabled.
Personal scope follows the existing Session owner/collaboration model; it is not
a private-Session ACL or per-message credential delegation. Registered endpoints
must satisfy deployment egress/proxy configuration. See [MCP operations](../MCP.md).

The separate low-priority external Session SDK issue #41 remains deferred. The
existing native Cube template cleanup problem also remains: this publication
reported eight older template candidates with unconfirmed native deletion.
Their ownership records remain pending; this does not prevent current template
use or original-machine recovery. WSL capacity and BoxLite isolation settings
were not changed.
