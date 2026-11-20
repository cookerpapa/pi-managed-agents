# ADR-0341. Measure shared Forks on the minimal message projection

Use the isolated performance branch and the ADR-0340 transaction-local policy.
Compare identical Kafka bytes and PG durability before and after adding real
Fork retention. Reuse the immutable range directory and its PostgreSQL adapter
from ADR-0287. Keep the existing message as its own immutable body. Guard its
source stream independently of the scheduling Run, add a covering range index,
and retain a small completed-Run header in the existing finish call. Freeze
selected Conversation and Entry bounds together with the observed native
publication sequence. Directory fingerprints in this experiment cover stable
record identities and metadata. The benchmark independently verifies actual
body bytes.

Manual Fork creates an independent root and local stream in a synchronous PG
transaction at a projected completed assistant anchor. A client retry reuses
its operation identity. Source pruning rotates the writable stream under
quiescence. Retired history stays reachable through destination roots. GC
coordinates cold reference mutations and never changes execution ownership.
The normal append has no branch enumeration, per-Fork updates or protocol fields.

Measure projection with the feature disabled, enabled, with existing Forks and
with concurrent Fork traffic. Measure transaction and first-read latency by
entry count, body size, selected position and nested branches. Check source
prune/deletion, late lower IDs, independent appends, and actual reference
collection. This assay measures the message storage path. Serving API, native
document import and presentation/attachment integration retain their existing
independent acceptance requirements.
