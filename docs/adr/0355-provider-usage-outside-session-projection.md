# ADR-0355. Measure model usage at the gateway

Accepted for the matched #48 candidate on 2026-11-23. Supersedes the online execution-proof and financial writer-closure requirements of ADR-0330 and ADR-0342.

The gateway's per-HTTP invocation UUID, execution attempt IDs and provider receipts identify actual model consumption. The Worker supplies attribution at its Models HTTP port. Session publication, projection, replay and writer closure perform no accounting work. Ordinary Session fencing still controls accepted history and tool dispatch. When replaying a pre-cutover native commit, the historical fingerprint includes the original opaque metadata. Verifying those bytes creates no accounting event.

The Session API aggregates complete provider receipts for the authorized conversation family on demand. A late receipt can add newly observed usage. Repeated delivery of the same receipt or rejection of an obsolete Worker message cannot subtract tokens or count the receipt twice. Source gaps stay visible as unconfirmed usage.

New customer charge eligibility remains unconfirmed. Historical confirmed or waived statements keep their evidence and price versions. No payment collection or retrospective matching is introduced. A future metering reader could join accepted history to gateway receipts using a stable per-request identity shared by both sides. Identical message bodies, provider retries and interrupted requests make content hashes alone ambiguous. Decide about retaining that reference when implementing reconciliation.

The previous projection-cost comparison and its fixture are reproducible at commit 716d13ef. Verify removal on the same 20-tool workload, including SQL round trips, and exercise provider usage, stale-writer rejection, replay and root/child reads.
