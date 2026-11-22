# ADR-0347. Remember the last confirmed compute allocation for each Conversation

The #52 storage candidate uses the existing resource allocation ID for equality.
There is one latest observation per tenant, physical Session stream and native
Conversation. Keep the scoped value in PostgreSQL across normal Run settlement.
Pi's confirmed active world observation is already ordered in the Session log.
Project it with the accepted native commit and its offset. On the candidate's
ordinary remote Guest path the execution adapter supplies a known allocation
at route selection. Keep the allocation on the private route span. Match the
canonical completed Tool to its original span in the existing settlement
transaction. A failed or aborted native task supplies no physical proof.
A per-lane last span in the active view avoids repeating the lookup for the
same route. The row remembers its original stream position on change so a
late result from older work cannot reverse a newer observation. An inactive observation,
a tool proposal, an uncertain result and a new chat admission cannot establish
that a Conversation used a different allocation.

At Fork creation, copy the selected source Conversation's current observation into
the independent branch root in the directory transaction. Nested branches take
the immediate parent's observation. Pruning retains the current observation and
deleting the source leaves its branches independent. A copy starts with an independent stream position. The history anchor continues
to select chat Entries and agent configuration under their existing policies.

When retaining existing elastic compute, compare its resource scope and allocation
with the registered Conversation observation. Persist the model-facing outcome in
the Run's admission snapshot. The Worker receives the consequence of a confirmed
replacement. A different ID always establishes a different allocation. Without a retained
release record, the prompt describes the change without claiming physical termination. Renewing an allocation in another compute scope cannot assert that
the old process ended. A cold native comparison can use a one-way equality token.
Admission does not transfer a physical routing handle or advance a
Conversation's last confirmed use. Reuse existing native storage, PG transactions,
resource identity and the Fork directory. No new provider API, counter, heartbeat
or historical observation log is required. The online storage cutover remains gated.
