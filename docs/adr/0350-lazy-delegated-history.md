# ADR-0350. Load a settled delegated transcript when it is continued

In the root-only Session experiment, a new Worker reads the root's settled suffix and configuration. The Projector initializes its checkpoint from the same root boundary. Explicit continuation resolves its frozen source, then attaches the corresponding immutable Pi transcript, walking history ancestry rather than caller ancestry. Finished siblings have no bootstrap role. Keep authored Entries in the existing messages table and retain the same accepted partition, Entry IDs, commit coordinates and allocation head.

Use Pi's pinned public MemoryStorage to resolve paging, document copies and compaction within the selected path. A Worker can hold one writable Session and bounded-by-use, per-source in-memory views. The storage port publishes the original ordered native batch and applies the prepared writes locally only after acknowledgement. A failed load may be retried, and concurrent loads of the same source share the read.

On forced takeover, restore live Tasks, their referenced Entries and their required ancestors before Pi resumes. Unrelated settled transcripts stay in PG. Measure selected Entry counts and worker cold-read latency using the previous 1/100/500-agent fixture. The matched production release and database schema remain outside this prototype.
