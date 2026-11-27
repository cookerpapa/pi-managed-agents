# ADR-0375. Isolated serving entry for the first independent Session conversation

Run the public Session API, the independent Session Projector and the Worker as separate image processes in an isolated installation. Use a separate database and a dedicated multi-partition Session topic. A selected database owns exactly one admission queue and one writer lineage. Selecting the new mode on the existing installation is outside this gate.

The first slice covers authenticated text input, directed claim, the pinned Pi Harness, a run-bound local Model Gateway capability, CLIProxyAPI, ordered Kafka output, PG history, live browser reads and cold continuation on another Worker. The public Session and its writable Flow are registered atomically. A Worker uses the same logical Session partition for native writes and terminal publication. The Projector publishes browser updates after admission. It retires the prior writer before a successor may resume its Turn.

Use the actual provider for a small functional check and deterministic traffic for the throughput comparison. Record the outstanding product parity and cutover dependencies after integration. Preserve accounts, provider configuration, credentials and current writers throughout staging.

A directed wake can claim several ready Sessions in one transaction, bounded by the Worker’s free slots and a configurable batch limit (default 8, maximum 32). A targeted warm-history Session takes the first slot. Each claimed Session receives its own lease and fencing epoch under the same admission checks.
