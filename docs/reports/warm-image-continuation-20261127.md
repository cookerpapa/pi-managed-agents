# Warm continuation for image-heavy Session histories

The subsequent [Worker SSD acceptance](worker-ssd-continuation-20261127.md) covers disk hits, incremental writes and restart with a retained directory.

[ADR-0370](../adr/0370-large-history-warm-continuation.md) applies to the isolated #52 runtime. The first Turn of a new window or Fork loads its visible prefix from PG. A Worker can then hold the immutable, parsed native Entries after a normal completion. On a matching successor it constructs a fresh Pi instance using these Entries and their original commit positions. The new claim and the predecessor metadata are still read from PG. Image blocks in user messages and tool results share these rules.

The Worker advertises a small tenant/Flow/version hint over the existing management socket. Reuse also checks the logical Session and the confirmed predecessor’s version, native sequence, active lower bound, local Entry count and byte coverage. Writer replacement follows the ordered handoff. A changed Flow after pruning misses the old cache. A new head prunes the cached range, including inherited Entries below that head. A fresh model, Workspace, working directory, mailbox, Turn lease and fencing token are selected through the existing paths.

## Bounds

- Each Worker retains at most 256 MiB of serialized Entry payload across inactive snapshots by default. The minimum cacheable history is 256 KiB and the TTL is five minutes. These values are Worker constructor options; the byte budget excludes live Pi instances and JS heap overhead.
- For an advertised history of at least 16 MiB, a healthy busy holder can get up to two seconds before another available Worker cold-loads it. An available holder can take even a small cached continuation immediately. Eviction and disconnection release a known wait. Reconnecting Workers advertise their live cache again.
- Only targeted turns can bypass the PG ready time. Ordinary claims still satisfy ordering, lease and capacity checks. Uncached or stale hints can be discarded. A missing scheduler notice is covered by ordinary PG eligibility and the existing reconciliation tick; observed end-to-end dispatch can include the time to that tick and the availability of a slot. PG treats an inherited range conservatively when it cannot tell its size from the local head alone.
- The router tracks at most 4,096 advertised versions and 1,024 exact continuation notices. Under pressure it can drop a hint and choose the cold path. Cache state is volatile.

## Acceptance workload

The PG acceptance registers two independent, single-slot Workers with separate management connections and leases in the same test process. It constructs real PNGs, stores inline base64 across native user and tool-result Entries, opens Pi, and checks the entire selected model context with SHA-256. The large workload measures Worker-side history reads and Pi context construction with a deterministic in-process provider and automatic compaction disabled. The first historical segment is seeded directly in PG, subsequent Turn commits go through the candidate projector, and a separate RF3 suite covers process loss and native handoff. Timing ends at context construction, before a provider upload. An explicit native compaction head exercises shrinking the retained range.

The tests exercise ordinary priority, an unrelated Session admitted while the holder is full, expiry of a busy-owner preference, early disposal of a cached version, source and Fork continuation, a prune that keeps the logical Session ID, and ordinary claiming after all exact wake notifications have been lost.

## Development host results

Three serial trials used 104 inline PNGs (104.13 MiB of serialized history) with both user and tool-result image blocks. A PNG was also appended during a live native Turn to exercise cache capture of newly published images. Timed loads cover opening the Worker history and constructing Pi’s model context. Hashing the complete returned context is verified separately from that timing. Claims are measured from enqueue, except for eviction which is measured from disposal.

| Operation | Median, ms | Observed range, ms |
| --- | ---: | ---: |
| Ordinary cold continuation | 405 | 389–680 |
| Same history in a verified local cache | 5 | 5–6 |
| Claim by cache holder (fixture occupied it for at least 150 ms) | 225 | 221–244 |
| Claim by a different Worker after grace expiry | 2018 | 2018–2021 |
| Claim after the holder discarded its snapshot | 18 | 16–23 |
| Ordinary claim with the router disconnected | 2022 | 2017–2031 |
| Claim of unrelated work during the preferred wait | 49 | 20–63 |

Cold continuation returned 109,190,022–109,190,023 Entry-body bytes from PG. Matching root and Fork continuations returned zero history-body bytes. The cold control and hot context had the same full-message digest. After replacing the image prefix with a compaction head, cold loading returned 837 bytes and the context contained zero old images. A pruned window retained its public Session ID, opened cold on its new Flow and matched the retained context. This is a single-host comparison with three trials, rather than a service-level latency bound.

```sh
TSX_TSCONFIG_PATH=packages/control-plane/tsconfig.json node --import tsx --test scripts/experiments/minimal-session-storage/session-prefix.test.mjs
PMA_WARM_LARGE=1 TSX_TSCONFIG_PATH=packages/control-plane/tsconfig.json node scripts/run-with-test-postgres.mjs -- node_modules/.bin/vitest run scripts/experiments/minimal-session-storage/segment-warm-history.integration.spec.mjs --maxWorkers=1
PMA_SEGMENT_BROWSER=1 npm run test:transport
```

The RF3/PG transport suite passed 53 tests in 30 files. The separate four-case prefix suite covered authority, expiry and byte limits. A 1,000-Worker directed-wake regression used one PG listener: an idle fleet made zero claim queries during the observation interval, one arrival produced one claim, and eight independent arrivals produced eight claims. The 1,000 logical Workers used distinct registrations and sockets on the same development host.
