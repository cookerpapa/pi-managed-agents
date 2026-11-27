# Indexed Turn selection in the serving candidate

The independent Session Worker now reads the presence of input through a bounded lookup on the existing per-Turn mailbox index. Empty input returns false. A Turn containing only withdrawn input returns true with no eligible first message. The scheduler checks blockers against a partial index over tenant, physical Flow and mailbox position. It includes queued and other nonterminal Turns and any Turn carrying a seal intent whose seal has not yet been projected. Both changes are active in the isolated serving entry by default. [Worker query and index definition](../../scripts/experiments/minimal-session-storage/independent-session-leases.mjs) · [Original investigation](session-claim-selector-20261127.md).

## Installing the index

A fresh installation builds the index in its schema transaction. When upgrading an existing installation, the migrator validates the topic and partition count, holds the existing installer advisory lock, and runs CREATE INDEX CONCURRENTLY. Ordinary inserts and updates can continue during the scan. PG may wait for older transactions before completing the build. A failed concurrent build can leave an invalid index. Retrying the migration drops that invalid index concurrently and rebuilds it. A healthy index is reused with the same OID. The installation marker and persisted Session identities retain their existing format. [Migrator](../../scripts/serving/independent-session/migrate.mjs).

The image check exercised all of those paths in a disposable database with an authenticated Session. It removed the fresh index to simulate the previous installation, deliberately held a writer transaction, executed a separate Turn insert while the build was waiting, and rolled that test insert back. Cancelling the build produced the invalid index. The next migration repaired it and a further migration preserved the valid index. Real HTTP admissions, Kafka projection and Stop ran against the repaired installation.

For rollback, the previous Worker can run with the index present. To revert the index itself, use the previous migrator revision and run the following outside a transaction on the segmented database. A subsequent upgrade with the new migrator recreates it.

```sql
DROP INDEX CONCURRENTLY IF EXISTS pma_segments.turns_unsettled_by_session;
```

## Reading through the integrated Worker

The study takes the exact FIFO and warm-hint SQL emitted by the Worker. It reconstructs the previous input expression only for the baseline and removes the fixture index for the unindexed comparisons. PG execution medians below exclude planning and network time. The 4,105-Session dataset includes nine histories of 4,096 completed Turns each and 41,160 input rows. Both revisions returned the same eight Session IDs in FIFO order.

| Fixture, request size | Previous selector | Integrated selector | Shared buffer hits, previous → integrated |
| --- | ---: | ---: | ---: |
| 192 fresh Sessions after ANALYZE, take eight | 0.61 ms | 0.66 ms | 391 → 376 |
| Nine ready long Sessions, take eight | 9.54 ms | 0.53 ms | 3,120 → 172 |
| 4,105 ready Sessions including long histories, take eight | 26.33 ms | 0.54 ms | 41,749 → 455 |

The final plan on the large fixture used the unfinished-Turn index and avoided an all-input scan. There were zero physical reads in these warm-buffer samples. A preferred warm Turn returned zero candidates while its predecessor had an unprojected seal and one after the seal became visible. The populated-queue concurrency test also verifies the exact first input and boolean for empty, placed and withdrawn-only inputs. [Plan gate](../../scripts/experiments/minimal-session-storage/claim-selector-study.mjs) · [Real claim and concurrency gate](../../scripts/experiments/minimal-session-storage/segment-wake.integration.spec.mjs).

## Cost of maintaining the index

Two schemas on the same disposable two-CPU PG used the current Worker query, the same Session defaults and the same workload. Only one schema had the new index. Each arm reused 64 Sessions over ten Turns per Session, alternating arm order. The first two rounds warmed the fixture. Each measured round admitted 64 queued Turns, claimed them in batches of eight with real registered Worker leases and projected 64 ordered completions individually. Each completion verified a seal. The timer includes PG round trips and normal triggers. WAL is measured from cluster WAL insert positions with the other arm idle, outside schema setup and index construction. The fixture uses empty submissions and zero native commits to isolate Turn scheduling and settlement. [Write study](../../scripts/experiments/minimal-session-storage/claim-index-write-study.mjs).

| Stage | Without index, ms/Turn | With index, ms/Turn | Without index, WAL bytes/Turn | With index, WAL bytes/Turn |
| --- | ---: | ---: | ---: | ---: |
| Admit | 0.259 | 0.263 | 1,426 | 1,476 |
| Claim | 1.832 | 1.858 | 1,776 | 1,930 |
| Settle and seal | 6.218 | 6.318 | 1,078 | 1,079 |
| Complete lifecycle | 8.379 | 8.429 | 4,289 | 4,537 |

The rows show medians over eight rounds. The total is the median of each round’s full lifecycle, so the individual medians need not sum to it. The overall difference was about 248 extra WAL bytes per Turn (5.8%). Per-round paired differences in elapsed time ranged from −0.14 to +0.28 ms/Turn. There was no consistent large timing difference on this fixture.

## End-to-end serving measurements

All trials used 64 HTTP submission clients, 64 Worker slots, batch limit eight, one Projector, four RF3 Kafka partitions and two PG CPUs on the same host. Each Turn produces one visible completed assistant answer. Model delay is zero. The older image had the previous SELECT and no new index. The candidate relied on its own migration. All trials used the indexes installed by their images. The higher-rate pair was repeated in reverse order. Every trial sealed the expected Turns, reached zero final lag and passed Stop during an in-flight request.

| Input target/s | Image, in execution order | Visible answers/s including drain | Peak raw Kafka lag | Drain after final admission | Terminal to PG p99 |
| ---: | --- | ---: | ---: | ---: | ---: |
| 20 | Integrated | 19.83 | 9 | 0.21 s | 71 ms |
| 60 | Previous | 48.47 | 437 | 4.76 s | 3,345 ms |
| 60 | Integrated | 29.32 | 441 | 20.91 s | 4,005 ms |
| 60 | Integrated | 26.82 | 447 | 24.69 s | 3,978 ms |
| 60 | Previous | 25.57 | 448 | 26.90 s | 5,082 ms |

Actual admission rates in the 60/s trials were 59.85–60.00/s. Both integrated runs accumulated a backlog at that rate. The first comparison favored the previous image. The reversed pair was slightly faster on the integrated image. The short, fresh-Turn workload establishes correctness through serving and exposes backlog on this host. The end-to-end capacity gain from the long-history optimization remains unproven. Production storage cutover remains a separate decision.

Seventeen focused regression cases passed across the Session mailbox, directed claims, independent leases, parallel Worker admission and authority timing. The claim check uses the production selection path for empty, placed and withdrawn-only mailboxes. The image and migration checks used a dedicated database and the synthetic model provider.

## Reproduction

Use the candidate role images and run each workload serially. The first two commands create their own disposable PG fixtures. The image check uses the local deterministic model upstream.

```sh
export TSX_TSCONFIG_PATH="$PWD/packages/control-plane/tsconfig.json"
node --import tsx scripts/experiments/minimal-session-storage/claim-selector-study.mjs
node --import tsx scripts/experiments/minimal-session-storage/claim-index-write-study.mjs
PI_CLOUD_CHECK_ARRIVALS_PER_SECOND=20 PI_CLOUD_CHECK_TURNS=400 node --import tsx scripts/run-independent-session-serving-check.mjs "$IMAGE_TAG" --load --verify-claim-upgrade
PI_CLOUD_CHECK_ARRIVALS_PER_SECOND=60 PI_CLOUD_CHECK_TURNS=1200 node --import tsx scripts/run-independent-session-serving-check.mjs "$IMAGE_TAG" --load
```
