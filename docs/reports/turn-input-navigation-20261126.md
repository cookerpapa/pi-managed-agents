# Turn settings, adopted input navigation and Stop

Verified on the isolated #52 storage composition with the public Session HTTP client, PostgreSQL, three Kafka brokers and separate Worker processes. Model responses and development-machine registrations are fixtures. The matched production storage cutover remains gated under #48.

## Session behavior

- A Turn admitted with model A kept its model, thinking level, service tier and working directory after a Worker was killed. Model B had already been selected in the browser-facing API. The replacement resumed the same Turn on model A and the next Turn used B. The public Session configuration and a named child's inherited grant were checked independently.
- A queued or running Turn prevented a directory rebind. Rebinding an idle Session to a dedicated development-machine Workspace changed the directory in the following model request. A peer Fork retained its previous settings.
- Navigation returned each adopted human or agent input, including multiple inputs during one running Turn. Paging and transcript seeks used canonical Entry IDs. Pending mail, withdrawn input, native maintenance entries and assistant answers were absent from the navigation list. Transcript Fork and prune actions kept their own answer IDs.
- A completed Turn was forked through the public API. The source then received more Turns, was deleted through the public API and its editable mailbox was removed. The Fork's navigation returned the same Entry IDs and captions after source collection. Removing the last peer freed the original adopted inputs.
- During model sampling, a user Stop produced native cancellation and an ordered Kafka terminal record. Unread Agent mail stayed held until a new human Turn. The model subsequently received the new instruction followed by the earlier report once. Additional checks cover mail received during Stop, reopening before the old terminal arrives, idempotent Stop retries, preservation of a reserved report and return of a human draft. Worker loss before and after native placement continues to use the ordered seal.
- The public cancellation notice travels through the existing directed Worker connection. A missed notice is repaired by the existing per-boot batched reconciliation. A pending inter-agent message has no Turn ID while held by Stop.

[Settings decision](../adr/0368-session-defaults-and-turn-snapshots.md) · [Navigation and Stop decision](../adr/0369-placed-input-navigation.md)

## Projection cost on the development host

Each trial replayed the same Pi commit trace for 32 Sessions, with 20 tool calls and 41 user-visible records per Session. Trials used 32 partitions, a pool of 12 PG connections and 2,752 projection transactions. These measurements cover projection into PG. Model-provider latency and VM dispatch are outside the timed interval. Each comparison discarded two warm-ups and used twelve counterbalanced pairs.

| Comparison | Median visible records/s in control | Median visible records/s with feature | Median change within pairs |
| --- | ---: | ---: | ---: |
| Previous revision `83fe39cf`, mailbox receipt disabled / enabled | 1,077 | 1,010.5 | −5.8% |
| New Turn layout, receipt path through the shared SQL view | 1,064 | 905.5 | −14.4% |
| Delivered Turn layout, matched projection SQL uses the physical table | 1,031 | 965.5 | −7.4% |
| Delivered layout with receipt confirmation in both arms, only canonical input metadata and its indexes disabled / enabled | 995 | 960.5 | −4.2% |

A separate same-layout trial switched only which relation name the hot writer reached: the shared view cost 5.1% within pairs. Binding the projection functions directly to the physical Turn table kept the public reader shape and did not add a database round trip. Absolute medians varied between trials on this shared host. The final row isolates the navigator’s write cost and checks that the expected number of input Entries actually received metadata.

```sh
TSX_TSCONFIG_PATH=packages/control-plane/tsconfig.json node --import tsx scripts/experiments/minimal-session-storage/model-settlement-rate.mjs 1 session-inputs pairs
TSX_TSCONFIG_PATH=packages/control-plane/tsconfig.json node --import tsx scripts/experiments/minimal-session-storage/model-settlement-rate.mjs 1 input-history pairs
PMA_SEGMENT_BROWSER=1 npm run test:transport
npm run browser:presentation:check
```

## Verification

The final RF3/PG transport suite passed 52 tests in 29 files, including the browser mailbox scenario. A further 55 targeted tests, workspace type checks, the Chromium presentation suite, documentation checks and changed-file formatting passed. Repository-wide formatting also reports three pre-existing generated JSON reports dated 2026-11-19 (Session continuation throughput, tail attribution and Worker claim sizing). Their content matches the parent commit.
