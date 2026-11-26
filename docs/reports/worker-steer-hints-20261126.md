# Worker-confirmed steer hints

The #52 candidate uses one in-memory hint per owned Session lease. Clean ordinary tool boundaries return after the local authority check. A positive hint preserves the ordered PG selection from ADR-0366. Native startup and settlement still check the queue. [Decision](../adr/0367-worker-steer-hints.md).

## Notification and receipt

A committed steer, a promotion, or an eligible input returned to pending publishes its notice on the existing schema-specific PG channel. The Session lock orders notice allocation across PG connections. The readiness listener forwards the owning lease ID and notice number over the Worker management WebSocket. The mailbox body stays in PG. Ready hints for new Sessions continue to use available Worker slots; a steer can reach its owner while every slot is occupied.

The Worker marks its local hint before sending an acknowledgement. The router coalesces pending receipts per writer, buffers up to 1,024 writers and runs up to four PG receipt writes concurrently, one per writer. Each write checks the sending boot and live lease in PG. Its receipt covers eligible notices up to the acknowledged number. A repeat promotion or requeue after a reservation needs a fresh notice. Reordering preserves the notice and PG queue order. A stale socket, old fencing epoch, or delayed receipt for an earlier notice cannot certify a successor input.

The browser distinguishes durable registration (`sending` for a pending steer) from owner confirmation (`steering`). If the Worker already placed the input it can go straight into the transcript. The queue snapshot is tenant- and sender-scoped. A follow-up uses the existing queued state until adoption or promotion. A normal tool boundary can race a committed signal while the browser shows sending.

## Repair

Bootstrap registers the local lease before reading pending steers. A socket reconnect checks the held Sessions. Renewal asynchronously repairs unacknowledged steers in one batched lookup per Worker boot with active leases. Repair has one in-flight scan and does not delay the next renewal. If the bootstrap query fails, the initial hint allows a conservative boundary check. A provisional reservation, withdrawal or writer crash still settles through the existing PG and ordered Kafka seal.

When a tool boundary finds no eligible mail, or adopts the last eligible steer, it clears only the hint it originally observed. A later notification keeps the hint armed. A boundary that already adopted an input may still have pending steers for a subsequent round.

## Acceptance

The PG and Pi integration uses a full one-slot Worker, the real management WebSocket, the Session HTTP API and a deterministic model. It checks 100 empty boundaries, a withdrawn steer, two queued steers including an agent message, and normal follow-ups. The Chrome test suppresses a receipt write after the Worker sets its flag, observes the sending state, releases the fault, observes the acknowledged state and restores the input into an existing draft. The integration also disconnects and replaces the scheduler while the Worker retains its lease. RF3 handoff attempts a delayed previous-owner confirmation before and after claim by the successor.

The matched run passed all 51 transport tests across 29 files. The router and boundary checks also passed. In 32 sampled human steers, PG registration to the acknowledgement row update measured 8.2 ms P95 and 8.6 ms P99. This timing is sampled inside the PG transaction and excludes the browser refresh. The Chrome fixture separately verified both labels using an intentionally lost acknowledgement.

The boundary microbenchmark compared 32 clients, 256 pending follow-ups per Session and zero eligible steers. Direct PG selection performed 3,200 empty reads in 302 ms with 4.0 ms P95 including pool wait. With a confirmed clean local hint the Worker path ran the same 3,200 boundary checks with **zero selection queries**. These counts exclude bootstrap, renewal repair and final settlement. The benchmark exercises the selection path on a local fixture; it does not measure end-to-end model throughput.

```sh
PMA_SEGMENT_BROWSER=1 npm run test:transport
node_modules/.bin/vitest run scripts/experiments/minimal-session-storage/segment-input-boundary.test.mjs
TSX_TSCONFIG_PATH="$PWD/packages/control-plane/tsconfig.json" node --import tsx scripts/experiments/minimal-session-storage/model-settlement-rate.mjs 1 session-inputs boundaries
```

Production cutover remains separately gated under #48.
