# Directed queue notifications for independent Session workers

The #52 Session runtime listens to the segmented queue through one PG connection per router. A committed enqueue signals that listener. The router selects an available Worker from its FIFO of live management sockets. The Worker claims one eligible Turn through the existing PG lease transaction. A child and a root follow the same path. [Decision](../adr/0364-directed-segment-worker-wake.md) · [Earlier directed-wake study](directed-worker-wake-20261119.md).

## Wake and recovery

- On joining, reconnecting and freeing capacity, a Worker advertises availability. An idle Worker reannounces readiness over its socket with jitter. This sends no PG claim. Each directed arrival or reconciliation probe can trigger one claim. A full Worker and a Worker already reserving its last slot still apply their local capacity guard.
- The queue trigger uses a schema-specific PG channel. The payload identifies the writable Flow. PG coalesces repeated hints for the same Flow in the same transaction. Different Sessions in a burst retain independent hints. When a Turn settles with a queued successor, its terminal transaction signals again.
- The router retains up to 1,024 undelivered hints as a counter. PG holds the actual jobs. A router-level timer probes one available connection per second by default. A successful probe passes a hint to one more available Worker, so missed commits can drain after restart. Listener registration/reconnection also prompts a check. There is no per-Worker one-second queue query.
- The internal socket uses a bearer credential. Existing PG checks still verify the registered Worker boot, live management connection, capacity and current writer. Workers continue running and renewing existing leases during a wake-router restart. New claims resume when the route is available.
- `runtime.serveScheduler({connectionString, token, host, port})` starts the listener and wake endpoint. Pass its reachable URL and the credential to `worker.serve({scheduler: {url, token}, signal, execute, onError})`. Shutting down the service stops new wake-driven requests and drains already-issued claims. The tested topology uses one router per candidate queue; independent routers listening to the same PG channel each receive its notifications. This candidate does not elect a global wake leader.

## Measured fan-out

A disposable PG primary held 1,000 independently registered Worker boot identities. One Node process ran 1,000 Worker clients on separate loopback WebSockets with a shared, bounded claim pool. A query hook counted the actual queue-selection statements issued to PG. The lease transaction includes further registration, capacity and ownership SQL. During these measurements the central probe interval was extended to separate arrival-triggered work from background reconciliation.

| Work offered | Worker arrival wakes | Queue-selection statements | Runs claimed |
|---|---:|---:|---:|
| Idle for 1.2 s with an uncommitted insert, then rollback | 0 | 0 | 0 |
| One committed Turn | 1 | 1 | 1 |
| Eight distinct Sessions committed in one transaction | 8 | 8 | 8 |

After all 1,000 Workers became available again, three central reconciliation ticks produced exactly three queue selections and zero arrival wakes. The RF3/PG transport suite passed 50 tests in 28 files.

The test also checked queued work predating the listener, successor eligibility after a seal, PG listener replacement and three jobs queued while the router was down. The native Pi/RF3 acceptance starts a waiting Worker process before admitting the child, takes the first lease through its socket, and exercises the same path on replacement. Lock-race tests still invoke the primitive claim directly to control the interleaving. This is a queue wake measurement; the 1,000-client test does not run 1,000 models or establish sustained model throughput. [Fan-out test](../../scripts/experiments/minimal-session-storage/segment-wake.integration.spec.mjs) · [Child and Worker fault tests](../../scripts/experiments/minimal-session-storage/independent-session-leases.integration.spec.mjs).

```sh
export TSX_TSCONFIG_PATH="$PWD/packages/control-plane/tsconfig.json"
node --test scripts/experiments/minimal-session-storage/wake-channel.test.mjs
PMA_SEGMENT_WAKE_WORKERS=1000 node scripts/run-with-test-postgres.mjs -- node_modules/.bin/vitest run scripts/experiments/minimal-session-storage/segment-wake.integration.spec.mjs
npm run test:transport
```
