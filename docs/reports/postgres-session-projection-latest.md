# PostgreSQL Session projection acceptance

- Complete messages: 8000
- Throughput: 116.05 messages/s
- Latency p50/p95/p99: 1688.61 / 4611.92 / 7136.54 ms
- WAL: 40974512 bytes (5121.81 bytes/message)
- Log replay: 227.3 Sessions/s, 909.19 events/s
- Log replay latency p50/p95/p99: 905.86 / 1402.58 / 1600.67 ms
- Failures: 0

This measures complete semantic Session projection, not token deltas.
