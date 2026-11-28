import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import kafkaNative from "@confluentinc/kafka-javascript";
import { kafkaProducerLane } from "@pi-cloud/runtime-core/kafka-accepted-fact";

export async function checkTerminalOrder({
  api,
  pool,
  session,
  partitions,
  topic,
  brokers,
  fake,
  startWorker,
  until,
  stop,
}) {
  const partition = kafkaProducerLane(session, partitions);
  const tap = new kafkaNative.KafkaConsumer(
    {
      "metadata.broker.list": brokers.join(","),
      "group.id": "terminal-check-" + randomUUID(),
      "enable.auto.commit": false,
    },
    {},
  );
  await new Promise((ok, fail) => tap.connect({}, (error) => (error ? fail(error) : ok())));
  tap.setDefaultConsumeTimeout(100);
  const events = new Map();
  const held = await pool.connect();
  let holding = false;
  try {
    tap.assign([{ topic, partition, offset: 0 }]);
    await startWorker(0);
    await api.acceptTurn(
      session,
      fake.prompt("delayed_finish", { firstByteMs: 4000 }),
      "turn:" + randomUUID(),
    );
    await until("model request in flight", () => fake.metrics().inflight > 0, 15000);
    await held.query("begin");
    holding = true;
    await held.query(
      "select next_offset from pma_segments.accepted_fact_projection_offsets where topic=$1 and partition=$2 for update",
      [topic, partition],
    );
    const observe = async (kind) => {
      if (!events.has(kind)) {
        const messages = await new Promise((ok, fail) =>
          tap.consume(64, (e, rows) => (e ? fail(e) : ok(rows))),
        );
        for (const record of messages) {
          const fact = JSON.parse(record.value.toString());
          if (fact.scope?.sessionId === session && !events.has(fact.kind))
            events.set(fact.kind, { offset: record.offset, scope: fact.scope });
        }
      }
      return events.get(kind);
    };
    const done = await until(
      "normal terminal accepted by Kafka",
      () => observe("work_finished"),
      20000,
    );
    const forced = await until(
      "later expiry seal in the same partition",
      () => observe("execution_seal"),
      60000,
    );
    assert.equal(done.scope.writerId, forced.scope.writerId);
    assert(done.offset < forced.offset);
    if (stop) await api.cancelTurn(session, done.scope.turnId, "stop:" + randomUUID());
    await held.query("commit");
    holding = false;
    const result = await until(
      "both boundary records projected",
      async () =>
        (
          await pool.query(
            `select t.state,t.output_seal_offset,l.writer_seal_offset,t.native_work_handoff is null as retired,
        l.tool_bindings_closed_at is not null as revoked,l.writer_failed_at is not null as expired,e.type
        from pma_segments.turns t join public.session_leases l on l.lease_id=$2
        join pma_segments.session_terminal_events e on e.event_id=t.output_seal_id
        join pma_segments.accepted_fact_projection_offsets p on p.topic=$3 and p.partition=$4 and p.next_offset>$5
        where t.id=$1 and l.released_at is not null`,
            [done.scope.turnId, done.scope.writerId, topic, partition, forced.offset],
          )
        ).rows[0],
      20000,
    );
    assert.deepEqual(result, {
      state: stop ? "cancelled" : "completed",
      output_seal_offset: String(done.offset),
      writer_seal_offset: String(done.offset),
      retired: true,
      revoked: true,
      expired: true,
      type: stop ? "turn.cancelled" : "turn.completed",
    });
    console.log(
      JSON.stringify({
        event: "session.terminal.order",
        normal: done.offset,
        expiry: forced.offset,
        ...result,
      }),
    );
  } finally {
    if (holding) await held.query("rollback");
    held.release();
    await new Promise((resolve) => tap.disconnect(() => resolve()));
  }
}
