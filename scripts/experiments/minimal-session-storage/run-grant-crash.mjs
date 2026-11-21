// A real consumer process dies after confirmed admission of eight and two simulated
// deliveries. The independent PG witness intentionally has no uniqueness on effects.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { fork } from "node:child_process";
import { fileURLToPath } from "node:url";
import { once } from "node:events";
import pg from "pg";
import native from "@confluentinc/kafka-javascript";
import { KafkaLogConsumer } from "../../../packages/event-log/src/kafka-log-consumer.ts";
import {
  encodeWorkspaceFact,
  decodeWorkspaceFact,
  operationsFor,
} from "./workspace-operations.mjs";
import { fanoutFixture, targets } from "./fanout-fixture.mjs";
import { FanoutSender } from "./fanout-network.mjs";
import { plainExecution, prepareToolGrants } from "./tool-grant-prefix.mjs";
import { testPostgres } from "../../lib/test-postgres.mjs";
import { testKafka } from "../../lib/test-kafka.mjs";

function tape(token) {
  const fixture = fanoutFixture(token),
    records = [],
    forbidden = new Set(),
    barriers = new Set();
  for (let i = 0; i < 90; i++) {
    records.push(fixture.frame(1, i, false).value);
    if (![27, 51, 75].includes(i)) continue;
    const scope = fixture.owners[i].scope;
    let marker;
    if (i === 27) {
      const aborted = decodeWorkspaceFact(records.at(-1)).fact,
        task = aborted.commit.writes[0].value;
      marker = {
        ...aborted,
        factId: randomUUID(),
        commit: {
          ...aborted.commit,
          seq: 3,
          writes: [
            {
              type: "task",
              value: {
                ...task,
                abortRequested: true,
                state: { status: "terminal", outcome: { status: "aborted" } },
              },
            },
          ],
        },
        workspaceOperations: [{ action: "cancel", operationId: fixture.op(1, i), taskId: task.id }],
      };
    } else if (i === 51)
      marker = {
        kind: "control_change",
        factId: randomUUID(),
        entity: "sandbox_binding",
        entityId: fixture.owners[i].binding,
        version: "1",
        runId: scope.runId,
        closed: true,
        scope: { tenantId: scope.tenantId, piSessionId: scope.piSessionId },
        occurredAt: new Date().toISOString(),
      };
    else
      marker = {
        kind: "execution_seal",
        factId: randomUUID(),
        scope,
        agentId: "root",
        baseSequence: 2,
        recover: true,
        occurredAt: new Date().toISOString(),
      };
    barriers.add(records.length);
    records.push(encodeWorkspaceFact(marker));
    forbidden.add(records.length);
    records.push(fixture.frame(1, targets + i, false).value);
  }
  return { records, forbidden, barriers };
}
async function owner({ brokers, url, topic, who, end }) {
  const db = new pg.Client({ connectionString: url });
  await db.connect();
  const sender = new FanoutSender(),
    prepared = new WeakSet(),
    blocked = new Set();
  sender.reset();
  async function witness(r) {
    await db.query("insert into seen values($1,$2,$3) on conflict(who,pos) do nothing", [
      who,
      String(r.offset),
      r.fact.factId,
    ]);
  }
  const projectOperation = async (r) => {
    await witness(r);
    if (blocked.has(r.fact.scope.piSessionId)) return;
    return { commands: [{ op: plainExecution(r.fact) }], cancels: [] };
  };
  let killAt;
  async function send(r) {
    await db.query("insert into sent values($1,$2)", [who, String(r.offset)]);
    if (Number(r.offset) === killAt) process.kill(process.pid, "SIGKILL");
  }
  const reader = new KafkaLogConsumer({
    brokers,
    topic,
    groupId: topic,
    clientId: topic + "-" + who,
    batchRead: true,
    orderedEffects: true,
    groupRecovery: true,
    effectPrefixLimit: 8,
    replayOffsets: async () => new Map([[0, 0n]]),
    decode: (b) => decodeWorkspaceFact(b).fact,
    async prepareReady(ready, current, grant) {
      const n = await prepareToolGrants(
        ready,
        current,
        async (count) => {
          const first = await grant(count),
            lo = Number(ready[0].offset),
            hi = Number(ready[count - 1].offset);
          await db.query("insert into grants values($1,$2,$3,$4)", [who, lo, hi, first]);
          if (who === "A" && count === 8 && first === 0) killAt = lo + 1;
          return first;
        },
        {
          limit: 8,
          sender,
          router: { projectOperation },
          targetFor: (r) => r.fact.scope.piSessionId,
          prepareSend: (r) => () => send(r),
        },
      );
      for (const r of ready.slice(0, n)) prepared.add(r.fact);
      return n || 1;
    },
    async handler(r, current, grant) {
      if (prepared.delete(r.fact)) return;
      if (!plainExecution(r.fact)) {
        await witness(r);
        assert(
          r.fact.kind === "control_change" ||
            r.fact.kind === "execution_seal" ||
            operationsFor(r.fact)[0]?.action === "cancel",
        );
        blocked.add(r.fact.scope.piSessionId);
      } else if ((await projectOperation(r)) && (await grant()) && current()) await send(r);
    },
  });
  try {
    await reader.start();
    await reader.waitUntilAssigned();
    await reader.waitUntilInitialReplay([BigInt(end)], 45000);
  } finally {
    await reader.close();
    await db.end();
  }
}
async function run() {
  const pgServer = await testPostgres({});
  let cluster;
  try {
    cluster = await testKafka({});
    const kafka = new native.KafkaJS.Kafka({
      kafkaJS: { brokers: cluster.brokers, logLevel: native.KafkaJS.logLevel.NOTHING },
    });
    const admin = kafka.admin(),
      producer = kafka.producer({ kafkaJS: { acks: -1, idempotent: true } });
    const db = new pg.Client({ connectionString: pgServer.url });
    const token = randomUUID().replaceAll("-", "").slice(0, 16),
      topic = "prefix-kill-" + token;
    await db.connect();
    try {
      await admin.connect();
      await producer.connect();
      await db.query(
        "create table seen(who text,pos int,fact uuid,primary key(who,pos));create table sent(who text,pos int);create table grants(who text,lo int,hi int,covered int)",
      );
      await admin.createTopics({
        topics: [
          {
            topic,
            numPartitions: 1,
            replicationFactor: 3,
            configEntries: [{ name: "min.insync.replicas", value: "2" }],
          },
        ],
      });
      const { records, barriers, forbidden } = tape(token);
      await producer.send({
        topic,
        messages: records.map((value) => ({ partition: 0, value, key: "study" })),
      });
      const spawnOwner = async (who) => {
        const child = fork(fileURLToPath(import.meta.url), ["owner"], {
          execArgv: ["--import", "tsx"],
          stdio: ["ignore", "inherit", "inherit", "ipc"],
        });
        const timer = setTimeout(() => child.kill("SIGTERM"), 90000);
        try {
          const exit = once(child, "exit");
          child.send({
            who,
            end: records.length,
            url: pgServer.url,
            brokers: cluster.brokers,
            topic,
          });
          assert.deepEqual(await exit, who === "A" ? [null, "SIGKILL"] : [0, null]);
        } finally {
          clearTimeout(timer);
        }
      };
      await spawnOwner("A");
      const last = (
        await db.query("select lo,hi,covered from grants where who='A' order by hi desc limit 1")
      ).rows[0];
      assert(last && last.hi === last.lo + 7 && last.covered === 0);
      assert.equal(
        (await admin.fetchOffsets({ groupId: topic, topics: [topic] }))[0].partitions[0].offset,
        String(last.hi + 1),
      );
      assert.equal(
        (
          await db.query(
            "select count(*)::int n from sent where who='A' and pos between $1 and $2",
            [last.lo, last.hi],
          )
        ).rows[0].n,
        2,
      );
      await spawnOwner("B");
      const seen = (await db.query("select pos,fact from seen where who='B' order by pos")).rows;
      assert.deepEqual(
        seen.map((r) => [r.pos, r.fact]),
        records.map((v, i) => [i, decodeWorkspaceFact(v).fact.factId]),
      );
      const allGrants = (await db.query("select lo,hi from grants")).rows;
      for (const { lo, hi } of allGrants)
        for (const boundary of [...barriers, ...forbidden]) assert(boundary < lo || boundary > hi);
      const effects = (await db.query("select pos from sent order by pos")).rows.map((r) => r.pos);
      assert.deepEqual(
        effects,
        records.flatMap((_, i) =>
          barriers.has(i) || forbidden.has(i) || (i > last.lo + 1 && i <= last.hi) ? [] : [i],
        ),
      );
      assert.equal(
        (await admin.fetchOffsets({ groupId: topic, topics: [topic] }))[0].partitions[0].offset,
        String(records.length),
      );
      console.log(
        JSON.stringify({
          granted: [last.lo, last.hi],
          sentBeforeKill: 2,
          unsentNotReplayed: 6,
          replayed: seen.length,
          sent: effects.length,
          barriers: barriers.size,
          blockedAfterBarrier: forbidden.size,
          duplicateEffects: 0,
        }),
      );
    } finally {
      await producer.disconnect();
      await admin.disconnect();
      await db.end();
    }
  } finally {
    try {
      await cluster?.close();
    } finally {
      await pgServer.close();
    }
  }
}
if (process.argv[2] === "owner") {
  const [init] = await once(process, "message");
  await owner(init);
  process.disconnect();
} else await run();
