// Native Pi + real PG and RF3 Kafka. Session control is driven at explicit log
// barriers. The sandbox executes one fixed marker command in a disposable container.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import native from "@confluentinc/kafka-javascript";
import pg from "pg";
import { createDatabase } from "@pi-cloud/database";
import { sql } from "kysely";
import { KafkaLogConsumer } from "@pi-cloud/event-log";
import { KafkaToolReplyPublisher } from "../../../packages/event-log/src/tool-replies.ts";
import { AcceptedFamilyCheckpoint, saveSealedFamily } from "@pi-cloud/pi-session-postgres/durable";
import { toolResultIsUnknown } from "@pi-cloud/protocol";
import { testPostgres } from "../../lib/test-postgres.mjs";
import { testKafka } from "../../lib/test-kafka.mjs";
import { installPagingProjection } from "./flow-paging-fixture.mjs";
import { normalRootProjection } from "./normal-root-projection.mjs";
import { installWorkspaceRoutes } from "./workspace-route-spans.mjs";
import { familyWorker, until } from "./family-process.mjs";
import { decodeWorkspaceFact } from "./workspace-operations.mjs";

const exec = promisify(execFile);
const command = "printf 'ran\\n' >> executions; printf 'fixture-ok\\n'";
const image = "postgres@sha256:ef257d85f76e48da1c64832459b59fcaba1a4dac97bf5d7450c77753542eee94";
const guest = "pma-bash-loss-" + randomUUID(),
  topic = "bash-loss-" + randomUUID();
const pgFixture = await testPostgres({}),
  pool = new pg.Pool({ connectionString: pgFixture.url, max: 5 }),
  db = createDatabase({ connectionString: pgFixture.url, maxConnections: 2 });
let kafka,
  reader,
  admin,
  producer,
  replies,
  guestStarted = false,
  state,
  failed;
pool.on("error", (e) => {
  failed ??= e;
});
const resultRows = async (run) =>
  (
    await pool.query(
      "select content from messages where run_id=$1 and role='toolResult' order by commit_seq,ordinal",
      [run],
    )
  ).rows.map((r) => JSON.parse(r.content).model[0]);
const gate = async (fn, description) =>
  until(
    () => {
      if (failed) throw failed;
      return fn();
    },
    description,
    90000,
  );
try {
  await installPagingProjection(pool);
  await installWorkspaceRoutes(pool);
  await pool.query(`alter table runs add output_seal_offset bigint,add fencing_token bigint,add recovery_requested_at timestamptz;
   create table session_leases(tenant_id uuid,pi_session_id text,lease_id uuid primary key,fencing_token bigint,valid_until timestamptz,writer_failed_at timestamptz,writer_sealed_at timestamptz,released_at timestamptz,writer_seal_offset bigint)`);
  await exec("docker", [
    "run",
    "-d",
    "--name",
    guest,
    "--network",
    "none",
    "--read-only",
    "--cap-drop",
    "ALL",
    "--security-opt",
    "no-new-privileges",
    "--user",
    "65534:65534",
    "--tmpfs",
    "/var/lib/postgresql/data",
    "--tmpfs",
    "/tmp",
    image,
    "sleep",
    "infinity",
  ]);
  guestStarted = true;
  kafka = await testKafka({});
  const client = new native.KafkaJS.Kafka({
    kafkaJS: { brokers: kafka.brokers, logLevel: native.KafkaJS.logLevel.NOTHING },
  });
  admin = client.admin();
  producer = client.producer({ kafkaJS: { acks: -1, idempotent: true } });
  await admin.connect();
  for (let i = 0; ; i++)
    try {
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
      break;
    } catch (e) {
      if (i >= 3 || !e.errors?.every((x) => x.type === "ERR__DESTROY_BROKER")) throw e;
    }
  await producer.connect();
  replies = new KafkaToolReplyPublisher(kafka.brokers, topic);
  await replies.start();
  const failure = Promise.withResolvers();
  failure.promise.catch(() => {});
  const p = normalRootProjection(
    pool,
    topic,
    (s) => ({ bindingId: s.runId, allocationId: s.runId }),
    () => ({
      async submit(r, c, reply) {
        assert.equal(c.request.args.command, command);
        const s = state;
        assert.equal(c.scope?.runId ?? r.fact.scope.runId, s.scope.runId);
        assert(!s.offered, "Bash offered more than once");
        s.offered = true;
        s.replyDeadline = reply.deadlineAt;
        if (s.drop) return;
        const { stdout } = await exec("docker", [
          "exec",
          "-w",
          s.directory,
          guest,
          "bash",
          "-c",
          command,
        ]);
        s.executions.push(r.fact.scope.writerId);
        s.reply = {
          kind: "tool_result",
          operationId: c.request.operationId,
          sequence: 1,
          result: {
            toolCallId: reply.toolCallId,
            toolName: "bash",
            isError: false,
            result: { content: [{ type: "text", text: stdout.trim() }] },
          },
        };
        s.destination = reply.topic;
        if (!s.holdReply) {
          await replies.publish(s.destination, s.reply);
          s.replySent = true;
        }
      },
    }),
    undefined,
    { partitions: 1 },
  );
  await p.view.reset(new Map([[0, 0n]]), 16, "recorded-history");
  reader = new KafkaLogConsumer({
    brokers: kafka.brokers,
    topic,
    clientId: topic,
    groupId: topic,
    groupRecovery: true,
    orderedEffects: true,
    batchRead: true,
    replayOffsets: async () => new Map([[0, 0n]]),
    decode(bytes) {
      try {
        const wire = JSON.parse(bytes.toString());
        return wire.studySeal ? wire : decodeWorkspaceFact(bytes).fact;
      } catch (error) {
        failed = error;
        failure.reject(error);
        throw error;
      }
    },
    handler: async (r, current, grant) => {
      try {
        const s = state,
          f = r.fact;
        if (f.studySeal) {
          assert.equal(f.studySeal, s.scope.writerId);
          const checkpoint = s.family.snapshot();
          await db.transaction().execute(async (tx) => {
            await sql`update runs set recovery_requested_at=now(),output_seal_id=${f.id}::uuid where id=${s.scope.runId}::uuid`.execute(
              tx,
            );
            await saveSealedFamily(tx, s.scope, r, f.id, checkpoint);
            await sql`update session_leases set writer_sealed_at=now(),released_at=now(),writer_seal_offset=${String(r.offset)} where lease_id=${s.scope.writerId}::uuid`.execute(
              tx,
            );
            await sql`update accepted_fact_projection_offsets set next_offset=${String(r.offset + 1n)} where topic=${topic} and partition=0`.execute(
              tx,
            );
          });
          p.nativeStore.controlProjected(r);
          p.nativeStore.projection.releaseSettled(s.scope);
          s.seal = checkpoint;
          return;
        }
        assert.equal(f.scope.writerId, s.scope.writerId);
        if (
          s.blockResult &&
          f.commit.writes.some((w) => w.type === "entry" && w.value.kind === "pi.tool-result")
        ) {
          s.blocked = true;
          await s.blockResult.promise;
        }
        const projected = await p.nativeStore.projection.project(f, r, current);
        assert(projected.admitted);
        s.family.observe(f);
        await p.dispatch(r, current, grant);
        if (
          f.commit.writes.some(
            (w) =>
              w.type === "entry" &&
              w.value.kind === "pi.tool-result" &&
              w.value.model.some(toolResultIsUnknown),
          )
        )
          s.unknownAt = Date.now();
        s.projected = f.commit.seq;
      } catch (e) {
        if (!failed)
          console.error("projection failed", e.stack?.split("\n").slice(0, 6).join("\n"));
        failed = e;
        failure.reject(e);
        throw e;
      }
    },
  });
  await reader.start();
  await reader.waitForPartition(0);
  for (const scene of [
    "proposal",
    "pending",
    "not-delivered",
    "mailbox",
    "canonical-kafka",
    "canonical-pg",
    "twice-before",
    "twice-after",
    "stop",
  ]) {
    console.log(JSON.stringify({ starting: scene }));
    const run = randomUUID(),
      session = randomUUID(),
      tenant = randomUUID();
    const s = (state = {
      scope: {
        tenantId: tenant,
        piSessionId: session,
        sessionId: session,
        turnId: run,
        runId: run,
        writerId: randomUUID(),
        fencingToken: 1,
      },
      directory: "/tmp/" + run,
      family: new AcceptedFamilyCheckpoint(),
      executions: [],
      drop: scene === "not-delivered",
      holdReply: ["pending", "twice-before", "twice-after", "stop"].includes(scene),
      blockResult: scene === "canonical-kafka" ? Promise.withResolvers() : undefined,
    });
    await exec("docker", ["exec", guest, "mkdir", s.directory]);
    await pool.query(
      "insert into runs(id,tenant_id,session_id,lease_id,fencing_token,idempotency_key,mailbox_position,initial_input,model_ref,state) values($1,$2,$3,$4,1,$1,1,'run','fixture','running')",
      [run, tenant, session, s.scope.writerId],
    );
    const setLease = () =>
      pool.query(
        "insert into session_leases(tenant_id,pi_session_id,lease_id,fencing_token,valid_until) values($1,$2,$3,$4,now()+interval '5 minutes')",
        [tenant, session, s.scope.writerId, s.scope.fencingToken],
      );
    await setLease();
    const actors = [],
      topics = [];
    async function spawn(hold, stop = false) {
      const a = familyWorker(
        { scope: s.scope, topic, brokers: kafka.brokers, url: pgFixture.url, hold, stop, command },
        "./bash-loss-worker.mjs",
      );
      actors.push(a);
      const boot = await a.wait("boot");
      assert(!topics.includes(boot.topic));
      topics.push(boot.topic);
      return a;
    }
    async function killAndSeal(a) {
      a.proc.kill("SIGKILL");
      assert.equal((await a.ended).signal, "SIGKILL");
      s.blockResult?.resolve();
      s.seal = undefined;
      await producer.send({
        topic,
        messages: [
          {
            partition: 0,
            value: JSON.stringify({ studySeal: s.scope.writerId, id: randomUUID() }),
          },
        ],
      });
      await gate(() => s.seal, "ordered seal");
      s.scope = { ...s.scope, writerId: randomUUID(), fencingToken: s.scope.fencingToken + 1 };
      await setLease();
      await pool.query(
        "update runs set lease_id=$2,fencing_token=$3,recovery_requested_at=null,output_seal_id=null where id=$1",
        [run, s.scope.writerId, s.scope.fencingToken],
      );
      const inbox = await pool.query("select doc_id from inboxes where pi_session_id=$1", [
        session,
      ]);
      s.family = AcceptedFamilyCheckpoint.resume(
        s.scope.writerId,
        s.seal,
        inbox.rows.map((x) => Number(x.doc_id)),
      );
    }
    try {
      const a = await spawn(
        scene === "proposal" ? "proposal" : scene.startsWith("canonical") ? "result" : "intent",
      );
      await a.wait("parked");
      if (scene === "proposal") {
        await gate(
          async () =>
            (
              await pool.query(
                "select count(*)::int n from messages where run_id=$1 and role='assistant'",
                [run],
              )
            ).rows[0].n === 1,
          "proposal",
        );
        assert.equal(s.executions.length, 0);
      } else if (s.drop) await gate(() => s.offered, "unconfirmed dispatch");
      else await gate(() => s.executions.length === 1, "sandbox marker");
      if (scene === "mailbox")
        await gate(() => s.replySent, "original reply acknowledged by Kafka");
      if (scene === "canonical-kafka") {
        await gate(() => s.blocked, "result waiting for PG");
        assert.equal((await resultRows(run)).length, 0);
      }
      if (scene === "canonical-pg")
        await gate(async () => (await resultRows(run)).length === 1, "recorded result");
      await killAndSeal(a);
      let b = await spawn(
        scene === "twice-before" ? "before-result" : scene === "twice-after" ? "result" : undefined,
        scene === "stop",
      );
      if (scene.startsWith("twice-")) {
        await b.wait("parked");
        if (scene === "twice-before") {
          assert.equal((await resultRows(run)).length, 0);
          await replies.publish(s.destination, s.reply);
          s.replySent = true;
        } else await gate(async () => (await resultRows(run)).length === 1, "B result");
        await killAndSeal(b);
        b = await spawn();
      }
      const completed = await Promise.race([b.wait("finished"), failure.promise]);
      assert.equal(completed.restores, 0);
      await gate(() => s.projected === completed.lastSeq, "all native commits projected");
      assert.equal(completed.count, scene === "proposal" ? 1 : 0);
      assert.equal((await b.ended).code, 0);
      await gate(async () => (await resultRows(run)).length > 0, "final native result");
      if (s.holdReply && !s.replySent) await replies.publish(s.destination, s.reply);
      const results = await resultRows(run);
      assert.equal(results.length, 1);
      assert.equal(results[0].toolCallId, "marker-call");
      const unknown = [
        "not-delivered",
        "pending",
        "mailbox",
        "twice-before",
        "twice-after",
      ].includes(scene);
      assert.equal(toolResultIsUnknown(results[0]), unknown);
      if (unknown)
        assert(s.unknownAt < s.replyDeadline, "Successor waited for the old mailbox deadline");
      const remaining = (await pool.query("select projection_tools from runs where id=$1", [run]))
        .rows[0].projection_tools;
      assert.deepEqual(remaining, {}, "Canonical settlement must retire the pending operation");
      if (scene === "stop") {
        assert.equal(completed.modelCalls, 0);
        const native = s.family
          .snapshot()
          .records.find(({ write }) => write.type === "task" && write.value.kind === "pi.tool");
        assert.equal(native.write.value.state.outcome.status, "aborted");
        assert(results[0].isError);
      } else {
        const seen = await b.wait("modelResult");
        assert.equal(seen.unknown, unknown);
        const text = results[0].content.map((c) => c.text).join("");
        assert(unknown ? text.includes("may have run") : text === "fixture-ok");
      }
      const observed = await pool.query(
        "select state from conversation_worlds where tenant_id=$1 and pi_session_id=$2",
        [tenant, session],
      );
      assert.equal(
        observed.rowCount,
        unknown || scene === "stop" ? 0 : 1,
        "Only a received completion can confirm physical use",
      );
      assert.equal(s.executions.length, s.drop ? 0 : 1);
      if (s.drop)
        await exec("docker", ["exec", "-w", s.directory, guest, "test", "!", "-e", "executions"]);
      else
        assert.equal(
          (await exec("docker", ["exec", "-w", s.directory, guest, "cat", "executions"])).stdout,
          "ran\n",
        );
      // A callback or a later open must not be able to supersede the canonical result.
      assert.deepEqual(await resultRows(run), results);
      console.log(
        JSON.stringify({
          scene,
          workerBoots: topics.length,
          executions: s.executions.length,
          canonicalResults: 1,
          outcome: scene === "stop" ? "aborted" : unknown ? "unknown" : "success",
          oldMailboxSubscriptions: completed.restores,
          pendingAfterSettlement: Object.keys(remaining).length,
          ...(unknown ? { beforeOriginalDeadline: true } : {}),
        }),
      );
    } finally {
      s.blockResult?.resolve();
      await Promise.all(actors.map((a) => a.close()));
    }
  }
} catch (e) {
  console.error(e.message, e.stack?.split("\n").slice(1, 5));
  throw e;
} finally {
  await reader?.close();
  await replies?.close();
  await producer?.disconnect();
  await admin?.disconnect();
  await kafka?.close();
  if (guestStarted) await exec("docker", ["rm", "--force", "--volumes", guest]);
  await pool.end();
  await db.destroy();
  await pgFixture.close();
}
