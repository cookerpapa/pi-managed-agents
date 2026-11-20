// Standalone crash assay. Dependencies are private disposable containers.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { appendFile, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import pg from "pg";
import native from "@confluentinc/kafka-javascript";
import { Admin, ConfigResourceTypes } from "@platformatic/kafka";
import { testPostgres } from "../../lib/test-postgres.mjs";
import { testKafka } from "../../lib/test-kafka.mjs";
import { KafkaLogConsumer } from "../../../packages/event-log/src/kafka-log-consumer.ts";
import {
  KafkaToolReplyMailbox,
  KafkaToolReplyPublisher,
} from "../../../packages/event-log/src/tool-replies.ts";
import {
  encodeSessionLogFact,
  decodeSessionLogFact,
} from "../../../packages/runtime-core/src/kafka-accepted-fact.ts";
import { toolInvocationFixture } from "../../../packages/runtime-core/test/tool-invocation-fixture.ts";
import { ReadyProjection, recoveryConnectionError } from "./ready-projection.mjs";
import { projectionTemplates } from "./projection-statements.mjs";
import { projectRecord } from "./project-record.mjs";
import { movingStarts } from "./moving-checkpoint.mjs";

const [output] = process.argv.slice(2);
assert(output && process.env.PI_CLOUD_RUNTIME_LOAD_CHECK === "1");
process.umask(0o077);
async function until(check, label) {
  const end = Date.now() + 45000;
  while (!(await check())) {
    if (Date.now() >= end) throw new Error("Timed out: " + label);
    await delay(20);
  }
}
function owner(scope) {
  let seq = 0,
    id = 100;
  const fact = (writes, metadata = {}) => ({
    kind: "durable_commit",
    factId: randomUUID(),
    scope,
    occurredAt: new Date().toISOString(),
    commit: { seq: ++seq, nextId: ++id, writes },
    origins: writes
      .filter((w) => ["task", "submission", "entry"].includes(w.type))
      .map((w) => ({
        recordId: w.value.id,
        sessionId: scope.sessionId,
        runId: scope.runId,
        turnId: scope.turnId,
      })),
    tools: [],
    cancellations: [],
    display: [],
    ...metadata,
  });
  const doc = (id, kind, conversation, value) => ({
    type: "document.create",
    record: {
      id,
      kind,
      scope: { kind: "conversation", conversationId: conversation },
      history: "latest",
      fork: "initial",
    },
    content: { kind: "base", version: 1, value },
  });
  const change = (id, content) => ({
    type: "document.change",
    id,
    content: { version: 1, ...content },
  });
  const entry = (text, conversation) => ({
    type: "entry",
    value: {
      id: id++,
      conversationId: conversation,
      kind: "pi.assistant",
      version: 1,
      data: {},
      model: [
        {
          role: "assistant",
          content: [{ type: "text", text }],
          stopReason: "stop",
          timestamp: 1,
          model: "local-load",
          api: "faux",
          provider: "faux",
        },
      ],
    },
  });
  return {
    scope,
    fact,
    open: () =>
      fact([
        doc(2, "pi.inbox", 1, { items: [] }),
        doc(10, "pi.live", 1, {}),
        doc(11, "pi.live", 7, {}),
      ]),
    answer: (text, conversation = 1) => fact([entry(text, conversation)]),
    completeTool(t, text, isError) {
      const task = structuredClone(t.w);
      task.value.state = { status: "terminal", outcome: { type: "completed" } };
      return fact([
        task,
        {
          type: "entry",
          value: {
            id: id++,
            conversationId: task.value.conversationId,
            kind: "pi.tool-result",
            version: 1,
            data: {},
            model: [
              {
                role: "toolResult",
                toolCallId: t.reply.toolCallId,
                toolName: t.reply.toolName,
                isError,
                content: [{ type: "text", text }],
                timestamp: 1,
              },
            ],
          },
        },
      ]);
    },
    start: (id, text) =>
      fact([
        change(id, { kind: "base", value: { generation: { attempt: 1, message: { text } } } }),
      ]),
    append: (id, text) =>
      fact([change(id, { kind: "delta", ops: [["a", ["generation", "message", "text"], text]] })]),
    settle: (id, text) =>
      fact([change(id, { kind: "base", value: {} }), entry(text, id === 10 ? 1 : 7)]),
    finish: () => ({
      kind: "work_finished",
      factId: randomUUID(),
      scope,
      agentId: "root",
      baseSequence: 0,
      nativeSequence: seq,
      terminal: { type: "turn.completed", payload: { stopReason: "stop" } },
      occurredAt: new Date().toISOString(),
    }),
  };
}
async function exercise(brokers, postgres) {
  const topic = "moving-recovery-" + randomUUID(),
    k = new native.KafkaJS.Kafka({
      kafkaJS: { brokers, logLevel: native.KafkaJS.logLevel.NOTHING },
    });
  const admin = k.admin(),
    producer = k.producer({ kafkaJS: { acks: -1, idempotent: true } });
  const mailbox = new KafkaToolReplyMailbox({ brokers, replicas: 3, bootId: randomUUID() });
  const replacement = new KafkaToolReplyMailbox({ brokers, replicas: 3, bootId: randomUUID() });
  const publisher = new KafkaToolReplyPublisher(brokers, topic + "-result");
  const authority = new AbortController();
  const effectDir = await mkdtemp(join(tmpdir(), "pma-moving-effect-")),
    effectFile = join(effectDir, "writes");
  const db = new pg.Pool({
    connectionString: postgres.url,
    max: 4,
    idleTimeoutMillis: 0,
    connectionTimeoutMillis: 4000,
  });
  const runs = new Map(),
    view = new ReadyProjection(db, topic, runs, projectionTemplates);
  const fixture = toolInvocationFixture(),
    root = owner(fixture.fact.scope);
  const newOwner = () =>
    owner({
      ...root.scope,
      sessionId: randomUUID(),
      piSessionId: randomUUID(),
      runId: randomUUID(),
      turnId: randomUUID(),
      writerId: randomUUID(),
    });
  const other = newOwner();
  const register = ({ scope: s }) =>
    db.query(
      "insert into runs(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,model_ref,state,lease_id) values($1,$2,$3,1,$1,'fixture','local','running',$4)",
      [s.runId, s.tenantId, s.piSessionId, s.writerId],
    );
  const effects = [],
    faults = [],
    lengths = [],
    starts = [],
    deliveries = [],
    modes = [],
    pgRestores = [];
  const environment = {
    pool: db,
    sessions: runs,
    stage: { integrated: true, scene: "plain", sink: "single" },
    partitions: 1,
    metrics: {
      byPartition: [0],
      peakWaiting: 0,
      queries: 0,
      inboxSnapshots: 0,
      inboxBytes: 0,
      skipped: 0,
      stale: 0,
      duplicate: 0,
    },
    account() {},
    send() {},
    nextOffsets: new Map(),
  };
  let end = 0n,
    restoreGate,
    dispatchGate,
    lostReply = false,
    topicCreated = false,
    crashed = false;
  const freshPool = async () => {
    const c = await db.connect();
    return {
      async query(sql, args) {
        const res = await c.query(sql, args);
        modes.push(res.rows[0].mode);
        if (lostReply) {
          lostReply = false;
          const e = new Error("Simulated connection reset after actual commit");
          e.code = "ECONNRESET";
          throw e;
        }
        return res;
      },
      release() {
        c.release();
      },
    };
  };
  const reader = new KafkaLogConsumer({
    brokers,
    topic,
    clientId: topic,
    groupId: topic,
    orderedEffects: true,
    groupRecovery: true,
    batchRead: true,
    onRevoked: () => view.revoke(),
    replayOffsets: async (bounds) => {
      if (restoreGate) await restoreGate.promise;
      const begin = performance.now();
      const result = await view.recover(bounds);
      pgRestores.push(Math.round((performance.now() - begin) * 100) / 100);
      starts.push(result.get(0));
      return result;
    },
    decode: (v) => decodeSessionLogFact(v).fact,
    prepareReady: async (records, current) => {
      try {
        return await view.apply(
          records,
          current,
          (r, d) => projectRecord(r, d, environment),
          undefined,
          freshPool,
        );
      } catch (e) {
        if (recoveryConnectionError(e)) reader.reloadRecoveryState();
        else console.error("Study projection failure", e.message);
        throw e;
      }
    },
    handler: async (r, current, grant) => {
      deliveries.push(r.offset);
      if (!view.canDeliver(r)) return;
      const gate = dispatchGate,
        op = r.fact.tools[0].request.operationId;
      if (gate?.op === op && !gate.used) {
        gate.used = true;
        if (gate.before) {
          gate.entered = true;
          await gate.release.promise;
        }
        if (!current() || !(await grant())) return;
        if (!gate.before) {
          gate.entered = true;
          await gate.release.promise;
        }
      } else if (!current() || !(await grant())) return;
      if (current()) {
        await appendFile(effectFile, op + "\n", { mode: 0o600 });
        effects.push(op);
      }
    },
  });
  db.on("error", (e) => {
    if (recoveryConnectionError(e)) reader.reloadRecoveryState();
    else console.error("Study PG pool", e.message);
  });
  const publish = async (facts, wait = true) => {
    for (let i = 0; i < facts.length; i += 100) {
      const messages = facts.slice(i, i + 100).map((f) => {
        const value = encodeSessionLogFact(f);
        decodeSessionLogFact(value);
        return { partition: 0, value };
      });
      await producer.send({ topic, messages });
      end += BigInt(messages.length);
    }
    if (wait) await reader.waitUntilInitialReplay([end], 45000);
  };
  const group = async () =>
    BigInt(
      (await admin.fetchOffsets({ groupId: topic, topics: [topic] }))[0]?.partitions?.[0]?.offset ??
        -1,
    );
  const caught = () => until(async () => (await group()) >= end, "persisted Kafka group offset");
  // Read once directly from recovered PG, and once the way a scheduler that
  // depends on the projection must read: only after partition catch-up.
  const crashAndRecover = async (inspect) => {
    const generation = starts.length;
    const serverStart = async () =>
      (await db.query("select pg_postmaster_start_time()::text as started")).rows[0].started;
    const previousServer = await serverStart();
    restoreGate = Promise.withResolvers();
    await postgres.crash();
    crashed = true;
    await postgres.restart();
    crashed = false;
    assert.notEqual(await serverStart(), previousServer);
    await until(() => !reader.ownsPartition(0), "PG loss invalidated the live assignment");
    const raw = await inspect();
    let visible = false;
    const gated = reader.waitForPartition(0, 45000).then(async () => {
      const value = await inspect();
      visible = true;
      return value;
    });
    void gated.catch(() => {});
    await delay(30);
    assert(!visible, "Scheduler passed recovery gate before replay");
    restoreGate.resolve();
    restoreGate = undefined;
    await until(
      () => starts.length > generation,
      "surviving Projector reload after postmaster loss",
    );
    await reader.waitUntilInitialReplay([end], 45000);
    return { raw, ready: await gated };
  };
  const head = async () =>
    BigInt(
      (
        await db.query("select next_offset from accepted_fact_projection_offsets where topic=$1", [
          topic,
        ])
      ).rows[0].next_offset,
    );
  const rootState = async () =>
    (
      await db.query(
        "select projection_first,projection_state,projection_pin,projection_tools from runs where id=$1",
        [root.scope.runId],
      )
    ).rows[0];
  const reload = async () => {
    const t = performance.now();
    const n = starts.length;
    deliveries.length = 0;
    reader.reloadRecoveryState();
    await until(() => starts.length > n, "reload from PG");
    await reader.waitUntilAssigned(30000);
    await reader.waitUntilInitialReplay([end], 30000);
    return {
      includingGroupRejoinMs: Math.round(performance.now() - t),
      pgBootstrapMs: pgRestores.at(-1),
    };
  };
  const tool = async (taskId, lifetimeMs = 90000) => {
    const tool = structuredClone(fixture.fact.tools[0]),
      w = structuredClone(fixture.fact.commit.writes[0]);
    w.value.id = taskId;
    tool.taskId = taskId;
    tool.request.operationId = randomUUID();
    tool.replyTopic = mailbox.topic;
    tool.replyRecovery = {
      ...(await mailbox.replyPosition()),
      deadlineAt: Date.now() + lifetimeMs,
      toolCallId: w.value.input.callId,
      toolName: tool.request.toolName,
    };
    return {
      f: root.fact([w], { tools: [tool] }),
      id: tool.request.operationId,
      reply: tool.replyRecovery,
      taskId,
      w,
    };
  };
  const issueReply = (t) =>
    publisher.publish(mailbox.topic, {
      kind: "tool_result",
      operationId: t.id,
      sequence: 1,
      result: {
        toolCallId: t.reply.toolCallId,
        toolName: t.reply.toolName,
        isError: false,
        result: { content: [{ type: "text", text: "original " + t.id }] },
      },
    });
  const ackLoss = async () => {
    const n = starts.length;
    lostReply = true;
    await publish([root.answer("committed reply lost")], false);
    await until(() => !lostReply && starts.length > n, "lost PG acknowledgement recovery");
    await reader.waitUntilInitialReplay([end]);
  };
  async function verifyRecentCheckpoint() {
    for (const size of [32, 2048]) {
      await publish(
        Array.from({ length: size }, (_, i) => root.answer("stable-" + size + "-" + i)),
      );
      await caught();
      const coldStart = end;
      const stableRecovery = await reload();
      assert.equal(starts.at(-1), coldStart);
      assert.equal(deliveries.length, 0);
      await publish([root.start(10, "root-half"), root.start(11, "child-half")]);
      const commits = view.stats.commits;
      await publish([root.append(10, " two"), root.append(11, " two")]);
      assert.equal(view.stats.commits, commits, "ordinary live fragments touched PG");
      await publish([
        root.settle(10, "root complete"),
        other.answer("another Run advances this partition"),
      ]);
      await caught();
      const saved = await rootState();
      assert.equal(BigInt(saved.projection_pin), coldStart);
      assert(!JSON.stringify(saved).includes("child-half"));
      const partialRecovery = await reload();
      assert.equal(starts.at(-1), coldStart);
      assert.equal(
        runs.get(root.scope.runId).liveDocs[11].value.generation.message.text,
        "child-half two",
      );
      assert.deepEqual(runs.get(root.scope.runId).liveDocs[10].value, {});
      assert.equal(deliveries.length, Number(end - coldStart));
      lengths.push({
        settledBeforePartial: size,
        stableRecordsReplayed: 0,
        partialSuffixRecords: deliveries.length,
        stableRecovery,
        partialRecovery,
        checkpointBytes: Buffer.byteLength(JSON.stringify(saved.projection_state)),
      });
      assert(
        (
          await movingStarts(db, topic, [{ partition: 0, low: coldStart + 1n, high: end }])
        )[0][2] instanceof Error,
      );
      await publish([root.settle(11, "child complete")]);
      assert.equal((await rootState()).projection_pin, null);
    }
  }
  async function verifyDispatchWindows() {
    for (const before of [true, false]) {
      const t = await tool(before ? 40 : 41, before ? 90000 : 250),
        prior = view.stats.asyncTransactions;
      dispatchGate = {
        op: t.id,
        before,
        used: false,
        entered: false,
        release: Promise.withResolvers(),
      };
      await publish([t.f], false);
      await until(() => dispatchGate.entered, "reached Tool dispatch boundary");
      assert.equal(view.stats.asyncTransactions, prior + 1);
      const saved = (await rootState()).projection_tools[t.taskId];
      assert.equal(saved.operationId, t.id);
      assert.deepEqual(saved.reply, t.reply);
      const n = starts.length;
      reader.reloadRecoveryState();
      dispatchGate.release.resolve();
      await until(() => starts.length > n, "dispatch handoff");
      await reader.waitUntilInitialReplay([end]);
      assert.equal(effects.filter((id) => id === t.id).length, before ? 1 : 0);
      if (before) await issueReply(t);
      await replacement.restore([{ operationId: t.id, reply: saved.reply }], authority.signal);
      const outcome = replacement.resumeWait(t.id, saved.reply, authority.signal).result;
      if (before) {
        const result = await outcome;
        assert.equal(result.result.content[0].text, "original " + t.id);
        await publish([root.completeTool(t, result.result.content[0].text, false)]);
      } else {
        let failure;
        void outcome.catch((error) => {
          failure = error;
        });
        await new Promise((resolve) => setImmediate(resolve));
        assert.equal(failure?.code, "tool_reply_timeout", "Restoration granted another Tool wait");
        assert.equal(failure.effect, "unknown");
        assert(Date.now() >= saved.reply.deadlineAt);
        await publish([root.completeTool(t, "UNKNOWN: tool_reply_timeout", true)]);
        const unknown = await db.query(
          "select content from messages where run_id=$1 and role='toolResult' order by commit_seq desc limit 1",
          [root.scope.runId],
        );
        assert(JSON.parse(unknown.rows[0].content).model[0].isError);
      }
      assert(!Object.keys((await rootState()).projection_tools).length);
    }
    dispatchGate = undefined;
  }
  async function verifyLostToolProjection() {
    // The inherited Kafka grant and original operation survive a real PG
    // crash, even when its own task projection and later history are lost.
    for (let i = 0; i < 4; i++) {
      const t = await tool(50 + i);
      await db.query("checkpoint");
      const priorHead = await head(),
        before = view.stats.asyncTransactions;
      const batch = [
        t.f,
        ...Array.from({ length: 24 }, (_, n) => root.answer("crash-" + i + "-" + n, 7)),
      ];
      const beforeRows = (await db.query("select count(*)::int n from messages")).rows[0].n;
      await publish(batch);
      await caught();
      await issueReply(t);
      assert.equal(effects.filter((id) => id === t.id).length, 1);
      assert(view.stats.asyncTransactions > before);
      const acknowledged = await head();
      assert.equal(acknowledged, end);
      const savedGroup = await group();
      const { raw, ready } = await crashAndRecover(async () => ({
        head: await head(),
        intent: (await rootState()).projection_tools[t.taskId],
        rows: (await db.query("select count(*)::int n from messages")).rows[0].n,
      }));
      const recovered = raw.head,
        lostIntent = !raw.intent,
        recoveredRows = raw.rows;
      assert(recovered <= acknowledged);
      assert.equal(ready.head, end);
      assert.equal(effects.filter((id) => id === t.id).length, 1);
      assert.equal(
        (await db.query("select count(*)::int n from messages")).rows[0].n,
        beforeRows + 24,
      );
      const pending = (await rootState()).projection_tools[t.taskId];
      assert.equal(pending.operationId, t.id);
      assert.deepEqual(pending.reply, t.reply);
      await replacement.restore(
        [{ operationId: pending.operationId, reply: pending.reply }],
        authority.signal,
      );
      const original = await replacement.resumeWait(t.id, pending.reply, authority.signal).result;
      assert.deepEqual(original.result.content, [{ type: "text", text: "original " + t.id }]);
      const actual = (
        await db.query(
          "select content from messages where run_id=$1 and content like $2 order by commit_seq",
          [root.scope.runId, "%crash-" + i + "-%"],
        )
      ).rows.map((r) => JSON.parse(r.content).model[0].content[0].text);
      assert.deepEqual(
        actual,
        Array.from({ length: 24 }, (_, n) => "crash-" + i + "-" + n),
      );
      faults.push({
        groupNext: Number(savedGroup),
        previousHead: Number(priorHead),
        acknowledged: Number(acknowledged),
        recovered: Number(recovered),
        lostIntent,
        lostRows: beforeRows + 24 - recoveredRows,
        dispatches: 1,
        restoredRows: 24,
        originalReplyConsumed: true,
      });
      await publish([root.completeTool(t, original.result.content[0].text, false)]);
      if (lostIntent && recovered < savedGroup) break;
    }
    assert(
      faults.some((f) => f.lostIntent && f.lostRows > 0),
      "Fault did not exercise a missing tool projection and missing rows",
    );
  }
  async function verifyLostCompletion() {
    view.configure(16, "replay-finish");
    const completions = [];
    for (let i = 0; i < 4; i++) {
      const run = newOwner();
      await register(run);
      await publish([run.open(), run.answer("stable before finish")]);
      await db.query("checkpoint");
      const previousHead = await head(),
        txBefore = view.stats.asyncTransactions;
      await publish([run.answer("final answer"), run.finish()]);
      assert(view.stats.asyncTransactions > txBefore && view.stats.syncTransactions === 0);
      await caught();
      const status = async () => ({
        head: Number(await head()),
        state: (await db.query("select state from runs where id=$1", [run.scope.runId])).rows[0]
          .state,
        messages: (
          await db.query("select count(*)::int n from messages where run_id=$1", [run.scope.runId])
        ).rows[0].n,
      });
      const acknowledged = await status();
      assert.equal(acknowledged.state, "completed");
      const { raw, ready } = await crashAndRecover(status);
      assert.deepEqual(ready, acknowledged);
      completions.push({
        previousHead: Number(previousHead),
        groupNext: Number(end),
        acknowledged,
        rawAfterCrash: raw,
        schedulerAfterReplay: ready,
      });
      if (raw.state === "running") break;
    }
    assert(
      completions.some((case_) => case_.rawAfterCrash.state === "running"),
      "Crash did not exercise lost Run completion",
    );
    return completions;
  }
  async function verifyExpiredSuffix() {
    // Emulate a seven-day-old segment falling below the broker's low watermark
    // on this privately owned topic, without a week of wall time.
    const expired = newOwner();
    await register(expired);
    await publish([expired.open(), expired.start(10, "required suffix")]);
    await caught();
    await admin.deleteTopicRecords({ topic, partitions: [{ partition: 0, offset: String(end) }] });
    const installs = starts.length;
    reader.reloadRecoveryState();
    await until(() => starts.length > installs, "expired suffix recovery");
    assert(starts.at(-1) instanceof Error);
    await assert.rejects(
      reader.waitForPartition(0, 3000),
      /Required projection suffix is unavailable/,
    );
  }
  try {
    await admin.connect();
    await producer.connect();
    await admin.createTopics({
      topics: [
        {
          topic,
          numPartitions: 1,
          replicationFactor: 3,
          configEntries: [
            { name: "min.insync.replicas", value: "2" },
            { name: "retention.ms", value: "604800000" },
            { name: "cleanup.policy", value: "delete" },
            { name: "retention.bytes", value: "-1" },
          ],
        },
      ],
    });
    topicCreated = true;
    const policyAdmin = new Admin({
      bootstrapBrokers: brokers,
      clientId: topic + "-policy",
      autocreateTopics: false,
    });
    try {
      const [resource] = await policyAdmin.describeConfigs({
        resources: [
          {
            resourceType: ConfigResourceTypes.TOPIC,
            resourceName: topic,
            configurationKeys: ["retention.ms", "retention.bytes", "cleanup.policy"],
          },
        ],
      });
      assert.deepEqual(Object.fromEntries(resource.configs.map((c) => [c.name, c.value])), {
        "retention.ms": "604800000",
        "retention.bytes": "-1",
        "cleanup.policy": "delete",
      });
    } finally {
      await policyAdmin.close();
    }
    await db.query(await readFile(new URL("./schema.sql", import.meta.url), "utf8"));
    const cfg = (
      await db.query(
        "select current_setting('server_version') as pg,current_setting('fsync') as fsync,current_setting('synchronous_commit') as sync,current_setting('wal_writer_delay') as crash_wal_writer_delay",
      )
    ).rows[0];
    assert(cfg.fsync === "on" && cfg.sync === "on");
    await view.install();
    await view.reset(new Map([[0, 0n]]), 16, "selective");
    for (const o of [root, other]) await register(o);
    await reader.start();
    await reader.waitUntilAssigned(30000);
    await mailbox.start();
    await replacement.start();
    await publisher.start();
    await publish([root.open(), other.open()]);
    await verifyRecentCheckpoint();
    await ackLoss();
    await verifyDispatchWindows();
    await verifyLostToolProjection();
    await publish([root.finish(), other.finish()]);
    assert(
      (await db.query("select count(*)::int n from runs where state='completed'")).rows[0].n === 2,
    );
    assert(modes.includes("on") && modes.includes("off"));
    assert.deepEqual((await readFile(effectFile, "utf8")).trim().split("\n"), effects);
    const closed = await rootState();
    assert(
      closed.projection_state === null &&
        closed.projection_pin === null &&
        closed.projection_first === null,
    );
    const completions = await verifyLostCompletion();
    await verifyExpiredSuffix();
    const clients = await Promise.all(Array.from({ length: 4 }, () => db.connect()));
    try {
      for (const c of clients)
        assert.equal((await c.query("show synchronous_commit")).rows[0].synchronous_commit, "on");
    } finally {
      for (const c of clients) c.release();
    }
    return {
      settings: cfg,
      width: 16,
      kafkaReplicas: 3,
      topicRetentionDays: 7,
      overlap: lengths,
      pgCrashes: faults,
      asynchronousCompletions: completions,
      beforeGrantDispatchCount: 1,
      afterGrantBeforeSendDispatchCount: 0,
      missingReplySettledAtOriginalDeadline: "tool_reply_timeout:unknown",
      retentionGapBlocked: true,
      lostPgReplyRecovered: true,
      connectionsResetToOn: 4,
    };
  } finally {
    restoreGate?.resolve();
    dispatchGate?.release.resolve();
    if (crashed) await postgres.restart();
    authority.abort();
    await mailbox.shutdown();
    await replacement.shutdown();
    await publisher.close();
    await reader.close();
    await db.end();
    await producer.disconnect();
    try {
      if (topicCreated) await admin.deleteTopics({ topics: [topic] });
    } finally {
      try {
        await admin.disconnect();
      } finally {
        await rm(effectDir, { force: true, recursive: true });
      }
    }
  }
}
let kafka, postgres, report;
try {
  kafka = await testKafka({});
  postgres = await testPostgres({}, { crashable: true });
  report = await exercise(kafka.brokers, postgres);
} finally {
  const r = await Promise.allSettled([postgres?.close(), kafka?.close()]);
  for (const item of r) if (item.status === "rejected") throw item.reason;
}
await writeFile(output, JSON.stringify({ ...report, cleaned: true }, null, 2) + "\n", {
  flag: "wx",
});
console.log(JSON.stringify(report));
