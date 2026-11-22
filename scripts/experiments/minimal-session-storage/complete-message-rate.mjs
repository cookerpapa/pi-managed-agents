import assert from "node:assert/strict";
import { createHash, randomBytes, randomUUID } from "node:crypto";
import { distribution } from "../../lib/runtime-load-scheduler.mjs";
import { responseUsage, usageValue, usageStudy } from "./usage-study.mjs";
import { wall } from "./pg-path-replay.mjs";

const middle = 200,
  framesPerRun = 204;
const md5 = (s) => createHash("md5").update(s).digest("hex");

// Only the emitter holds encoded payloads. The verifier receives per-Run hashes.
export function messageTape(tenantId, perPartition, encode, lane, withUsage = false) {
  const groups = Array.from({ length: 32 }, () => []);
  while (groups.some((g) => g.length < perPartition)) {
    const sessionId = randomUUID(),
      p = lane(sessionId, 32);
    if (groups[p].length === perPartition) continue;
    const runId = randomUUID();
    groups[p].push({
      tenantId,
      piSessionId: sessionId,
      sessionId,
      runId,
      turnId: runId,
      writerId: randomUUID(),
      fencingToken: 1,
    });
  }
  const owners = Array.from({ length: perPartition }, (_, i) => groups.map((g) => g[i])).flat();
  const hashes = owners.map(() => createHash("md5")),
    frames = [];
  const doc = (id, kind, value) => ({
    type: "document.create",
    record: {
      id,
      kind,
      scope: { kind: "conversation", conversationId: 1 },
      history: "latest",
      fork: "initial",
    },
    content: { kind: "base", version: 1, value },
  });
  const stamp = new Date().toISOString();
  for (let seq = 1; seq <= framesPerRun; seq++)
    for (const [i, scope] of owners.entries()) {
      const factId = randomUUID();
      let f;
      if (seq === framesPerRun) {
        f = {
          kind: "work_finished",
          factId,
          scope,
          agentId: "root",
          baseSequence: 0,
          nativeSequence: seq - 1,
          terminal: { type: "turn.completed", payload: { stopReason: "stop" } },
          occurredAt: stamp,
        };
      } else {
        let writes;
        if (seq === 1) {
          writes = [doc(2, "pi.inbox", { items: [] }), doc(3, "pi.live", {})];
          if (withUsage) writes.push(doc(5, "pi.usage", usageValue(0)));
        } else {
          const n = seq - 3,
            callId = "call-" + Math.floor(n / 2),
            payload = n >= 0 && n < middle ? randomBytes(3072).toString("base64") : "";
          let m;
          if (seq === 2)
            m = { role: "user", content: [{ type: "text", text: "Replay recorded work" }] };
          else if (seq === 203)
            m = {
              role: "assistant",
              stopReason: "stop",
              content: [{ type: "text", text: "Finished" }],
            };
          else if (n % 2 === 0)
            m = {
              role: "assistant",
              stopReason: "toolUse",
              content: [
                {
                  type: "toolCall",
                  id: callId,
                  name: "write",
                  arguments: { path: "sample.txt", content: payload },
                },
              ],
            };
          else
            m = {
              role: "toolResult",
              toolName: "write",
              toolCallId: callId,
              isError: false,
              content: [{ type: "text", text: payload }],
            };
          if (withUsage && m.role === "assistant")
            Object.assign(m, {
              api: "faux",
              provider: "faux",
              model: "local-load",
              usage: responseUsage(1),
            });
          const entry = {
            id: seq + 6,
            conversationId: 1,
            kind: { user: "pi.user", assistant: "pi.assistant", toolResult: "pi.tool-result" }[
              m.role
            ],
            version: 1,
            data: {},
            model: [m],
          };
          hashes[i].update(md5(JSON.stringify(entry)));
          writes = [{ type: "entry", value: entry }];
          if (withUsage && m.role === "assistant")
            writes.push({
              type: "document.change",
              id: 5,
              content: { kind: "base", version: 1, value: usageValue(Math.floor((seq - 1) / 2)) },
            });
          if (seq === 2)
            writes.push({
              type: "submission",
              value: { id: 4, requestId: scope.runId, entry: entry.id, status: "placed" },
            });
        }
        f = {
          kind: "durable_commit",
          scope,
          factId,
          occurredAt: stamp,
          commit: { seq, nextId: seq + 8, writes },
          origins: writes
            .filter((w) => w.value)
            .map((w) => ({
              recordId: w.value.id,
              sessionId: scope.sessionId,
              runId: scope.runId,
              turnId: scope.turnId,
            })),
          tools: [],
          cancellations: [],
          display: [],
        };
      }
      frames.push({
        session: scope.sessionId,
        partition: lane(scope.sessionId, 32),
        id: factId,
        value: encode(f),
      });
    }
  return {
    frames,
    owners: owners.map((scope, i) => ({ ...scope, digest: hashes[i].digest("hex") })),
  };
}

export class MessageRateMeter {
  constructor(owners, rate, backlog, captureLagSamples = false) {
    this.lagSamples = captureLagSamples ? [] : undefined;
    this.index = new Map(owners.map((s, i) => [s.runId, i]));
    this.count = owners.length * framesPerRun;
    this.rate = rate;
    this.backlog = backlog;
    this.delivered = 0;
    this.latency = [];
    this.gate = Promise.withResolvers();
    this.done = Promise.withResolvers();
  }
  start(at, lag) {
    this.at = at;
    this.peakLag = 0;
    this.gate.resolve();
    this.timer = setInterval(() => {
      if (this.sample || wall() < at) return;
      this.sample = lag()
        .then((m) => {
          this.peakLag = Math.max(this.peakLag, m.lag);
          this.lagSamples?.push({ atMs: Math.round(wall() - at), lag: m.lag });
        })
        .catch((e) => {
          this.error = e;
        })
        .finally(() => {
          this.sample = undefined;
        });
    }, 500);
    this.timer.unref();
  }
  async stop() {
    clearInterval(this.timer);
    await this.sample;
    if (this.error) throw this.error;
  }
  apply(f) {
    const now = wall(),
      seq = f.commit?.seq;
    if (seq >= 3 && seq <= 202) {
      const idx = (seq - 1) * this.index.size + this.index.get(f.scope.runId);
      this.latency.push(
        this.backlog ? 0 : now - this.at - (idx * 1000) / ((this.rate * framesPerRun) / middle),
      );
    }
    if (++this.delivered === this.count) {
      assert.equal(this.latency.length, this.index.size * middle);
      this.done.resolve({
        settledAt: now,
        lagSamples: this.lagSamples,
        rps: (this.latency.length * 1000) / (now - this.at),
        elapsedMs: now - this.at,
        ...(this.backlog
          ? {}
          : {
              p95Ms: distribution(this.latency).p95,
              drainMs: Math.max(0, now - this.at - (this.latency.length * 1000) / this.rate),
            }),
      });
    }
  }
}

export async function messageTrials(
  {
    projector,
    emitter,
    configure,
    guard,
    observer,
    forkPool,
    report,
    save,
    cpuFixture,
    token,
    wave,
  },
  profile,
) {
  const workspace = profile.startsWith("workspace-rate");
  const inputControl = profile === "workspace-rate-input";
  const withUsage = profile.startsWith("usage-rate");
  const smoke = profile.endsWith("-smoke"),
    confirm = profile === "message-rate-confirm";
  assert.match(token, /^[0-9a-f]{16}$/);
  const {
    rows: [identity],
  } = await observer.query("select current_database() as db,current_user as role");
  assert.deepEqual(identity, { db: "pma_sessbench_" + token, role: "bench" });
  await configure({ label: "message-park", scene: "plain", slots: 0 });
  const request = async (a, kind, response, body = {}) => {
    const p = a.wait(response);
    a.write({ kind, ...body });
    return guard(p, 240000);
  };
  const { owners } = await request(emitter, "makeMessages", "messageTape", {
    tenantId: randomUUID(),
    withUsage,
    perPartition: workspace ? 24 : smoke ? 1 : confirm ? 24 : 8,
  });
  const expected = owners.map((o) => [o.runId, o.digest]).sort();
  const count = owners.length * middle;
  const fork = profile.startsWith("fork-rate")
    ? await (await import("./fork-trials.mjs")).forkStudy(forkPool, profile, owners, token)
    : undefined;
  const usage = withUsage ? await usageStudy(forkPool, profile, owners, token) : undefined;
  report.completeMessages = {
    count,
    runs: owners.length,
    bodyBytes: 4096,
    partitions: 32,
    connections: 12,
    replicas: 3,
    settings: report.settings,
    placement: await cpuFixture.pin("open"),
    trials: [],
  };
  const pairs = confirm
    ? [
        [0, true],
        [2315, true],
        [6000, false],
        [8000, true],
      ]
    : smoke
      ? [
          [2315, false],
          [0, true],
        ]
      : [
          [2315, false],
          [0, true],
          [4630, false],
          [0, false],
          [2315, true],
          [4630, true],
        ];
  // Independent repetitions reverse the order. Reuse exactly the same payloads.
  const trials = inputControl
    ? [0, 0].map((rate) => ({ rate, workspaceMode: "batch", policy: "recorded-history" }))
    : profile === "workspace-rate-confirm"
      ? ["direct", "batch", "batch", "direct"].map((workspaceMode) => ({
          rate: 4630,
          workspaceMode,
          policy: "recorded-history",
        }))
      : workspace
        ? [4630, 0, 0, 4630].flatMap((rate, i) =>
            (i % 2 ? ["single", "batch"] : ["batch", "single"]).map((workspaceMode) => ({
              rate,
              workspaceMode,
              policy: "recorded-history",
            })),
          )
        : (usage?.cases ??
          fork?.cases ??
          pairs.flatMap(([rate, reverse]) =>
            (reverse
              ? ["recorded-history", "moving-sync"]
              : ["moving-sync", "recorded-history"]
            ).map((policy) => ({ rate, policy })),
          ));
  for (const { rate, policy, layout, persistUsage, bulkUsage, workspaceMode } of trials) {
    if (usage) await usage.before();
    else if (fork) await fork.before(layout);
    else await observer.query("truncate messages,inboxes,runs");
    await observer.query(
      `insert into runs(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,model_ref,state,lease_id)
      select r,t,s,1,r,'Replay recorded work','local-load','running',w from unnest($1::uuid[],$2::uuid[],$3::uuid[],$4::uuid[]) b(r,t,s,w)`,
      [
        owners.map((o) => o.runId),
        owners.map((o) => o.tenantId),
        owners.map((o) => o.sessionId),
        owners.map((o) => o.writerId),
      ],
    );
    await request(projector, "prepareMessages", "messagesReady", {
      owners: owners.map((o) => ({ runId: o.runId })),
      workspaceMode,
      rate,
      policy,
      persistUsage,
      bulkUsage,
    });
    const walBefore =
      fork || usage
        ? (await observer.query("select pg_current_wal_insert_lsn()::text as lsn")).rows[0].lsn
        : undefined;
    const start = wall() + 500;
    let stopTraffic;
    let traffic;
    let sent, finished;
    try {
      if (rate) stopTraffic = fork?.start(layout, start);
      if (rate) await request(projector, "releaseMessages", "messagesReleased", { start });
      sent = await request(emitter, "emitMessages", "messagesEmitted", {
        start,
        frameRate: inputControl
          ? (4630 * framesPerRun) / middle
          : rate
            ? (rate * framesPerRun) / middle
            : 25000,
      });
      if (!rate) {
        stopTraffic = fork?.start(layout, Date.now());
        await request(projector, "releaseMessages", "messagesReleased");
      }
      finished = await request(projector, "finishMessages", "messagesFinished");
    } finally {
      traffic = await stopTraffic?.();
    }
    const { result, stats, lag } = finished;
    const walBytes =
      fork || usage
        ? Number(
            (
              await observer.query(
                "select pg_wal_lsn_diff(pg_current_wal_insert_lsn(),$1::pg_lsn)::text as bytes",
                [walBefore],
              )
            ).rows[0].bytes,
          )
        : undefined;
    assert.equal(sent.source.frames, owners.length * framesPerRun);
    assert.equal(lag, 0);
    assert.equal(stats.logicalWrites, owners.length * framesPerRun);
    assert.equal(
      stats.commits,
      policy === "moving-sync" ? stats.syncTransactions : stats.asyncTransactions,
    );
    const { rows } = await observer.query(
      `select run_id as id,count(*)::int n,md5(string_agg(md5(content),'' order by commit_seq,ordinal)) digest,
      count(*) filter(where commit_seq between 3 and 202 and role='toolResult')::int results,
      count(*) filter(where commit_seq between 3 and 202 and role='assistant')::int calls
      from message_entries ${fork ? "where run_id=any($1::uuid[])" : ""} group by run_id`,
      fork ? [owners.map((o) => o.runId)] : [],
    );
    assert.deepEqual(rows.map((r) => [r.id, r.digest]).sort(), expected);
    assert(rows.every((r) => r.n === 202 && r.results === 100 && r.calls === 100));
    const closed = await observer.query(
      "select count(*)::int n from runs where state='completed' and history_head is not null" +
        (fork ? " and id=any($1::uuid[])" : ""),
      fork ? [owners.map((o) => o.runId)] : [],
    );
    assert.equal(closed.rows[0].n, owners.length);
    await fork?.verify(layout);
    const usageRows = await usage?.verify(persistUsage);
    if (usage) assert.equal(stats.usageUpserts, persistUsage ? owners.length * 102 : 0);
    let usageStorage;
    if (bulkUsage !== undefined) {
      const {
        rows: [size],
      } =
        await observer.query(`select pg_total_relation_size('usage_latest')::int as "usageTableBytes",
        sum(pg_column_size(value))::int as "usageValueBytes" from usage_latest`);
      usageStorage = {
        ...size,
        bulkUsage,
        usageStatements: stats.usageStatements,
        multiUsageStatements: stats.multiUsageStatements,
      };
    }
    const { settledAt, lagSamples, ...timings } = result;
    const trial = {
      ...usageStorage,
      ...(workspaceMode ? { workspaceMode } : {}),
      rate: rate || "preloaded",
      policy,
      ...(fork ? { layout, ...traffic, walBytes } : {}),
      ...(usage ? { persistUsage, usageRows, usageUpserts: stats.usageUpserts, walBytes } : {}),
      ...timings,
      ...(workspace && (rate || inputControl)
        ? {
            inputAckRps: (count * 1000) / (sent.source.acknowledgedAt - start),
            afterLastAckMs: Math.max(0, settledAt - sent.source.acknowledgedAt),
            lagSamples,
          }
        : {}),
      sourceLateP95Ms: sent.source.lateMs.p95,
      transactions: stats.commits,
      syncTransactions: stats.syncTransactions,
      asyncTransactions: stats.asyncTransactions,
      holdMsPerTxn: stats.holdMs / stats.commits,
      poolMsPerTxn: stats.poolMs / stats.commits,
      maxBatch: stats.maxRecords,
      finalLag: lag,
      verifiedEntries: rows.length * 202,
    };
    report.completeMessages.trials.push(trial);
    await save();
    console.log(JSON.stringify({ ...trial, lagSamples: undefined }));
  }
  if (profile === "workspace-rate") {
    for (const [i, workspaceMode] of ["single", "batch", "batch", "single"].entries()) {
      await wave({
        label: "workspace-live-" + workspaceMode + "-" + i,
        workspaceMode,
        scene: "plain",
        integrated: true,
        cacheMode: "off",
        wakeMode: "directed",
        batch: 32,
        slots: 128,
        omitNotify: true,
        probeUnlisted: true,
        windowMs: 5,
        tickMs: 200,
        sink: "single",
        prefixWidth: 16,
        projectionPolicy: "recorded-history",
        count: 192,
        concurrency: 64,
      });
    }
  }
  if (fork) {
    report.forkStudy = await fork.finish();
    await save();
  }
}
