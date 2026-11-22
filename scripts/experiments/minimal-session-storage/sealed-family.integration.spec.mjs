import { randomUUID } from "node:crypto";
import { expect, it } from "vitest";
import { BACKGROUND_CONTEXT as ctx } from "@earendil-works/chord/context";
import { MemoryStorage } from "@earendil-works/pi-durable";
import { createDatabase } from "@pi-cloud/database";
import {
  AcceptedFamilyCheckpoint,
  hydrateSealedFamily,
  readSealedFamily,
  resumeFamilyCheckpoint,
  saveSealedFamily,
} from "@pi-cloud/pi-session-postgres/durable";
import { sql } from "kysely";

const source = process.env.PI_CLOUD_POSTGRES_INTEGRATION_URL;
it.skipIf(!source)(
  "requires an intact prefix and matching old/new identities before importing current state",
  async () => {
    const schema = "handoff_" + randomUUID().replaceAll("-", ""),
      admin = createDatabase({ connectionString: source, maxConnections: 1 });
    const url = new URL(source);
    url.searchParams.set("options", "-c search_path=" + schema);
    const db = createDatabase({ connectionString: url.href, maxConnections: 2 });
    const scope = {
      tenantId: randomUUID(),
      piSessionId: randomUUID(),
      sessionId: randomUUID(),
      turnId: randomUUID(),
      runId: randomUUID(),
      writerId: randomUUID(),
      fencingToken: 1,
    };
    const successor = { ...scope, writerId: randomUUID(), fencingToken: 2 },
      topic = "isolated",
      seal = randomUUID(),
      position = { topic, partition: 3, offset: 16n };
    const unusual = "preserve\u0000and\uD800",
      family = new AcceptedFamilyCheckpoint();
    const record = {
      kind: "pi.agent",
      id: 2,
      history: "latest",
      fork: "initial",
      scope: { kind: "conversation", conversationId: 1 },
    };
    const first = {
      scope,
      factId: randomUUID(),
      display: [],
      commit: {
        seq: 1,
        nextId: 4,
        writes: [
          { type: "conversation", value: { id: 1 } },
          {
            type: "document.create",
            record,
            content: { kind: "base", version: 1, value: { text: unusual } },
          },
        ],
      },
    };
    family.observe(first);
    family.observe(first);
    expect(() => family.observe({ ...first, factId: randomUUID() })).toThrow(/prefix/);
    expect(() => family.observe({ ...first, commit: { seq: 3, nextId: 4, writes: [] } })).toThrow(
      /prefix/,
    );
    family.observe({ ...first, factId: randomUUID(), commit: { seq: 2, nextId: 4, writes: [] } });
    try {
      await sql.raw("create schema " + schema).execute(admin);
      await sql
        .raw(
          `create table runs(id uuid,tenant_id uuid,session_id uuid,turn_id uuid,lease_id uuid,fencing_token bigint,state text,mailbox_position int,recovery_requested_at timestamptz,output_seal_id uuid,output_first_topic text,output_first_partition int,output_first_offset bigint,native_work_handoff jsonb,projection_tools jsonb);
      create table session_leases(tenant_id uuid,pi_session_id text,lease_id uuid,fencing_token bigint,writer_sealed_at timestamptz,writer_seal_offset bigint,released_at timestamptz,writer_failed_at timestamptz);
      create table messages(tenant_id uuid,pi_session_id text,run_id uuid,entry_id bigint,conversation_id bigint,commit_seq bigint,ordinal int,content text);
      create table inboxes(tenant_id uuid,pi_session_id text,doc_id bigint,conversation_id bigint,created_seq bigint,commit_seq bigint,items jsonb)`,
        )
        .execute(db);
      await sql`insert into runs values(${scope.runId}::uuid,${scope.tenantId}::uuid,${scope.sessionId}::uuid,${scope.turnId}::uuid,${scope.writerId}::uuid,1,'running',1,now(),${seal}::uuid,${topic},3,0,null,'{}')`.execute(
        db,
      );
      const save = () =>
        db.transaction().execute(async (tx) => {
          await sql`set local synchronous_commit=off`.execute(tx);
          await saveSealedFamily(tx, scope, position, seal, family.snapshot());
          expect(
            (await sql`select current_setting('synchronous_commit') as mode`.execute(tx)).rows[0]
              .mode,
          ).toBe("on");
        });
      await expect(
        db
          .transaction()
          .execute((tx) =>
            saveSealedFamily(tx, scope, { ...position, partition: 2 }, seal, family.snapshot()),
          ),
      ).rejects.toThrow(/seal/);
      await sql`update runs set projection_tools='{"9":{"taskId":9,"conversationId":1,"operationId":"orphan"}}'`.execute(
        db,
      );
      await expect(save()).rejects.toThrow();
      expect(
        (await sql`select native_work_handoff from runs`.execute(db)).rows[0].native_work_handoff,
      ).toBeNull();
      await sql`update runs set projection_tools='{}'`.execute(db);
      await save();
      await expect(readSealedFamily(db, successor, topic)).rejects.toThrow(/matching/);
      await sql`insert into session_leases values(${scope.tenantId}::uuid,${scope.piSessionId},${scope.writerId}::uuid,1,now(),16,now(),now()),(${scope.tenantId}::uuid,${scope.piSessionId},${successor.writerId}::uuid,2,null,null,null,null)`.execute(
        db,
      );
      await sql`update runs set lease_id=${successor.writerId}::uuid,fencing_token=2`.execute(db);
      for (const other of [
        scope,
        { ...successor, tenantId: randomUUID() },
        { ...successor, fencingToken: 3 },
      ])
        await expect(readSealedFamily(db, other, topic)).rejects.toThrow();
      await expect(readSealedFamily(db, successor, "other")).rejects.toThrow(/source/);
      // A claimant may publish new deltas or die with none. Both use the same
      // sealed predecessor, and an empty seal cannot hide an accepted suffix.
      const next = { ...position, offset: position.offset + 2n };
      const nextSeal = randomUUID();
      await expect(resumeFamilyCheckpoint(db, successor, position)).rejects.toThrow(/position/);
      await expect(
        resumeFamilyCheckpoint(db, successor, { ...next, partition: 1 }),
      ).rejects.toThrow(/position/);
      await expect(resumeFamilyCheckpoint(db, successor, next, true)).rejects.toThrow(/matched/);
      const base = await resumeFamilyCheckpoint(db, successor, next);
      expect(base.snapshot()).toEqual(family.snapshot());
      const successorDelta = {
        ...first,
        factId: randomUUID(),
        scope: successor,
        commit: {
          seq: 3,
          nextId: 4,
          writes: [
            {
              type: "document.change",
              id: 2,
              content: { kind: "base", version: 1, value: { text: "B wrote this" } },
            },
          ],
        },
      };
      expect(() => new AcceptedFamilyCheckpoint().observe(successorDelta)).toThrow(/prefix/);
      base.observe(successorDelta);
      base.observe(successorDelta);
      expect(
        base.snapshot().records.find((r) => r.write.record?.id === 2).write.content.value.text,
      ).toBe("B wrote this");
      expect(() =>
        base.observe({ ...successorDelta, commit: { ...successorDelta.commit, seq: 5 } }),
      ).toThrow(/prefix/);
      await expect(
        db.transaction().execute(async (tx) => {
          await sql`update runs set output_first_offset=null,output_first_topic=null,output_first_partition=null,output_seal_id=${nextSeal}::uuid`.execute(
            tx,
          );
          const empty = await resumeFamilyCheckpoint(tx, successor, next, true);
          await expect(
            saveSealedFamily(tx, successor, next, nextSeal, base.snapshot()),
          ).rejects.toThrow(/seal/);
          await saveSealedFamily(tx, successor, next, nextSeal, empty.snapshot());
          const stored = (await sql`select native_work_handoff as h from runs`.execute(tx)).rows[0]
            .h;
          expect(stored.scope.writerId).toBe(successor.writerId);
          expect(JSON.parse(stored.nativeText)).toEqual(family.snapshot());
          throw Error("rollback study");
        }),
      ).rejects.toThrow("rollback study");
      const image = await readSealedFamily(db, successor, topic),
        store = new MemoryStorage();
      await hydrateSealedFamily(image, store);
      expect((await store.document(2, "current", ctx)).value.text).toBe(unusual);
      expect(Number(await store.mintId())).toBe(4);
      expect(Number(store.prepareCommit([]).seq)).toBe(3);
      await store.close(ctx);
      // One current root, one child inheriting an older parent cutoff, and an
      // unrelated idle lane. The waiting tool pins one entry before compaction.
      const bounded = new AcceptedFamilyCheckpoint();
      const rec = (id, conversationId, head) => ({
        type: "entry",
        value: { id, conversationId, kind: "sample", ...(head ? { head } : {}) },
      });
      const writes = [
        first.commit.writes[0],
        {
          type: "conversation",
          value: {
            id: 20,
            parent: { conversationId: 1, at: 11 },
            owner: { conversationId: 1, taskId: 26 },
          },
        },
        { type: "conversation", value: { id: 30 } },
        {
          type: "task",
          value: {
            id: 26,
            conversationId: 1,
            kind: "pi.tool",
            input: { assistant: 3 },
            state: { status: "running", checkpoint: { phase: "call" } },
          },
        },
        ...[3, 4, 7, 8, 9, 10, 11, 12, 14, 15, 16, 17].map((id) =>
          rec(id, 1, id === 9 ? 7 : id === 17 ? 15 : undefined),
        ),
        rec(21, 20),
        rec(31, 30),
      ];
      bounded.observe({ ...first, commit: { seq: 1, nextId: 33, writes } });
      const updated = bounded.snapshot();
      await sql`update runs set native_work_handoff=jsonb_set(native_work_handoff,'{nativeText}',to_jsonb(${JSON.stringify(updated)}::text))`.execute(
        db,
      );
      for (const [ordinal, w] of writes.entries())
        if (w.type === "entry")
          await sql`insert into messages values(${scope.tenantId}::uuid,${scope.piSessionId},${scope.runId}::uuid,${w.value.id},${w.value.conversationId},1,${ordinal},${JSON.stringify(w.value)})`.execute(
            db,
          );
      const suffix = await readSealedFamily(db, successor, topic);
      expect(suffix.entries.map((e) => Number(e.entry_id))).toEqual([
        3, 7, 8, 9, 10, 11, 15, 16, 17, 21,
      ]);
      await sql`delete from messages where entry_id=3`.execute(db);
      await expect(readSealedFamily(db, successor, topic)).rejects.toThrow(
        /Referenced native history/,
      );
    } finally {
      await db.destroy();
      try {
        await sql.raw("drop schema if exists " + schema + " cascade").execute(admin);
      } finally {
        await admin.destroy();
      }
    }
  },
);
