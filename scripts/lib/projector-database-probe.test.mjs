import { expect, it } from "vitest";
import { installProjectorDatabaseProbe } from "./projector-database-probe.mjs";

const record = {
  partition: 1,
  offset: 2n,
  fact: { kind: "durable_commit", scope: { runId: "owned", piSessionId: "native" } },
};
function fixture() {
  class Client {
    processID = 3;
    query(text, values, callback) {
      const error = text === "fail" ? Object.assign(new Error("private"), { code: "57014" }) : null;
      const result = { rowCount: 1, rows: [{ secret: "private-result" }] };
      if (callback) {
        queueMicrotask(() => callback(error, result));
        return "callback-request";
      }
      return error ? Promise.reject(error) : Promise.resolve(result);
    }
  }
  class Pool {
    options = { max: 12, statement_timeout: 30000 };
    totalCount = 1;
    idleCount = 1;
    waitingCount = 0;
    client = new Client();
    connect(callback) {
      if (callback) {
        queueMicrotask(() => callback(null, this.client, () => {}));
        return "callback-acquire";
      }
      return Promise.resolve(this.client);
    }
  }
  return { Pool, Client };
}

it("preserves Promise results/errors and observes no SQL values or result bodies", async () => {
  const pg = fixture(),
    events = [],
    original = pg.Client.prototype.query;
  const probe = installProjectorDatabaseProbe(pg, (event) => events.push(event));
  try {
    const pool = new pg.Pool();
    await pool.client.query("select 'outside'", []);
    expect(events).toHaveLength(0);
    await probe.run(record, async () => {
      const client = await pool.connect();
      expect(
        await client.query("select $1, 'literal-secret'", ["parameter-secret"]),
      ).toHaveProperty("rows.0.secret", "private-result");
      await expect(client.query("fail", [])).rejects.toHaveProperty("code", "57014");
    });
    const projection = events.find((e) => e.event === "diag.sql.projection");
    expect(projection.spans.map((s) => s.type)).toEqual(["acquire", "query", "query"]);
    expect(projection.spans[2].error).toBe("57014");
    expect(JSON.stringify(events)).not.toMatch(/literal-secret|parameter-secret|private-result/);
  } finally {
    probe.uninstall();
  }
  expect(pg.Client.prototype.query).toBe(original);
});

it("preserves callback return values and keeps overlapping record scopes separate", async () => {
  const pg = fixture(),
    events = [];
  const probe = installProjectorDatabaseProbe(pg, (event) => events.push(event));
  try {
    await Promise.all(
      [1, 2].map((offset) =>
        probe.run(
          { ...record, offset },
          () =>
            new Promise((resolve) => {
              const pool = new pg.Pool();
              expect(
                pool.connect((error, client, release) => {
                  expect(error).toBeNull();
                  expect(typeof release).toBe("function");
                  expect(
                    client.query("select $1", ["private"], (error, result) => {
                      expect(error).toBeNull();
                      resolve(result);
                    }),
                  ).toBe("callback-request");
                }),
              ).toBe("callback-acquire");
            }),
        ),
      ),
    );
    const records = events.filter((e) => e.event === "diag.sql.projection");
    expect(records.map((r) => r.offset).sort()).toEqual(["1", "2"]);
    expect(records.every((r) => r.spans.length === 2)).toBe(true);
  } finally {
    probe.uninstall();
  }
});

it("cannot turn a successful operation into failure when the observation sink throws", async () => {
  const pg = fixture();
  const probe = installProjectorDatabaseProbe(pg, () => {
    throw new Error("sink failed");
  });
  try {
    await expect(
      probe.run(record, async () => {
        const client = await new pg.Pool().connect();
        return client.query("select $1", ["private"]);
      }),
    ).resolves.toHaveProperty("rowCount", 1);
  } finally {
    probe.uninstall();
  }
});

it("attributes each query once across a whole handler and its native projection", async () => {
  const pg = fixture();
  const events = [];
  const probe = installProjectorDatabaseProbe(pg, (event) => events.push(event));
  const fact = {
    ...record.fact,
    commit: {
      writes: [
        { type: "document.create", record: { id: 4, kind: "pi.live" }, content: "private-body" },
        { type: "document.change", id: 4, content: "private-delta" },
      ],
    },
  };
  try {
    const pool = new pg.Pool();
    await probe.run(
      { ...record, fact },
      async () => {
        const client = await pool.connect();
        await probe.run({ ...record, fact }, () => client.query("select 1", []));
        await client.query("select 2", []);
      },
      "diag.sql.handler",
    );
    const outer = events.find((e) => e.event === "diag.sql.handler");
    const inner = events.find((e) => e.event === "diag.sql.projection");
    expect(outer.documents).toEqual([[4, "pi.live"], [4]]);
    expect(outer.spans.map((s) => s.type)).toEqual(["acquire", "query"]);
    expect(inner.spans.map((s) => s.type)).toEqual(["query"]);
    expect(outer.spans[1].id).not.toBe(inner.spans[0].id);
    expect(JSON.stringify(events)).not.toMatch(/private-body|private-delta/);
  } finally {
    probe.uninstall();
  }
});
