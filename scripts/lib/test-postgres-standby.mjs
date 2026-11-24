// Create a physical hot standby for a primary owned by the disposable PG fixture.
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { setTimeout as delay } from "node:timers/promises";
import { randomUUID, randomBytes } from "node:crypto";
import { Pool } from "pg";

const exec = promisify(execFile);
export async function startTestStandby(primary, url, image) {
  const id = randomUUID(),
    network = "pma-replay-" + id,
    name = "pma-standby-" + id;
  let connected = false,
    created = false,
    networkCreated = false;
  async function docker(args) {
    return exec("docker", args, { timeout: 90000 });
  }
  const primaryDb = new Pool({ connectionString: url, max: 1 });
  async function close() {
    if (created) {
      await docker(["rm", "-fv", name]);
      created = false;
    }
    if (connected) {
      await docker(["network", "disconnect", network, primary]);
      connected = false;
    }
    if (networkCreated) {
      await docker(["network", "rm", network]);
      networkCreated = false;
    }
  }
  try {
    await docker(["network", "create", network]);
    networkCreated = true;
    await docker(["network", "connect", "--alias", "pma-primary", network, primary]);
    connected = true;
    // Only a fresh disposable primary receives this role. The password is
    // ephemeral and passed through stdin, outside process arguments and reports.
    const password = randomBytes(32).toString("hex");
    await primaryDb.query(
      "create role pma_replay with login replication password '" + password + "'",
    );
    const hba = (await primaryDb.query("show hba_file")).rows[0].hba_file;
    await docker([
      "exec",
      "--user",
      "postgres",
      primary,
      "sh",
      "-ec",
      `echo 'host replication pma_replay samenet scram-sha-256' >>"$1"`,
      "append-test-replication",
      hba,
    ]);
    await primaryDb.query("select pg_reload_conf()");
    await docker([
      "run",
      "--detach",
      "--name",
      name,
      "--network",
      network,
      "--memory=768m",
      "--cpus=2",
      "--publish",
      "127.0.0.1::5432",
      image,
      "sh",
      "-ec",
      `
     install -d -m 0700 -o postgres -g postgres "$PGDATA"
     while [ ! -f /tmp/pma-replication-ready ]; do sleep 0.05; done
     export PGPASSFILE=/tmp/pma-pgpass
     gosu postgres pg_basebackup -d 'host=pma-primary user=pma_replay application_name=pma_standby passfile=/tmp/pma-pgpass' -D "$PGDATA" -X stream -R -C -S pma_standby --checkpoint=fast
     exec gosu postgres postgres -D "$PGDATA" -c listen_addresses='*' -c hot_standby=on`,
    ]);
    created = true;
    const secret = execFile(
      "docker",
      [
        "exec",
        "-i",
        name,
        "sh",
        "-ec",
        "umask 077; cat >/tmp/pma-pgpass; chown postgres:postgres /tmp/pma-pgpass; touch /tmp/pma-replication-ready",
      ],
      { timeout: 10000 },
    );
    const finished = new Promise((res, rej) => {
      secret.once("error", rej);
      secret.once("close", (c) =>
        c === 0 ? res() : rej(Error("Standby credential setup failed")),
      );
    });
    secret.stdin.end("pma-primary:5432:*:pma_replay:" + password + "\n");
    await finished;
    const port = /^127\.0\.0\.1:(\d+)\s*$/.exec(
      (await docker(["port", name, "5432/tcp"])).stdout,
    )?.[1];
    if (!port) throw Error("Missing standby loopback port");
    const replicaUrl = new URL(url);
    replicaUrl.port = port;
    const replica = new Pool({
      connectionString: replicaUrl.href,
      max: 1,
      connectionTimeoutMillis: 800,
    });
    replica.on("error", () => {});
    try {
      const end = performance.now() + 60000;
      while (performance.now() < end) {
        try {
          if (
            (await replica.query("select pg_is_in_recovery() recovery")).rows[0].recovery &&
            (
              await primaryDb.query(
                "select exists(select from pg_stat_replication where application_name='pma_standby' and state='streaming' and sync_state='async') active",
              )
            ).rows[0].active
          )
            return {
              url: replicaUrl.href,
              close,
              stop: () => docker(["stop", "--timeout=5", name]),
            };
        } catch {}
        await delay(100);
      }
      throw Error("Disposable asynchronous standby did not enter streaming recovery");
    } finally {
      await replica.end();
    }
  } catch (error) {
    await close();
    throw error;
  } finally {
    await primaryDb.end();
  }
}
