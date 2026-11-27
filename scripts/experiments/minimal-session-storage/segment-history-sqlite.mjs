// Single threaded local I/O actor. A transaction owns both the index and
// all Entry rows until the new terminal version can be advertised.
import { DatabaseSync } from "node:sqlite";
import { createHash } from "node:crypto";
import { lstatSync, mkdirSync, chmodSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { parentPort, workerData } from "node:worker_threads";

const { directory, maxBytes, ttlMs } = workerData;
mkdirSync(directory, { recursive: true, mode: 0o700 });
const info = lstatSync(directory);
if (!info.isDirectory() || info.uid !== process.getuid() || (info.mode & 0o077) !== 0)
  throw Error("Private Worker cache directory required");
const path = join(directory, "history.sqlite");
const existing = lstatSync(path, { throwIfNoEntry: false });
if (existing && (!existing.isFile() || existing.uid !== info.uid || existing.nlink !== 1))
  throw Error("Invalid cache file");
const db = new DatabaseSync(path);
const first = Number(db.prepare("pragma user_version").get().user_version);
const applicationId = 0x504d4148;
if (first === 0 && db.prepare("select 1 from sqlite_schema limit 1").get())
  throw Error("Foreign SQLite database");
if (
  first !== 0 &&
  (first !== 1 || db.prepare("pragma application_id").get().application_id !== applicationId)
)
  throw Error("Unsupported cache schema");
chmodSync(path, 0o600);
db.exec(
  "pragma foreign_keys=on;pragma journal_mode=delete;pragma synchronous=full;pragma busy_timeout=250;pragma temp_store=memory",
);
if (!first) {
  db.exec(`pragma auto_vacuum=full;begin immediate;
    create table heads(k text primary key,session text not null,version text not null,fence integer not null,seq integer not null,floor integer not null,
      localBytes integer not null,bytes integer not null,locals integer not null,ancestors integer not null,expires integer not null,touched integer not null);
    create table entries(k text not null references heads(k) on delete cascade,ord integer not null,native integer not null,
      inherited integer not null,seq integer not null,weight integer not null,body text not null,digest text not null,primary key(k,ord)) without rowid;
    create index entry_floor on entries(k,native);create index disk_lru on heads(touched,k);
    pragma application_id=${applicationId};pragma user_version=1;commit`);
}
const pageSize = db.prepare("pragma page_size").get().page_size;
const maxPages = Math.max(1, Math.floor(maxBytes / pageSize));
const getHead = db.prepare("select * from heads where k=?"),
  allHeads = db.prepare("select k,session,version,bytes,expires from heads order by touched"),
  remove = db.prepare("delete from heads where k=?"),
  append = db.prepare("insert into entries values(?,?,?,?,?,?,?,?)"),
  oldest = db.prepare("select k,bytes from heads where k<>? order by touched,k limit 1");
// Reopening with a smaller budget trims old snapshots before lowering the
// SQLite page ceiling. A rollback journal can temporarily use extra space.
while (db.prepare("pragma page_count").get().page_count > maxPages) {
  const victim = oldest.get("");
  if (!victim) throw Error("Disk cache budget too small");
  remove.run(victim.k);
}
db.exec("pragma max_page_count=" + maxPages);
let writing, iterator;
const checksum = (k, ord, id, seq, weight, inherited, body) =>
  createHash("sha256")
    .update(JSON.stringify([k, ord, id, seq, weight, inherited]))
    .update(body)
    .digest("hex");
function expire(now) {
  db.prepare("delete from heads where expires<=?").run(now);
}
function ioBytes() {
  try {
    return Number(/^write_bytes:\s*(\d+)/m.exec(readFileSync("/proc/thread-self/io", "utf8"))[1]);
  } catch {
    return null;
  }
}
function abort() {
  iterator?.return();
  iterator = undefined;
  if (db.isTransaction) db.exec("rollback");
  writing = undefined;
}
function handle(op, m) {
  if (op === "usage")
    return {
      mainBytes: db.prepare("pragma page_count").get().page_count * pageSize,
      threadWriteBytes: ioBytes(),
    };
  if (op === "close") {
    abort();
    db.close();
    return;
  }
  if (op === "abort") {
    abort();
    return;
  }
  if (op === "list") {
    expire(m.now);
    return allHeads.all();
  }
  if (op === "evict") {
    const victim = db
      .prepare(
        "delete from heads where k=(select k from heads where k<>? order by touched,k limit 1) returning k",
      )
      .get(m.except);
    return victim?.k;
  }
  if (op === "forget") {
    db.prepare("delete from heads where k=? and version=?").run(m.key, m.version);
    return;
  }
  if (op === "begin") {
    const h = m.head;
    const startIo = ioBytes();
    db.exec("begin immediate");
    expire(m.now);
    const old = getHead.get(h.k);
    if (h.bytes > maxBytes || (old && (old.session !== h.session || old.fence >= h.fence))) {
      db.exec("rollback");
      return { skip: true };
    }
    const delta = Boolean(old && old.version === m.base && old.floor <= h.floor);
    const evicted = [];
    const totals = db
      .prepare("select count(*) as count,coalesce(sum(bytes),0) as bytes from heads where k<>?")
      .get(h.k);
    while (totals.bytes + h.bytes > maxBytes || totals.count >= 4096) {
      const victim = oldest.get(h.k);
      if (!victim) break;
      remove.run(victim.k);
      evicted.push(victim.k);
      totals.bytes -= victim.bytes;
      totals.count--;
    }
    if (!delta) remove.run(h.k);
    db.prepare(
      `insert into heads values(?,?,?,?,?,?,?,?,?,?,?,?)
      on conflict(k) do update set version=excluded.version,fence=excluded.fence,seq=excluded.seq,floor=excluded.floor,
      localBytes=excluded.localBytes,bytes=excluded.bytes,locals=excluded.locals,ancestors=excluded.ancestors,expires=excluded.expires,touched=excluded.touched`,
    ).run(
      h.k,
      h.session,
      h.version,
      h.fence,
      h.seq,
      h.floor,
      h.localBytes,
      h.bytes,
      h.locals,
      h.ancestors,
      m.now + ttlMs,
      m.now,
    );
    let locals = delta ? old.locals : 0,
      ancestors = delta ? old.ancestors : 0,
      localBytes = delta ? old.localBytes : 0,
      bytes = delta ? old.bytes : 0;
    if (delta && h.floor > old.floor) {
      for (const row of db
        .prepare("delete from entries where k=? and native<? returning inherited,weight")
        .all(h.k, h.floor)) {
        bytes -= row.weight;
        if (row.inherited) ancestors--;
        else {
          locals--;
          localBytes -= row.weight;
        }
      }
    }
    const { n } = db.prepare("select coalesce(max(ord),-1)+1 n from entries where k=?").get(h.k);
    writing = {
      h,
      ord: n,
      evicted,
      delta,
      payloadBytes: 0,
      startIo,
      locals,
      ancestors,
      bytes,
      localBytes,
    };
    return { delta };
  }
  if (op === "append") {
    for (const r of m.rows) {
      const { h } = writing,
        ord = writing.ord++,
        id = r.entry.id,
        body = JSON.stringify(r.entry),
        inherited = Number(m.inherited);
      append.run(
        h.k,
        ord,
        id,
        inherited,
        r.seq,
        r.bytes,
        body,
        checksum(h.k, ord, id, r.seq, r.bytes, inherited, body),
      );
      writing.payloadBytes += Buffer.byteLength(body);
      writing.bytes += r.bytes;
      if (inherited) writing.ancestors++;
      else {
        writing.locals++;
        writing.localBytes += r.bytes;
      }
    }
    return;
  }
  if (op === "commit") {
    const { h, evicted, delta, payloadBytes, startIo } = writing;
    for (const field of ["localBytes", "bytes", "locals", "ancestors"])
      if (h[field] !== writing[field]) throw Error("Incomplete disk snapshot");
    const end = getHead.get(h.k);
    db.exec("commit");
    writing = undefined;
    const after = ioBytes();
    return {
      end,
      evicted,
      delta,
      payloadBytes,
      deviceWriteBytes: after === null || startIo === null ? null : after - startIo,
    };
  }
  if (op === "read") {
    db.exec("begin immediate");
    const h = getHead.get(m.key);
    if (
      !h ||
      h.session !== m.session ||
      h.version !== m.version ||
      h.expires <= m.now ||
      h.bytes > maxBytes ||
      (m.expected && Object.entries(m.expected).some(([key, value]) => h[key] !== value))
    ) {
      db.exec("rollback");
      return;
    }
    db.prepare("update heads set touched=? where k=?").run(m.now, m.key);
    iterator = db
      .prepare(
        "select ord,native,inherited,seq,weight,body,digest from entries where k=? order by ord",
      )
      .iterate(m.key);
    return h;
  }
  if (op === "batch") {
    const rows = [];
    let bytes = 0;
    while (bytes < 4 * 1024 * 1024) {
      const n = iterator.next();
      if (n.done) {
        iterator = undefined;
        db.exec("commit");
        break;
      }
      const r = n.value;
      if (r.digest !== checksum(m.key, r.ord, r.native, r.seq, r.weight, r.inherited, r.body))
        throw Error("Entry checksum differs");
      const entry = JSON.parse(r.body);
      if (entry.id !== r.native || entry.conversationId !== 1) throw Error("Invalid local Entry");
      rows.push({ entry, seq: r.seq, bytes: r.weight, inherited: r.inherited });
      bytes += Buffer.byteLength(r.body);
    }
    return { rows, done: !iterator, bytes };
  }
  throw Error("Unknown disk operation");
}
parentPort.on("message", ({ id, op, ...m }) => {
  try {
    parentPort.postMessage({ id, result: handle(op, m) });
  } catch (e) {
    abort();
    parentPort.postMessage({ id, error: e.errcode === 13 ? "full" : "io" });
  }
});
