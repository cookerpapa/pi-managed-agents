import { createHash, timingSafeEqual } from "node:crypto";
import { once } from "node:events";
import { WebSocket, WebSocketServer } from "ws";

// Live connections advertise capacity. Undelivered hints occupy one bounded
// counter; PostgreSQL remains the authority on which Turns can be claimed.
export async function openWakeRouter(host, path, fail, port = 0, token, onSteerAck) {
  const digest = (value) => createHash("sha256").update(value).digest();
  const expected = token === undefined ? undefined : digest("Bearer " + token);
  const server = new WebSocketServer({
    host,
    port,
    path,
    perMessageDeflate: false,
    maxPayload: 256,
    ...(expected
      ? {
          verifyClient: ({ req }) =>
            timingSafeEqual(expected, digest(req.headers.authorization ?? "")),
        }
      : {}),
  });
  await once(server, "listening");
  server.on("error", fail);
  const connections = new Map(),
    waiting = new Set(),
    directory = new Map(),
    probes = new WeakSet();
  let stage,
    timer,
    pendingWakes = 0,
    armedFault,
    stats;
  function drop(fault) {
    if (armedFault !== fault) return false;
    armedFault = undefined;
    stats.faults++;
    return true;
  }
  function signal(socket, reason, hint) {
    waiting.delete(socket);
    if (reason === "arrival" && drop("disconnect")) return socket.terminate();
    if (reason === "arrival" && drop("lost-wake")) return;
    if (stage.perTurn && reason === "tick") probes.add(socket);
    stats[reason]++;
    socket.send(hint ? JSON.stringify(hint) : reason);
  }
  function signalWaiting(count, reason) {
    let sent = 0;
    for (const socket of waiting) {
      signal(socket, reason);
      if (++sent >= count) break;
    }
    return sent;
  }
  function tick() {
    if (signalWaiting(1, "tick") || !stage.probeUnlisted || !connections.size) return;
    // A lost ready/wake can leave a live, willing Worker off the list.
    // Rotate a single online connection; a full Worker ignores it locally.
    const [id, socket] = connections.entries().next().value;
    connections.delete(id);
    connections.set(id, socket);
    stats.unlistedTicks++;
    signal(socket, "tick");
  }
  function notify(label, count, hints = []) {
    if (!stage || stage.label !== label || !(count > 0)) return;
    stats.notifications++;
    if (drop("lost-notification")) return;
    if (stage.wakeMode === "broadcast") {
      for (const socket of connections.values()) signal(socket, "arrival");
      return;
    }
    if (stage.perTurn) {
      // Keep only a bounded count for Workers that are busy or reconnecting.
      const demand = pendingWakes + count;
      pendingWakes = Math.min(1024, demand - signalWaiting(demand, "arrival"));
      return;
    }
    const needed = Math.ceil(count / stage.batch);
    let targeted = 0;
    if (stage.cacheMode === "prefer")
      for (const hint of hints) {
        const key = hint[0] + "/" + hint[1];
        const known = directory.get(key);
        if (!known) continue;
        if (known.expires <= Date.now() || connections.get(known.id) !== known.socket) {
          directory.delete(key);
          continue;
        }
        if (known.version !== hint[3] || !waiting.has(known.socket)) continue;
        signal(known.socket, "arrival", hint);
        stats.preferred++;
        targeted++;
      }
    const remaining = Math.max(0, needed - targeted);
    pendingWakes = remaining > 0 && signalWaiting(remaining, "arrival") < remaining ? 1 : 0;
  }
  server.on("connection", (socket, request) => {
    const url = new URL(request.url, "http://localhost");
    const id = url.searchParams.get("id");
    if (!stage || url.pathname !== path || url.searchParams.get("stage") !== stage.label || !id) {
      socket.close(1008);
      return;
    }
    const previous = connections.get(id);
    if (previous) {
      waiting.delete(previous);
      previous.terminate();
    }
    connections.set(id, socket);
    stats.joins++;
    socket.on("error", () => socket.terminate());
    socket.on("close", () => {
      waiting.delete(socket);
      if (connections.get(id) === socket) connections.delete(id);
    });
    socket.on("message", (raw) => {
      if (connections.get(id) !== socket) return;
      const frame = raw.toString();
      if (onSteerAck && frame.startsWith("seen:")) {
        const match =
          /^seen:([0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}):([1-9][0-9]{0,18})$/.exec(frame);
        if (!match || BigInt(match[2]) > 9223372036854775807n) return socket.close(1008);
        onSteerAck(id, match[1], match[2]);
        return;
      }
      if (frame.startsWith("[") && stage.cacheMode === "prefer") {
        const [tenant, session, version, action] = JSON.parse(frame);
        const key = tenant + "/" + session;
        if (action === "drop") {
          const held = directory.get(key);
          if (held?.socket === socket && held.version === version) directory.delete(key);
          return;
        }
        directory.delete(key);
        if (directory.size >= 4096) directory.delete(directory.keys().next().value);
        directory.set(key, {
          id,
          socket,
          version,
          expires: Date.now() + (stage.cacheTtlMs ?? 300000),
        });
        return;
      }
      // A reconciliation probe found real work. Pass the probe to one ready
      // connection to catch up after a listener/router outage.
      if (stage.perTurn && frame === "found") {
        if (probes.delete(socket)) signalWaiting(1, "tick");
        return;
      }
      if (frame !== "ready") return;
      stats.offers++;
      if (drop("lost-ready")) return;
      waiting.add(socket);
      if (pendingWakes && stage.wakeMode === "directed") {
        pendingWakes--;
        signalWaiting(1, "arrival");
      }
    });
    socket.send("registered");
  });
  function reset() {
    clearInterval(timer);
    for (const socket of connections.values()) socket.terminate();
    connections.clear();
    waiting.clear();
    directory.clear();
    pendingWakes = 0;
    stage = undefined;
  }
  function startTimer() {
    clearInterval(timer);
    timer = setInterval(tick, stage.tickMs ?? 200);
  }
  return {
    port: server.address().port,
    notify,
    notifyStop(id, writer) {
      const socket = connections.get(id);
      if (socket?.readyState === WebSocket.OPEN && socket.bufferedAmount < 65536)
        socket.send("stop:" + writer);
    },
    notifySteer(id, writer, revision) {
      const socket = connections.get(id);
      if (socket?.readyState === WebSocket.OPEN && socket.bufferedAmount < 65536)
        socket.send("steer:" + writer + ":" + revision);
    },
    configure(config) {
      reset();
      stage = config;
      armedFault = config.fault;
      stats = {
        joins: 0,
        offers: 0,
        arrival: 0,
        tick: 0,
        unlistedTicks: 0,
        notifications: 0,
        faults: 0,
        preferred: 0,
      };
      if (!stage.fault) startTimer();
    },
    startTimer,
    snapshot: () => ({ ...stats, online: connections.size, waiting: waiting.size }),
    async close() {
      reset();
      await new Promise((resolve) => server.close(resolve));
    },
  };
}

// One pending PG claim reserves its complete batch. Committed rows finish even
// if the management socket disappears while the SQL request is in flight.
export function connectClaimWorker({
  url,
  id,
  stage,
  slots,
  claim,
  execute,
  fail,
  onRegistered,
  onSteer,
  onStop,
  token,
}) {
  const address = new URL(url);
  address.searchParams.set("stage", stage.label);
  address.searchParams.set("id", id);
  const stats = {
    queries: 0,
    empty: 0,
    rows: 0,
    maximumBatch: 0,
    maximumRunning: 0,
    arrival: 0,
    tick: 0,
    offers: 0,
    registrations: 0,
    bootstrap: 0,
    fullHints: 0,
    hintedClaims: 0,
    targetedRows: 0,
  };
  const drained = Promise.withResolvers();
  function finishDrain() {
    if (stopped && !checking && !running) drained.resolve();
  }
  let socket,
    reconnect,
    renew,
    online = false,
    stopped = false,
    checking = false,
    checkingAgain,
    offered = false,
    running = 0,
    reconnectMs = 100;
  function offer() {
    if (!online || stopped || checking || running >= slots) return;
    offered = true;
    stats.offers++;
    socket.send("ready");
    clearTimeout(renew);
    const timeout = (stage.reofferMs ?? 4000) * (0.8 + 0.4 * Math.random());
    renew = setTimeout(offer, timeout);
  }
  function request(reason, hint) {
    if (stopped || !online || running >= slots) return;
    if (checking) {
      if (!checkingAgain?.hint || hint) checkingAgain = { reason, hint };
      return;
    }
    clearTimeout(renew);
    offered = false;
    checking = true;
    checkingAgain = undefined;
    const limit = Math.min(slots - running, stage.batch);
    const start = performance.now();
    stats.queries++;
    void (async () => {
      if (hint) stats.hintedClaims++;
      const rows = await claim(limit, hint);
      if (hint && rows.some((r) => r.id === hint[2])) stats.targetedRows++;
      const elapsed = performance.now() - start;
      if (rows.length > limit) throw new Error("Claim exceeded reserved slots");
      if (!rows.length) stats.empty++;
      stats.rows += rows.length;
      stats.maximumBatch = Math.max(stats.maximumBatch, rows.length);
      running += rows.length;
      stats.maximumRunning = Math.max(stats.maximumRunning, running);
      for (const row of rows) {
        void execute(row, elapsed, reason)
          .catch(fail)
          .finally(() => {
            const wasFull = running === slots;
            running--;
            if (wasFull && !offered) {
              if (stage.perTurn) offer();
              else request("freed-slot");
            }
            finishDrain();
          });
      }
      checking = false;
      if (checkingAgain || (!stage.perTurn && rows.length === limit))
        request(checkingAgain?.reason ?? "drain", checkingAgain?.hint);
      else offer();
      if (stage.perTurn && reason === "tick" && rows.length && online && !stopped)
        socket.send("found");
      finishDrain();
    })().catch((error) => {
      checking = false;
      fail(error);
      finishDrain();
    });
  }
  function connect() {
    if (stopped) return;
    const current = new WebSocket(address, {
      perMessageDeflate: false,
      handshakeTimeout: 5000,
      ...(token ? { headers: { authorization: "Bearer " + token } } : {}),
    });
    socket = current;
    current.on("error", () => current.terminate());
    if (token)
      current.on("unexpected-response", (_request, response) => {
        response.resume();
        if (!stopped && [401, 403].includes(response.statusCode))
          fail(new Error("Worker wake authentication failed"));
        current.terminate();
      });
    current.on("message", (raw) => {
      if (stopped || socket !== current) return;
      const frame = raw.toString();
      if (onStop && frame.startsWith("stop:")) {
        onStop(frame.slice(5));
        return;
      }
      if (onSteer && frame.startsWith("steer:")) {
        const [, writer, revision] = frame.split(":");
        onSteer(writer, revision);
        return;
      }
      const hint = frame.startsWith("[") ? JSON.parse(frame) : undefined;
      const message = hint ? "arrival" : frame;
      if (message === "registered") {
        online = true;
        reconnectMs = 100;
        onRegistered?.();
        stats.registrations++;
        if (stage.perTurn) {
          offer();
          return;
        }
        stats.bootstrap++;
      } else if (message === "arrival" || message === "tick") {
        stats[message]++;
        if (running >= slots) stats.fullHints++;
      } else return fail(new Error("Unexpected prototype wake frame"));
      request(message === "registered" ? "bootstrap" : message, hint);
    });
    current.on("close", () => {
      if (socket !== current) return;
      online = false;
      offered = false;
      clearTimeout(renew);
      if (!stopped) {
        reconnect = setTimeout(connect, reconnectMs * (0.5 + 0.5 * Math.random()));
        reconnectMs = Math.min(4000, reconnectMs * 2);
      }
    });
  }
  connect();
  return {
    stats,
    ackSteer(writer, revision) {
      if (
        online &&
        !stopped &&
        socket.readyState === WebSocket.OPEN &&
        socket.bufferedAmount < 65536
      )
        socket.send("seen:" + writer + ":" + revision);
    },
    advertise(tenant, session, version, action) {
      if (online && !stopped && stage.cacheMode === "prefer")
        socket.send(
          JSON.stringify(action ? [tenant, session, version, action] : [tenant, session, version]),
        );
    },
    get busy() {
      return running > 0 || checking;
    },
    close() {
      stopped = true;
      online = false;
      clearTimeout(renew);
      clearTimeout(reconnect);
      socket?.terminate();
      finishDrain();
      return drained.promise;
    },
  };
}
