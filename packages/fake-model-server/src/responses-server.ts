import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { randomUUID, timingSafeEqual } from "node:crypto";
import { monitorEventLoopDelay } from "node:perf_hooks";
import { zstdDecompressSync } from "node:zlib";
import { setTimeout as delay, setImmediate as immediate } from "node:timers/promises";
import { WebSocketServer, WebSocket } from "ws";

export const BENCHMARK_MARKER = "PMA_BENCH_V1:";
export type BenchmarkPlan = {
  id: string;
  kind: "text" | "mcp" | "guest" | "children" | "probe";
  firstByteMs?: number;
  chunkMs?: number;
  chunkBytes?: number;
  outputBytes?: number;
  rounds?: number;
  toolName?: string;
  toolDelayMs?: number;
  toolResultBytes?: number;
  children?: number;
  fault?: "rate_limit_once" | "disconnect_once" | "hold";
};
type State = { plan: BenchmarkPlan; done: Set<number> };
type Frame = Record<string, unknown>;
type RequestSample = {
  planId: string;
  responseId: string;
  transport: "http" | "websocket";
  bytes: number;
  inputItems: number;
  imageCount: number;
  imageDataUrlCharacters: number;
  incremental: boolean;
  startedAt: number;
  firstFrameAt?: number;
  firstTextAt?: number;
  finishedAt?: number;
  outcome: string;
  frames: number;
};
type ResponseInput = {
  model: string;
  input: unknown[];
  previous_response_id?: string;
  tools?: { name?: string; type?: string; parameters?: { required?: string[] } }[];
  stream?: boolean;
  instructions?: string;
};

function textOf(value: unknown): string {
  if (typeof value === "string") return value;
  if (Array.isArray(value)) return value.map(textOf).join("\n");
  if (value && typeof value === "object") {
    const v = value as { text?: unknown; content?: unknown };
    return typeof v.text === "string" ? v.text : v.content === undefined ? "" : textOf(v.content);
  }
  return "";
}
export function parseBenchmarkPlan(value: unknown): BenchmarkPlan {
  const p = value as BenchmarkPlan;
  if (
    !p ||
    typeof p.id !== "string" ||
    !/^[A-Za-z0-9_-]{1,80}$/.test(p.id) ||
    !["text", "mcp", "guest", "children", "probe"].includes(p.kind)
  )
    throw new Error("Invalid benchmark plan");
  for (const [key, max] of Object.entries({
    firstByteMs: 60000,
    chunkMs: 1000,
    chunkBytes: 65536,
    outputBytes: 262144,
    rounds: 32,
    toolDelayMs: 10000,
    toolResultBytes: 32768,
    children: 8,
  })) {
    const v = p[key as keyof BenchmarkPlan];
    if (v !== undefined && (!Number.isSafeInteger(v) || Number(v) < 0 || Number(v) > max))
      throw new Error(`Invalid benchmark ${key}`);
  }
  if (p.chunkBytes === 0 || p.children === 0)
    throw new Error("Invalid benchmark chunk/child count");
  if (p.toolName !== undefined && !/^[a-z][a-z0-9_]{0,63}$/.test(p.toolName))
    throw new Error("Invalid benchmark tool");
  if (p.fault !== undefined && !["rate_limit_once", "disconnect_once", "hold"].includes(p.fault))
    throw new Error("Invalid benchmark fault");
  return { ...p };
}
export const benchmarkPrompt = (plan: BenchmarkPlan, paddingBytes = 0) =>
  BENCHMARK_MARKER + JSON.stringify(parseBenchmarkPlan(plan)) + "\n" + "p".repeat(paddingBytes);
const errorFrame = (code: string, message: string, status = 400) => ({
  type: "error",
  status,
  code,
  message,
  error: { type: "invalid_request_error", code, message },
});
function imagesIn(value: unknown): { count: number; characters: number } {
  if (!value || typeof value !== "object") return { count: 0, characters: 0 };
  if (!Array.isArray(value)) {
    const v = value as { type?: string; image_url?: unknown };
    if (v.type === "input_image" && typeof v.image_url === "string")
      return { count: 1, characters: v.image_url.length };
  }
  return Object.values(value).reduce<{ count: number; characters: number }>(
    (n, v) => {
      const x = imagesIn(v);
      return { count: n.count + x.count, characters: n.characters + x.characters };
    },
    { count: 0, characters: 0 },
  );
}

/** Deterministic protocol fixture, not a model or a durable execution store.
 * It owns no upstream client/credentials and rejects non-benchmark model names. */
export class FakeResponsesServer {
  readonly #http;
  readonly #ws = new WebSocketServer({ noServer: true, maxPayload: 32 * 1024 * 1024 });
  readonly #controllers = new Set<AbortController>();
  readonly #cache = new Map<string, State>();
  readonly #faulted = new Set<string>();
  readonly #released = new Set<string>();
  readonly #samples: RequestSample[] = [];
  readonly #histogram = monitorEventLoopDelay({ resolution: 20 });
  #sequence = 0;
  #wsConnections = 0;
  #inflight = 0;
  #maximumInflight = 0;
  #receivedBytes = 0;
  #sentBytes = 0;
  #completed = 0;
  #aborted = 0;
  #errors = 0;
  #frames = 0;
  #rejected = 0;
  #sampled = 0;
  #maximumRss = 0;
  #rejectWebsockets = false;
  readonly #started = Date.now();
  constructor(
    readonly options: {
      model: string;
      apiKey: string;
      host?: string;
      port?: number;
      allowNetwork?: boolean;
      maxRequestBytes?: number;
      cacheEntries?: number;
      sampleEntries?: number;
    },
  ) {
    if (!/^pma-benchmark-[a-z0-9-]+$/.test(options.model) || options.apiKey.length < 16)
      throw new Error("Use an owned benchmark model and credential");
    const host = options.host ?? "127.0.0.1";
    if (!["127.0.0.1", "::1", "localhost"].includes(host) && !options.allowNetwork)
      throw new Error("Network binding requires an explicit owned fixture");
    this.#http = createServer((req, res) => {
      void this.#handle(req, res).catch(() => {
        if (res.headersSent) res.destroy();
        else
          this.#json(
            res,
            400,
            errorFrame("invalid_benchmark_request", "Invalid benchmark request"),
          );
      });
    });
    this.#http.on("upgrade", (req, socket, head) => {
      if (!this.#authorized(req) || !this.#route(req.url)) {
        socket.end("HTTP/1.1 401 Unauthorized\r\nConnection: close\r\n\r\n");
        return;
      }
      if (this.#rejectWebsockets) {
        socket.end(
          "HTTP/1.1 426 Upgrade Required\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        );
        return;
      }
      this.#ws.handleUpgrade(req, socket, head, (ws) => this.#ws.emit("connection", ws, req));
    });
    this.#ws.on("connection", (ws) => {
      this.#wsConnections++;
      const lifetime = new AbortController();
      let busy = false;
      ws.once("close", () => lifetime.abort(new Error("fixture peer closed")));
      ws.on("error", () => lifetime.abort(new Error("fixture peer failed")));
      ws.on("message", (raw) => {
        if (busy) {
          ws.send(JSON.stringify(errorFrame("busy", "One response per connection", 409)));
          return;
        }
        let input: ResponseInput;
        try {
          const v = JSON.parse(raw.toString());
          input = v.response ?? v;
        } catch {
          ws.send(JSON.stringify(errorFrame("invalid_json", "Invalid JSON")));
          return;
        }
        busy = true;
        const bytes = Array.isArray(raw)
          ? raw.reduce((n, b) => n + b.byteLength, 0)
          : raw.byteLength;
        void this.#respond(
          input,
          bytes,
          "websocket",
          lifetime.signal,
          async (frame) => {
            if (ws.readyState !== WebSocket.OPEN) throw new Error("fixture websocket closed");
            const data = JSON.stringify(frame);
            this.#sentBytes += Buffer.byteLength(data);
            await new Promise<void>((resolve, reject) =>
              ws.send(data, (error) => (error ? reject(error) : resolve())),
            );
          },
          () => ws.terminate(),
        )
          .catch(() => {
            if (ws.readyState === WebSocket.OPEN)
              ws.send(JSON.stringify(errorFrame("fixture_error", "Fixture request failed")));
          })
          .finally(() => {
            busy = false;
          });
      });
    });
  }
  #authorized(req: IncomingMessage) {
    const a = Buffer.from(req.headers.authorization ?? ""),
      b = Buffer.from(`Bearer ${this.options.apiKey}`);
    return a.length === b.length && timingSafeEqual(a, b);
  }
  #route(path: string | undefined) {
    return [
      "/responses",
      "/v1/responses",
      "/codex/responses",
      "/backend-api/codex/responses",
    ].includes(path ?? "");
  }
  #json(res: ServerResponse, status: number, body: unknown) {
    res.writeHead(status, { "content-type": "application/json" });
    res.end(JSON.stringify(body));
  }
  async #read(req: IncomingMessage) {
    let size = 0;
    const chunks: Buffer[] = [];
    for await (const c of req) {
      size += c.length;
      if (size > (this.options.maxRequestBytes ?? 32 * 1024 * 1024))
        throw new Error("Fixture request exceeds budget");
      chunks.push(c);
    }
    const raw = Buffer.concat(chunks);
    const decoded =
      req.headers["content-encoding"] === "zstd"
        ? zstdDecompressSync(raw, {
            maxOutputLength: this.options.maxRequestBytes ?? 32 * 1024 * 1024,
          })
        : raw;
    return { body: JSON.parse(decoded.toString()), bytes: size };
  }
  async #handle(req: IncomingMessage, res: ServerResponse) {
    if (!this.#authorized(req)) {
      this.#rejected++;
      this.#json(res, 401, errorFrame("invalid_api_key", "Owned fixture credential required", 401));
      return;
    }
    if (req.url?.split("?")[0] === "/_fixture/metrics" && req.method === "GET") {
      const data = this.metrics(),
        { samples, ...summary } = data;
      this.#json(res, 200, req.url.includes("summary=1") ? summary : data);
      return;
    }
    if (req.url === "/_fixture/release" && req.method === "POST") {
      const { body } = await this.#read(req);
      this.#released.add(String(body.id));
      this.#json(res, 200, { released: true });
      return;
    }
    if (req.url === "/_fixture/transport" && req.method === "POST") {
      const { body } = await this.#read(req);
      this.#rejectWebsockets = body.rejectWebsockets === true;
      if (body.forgetResponses === true) this.#cache.clear();
      this.#json(res, 200, { configured: true });
      return;
    }
    if (req.url === "/v1/models" && req.method === "GET") {
      this.#json(res, 200, {
        object: "list",
        data: [{ id: this.options.model, object: "model", owned_by: "owned-benchmark" }],
      });
      return;
    }
    if (!this.#route(req.url) || req.method !== "POST") {
      this.#json(res, 404, errorFrame("not_found", "Fixture route not found", 404));
      return;
    }
    const { body, bytes } = await this.#read(req),
      controller = new AbortController();
    const abort = () => {
      if (!res.writableEnded) controller.abort(new Error("fixture caller disconnected"));
    };
    res.once("close", abort);
    let json: unknown;
    try {
      await this.#respond(
        body,
        bytes,
        "http",
        controller.signal,
        async (frame) => {
          if (frame.type === "error" && !res.headersSent) {
            this.#json(res, Number(frame.status ?? 400), frame);
            return;
          }
          if (body.stream === false) {
            if (frame.type === "response.completed") json = frame.response;
            return;
          }
          if (!res.headersSent)
            res.writeHead(200, {
              "content-type": "text/event-stream",
              "cache-control": "no-store",
            });
          const chunk = `event: ${frame.type}\ndata: ${JSON.stringify(frame)}\n\n`;
          this.#sentBytes += Buffer.byteLength(chunk);
          if (!res.write(chunk))
            await new Promise<void>((resolve, reject) => {
              const done = () => {
                res.off("drain", drain);
                res.off("close", closed);
              };
              const drain = () => {
                  done();
                  resolve();
                },
                closed = () => {
                  done();
                  reject(new Error("fixture backpressure closed"));
                };
              res.once("drain", drain);
              res.once("close", closed);
            });
        },
        () => res.destroy(),
      );
      if (!res.destroyed && !res.writableEnded) {
        if (body.stream === false) this.#json(res, 200, json);
        else res.end();
      }
    } finally {
      res.off("close", abort);
    }
  }
  #resolve(input: ResponseInput): State {
    if (input.model !== this.options.model || !Array.isArray(input.input))
      throw new Error("Only the owned model is supported");
    const previous = input.previous_response_id
      ? this.#cache.get(input.previous_response_id)
      : undefined;
    if (input.previous_response_id && !previous) throw new Error("previous_response_not_found");
    let state: State | undefined = previous
      ? { plan: { ...previous.plan }, done: new Set(previous.done) }
      : undefined;
    for (const item of input.input) {
      const v = item as { role?: string; content?: unknown };
      if (v?.role !== "user") continue;
      const match = textOf(v.content).match(/PMA_BENCH_V1:(\{[^{}\n]*\})/);
      if (match) {
        try {
          const plan = parseBenchmarkPlan(JSON.parse(match[1]!));
          if (state?.plan.id !== plan.id) state = { plan, done: new Set() };
        } catch (error) {
          if (input.tools?.length) throw error;
        }
      }
    }
    if (!state && !input.tools?.length && JSON.stringify(input.input).includes(BENCHMARK_MARKER))
      return {
        plan: { id: `compact_${this.#sequence}`, kind: "text", outputBytes: 256 },
        done: new Set(),
      };
    if (
      !state &&
      input.tools?.some((t) => t.name === "catalog_probe") &&
      JSON.stringify(input.input).includes("Call catalog_probe once")
    )
      state = { plan: { id: "catalog_probe", kind: "probe", outputBytes: 16 }, done: new Set() };
    if (!state) throw new Error("Benchmark plan marker required");
    for (const item of input.input) {
      const v = item as { type?: string; call_id?: string };
      if (v.type !== "function_call_output" || !v.call_id) continue;
      const prefix = `call_pma_${state.plan.id}_`;
      if (v.call_id.startsWith(prefix)) {
        const n = Number(v.call_id.slice(prefix.length).split("|")[0]);
        if (Number.isSafeInteger(n)) state.done.add(n);
      }
    }
    return state;
  }
  async #respond(
    input: ResponseInput,
    bytes: number,
    transport: RequestSample["transport"],
    signal: AbortSignal,
    write: (frame: Frame) => Promise<void>,
    disconnect: () => void,
  ) {
    const controller = new AbortController(),
      active = AbortSignal.any([signal, controller.signal]);
    this.#controllers.add(controller);
    this.#inflight++;
    this.#maximumInflight = Math.max(this.#maximumInflight, this.#inflight);
    this.#receivedBytes += bytes;
    this.#maximumRss = Math.max(this.#maximumRss, process.memoryUsage().rss);
    const responseId = `resp_pma_${++this.#sequence}_${randomUUID().slice(0, 8)}`;
    const images = imagesIn(input.input);
    const sample: RequestSample = {
      planId: "",
      responseId,
      transport,
      bytes,
      inputItems: input.input?.length ?? 0,
      imageCount: images.count,
      imageDataUrlCharacters: images.characters,
      incremental: !!input.previous_response_id,
      startedAt: Date.now(),
      outcome: "pending",
      frames: 0,
    };
    let seq = 0;
    const send = async (frame: Frame) => {
      active.throwIfAborted();
      sample.firstFrameAt ??= Date.now();
      sample.frames++;
      this.#frames++;
      await write({ ...frame, sequence_number: seq++ });
    };
    try {
      let state: State;
      try {
        state = this.#resolve(input);
      } catch (e) {
        this.#rejected++;
        sample.outcome = "rejected";
        const missing = e instanceof Error && e.message === "previous_response_not_found";
        await send(
          errorFrame(
            missing ? "previous_response_not_found" : "invalid_benchmark_request",
            missing
              ? "Original synthetic response expired"
              : "An owned model and plan are required",
          ),
        );
        return;
      }
      const plan = state.plan;
      sample.planId = plan.id;
      if (plan.fault === "rate_limit_once" && !this.#faulted.has(plan.id)) {
        this.#faulted.add(plan.id);
        sample.outcome = "rate_limited";
        this.#errors++;
        await send(errorFrame("rate_limit_exceeded", "Synthetic once-only rate limit", 429));
        return;
      }
      if (plan.fault === "hold")
        while (!this.#released.has(plan.id)) await delay(25, undefined, { signal: active });
      await delay(plan.firstByteMs ?? 0, undefined, { signal: active });
      await send({
        type: "response.created",
        response: { id: responseId, model: input.model, status: "in_progress", output: [] },
      });
      const offered = new Set(
        input.tools?.filter((t) => t.type === "function").map((t) => t.name) ?? [],
      );
      const outputs: Frame[] = [];
      const text = async (value: string, phase = "final_answer") => {
        const index = outputs.length,
          id = `msg_${responseId}_${index}`;
        await send({
          type: "response.output_item.added",
          output_index: index,
          item: {
            id,
            type: "message",
            role: "assistant",
            status: "in_progress",
            phase,
            content: [],
          },
        });
        await send({
          type: "response.content_part.added",
          item_id: id,
          output_index: index,
          content_index: 0,
          part: { type: "output_text", text: "", annotations: [] },
        });
        const chunk = Math.max(1, plan.chunkBytes ?? 256);
        for (let i = 0; i < value.length; i += chunk) {
          sample.firstTextAt ??= Date.now();
          await send({
            type: "response.output_text.delta",
            item_id: id,
            output_index: index,
            content_index: 0,
            delta: value.slice(i, i + chunk),
          });
          if (plan.fault === "disconnect_once" && !this.#faulted.has(plan.id)) {
            this.#faulted.add(plan.id);
            disconnect();
            throw new Error("Synthetic disconnect");
          }
          if (plan.chunkMs) await delay(plan.chunkMs, undefined, { signal: active });
          else await immediate(undefined, { signal: active });
        }
        const part = { type: "output_text", text: value, annotations: [] };
        await send({
          type: "response.output_text.done",
          item_id: id,
          output_index: index,
          content_index: 0,
          text: value,
        });
        await send({
          type: "response.content_part.done",
          item_id: id,
          output_index: index,
          content_index: 0,
          part,
        });
        const item = {
          id,
          type: "message",
          role: "assistant",
          status: "completed",
          phase,
          content: [part],
        };
        outputs.push(item);
        await send({ type: "response.output_item.done", output_index: index, item });
      };
      const tool = async (name: string, args: Record<string, unknown>, number: number) => {
        if (!offered.has(name)) throw new Error("Requested benchmark tool was not offered");
        const index = outputs.length,
          id = `fc_${responseId}_${number}`,
          call_id = `call_pma_${plan.id}_${number}`,
          argumentsText = JSON.stringify(args);
        await send({
          type: "response.output_item.added",
          output_index: index,
          item: { id, type: "function_call", call_id, name, arguments: "", status: "in_progress" },
        });
        for (let i = 0; i < argumentsText.length; i += 64) {
          await send({
            type: "response.function_call_arguments.delta",
            output_index: index,
            item_id: id,
            delta: argumentsText.slice(i, i + 64),
          });
          await immediate(undefined, { signal: active });
        }
        await send({
          type: "response.function_call_arguments.done",
          output_index: index,
          item_id: id,
          arguments: argumentsText,
        });
        const item = {
          id,
          type: "function_call",
          call_id,
          name,
          arguments: argumentsText,
          status: "completed",
        };
        outputs.push(item);
        await send({ type: "response.output_item.done", output_index: index, item });
      };
      const rounds =
        plan.kind === "probe"
          ? 1
          : plan.kind === "children"
            ? (plan.children ?? 2)
            : (plan.rounds ?? 3);
      const remaining = Array.from({ length: rounds }, (_, i) => i).filter(
        (i) => !state.done.has(i),
      );
      if (plan.kind !== "text" && remaining.length && offered.size) {
        await text(`PMA_BENCH ${plan.id} tool ${remaining[0]}\n`, "commentary");
        if (plan.kind === "children")
          for (const n of remaining)
            await tool(
              "subagent",
              {
                ...(input.tools
                  ?.find((t) => t.name === "subagent")
                  ?.parameters?.required?.includes("name")
                  ? { name: "child-" + n }
                  : {}),
                task: benchmarkPrompt({
                  id: `${plan.id}_c${n}`,
                  kind: "text",
                  firstByteMs: plan.firstByteMs ?? 0,
                  chunkMs: plan.chunkMs ?? 0,
                  outputBytes: plan.outputBytes ?? 1024,
                }),
                context: "fresh",
                workspace: null,
              },
              n,
            );
        else {
          const n = remaining[0]!;
          if (plan.kind === "probe") await tool("catalog_probe", {}, n);
          else if (plan.kind === "mcp")
            await tool(
              plan.toolName!,
              { text: `PMA_TOOL ${plan.id} ${n} ` + "r".repeat(plan.toolResultBytes ?? 1024) },
              n,
            );
          else if (n % 2 === 1) await tool("read", { path: "/etc/os-release", limit: 40 }, n);
          else
            await tool(
              "bash",
              {
                command: `sleep ${(plan.toolDelayMs ?? 0) / 1000}; printf 'PMA_TOOL ${plan.id} ${n}\\n'`,
                timeout: 30,
              },
              n,
            );
        }
      } else {
        const prefix = plan.kind === "probe" ? "CATALOG_ROUTE_OK" : `PMA_DONE ${plan.id}\n`;
        await text(prefix + "x".repeat(Math.max(0, (plan.outputBytes ?? 2048) - prefix.length)));
      }
      const usage = {
        input_tokens: Math.max(
          1,
          Math.ceil(Math.max(0, bytes - images.characters) / 4) + images.count * 1200,
        ),
        output_tokens: Math.max(1, Math.ceil(JSON.stringify(outputs).length / 4)),
        input_tokens_details: { cached_tokens: 0 },
        output_tokens_details: { reasoning_tokens: 0 },
      };
      this.#cache.set(responseId, state);
      while (this.#cache.size > (this.options.cacheEntries ?? 10000))
        this.#cache.delete(this.#cache.keys().next().value!);
      await send({
        type: "response.completed",
        response: {
          id: responseId,
          model: input.model,
          status: "completed",
          created_at: Math.floor(Date.now() / 1000),
          output: outputs,
          usage: { ...usage, total_tokens: usage.input_tokens + usage.output_tokens },
        },
      });
      this.#completed++;
      sample.outcome = "completed";
    } catch (e) {
      if (active.aborted) {
        this.#aborted++;
        sample.outcome = "aborted";
      } else {
        this.#errors++;
        sample.outcome = "error";
        throw e;
      }
    } finally {
      sample.finishedAt = Date.now();
      this.#sampled++;
      this.#samples.push(sample);
      if (this.#samples.length > (this.options.sampleEntries ?? 10000)) this.#samples.shift();
      this.#inflight--;
      this.#controllers.delete(controller);
      this.#maximumRss = Math.max(this.#maximumRss, process.memoryUsage().rss);
    }
  }
  metrics() {
    return {
      startedAt: this.#started,
      uptimeSeconds: process.uptime(),
      cpu: process.cpuUsage(),
      memory: process.memoryUsage(),
      maximumRss: this.#maximumRss,
      eventLoopP95Ms: this.#histogram.percentile(95) / 1e6,
      inflight: this.#inflight,
      maximumInflight: this.#maximumInflight,
      completed: this.#completed,
      aborted: this.#aborted,
      errors: this.#errors,
      rejected: this.#rejected,
      receivedBytes: this.#receivedBytes,
      sentBytes: this.#sentBytes,
      frames: this.#frames,
      websocketConnections: this.#wsConnections,
      cacheEntries: this.#cache.size,
      sampled: this.#sampled,
      samples: [...this.#samples],
    };
  }
  get origin() {
    const a = this.#http.address();
    if (!a || typeof a === "string") throw new Error("Fixture not started");
    return `http://127.0.0.1:${a.port}`;
  }
  async start() {
    await new Promise<void>((resolve, reject) => {
      this.#http.once("error", reject);
      this.#http.listen(this.options.port ?? 0, this.options.host ?? "127.0.0.1", resolve);
    });
    this.#histogram.enable();
  }
  async stop() {
    this.#histogram.disable();
    for (const c of this.#controllers) c.abort(new Error("Fixture stopped"));
    for (const ws of this.#ws.clients) ws.terminate();
    this.#http.closeAllConnections();
    await new Promise<void>((resolve) => this.#http.close(() => resolve()));
    this.#cache.clear();
  }
}
