import { createHash, timingSafeEqual } from "node:crypto";
import type { FastifyInstance } from "fastify";
import type { Kysely } from "kysely";
import { withDatabaseReadSignal, type Database } from "@pi-cloud/database";
import {
  parseUuidPathParameter,
  NativeInputConfirmationSchema,
  type NativeInputConfirmation,
} from "@pi-cloud/protocol";
import { Value } from "typebox/value";
import type { SessionEventHub } from "@pi-cloud/runtime-core/session-event-hub";
import {
  INPUT_CONFIRMATIONS_PATH,
  INPUT_CONFIRMATION_WAIT_MS,
} from "./input-confirmation-client.ts";
import {
  INTERNAL_SESSION_EVENTS_PATH,
  SessionEventStream,
} from "@pi-cloud/runtime-core/session-event-stream";
import { ConversationReader } from "./conversation-reader.ts";

/** A read-only stream from the single projection owner. The public API has
 * already authenticated the tenant; no browser credential crosses this port. */
export class SessionLiveService {
  constructor(
    readonly options: {
      database: Kysely<Database>;
      stream: SessionEventStream;
      rootMessageHistory?: boolean;
      token: string;
      inputConfirmations?: Pick<SessionEventHub, "watchInput">;
    },
  ) {}

  install(server: FastifyInstance): void {
    const hash = (value: string) => createHash("sha256").update(value).digest();
    const expected = hash(`Bearer ${this.options.token}`);
    server.post(INPUT_CONFIRMATIONS_PATH, async (request, reply) => {
      if (!timingSafeEqual(expected, hash(request.headers.authorization ?? "")))
        return reply.code(401).send();
      if (!Value.Check(NativeInputConfirmationSchema, request.body)) return reply.code(400).send();
      const confirmation = request.body as NativeInputConfirmation;
      const controller = new AbortController();
      const disconnected = () => controller.abort();
      reply.raw.once("close", disconnected);
      let watch: ReturnType<SessionEventHub["watchInput"]> | undefined;
      let timer: NodeJS.Timeout | undefined;
      try {
        const owner = await this.options.stream.owner(
          confirmation.tenantId,
          confirmation.sessionId,
        );
        if (owner)
          return reply
            .code(307)
            .header("location", new URL(INPUT_CONFIRMATIONS_PATH, owner).toString())
            .send();
        if (!this.options.inputConfirmations) return reply.code(503).send();
        watch = this.options.inputConfirmations.watchInput(confirmation);
        controller.signal.addEventListener("abort", () => watch?.close(), { once: true });
        if (controller.signal.aborted) {
          watch.close();
          return;
        }
        timer = setTimeout(() => watch?.close(), INPUT_CONFIRMATION_WAIT_MS);
        timer.unref();
        reply.hijack();
        reply.raw.writeHead(200, {
          "content-type": "text/event-stream",
          "cache-control": "no-cache, no-transform",
          "x-accel-buffering": "no",
        });
        reply.raw.write("event: ready\ndata: {}\n\n");
        reply.raw.flushHeaders();
        if (await watch.result)
          if (!reply.raw.destroyed)
            reply.raw.write(`event: applied\ndata: ${JSON.stringify(confirmation)}\n\n`);
        if (!reply.raw.destroyed && !reply.raw.writableEnded) reply.raw.end();
      } catch {
        if (reply.raw.headersSent) reply.raw.destroy();
        else if (!reply.raw.destroyed) await reply.code(503).send();
      } finally {
        clearTimeout(timer);
        watch?.close();
        reply.raw.off("close", disconnected);
        controller.abort();
      }
    });
    server.get(`${INTERNAL_SESSION_EVENTS_PATH}/:tenantId/:sessionId`, async (request, reply) => {
      if (!timingSafeEqual(expected, hash(request.headers.authorization ?? "")))
        return reply.code(401).send();
      let tenantId: string, sessionId: string;
      try {
        const parameters = request.params as { tenantId: unknown; sessionId: unknown };
        tenantId = parseUuidPathParameter(parameters.tenantId, "tenantId");
        sessionId = parseUuidPathParameter(parameters.sessionId, "sessionId");
      } catch {
        return reply.code(400).send();
      }
      const abort = new AbortController();
      const close = () => abort.abort();
      reply.raw.once("close", close);
      let stream: Awaited<ReturnType<SessionEventStream["open"]>> | undefined;
      try {
        const owner = await this.options.stream.owner(tenantId, sessionId);
        if (owner)
          return reply
            .code(307)
            .header(
              "location",
              new URL(`${INTERNAL_SESSION_EVENTS_PATH}/${tenantId}/${sessionId}`, owner).toString(),
            )
            .send();
        stream = await withDatabaseReadSignal(abort.signal, () =>
          this.options.stream.open({
            tenantId,
            sessionId,
            loadCanonical: () =>
              new ConversationReader(
                this.options.database,
                tenantId,
                this.options.rootMessageHistory,
              ).getConversationView(sessionId),
          }),
        );
        if (abort.signal.aborted) {
          stream.close();
          return;
        }
        reply.hijack();
        reply.raw.writeHead(200, {
          "content-type": "text/event-stream; charset=utf-8",
          "cache-control": "no-cache, no-transform",
          "x-accel-buffering": "no",
        });
        reply.raw.flushHeaders();
        await stream.pipe(reply.raw);
        if (!reply.raw.destroyed && !reply.raw.writableEnded) reply.raw.end();
      } catch {
        if (reply.raw.headersSent) reply.raw.destroy();
        else if (!reply.raw.destroyed) await reply.code(503).send();
      } finally {
        stream?.close();
        reply.raw.off("close", close);
        abort.abort();
      }
    });
  }
}
