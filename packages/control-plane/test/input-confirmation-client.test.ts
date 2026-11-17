import Fastify from "fastify";
import { afterEach, expect, it } from "vitest";
import type { NativeInputConfirmation } from "@pi-cloud/protocol";
import { SessionEventHub } from "@pi-cloud/runtime-core/session-event-hub";
import type { SessionEventStream } from "@pi-cloud/runtime-core/session-event-stream";
import type { Database } from "@pi-cloud/database";
import type { Kysely } from "kysely";
import {
  HttpInputConfirmations,
  INPUT_CONFIRMATIONS_PATH,
} from "../src/input-confirmation-client.ts";
import { SessionLiveService } from "../src/session-live-service.ts";

const expected: NativeInputConfirmation = {
  tenantId: "00000000-0000-4000-8000-000000000001",
  sessionId: "00000000-0000-4000-8000-000000000002",
  runId: "00000000-0000-4000-8000-000000000003",
  action: "submit",
  operationId: `input-${"a".repeat(64)}`,
  fingerprint: "b".repeat(64),
};
const token = "t".repeat(48);
const cleanup: (() => Promise<unknown> | void)[] = [];
afterEach(async () => {
  for (const close of cleanup.splice(0).reverse()) await close();
});

async function fixture(owner?: string) {
  const server = Fastify();
  const hub = new SessionEventHub();
  new SessionLiveService({
    database: {} as Kysely<Database>,
    token,
    stream: { owner: async () => owner } as unknown as SessionEventStream,
    inputConfirmations: hub,
  }).install(server);
  const baseUrl = await server.listen({ port: 0, host: "127.0.0.1" });
  cleanup.push(
    () => server.close(),
    () => hub.onApplicationShutdown(),
  );
  return {
    server,
    hub,
    baseUrl,
    client: new HttpInputConfirmations({ baseUrl, token, waitMs: 1500 }),
  };
}

it("registers before returning and confirms a fast consumed operation without history queries", async () => {
  const f = await fixture();
  const wait = await f.client.open(expected);
  cleanup.push(wait.close);
  f.hub.confirmInput(expected);
  expect(await wait.result).toBe(true);
});

it("keeps concurrent identities and fingerprints isolated", async () => {
  const f = await fixture();
  const first = await f.client.open(expected);
  const other = { ...expected, fingerprint: "c".repeat(64) };
  const second = await f.client.open(other);
  cleanup.push(first.close, second.close);
  f.hub.confirmInput(expected);
  expect(await first.result).toBe(true);
  second.close();
  expect(await second.result).toBe(false);
});

it("makes assignment loss unconfirmed without fabricating rejection or acceptance", async () => {
  const f = await fixture();
  const wait = await f.client.open(expected);
  cleanup.push(wait.close);
  f.hub.resyncAll();
  expect(await wait.result).toBe(false);
  f.hub.confirmInput(expected);
  const retry = await f.client.open(expected);
  cleanup.push(retry.close);
  retry.close();
  expect(await retry.result).toBe(false);
});

it("routes the same exact operation to its current Projector owner", async () => {
  const owner = await fixture();
  const router = await fixture(owner.baseUrl);
  const wait = await router.client.open(expected);
  cleanup.push(wait.close);
  owner.hub.confirmInput(expected);
  expect(await wait.result).toBe(true);
});

it("expires a missing notification so the caller can reconcile its durable receipt", async () => {
  const f = await fixture();
  const client = new HttpInputConfirmations({ baseUrl: f.baseUrl, token, waitMs: 50 });
  const wait = await client.open(expected);
  cleanup.push(wait.close);
  expect(await wait.result).toBe(false);
});

it("does not open a private confirmation for an unauthenticated caller", async () => {
  const f = await fixture();
  expect(
    (await f.server.inject({ method: "POST", url: INPUT_CONFIRMATIONS_PATH, payload: expected }))
      .statusCode,
  ).toBe(401);
});
