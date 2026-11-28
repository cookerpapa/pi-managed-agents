import { AttachmentReaper, configuredImageAttachments } from "@pi-cloud/session-attachments";
import { ExecutionTargetService } from "../../../packages/control-plane/src/execution-target-service.ts";
import { readInputUploadsForUpdate } from "../../../packages/control-plane/src/workspace-upload-service.ts";
import { segmentSchedules } from "./schedules.mjs";
import { SessionScheduleController } from "../../../packages/control-plane/src/session-schedule-controller.ts";
import { createDatabase } from "@pi-cloud/database";
import { SessionEventHub } from "@pi-cloud/runtime-core/session-event-hub";
import { sql } from "kysely";
import { installation, openStore, required, integer, secret, schema } from "./config.mjs";
import { createControlPlaneApplication } from "../../../packages/control-plane/src/application.ts";
import { ProductionHttpGateway } from "../../../packages/control-plane/src/production-http-gateway.ts";
import {
  PostgresTenantApiAuthenticator,
  tenantRequestIdentity,
} from "../../../packages/control-plane/src/tenant-identity.ts";
import { segmentHistoryTransactions } from "../../../packages/control-plane/src/segment-history-transactions.ts";
import { ControlPlaneStoreError } from "../../../packages/control-plane/src/control-plane-store-error.ts";
import { ForkStorage } from "../../experiments/minimal-session-storage/fork-storage.mjs";
import { SegmentInputs } from "../../experiments/minimal-session-storage/segment-inputs.mjs";
import { installSegmentInputApi } from "../../experiments/minimal-session-storage/segment-input-api.mjs";
import { WebAuthenticationService } from "../../../packages/control-plane/src/web-authentication.ts";

const pendingReads = new Set([
  "/v1/runs/:id",
  "/v1/conversations/:id/compact",
  "/v1/conversations/:id/compact/:id",
]);
const supportedWrites = new Set([
  "/v1/auth/login",
  "/v1/auth/logout",
  "/v1/projects",
  "/v1/projects/:id/sessions",
  "/v1/sessions/:id/turns",
  "/v1/sessions/:id/turns/:id/cancellations",
  "/v1/sessions/:id/queued-inputs/:id/actions",
  "/v1/sessions/:id/returned-inputs/acknowledgements",
  "/v1/sessions/:id/model",
  "/v1/conversations/:id/workspace",
  "/v1/conversations/:id/schedules/:id",
  "/v1/conversations/:id/forks",
  "/v1/conversations/:id/prunes",
  "/v1/conversations/:id",
]);
// The first isolated entry admits only operations whose writes have a segmented owner.
export async function startIndependentSessionApi() {
  const config = await installation(),
    pool = await openStore(config),
    store = new ForkStorage(pool);
  const db = createDatabase({ connectionString: config.url, maxConnections: 8 }),
    reads = createDatabase({ connectionString: config.read, maxConnections: 5 });
  const inputs = (store.inputs = new SegmentInputs(store.db));
  const schedules = segmentSchedules(reads, inputs);
  const timer = new SessionScheduleController({
    service: schedules,
    notificationConnectionString: config.url,
  });
  let app, images, reaper;
  try {
    images = await configuredImageAttachments(db, reads, undefined, schema);
    const auth = new WebAuthenticationService({
      database: db,
      enabled: false,
      maximumTenants: null,
      tenantQuotas: { maximumProjects: null, maximumSessions: null },
    });
    const hub = new SessionEventHub();
    app = await createControlPlaneApplication({
      database: db,
      schedules,
      executionTargets: new ExecutionTargetService({ database: reads, independentTurns: true }),
      conversationReadDatabase: reads,
      rootMessageHistory: true,
      flowRootHistory: true,
      registerSessionHistory: async (tx, tenant, id) => {
        const {
          rows: [old],
        } = await sql`select current_setting('search_path') as path`.execute(tx);
        await sql`select set_config('search_path',${schema + ",public"},true)`.execute(tx);
        await store.registerIn(tx, tenant, [id]);
        await sql`select set_config('search_path',${old.path},true)`.execute(tx);
      },
      messageHistoryMutations: segmentHistoryTransactions(schema, true),
      webAuthentication: auth,
      eventRuntime: {
        eventHub: hub,
        eventStore: {
          snapshot() {
            throw Error("Use the Projector live service");
          },
        },
      },
      sessionEventStreamOptions: {
        remote: {
          baseUrl: required("PI_CLOUD_SESSION_RUNTIME_URL"),
          token: await secret("PI_CLOUD_SUPERVISOR_MANAGEMENT_TOKEN"),
        },
      },
      productionHttpGateway: new ProductionHttpGateway({
        authenticator: new PostgresTenantApiAuthenticator({ database: db }),
        webSessionAuthenticator: auth,
        readiness: () => true,
      }),
      mountSessionInputs(server) {
        server.addHook("preHandler", async (req, res) => {
          const route = req.routeOptions.url?.replaceAll(/:[A-Za-z][A-Za-z0-9]*/g, ":id");
          const read = ["GET", "HEAD", "OPTIONS"].includes(req.method);
          if (read ? pendingReads.has(route) : !supportedWrites.has(route))
            return res.code(501).send({
              error: {
                code: "unsupported_operation",
                message: "This operation is outside the independent Session deployment.",
              },
            });
        });
        installSegmentInputApi(server, inputs, async (req, session) => {
          const identity = tenantRequestIdentity(req);
          if (!identity || (req.method !== "GET" && identity.role === "viewer")) return;
          const canAddressChild =
            req.method === "GET" ||
            (req.method === "POST" &&
              req.routeOptions.url === "/v1/sessions/:session/turns/:turnId/cancellations");
          const {
            rows: [s],
          } =
            await sql`select s.id from sessions s join public.sessions p on p.tenant_id=s.tenant_id and p.id=s.id
            where s.tenant_id=${identity.tenantId}::uuid and s.id=${session}::uuid and p.pruned_at is null and (s.parent_session_id is null or ${canAddressChild}::boolean)`.execute(
              store.db,
            );
          if (!s) return;
          return {
            tenant: identity.tenantId,
            sender: identity.userId,
            resolveUploads: async (ids, tx, workspace) => {
              if (!workspace)
                throw new ControlPlaneStoreError("conflict", "Session workspace is unavailable");
              const uploads = await readInputUploadsForUpdate(tx, {
                tenantId: identity.tenantId,
                sessionId: session,
                workspaceId: workspace.workspaceId,
                workingDirectory: workspace.cwd,
                ids,
              });
              if (uploads.some((f) => f.image))
                throw new ControlPlaneStoreError(
                  "invalid_request",
                  "Image input preparation is unavailable in this installation",
                );
              return uploads;
            },
          };
        });
      },
    });
    await app.listen(integer("PORT", 3000), process.env.HOST ?? "127.0.0.1");
    timer.start();
    if (images)
      reaper = new AttachmentReaper(images.store, () =>
        console.error("session.attachment_cleanup_failed"),
      );
    return {
      close: async () => {
        await timer.close();
        await reaper?.close();
        await images?.blobs.close();
        await app.close();
        await Promise.all([db.destroy(), reads.destroy(), store.db.destroy()]);
      },
    };
  } catch (e) {
    await timer.close();
    await reaper?.close();
    await images?.blobs.close();
    await app?.close();
    await Promise.all([db.destroy(), reads.destroy(), store.db.destroy()]);
    throw e;
  }
}
