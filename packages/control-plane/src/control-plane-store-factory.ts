import type { Database } from "@pi-cloud/database";
import type { Kysely } from "kysely";
import { ControlPlaneStore } from "./control-plane-store.ts";
import type { TenantRequestIdentity } from "./tenant-identity.ts";
import type { PiCloudMetrics } from "@pi-cloud/observability";

export class ControlPlaneStoreFactory {
  readonly #delegatedRunHistory: boolean;
  readonly #database: Kysely<Database>;
  readonly #conversationReadDatabase: Kysely<Database>;
  readonly #rootMessageHistory: boolean;
  readonly #idGenerator: (() => string) | undefined;
  readonly #environmentImageRevision: string | undefined;
  readonly #metrics: PiCloudMetrics | undefined;

  constructor(options: {
    database: Kysely<Database>;
    conversationReadDatabase?: Kysely<Database>;
    rootMessageHistory?: boolean;
    delegatedRunHistory?: boolean;
    idGenerator?: () => string;
    environmentImageRevision?: string;
    metrics?: PiCloudMetrics;
  }) {
    this.#delegatedRunHistory = options.delegatedRunHistory ?? false;
    this.#database = options.database;
    this.#conversationReadDatabase = options.conversationReadDatabase ?? options.database;
    this.#rootMessageHistory = options.rootMessageHistory ?? false;
    this.#idGenerator = options.idGenerator;
    this.#environmentImageRevision = options.environmentImageRevision;
    this.#metrics = options.metrics;
  }

  forIdentity(identity: TenantRequestIdentity): ControlPlaneStore {
    return new ControlPlaneStore({
      database: this.#database,
      conversationReadDatabase: this.#conversationReadDatabase,
      tenantId: identity.tenantId,
      rootMessageHistory: this.#rootMessageHistory,
      delegatedRunHistory: this.#delegatedRunHistory,
      defaultModelProfileId: identity.defaultModelProfileId,
      ...(this.#environmentImageRevision === undefined
        ? {}
        : { environmentImageRevision: this.#environmentImageRevision }),
      ...(this.#idGenerator === undefined ? {} : { idGenerator: this.#idGenerator }),
      ...(this.#metrics === undefined ? {} : { metrics: this.#metrics }),
    });
  }
}
