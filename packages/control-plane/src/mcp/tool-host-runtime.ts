import { sql, type Kysely } from "kysely";
import { PostgresNotificationWake, type Database } from "@pi-cloud/database";
import {
  TOOL_REPLY_DELIVERY_MARGIN_MS,
  type McpConnectionTool,
  type ToolCatalogPublication,
} from "@pi-cloud/protocol";
import type { AcceptedControlToolCommand } from "@pi-cloud/runtime-core/accepted-fact";
import { controlToolCatalog, normalizeCompletedControlResult } from "../control-tool-registry.ts";
import type { ControlToolOutcome } from "../control-tool-dispatcher.ts";
import { ToolCatalogAuthority } from "../tool-catalog-authority.ts";
import { HttpMcpConnection, mcpFailureCode } from "./http-client.ts";
import type { McpCredentialVault } from "./credential-vault.ts";
import type { McpConnectionRow } from "./connection-service.ts";

export class McpInvocationRefused extends Error {
  constructor() {
    super("MCP connection is not accepting this invocation");
  }
}
type ClientSlot = { revision: string; connection: HttpMcpConnection; connected: Promise<void> };

/** Connection/catalog reconciliation only. Session admission remains in Projector. */
export class McpToolHostRuntime {
  readonly #authority: ToolCatalogAuthority;
  readonly #wake: PostgresNotificationWake;
  readonly #abort = new AbortController();
  readonly #clients = new Map<string, ClientSlot>();
  readonly #allClients = new Set<HttpMcpConnection>();
  readonly #dirty = new Set<string>();
  readonly #published = new Map<string, string>();
  #running: Promise<void> | undefined;
  #tail: Promise<unknown> = Promise.resolve();
  constructor(
    readonly options: {
      database: Kysely<Database>;
      databaseUrl: string;
      releaseId: string;
      vault?: McpCredentialVault;
      fetch: typeof globalThis.fetch;
      publish(snapshot: ToolCatalogPublication): Promise<void>;
      report?(event: string): void;
    },
  ) {
    this.#authority = new ToolCatalogAuthority(options.database);
    this.#wake = new PostgresNotificationWake(options.databaseUrl, "pi_cloud_mcp_connections");
  }
  start(): void {
    this.#running ??= this.#run();
  }
  reconcile(): Promise<void> {
    return this.#serialize(() => this.#tick());
  }
  async #run(): Promise<void> {
    while (!this.#abort.signal.aborted) {
      this.#wake.refresh();
      const observed = this.#wake.generation;
      await this.reconcile().catch(() => this.options.report?.("mcp.reconciliation_pending"));
      await this.#wake.wait(observed, 30_000, this.#abort.signal);
    }
  }
  #serialize<T>(work: () => Promise<T>): Promise<T> {
    const next = this.#tail.catch(() => {}).then(work);
    this.#tail = next;
    return next;
  }

  /** Publish stored connection definitions before the platform's new release
   * record. Same-partition ordering makes the switch complete at every consumer. */
  adoptRelease(): Promise<void> {
    return this.#serialize(async () => {
      let after: string | null = null;
      for (;;) {
        const { rows } = await sql<McpConnectionRow>`select * from mcp_connections
          where (${after}::uuid is null or id>${after}::uuid) order by id limit 64`.execute(
          this.options.database,
        );
        for (const row of rows) await this.#publish(row);
        if (rows.length < 64) break;
        after = rows.at(-1)!.id;
      }
    });
  }

  async #tick(): Promise<void> {
    const platform = await this.#authority.read();
    if (platform?.releaseId !== this.options.releaseId) return;
    const dirty = [...this.#dirty];
    const { rows } = await sql<McpConnectionRow>`select c.* from mcp_connections c
      left join tool_catalog_publication p on p.scope='mcp:'||c.id::text
      where c.checked_revision is distinct from c.revision or c.next_check_at<=clock_timestamp()
        or p.release_id is distinct from ${this.options.releaseId}
        or p.published_generation is distinct from p.generation
        or c.id=any(${dirty}::uuid[])
      order by c.next_check_at,c.id limit 64`.execute(this.options.database);
    for (let offset = 0; offset < rows.length && !this.#abort.signal.aborted; offset += 4)
      await Promise.allSettled(
        rows.slice(offset, offset + 4).map(async (row) => {
          this.#dirty.delete(row.id);
          await this.#reconcile(row).catch(() =>
            this.options.report?.("mcp.connection_refresh_pending"),
          );
        }),
      );
    if (rows.length === 64) this.#wake.notify();
  }

  async #publish(row: McpConnectionRow): Promise<void> {
    const enabled =
      row.enabled &&
      !row.deleted_at &&
      row.status === "ready" &&
      row.checked_revision === row.revision;
    const tools = enabled
      ? row.tools.filter((t) => row.allowed_tools === null || row.allowed_tools.includes(t.name))
      : [];
    const scope = `mcp:${row.id}`;
    const old = await this.#authority.read(scope);
    const snapshot = await this.#authority.select({
      scope,
      releaseId: this.options.releaseId,
      expectedGeneration: old?.generation ?? 0,
      replayable: [],
      mcp: {
        tenantId: row.tenant_id,
        ownerUserId: row.owner_user_id,
        visibility: row.visibility,
        revision: Number(row.revision),
        enabled: Boolean(enabled),
      },
      catalog: controlToolCatalog(
        tools.map((t) => ({
          name: t.platformName,
          description: t.description,
          parameters: t.parameters,
          environment: { kind: "none" },
          availability: "root",
        })),
      ),
    });
    const identity = `${snapshot.releaseId}:${snapshot.generation}`;
    if (this.#published.get(scope) !== identity) {
      await this.options.publish(snapshot);
      this.#published.set(scope, identity);
    }
  }

  async #reconcile(row: McpConnectionRow): Promise<void> {
    await this.#publish(row);
    if (!row.enabled || row.deleted_at) {
      this.#clients.get(row.id)?.connection.retire();
      this.#clients.delete(row.id);
      await sql`update mcp_connections set checked_revision=revision,checked_at=clock_timestamp(),
        next_check_at='infinity'::timestamptz,status=case when deleted_at is null then 'disabled' else 'deleted' end
        where id=${row.id}::uuid and revision=${row.revision}::bigint`.execute(
        this.options.database,
      );
      return;
    }
    let tools: McpConnectionTool[] = row.tools,
      error: string | null = null;
    try {
      tools = await (await this.#connection(row)).discover();
    } catch (e) {
      error = mcpFailureCode(e);
    }
    const { rows } =
      await sql<McpConnectionRow>`update mcp_connections set tools=${JSON.stringify(tools)}::jsonb,
      checked_revision=revision,checked_at=clock_timestamp(),last_error=${error},
      status=${error ? "error" : "ready"},next_check_at=clock_timestamp()+${error ? "120 seconds" : "60 seconds"}::interval,
      updated_at=clock_timestamp() where id=${row.id}::uuid and revision=${row.revision}::bigint
      returning *`.execute(this.options.database);
    if (rows[0]) await this.#publish(rows[0]);
  }

  async #connection(row: McpConnectionRow): Promise<HttpMcpConnection> {
    const old = this.#clients.get(row.id);
    if (old?.revision === row.revision && old.connection.client.connectionState !== "closed") {
      await old.connected;
      return old.connection;
    }
    old?.connection.retire();
    let token: string | undefined;
    if (row.credential) {
      if (!this.options.vault) throw new Error("MCP credential storage is not configured");
      token = this.options.vault.open(
        row.tenant_id,
        row.id,
        row.credential_revision,
        row.credential,
      );
    }
    const connection = new HttpMcpConnection({
      id: row.id,
      name: row.name,
      endpoint: row.endpoint,
      ...(token ? { token } : {}),
      fetch: this.options.fetch,
      changed: () => {
        this.#dirty.add(row.id);
        this.#wake.notify();
      },
    });
    this.#allClients.add(connection);
    connection.client.onClose(() => this.#allClients.delete(connection));
    const connected = connection.connect();
    const slot = { revision: row.revision, connection, connected };
    this.#clients.set(row.id, slot);
    try {
      await connected;
      return connection;
    } catch (e) {
      if (this.#clients.get(row.id) === slot) this.#clients.delete(row.id);
      connection.retire();
      throw e;
    }
  }

  async execute(
    command: AcceptedControlToolCommand,
    current: () => boolean,
    signal: AbortSignal,
  ): Promise<Extract<ControlToolOutcome, { result: unknown }>> {
    const identity = command.implementation,
      scope = identity?.catalogScope;
    const remaining =
      (command.replyRecovery?.deadlineAt ?? 0) - TOOL_REPLY_DELIVERY_MARGIN_MS - Date.now();
    if (!identity || !scope?.startsWith("mcp:") || !current() || signal.aborted || remaining <= 0)
      throw new McpInvocationRefused();
    signal = AbortSignal.any([signal, AbortSignal.timeout(remaining)]);
    // Business ownership and current connection admission, never a Run lease check.
    const { rows } = await sql<McpConnectionRow>`select c.* from mcp_connections c
      join tool_catalog_publication p on p.scope='mcp:'||c.id::text
      join tool_catalog_publication a on a.scope='platform' and a.release_id=p.release_id
      join runs r on r.id=${command.scope.runId}::uuid and r.tenant_id=c.tenant_id
      join sessions s on s.id=r.session_id and s.tenant_id=r.tenant_id
      where c.id=${scope.slice(4)}::uuid and c.tenant_id=${command.scope.tenantId}::uuid
        and s.id=${command.scope.sessionId}::uuid and s.pi_session_id=${command.scope.piSessionId}
        and s.pi_conversation_id=${command.conversationId} and s.session_kind='conversation'
        and s.created_by_user_id is not null
        and (c.visibility='organization' or c.owner_user_id=s.created_by_user_id)
        and c.enabled and c.deleted_at is null and c.status='ready' and c.checked_revision=c.revision
        and (p.mcp->>'revision')::bigint=c.revision and (p.mcp->>'enabled')::boolean
        and p.generation=${identity.generation} and p.published_generation=p.generation
        and p.release_id=${identity.releaseId} and p.release_id=${this.options.releaseId}`.execute(
      this.options.database,
    );
    const row = rows[0];
    const tool = row?.tools.find(
      (t) =>
        t.platformName === command.toolName &&
        (row.allowed_tools === null || row.allowed_tools.includes(t.name)),
    );
    if (!row || !tool || !current() || signal.aborted) throw new McpInvocationRefused();
    try {
      const connection = await this.#connection(row);
      if (!current() || signal.aborted) throw new McpInvocationRefused();
      const outcome = await connection.call(tool, command.arguments, signal);
      return normalizeCompletedControlResult(command, outcome);
    } catch (error) {
      if (error instanceof McpInvocationRefused) throw error;
      if (signal.aborted) throw signal.reason;
      return {
        isError: true,
        result: {
          content: [
            {
              type: "text",
              text: `tool_result_unknown: ${mcpFailureCode(error)}. No automatic retry was made. Inspect the external state before repeating this operation.`,
            },
          ],
        },
      };
    }
  }

  async close(): Promise<void> {
    this.#abort.abort();
    await this.#running;
    await this.#tail.catch(() => {});
    for (const client of this.#allClients) await client.close();
    await this.#wake.close();
  }
}
