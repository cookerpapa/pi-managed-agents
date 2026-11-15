import { randomBytes, randomUUID } from "node:crypto";
import { expect, it } from "vitest";
import { sql } from "kysely";
import { ToolCatalogView } from "@pi-cloud/event-log";
import type { McpConnectionChange } from "@pi-cloud/protocol";
import { McpCredentialVault } from "../../src/mcp/credential-vault.ts";
import { McpConnectionService } from "../../src/mcp/connection-service.ts";
import { McpInvocationRefused, McpToolHostRuntime } from "../../src/mcp/tool-host-runtime.ts";
import { ToolCatalogAuthority } from "../../src/tool-catalog-authority.ts";
import { controlToolCatalog } from "../../src/control-tool-registry.ts";
import { nativeSubagentFixture } from "../native-subagent-fixture.ts";
import { httpMcpFixture } from "./http-fixture.ts";

it("persists scoped connections once, seals credentials, enforces CAS and prevents deleted identity reuse", async () => {
  const f = await nativeSubagentFixture();
  try {
    const owner = await f.database
      .selectFrom("users")
      .select("id")
      .where("tenant_id", "=", f.scope.tenantId)
      .executeTakeFirstOrThrow();
    const actor = { tenantId: f.scope.tenantId, userId: owner.id, role: "owner" as const };
    const vault = new McpCredentialVault(randomBytes(32).toString("base64url"));
    const service = new McpConnectionService(f.database, vault),
      id = randomUUID();
    const input: McpConnectionChange = {
      expectedRevision: null,
      name: "Glossary",
      endpoint: "http://127.0.0.1:1234/mcp",
      visibility: "personal",
      enabled: true,
      allowedTools: null,
      credential: { kind: "bearer", token: "fixture-secret" },
    };
    const first = await service.save(actor, id, input);
    expect(first.connection.revision).toBe("1");
    expect(JSON.stringify(first)).not.toContain("fixture-secret");
    expect((await service.save(actor, id, input)).connection.revision).toBe("1");
    const { rows } = await sql<{
      credential: string;
      credential_revision: string;
    }>`select credential,credential_revision from mcp_connections where id=${id}::uuid`.execute(
      f.database,
    );
    expect(rows[0]!.credential).not.toContain("fixture-secret");
    expect(vault.open(actor.tenantId, id, "1", rows[0]!.credential)).toBe("fixture-secret");
    expect(() => vault.open(actor.tenantId, randomUUID(), "1", rows[0]!.credential)).toThrow();
    await expect(service.read({ ...actor, tenantId: randomUUID() }, id)).rejects.toThrow(
      "not found",
    );
    await expect(service.read({ ...actor, userId: randomUUID() }, id)).rejects.toThrow("not found");
    await expect(service.save({ ...actor, role: "member" }, randomUUID(), input)).rejects.toThrow(
      "owner",
    );
    await expect(service.save(actor, id, { ...input, expectedRevision: "9" })).rejects.toThrow(
      "changed",
    );
    const { credential: _credential, ...withoutCredential } = input;
    await expect(
      service.save(actor, id, {
        ...withoutCredential,
        expectedRevision: "1",
        endpoint: "https://different.example/mcp",
      }),
    ).rejects.toThrow("credential");
    const second = await service.save(actor, id, {
      ...input,
      expectedRevision: "1",
      visibility: "organization",
    });
    expect((await service.read({ ...actor, userId: randomUUID() }, id)).connection.canEdit).toBe(
      false,
    );
    await service.remove(actor, id, second.connection.revision);
    await service.remove(actor, id, second.connection.revision);
    await expect(service.save(actor, id, input)).rejects.toThrow("changed");
    expect(
      (
        await sql<{
          credential: null;
        }>`select credential from mcp_connections where id=${id}::uuid`.execute(f.database)
      ).rows[0]!.credential,
    ).toBeNull();
  } finally {
    await f.close();
  }
});

it("publishes discovered tools and checks live connection authority without reading Worker leases", async () => {
  const f = await nativeSubagentFixture(),
    server = await httpMcpFixture();
  let runtime: McpToolHostRuntime | undefined;
  try {
    const owner = await f.database
      .selectFrom("users")
      .select("id")
      .where("tenant_id", "=", f.scope.tenantId)
      .executeTakeFirstOrThrow();
    await f.database
      .updateTable("sessions")
      .set({ created_by_user_id: owner.id })
      .where("id", "=", f.scope.sessionId)
      .execute();
    const actor = { tenantId: f.scope.tenantId, userId: owner.id, role: "owner" as const };
    const vault = new McpCredentialVault(randomBytes(32).toString("base64url"));
    const service = new McpConnectionService(f.database, vault),
      authority = new ToolCatalogAuthority(f.database),
      view = new ToolCatalogView();
    const platform = await authority.select({
      releaseId: "code-1",
      catalog: controlToolCatalog([]),
      replayable: [],
      expectedGeneration: 0,
    });
    view.accept(platform);
    await authority.confirmPublished(platform);
    runtime = new McpToolHostRuntime({
      database: f.database,
      databaseUrl: f.url,
      releaseId: "code-1",
      vault,
      fetch,
      publish: async (value) => {
        expect(await authority.current(value)).toBe(true);
        view.accept(value);
        await authority.confirmPublished(value);
      },
    });
    const id = randomUUID(),
      input: McpConnectionChange = {
        expectedRevision: null,
        name: "Glossary",
        endpoint: server.url,
        visibility: "personal",
        enabled: true,
        allowedTools: null,
        credential: { kind: "bearer", token: "fixture-token" },
      };
    await service.save(actor, id, input);
    await runtime.reconcile();
    expect((await service.read(actor, id)).connection.status).toBe("ready");
    const tools = view
      .forActor({ tenantId: actor.tenantId, userId: actor.userId, root: true })
      .read().tools;
    expect(tools).toHaveLength(1);
    const command = {
      ...(await f.command({ text: "MCP_OK" }, undefined, tools[0]!.name)),
      implementation: view.implementation(tools[0]!.name)!,
      replyRecovery: {
        topic: `pi-cloud.tool-replies.v4.${randomUUID()}`,
        partition: 0 as const,
        offset: "0",
        deadlineAt: Date.now() + 420000,
        toolCallId: "fixture",
        toolName: tools[0]!.name,
      },
    };
    expect(await runtime.execute(command, () => true, new AbortController().signal)).toHaveProperty(
      "result.content.0.text",
      "MCP_OK",
    );
    await expect(
      runtime.execute(
        { ...command, conversationId: 999 },
        () => true,
        new AbortController().signal,
      ),
    ).rejects.toBeInstanceOf(McpInvocationRefused);
    const disabled = await service.save(actor, id, {
      ...input,
      expectedRevision: "1",
      enabled: false,
    });
    // The old directory is still present; the effect endpoint already refuses it.
    expect(view.publicationFor(command.toolName)).toBeDefined();
    await expect(
      runtime.execute(command, () => true, new AbortController().signal),
    ).rejects.toBeInstanceOf(McpInvocationRefused);
    expect(server.state.calls).toEqual(["echo"]);
    await runtime.reconcile();
    expect(view.publicationFor(command.toolName)).toBeUndefined();
    await service.save(actor, id, { ...input, expectedRevision: disabled.connection.revision });
    await runtime.reconcile();
    await expect(
      runtime.execute(command, () => true, new AbortController().signal),
    ).rejects.toBeInstanceOf(McpInvocationRefused);
    expect(view.snapshot?.generation).toBe(platform.generation);
    expect(await authority.minimumGenerations()).toHaveLength(2);
    const beforeRotation = await service.read(actor, id);
    const invalid = await service.save(actor, id, {
      ...input,
      expectedRevision: beforeRotation.connection.revision,
      credential: { kind: "bearer", token: "rotated-invalid-token" },
    });
    await runtime.reconcile();
    expect((await service.read(actor, id)).connection).toMatchObject({
      status: "error",
      error: "mcp_authentication_failed",
    });
    expect(view.publicationFor(command.toolName)).toBeUndefined();
    expect(server.state.calls).toEqual(["echo"]);
    await service.save(actor, id, { ...input, expectedRevision: invalid.connection.revision });
    await runtime.reconcile();
    const restored = {
      ...command,
      operationId: randomUUID(),
      implementation: view.implementation(command.toolName)!,
    };
    expect(
      await runtime.execute(restored, () => true, new AbortController().signal),
    ).toHaveProperty("result.content.0.text", "MCP_OK");
    expect(server.state.calls).toEqual(["echo", "echo"]);
  } finally {
    await runtime?.close();
    await server.close();
    await f.close();
  }
});
