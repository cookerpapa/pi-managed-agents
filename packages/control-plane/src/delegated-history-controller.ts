import { Controller, Get, Inject, Param, Query, Req } from "@nestjs/common";
import type { FastifyRequest } from "fastify";
import { withDatabaseReadSignal } from "@pi-cloud/database";
import { ControlPlaneApiValidationError, parseUuidPathParameter } from "@pi-cloud/protocol";
import { abortableHistoryRead } from "./abortable-history-read.ts";
import { DelegatedHistoryReader } from "./delegated-history-reader.ts";
import { TenantRequestContext } from "./tenant-request-context.ts";
@Controller("v1/conversations/:sessionId/agents")
export class DelegatedHistoryController {
  constructor(
    @Inject(DelegatedHistoryReader) readonly reader: DelegatedHistoryReader,
    @Inject(TenantRequestContext) readonly identity: TenantRequestContext,
  ) {}
  @Get()
  directory(
    @Req() request: FastifyRequest,
    @Param("sessionId") root: string,
    @Query("cursor") cursor?: unknown,
  ) {
    return this.reader.directory(
      this.identity.resolve(request).tenantId,
      parseUuidPathParameter(root, "sessionId"),
      cursor === undefined ? undefined : parseUuidPathParameter(cursor, "cursor"),
    );
  }
  @Get(":agentId/history")
  history(
    @Req() request: FastifyRequest,
    @Param("sessionId") root: string,
    @Param("agentId") agent: string,
    @Query("before") before?: unknown,
  ) {
    if (before !== undefined && typeof before !== "string")
      throw new ControlPlaneApiValidationError("Invalid delegated history cursor");
    const tenant = this.identity.resolve(request).tenantId,
      id = parseUuidPathParameter(root, "sessionId"),
      agentId = parseUuidPathParameter(agent, "agentId");
    return abortableHistoryRead(request, (signal) =>
      withDatabaseReadSignal(signal, () => this.reader.history(tenant, id, agentId, before)),
    );
  }
}
