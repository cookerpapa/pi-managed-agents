import {
  Body,
  Controller,
  Delete,
  Get,
  Inject,
  Param,
  Post,
  Put,
  Query,
  Req,
} from "@nestjs/common";
import type { FastifyRequest } from "fastify";
import {
  ControlPlaneApiValidationError,
  parseMcpConnectionChange,
  parseMcpConnectionRevision,
  parseUuidPathParameter,
} from "@pi-cloud/protocol";
import { TenantRequestContext } from "../tenant-request-context.ts";
import { McpConnectionService } from "./connection-service.ts";

@Controller("v1/mcp/connections")
export class McpConnectionController {
  constructor(
    @Inject(McpConnectionService) private readonly connections: McpConnectionService,
    @Inject(TenantRequestContext) private readonly identity: TenantRequestContext,
  ) {}
  @Get()
  list(@Req() request: FastifyRequest, @Query("cursor") cursor?: unknown) {
    return this.connections.list(
      this.identity.resolve(request),
      cursor === undefined ? undefined : parseUuidPathParameter(cursor, "cursor"),
    );
  }
  @Get(":id")
  read(@Req() request: FastifyRequest, @Param("id") id: unknown) {
    return this.connections.read(this.identity.resolve(request), parseUuidPathParameter(id, "id"));
  }
  @Put(":id")
  save(@Req() request: FastifyRequest, @Param("id") id: unknown, @Body() body: unknown) {
    const actor = this.identity.requireOwner(request);
    return this.connections.save(
      actor,
      parseUuidPathParameter(id, "id"),
      this.#parse(() => parseMcpConnectionChange(body)),
    );
  }
  @Post(":id/refresh")
  refresh(@Req() request: FastifyRequest, @Param("id") id: unknown, @Body() body: unknown) {
    return this.connections.refresh(
      this.identity.requireOwner(request),
      parseUuidPathParameter(id, "id"),
      this.#parse(() => parseMcpConnectionRevision(body)),
    );
  }
  @Delete(":id")
  remove(@Req() request: FastifyRequest, @Param("id") id: unknown, @Body() body: unknown) {
    return this.connections.remove(
      this.identity.requireOwner(request),
      parseUuidPathParameter(id, "id"),
      this.#parse(() => parseMcpConnectionRevision(body)),
    );
  }
  #parse<T>(read: () => T): T {
    try {
      return read();
    } catch {
      throw new ControlPlaneApiValidationError("Invalid MCP connection settings or revision");
    }
  }
}
