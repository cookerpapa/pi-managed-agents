import {
  Body,
  Controller,
  Get,
  Headers,
  HttpCode,
  Inject,
  Param,
  Post,
  Put,
  Query,
  Req,
} from "@nestjs/common";
import {
  ControlPlaneApiValidationError,
  parseAcceptTurnRequest,
  parseCreateTurnCancellationRequest,
  parseRebindConversationWorkspaceRequest,
  parseIdempotencyKey,
  parseUpdateSessionModelRequest,
  parseUuidPathParameter,
  type AcceptedConversationInputResource,
  parseQueuedInputId,
  type AcceptedTurnCancellationResource,
  type ConversationWorkspaceBindingResource,
  type SessionModelResource,
} from "@pi-cloud/protocol";
import type { FastifyRequest } from "fastify";
import { ControlPlaneStoreFactory } from "./control-plane-store-factory.ts";
import { TenantRequestContext } from "./tenant-request-context.ts";
import { QueuedInputService } from "./queued-input-service.ts";
import { parseQueuedInputAction, parseAcknowledgeReturnedInputs } from "@pi-cloud/protocol";

@Controller("v1")
export class SessionInputController {
  constructor(
    @Inject(ControlPlaneStoreFactory) private readonly controlPlaneStores: ControlPlaneStoreFactory,
    @Inject(TenantRequestContext) private readonly tenantRequestContext: TenantRequestContext,
    @Inject(QueuedInputService) private readonly queuedInputs: QueuedInputService,
  ) {}
  @Get("sessions/:sessionId/model")
  async getSessionModel(
    @Req() request: FastifyRequest,
    @Param("sessionId") sessionIdValue: unknown,
  ): Promise<SessionModelResource> {
    const identity = this.tenantRequestContext.resolve(request);
    return this.controlPlaneStores
      .forIdentity(identity)
      .getSessionModel(parseUuidPathParameter(sessionIdValue, "sessionId"));
  }

  @Put("sessions/:sessionId/model")
  async updateSessionModel(
    @Req() request: FastifyRequest,
    @Param("sessionId") sessionIdValue: unknown,
    @Body() body: unknown,
  ): Promise<SessionModelResource> {
    const identity = this.tenantRequestContext.requireMutation(request);
    return this.controlPlaneStores
      .forIdentity(identity)
      .updateSessionModel(
        parseUuidPathParameter(sessionIdValue, "sessionId"),
        parseUpdateSessionModelRequest(body),
      );
  }

  @Put("conversations/:sessionId/workspace")
  async rebindConversationWorkspace(
    @Req() request: FastifyRequest,
    @Param("sessionId") sessionIdValue: unknown,
    @Headers("idempotency-key") idempotencyKeyValue: unknown,
    @Body() body: unknown,
  ): Promise<ConversationWorkspaceBindingResource> {
    const identity = this.tenantRequestContext.requireMutation(request);
    const binding = parseRebindConversationWorkspaceRequest(body);
    return this.controlPlaneStores
      .forIdentity(identity)
      .rebindConversationWorkspace(
        parseUuidPathParameter(sessionIdValue, "sessionId"),
        binding.workspaceId,
        parseIdempotencyKey(idempotencyKeyValue),
        {
          ownerUserId: identity.userId,
          ...(binding.workingDirectory === undefined
            ? {}
            : { workingDirectory: binding.workingDirectory }),
        },
      );
  }

  @Post("sessions/:sessionId/turns")
  @HttpCode(202)
  async acceptTurn(
    @Req() httpRequest: FastifyRequest,
    @Param("sessionId") sessionIdValue: unknown,
    @Headers("idempotency-key") idempotencyKeyValue: unknown,
    @Body() body: unknown,
  ): Promise<AcceptedConversationInputResource> {
    const sessionId = parseUuidPathParameter(sessionIdValue, "sessionId");
    const idempotencyKey = parseIdempotencyKey(idempotencyKeyValue);
    const request = parseAcceptTurnRequest(body);
    const identity = this.tenantRequestContext.requireMutation(httpRequest);
    return this.queuedInputs.accept(
      this.controlPlaneStores.forIdentity(identity),
      identity.tenantId,
      sessionId,
      idempotencyKey,
      request,
    );
  }

  @Get("sessions/:sessionId/input-receipt")
  inputReceipt(
    @Req() request: FastifyRequest,
    @Param("sessionId") session: unknown,
    @Query("key") key: unknown,
  ) {
    const identity = this.tenantRequestContext.resolve(request);
    return this.queuedInputs.receipt(
      identity.tenantId,
      parseUuidPathParameter(session, "sessionId"),
      parseIdempotencyKey(key),
    );
  }

  @Post("sessions/:sessionId/turns/:turnId/cancellations")
  @HttpCode(202)
  async acceptTurnCancellation(
    @Req() httpRequest: FastifyRequest,
    @Param("sessionId") sessionIdValue: unknown,
    @Param("turnId") turnIdValue: unknown,
    @Headers("idempotency-key") idempotencyKeyValue: unknown,
    @Body() body: unknown,
  ): Promise<AcceptedTurnCancellationResource> {
    const sessionId = parseUuidPathParameter(sessionIdValue, "sessionId");
    const turnId = parseUuidPathParameter(turnIdValue, "turnId");
    const idempotencyKey = parseIdempotencyKey(idempotencyKeyValue);
    const request = parseCreateTurnCancellationRequest(body);
    const identity = this.tenantRequestContext.requireMutation(httpRequest);
    return this.controlPlaneStores
      .forIdentity(identity)
      .acceptTurnCancellation(sessionId, turnId, idempotencyKey, request);
  }

  @Get("sessions/:sessionId/queued-inputs")
  async getQueuedInputs(
    @Req() request: FastifyRequest,
    @Param("sessionId") sessionId: unknown,
    @Query("afterPosition") afterPosition: unknown,
    @Query("revision") revision: unknown,
  ) {
    const identity = this.tenantRequestContext.resolve(request);
    if (
      (afterPosition !== undefined &&
        (typeof afterPosition !== "string" || !/^(0|[1-9][0-9]*)$/.test(afterPosition))) ||
      (revision !== undefined && typeof revision !== "string")
    )
      throw new ControlPlaneApiValidationError("Invalid queue page");
    return this.queuedInputs.list(
      identity.tenantId,
      parseUuidPathParameter(sessionId, "sessionId"),
      {
        ...(afterPosition === undefined ? {} : { afterPosition: Number(afterPosition) }),
        ...(revision === undefined ? {} : { revision: revision as string }),
      },
    );
  }

  @Post("sessions/:sessionId/queued-inputs/:inputId/actions")
  @HttpCode(200)
  async mutateQueuedInput(
    @Req() request: FastifyRequest,
    @Param("sessionId") sessionId: unknown,
    @Param("inputId") inputId: unknown,
    @Headers("idempotency-key") key: unknown,
    @Body() body: unknown,
  ) {
    const identity = this.tenantRequestContext.requireMutation(request);
    return this.queuedInputs.mutate(
      identity.tenantId,
      parseUuidPathParameter(sessionId, "sessionId"),
      parseQueuedInputId(inputId),
      parseIdempotencyKey(key),
      parseQueuedInputAction(body),
    );
  }

  @Get("sessions/:sessionId/returned-inputs")
  async getReturnedInputs(
    @Req() request: FastifyRequest,
    @Param("sessionId") sessionId: unknown,
    @Query("requestId") requestId: unknown,
    @Query("afterPosition") afterPosition: unknown,
  ) {
    const identity = this.tenantRequestContext.resolve(request);
    if (
      afterPosition !== undefined &&
      (typeof afterPosition !== "string" || !/^(0|[1-9][0-9]*)$/.test(afterPosition))
    )
      throw new ControlPlaneApiValidationError("Invalid returned input page");
    return this.queuedInputs.returned(
      identity.tenantId,
      parseUuidPathParameter(sessionId, "sessionId"),
      {
        ...(requestId === undefined
          ? {}
          : { requestId: parseUuidPathParameter(requestId, "requestId") }),
        ...(afterPosition === undefined ? {} : { afterPosition: Number(afterPosition) }),
      },
    );
  }

  @Post("sessions/:sessionId/returned-inputs/acknowledgements")
  @HttpCode(204)
  async acknowledgeReturnedInputs(
    @Req() request: FastifyRequest,
    @Param("sessionId") sessionId: unknown,
    @Body() body: unknown,
  ) {
    const identity = this.tenantRequestContext.requireMutation(request);
    return this.queuedInputs.acknowledgeReturned(
      identity.tenantId,
      parseUuidPathParameter(sessionId, "sessionId"),
      parseAcknowledgeReturnedInputs(body),
    );
  }
}
