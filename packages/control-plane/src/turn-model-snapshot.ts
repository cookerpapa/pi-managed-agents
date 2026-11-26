import type { Transaction } from "kysely";
import type { Database } from "@pi-cloud/database";
import {
  DomainModelValidationError,
  resolveTurnModel,
  type ModelProfile,
  type ModelThinkingLevel,
} from "@pi-cloud/domain";
import { catalogModel, currentModelCatalog, freezeModelRuntime } from "./runtime-model-catalog.ts";
import { positiveSafeInteger } from "./store-resource-mapping.ts";
import { ControlPlaneStoreError } from "./control-plane-store-error.ts";
type ModelSnapshotRow = {
  profileId: string;
  provider: string;
  modelId: string;
  defaultThinkingLevel: string;
  allowedThinkingLevels: string[];
  credentialBindingId: string;
  credentialBindingVersion: string;
  profileEnabled: boolean;
  credentialStatus: string;
  credentialProvider: string;
};

export async function resolveTurnModelSnapshot(
  transaction: Transaction<Database>,
  tenantId: string,
  modelProfileId: string,
  requestedThinkingLevel?: ModelThinkingLevel,
  serviceTier: "fast" | null = null,
) {
  const row = (await transaction
    .selectFrom("model_profiles as profile")
    .innerJoin("credential_bindings as credential", (join) =>
      join
        .onRef("credential.tenant_id", "=", "profile.tenant_id")
        .onRef("credential.id", "=", "profile.credential_binding_id")
        .onRef("credential.version", "=", "profile.credential_binding_version"),
    )
    .select([
      "profile.id as profileId",
      "profile.provider as provider",
      "profile.model_id as modelId",
      "profile.default_thinking_level as defaultThinkingLevel",
      "profile.allowed_thinking_levels as allowedThinkingLevels",
      "profile.credential_binding_id as credentialBindingId",
      "profile.credential_binding_version as credentialBindingVersion",
      "profile.enabled as profileEnabled",
      "credential.status as credentialStatus",
      "credential.provider as credentialProvider",
    ])
    .where("profile.tenant_id", "=", tenantId)
    .where("profile.id", "=", modelProfileId)
    .executeTakeFirst()) as ModelSnapshotRow | undefined;

  if (
    !row ||
    !row.profileEnabled ||
    row.credentialStatus !== "active" ||
    row.credentialProvider !== row.provider
  ) {
    throw new ControlPlaneStoreError(
      "control_plane_misconfigured",
      "The configured model profile is unavailable",
    );
  }

  const profile: ModelProfile = {
    profileId: row.profileId,
    provider: row.provider,
    modelId: row.modelId,
    defaultThinkingLevel: row.defaultThinkingLevel as ModelThinkingLevel,
    allowedThinkingLevels: row.allowedThinkingLevels as ModelThinkingLevel[],
    credentialBindingId: row.credentialBindingId,
    credentialBindingVersion: positiveSafeInteger(
      row.credentialBindingVersion,
      "Credential binding version",
    ),
    enabled: row.profileEnabled,
  };
  try {
    const catalog = currentModelCatalog();
    const supported = catalogModel(profile.provider, profile.modelId, catalog);
    if (profile.provider !== "pi-cloud-fake" && (!supported || !supported.visible)) {
      throw new DomainModelValidationError(
        "The selected model is unavailable in the current catalog",
      );
    }
    // Credential identity/enabled state belongs to PG; real provider capabilities
    // belong to the one deployment catalog, not an older profile-row copy.
    const resolved = resolveTurnModel(
      supported
        ? {
            ...profile,
            defaultThinkingLevel: supported.defaultThinkingLevel,
            allowedThinkingLevels: [...supported.thinkingLevels],
          }
        : profile,
      requestedThinkingLevel,
    );
    if (serviceTier === "fast" && supported?.fastModeAvailable !== true) {
      throw new DomainModelValidationError(`Fast mode is not supported by ${resolved.modelId}`);
    }
    return {
      ...resolved,
      serviceTier,
      runtime: supported ? freezeModelRuntime(supported, catalog) : null,
    };
  } catch (error) {
    if (error instanceof DomainModelValidationError) {
      throw new ControlPlaneStoreError(
        requestedThinkingLevel === undefined ? "control_plane_misconfigured" : "invalid_request",
        error.message,
      );
    }
    throw error;
  }
}
