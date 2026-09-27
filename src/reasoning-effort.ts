import type { OcxProviderConfig } from "./types";
import { dropLearnedUnsupportedReasoningEfforts, ensureReasoningMetadataSnapshot, reasoningEffortsFromMetadata } from "./providers/reasoning-metadata";
import { configuredReasoningEffortsWith, mapReasoningEffortWith, type ReasoningMetadataAccess } from "./reasoning-effort-core";

export * from "./reasoning-effort-core";

const DISK_METADATA: ReasoningMetadataAccess = {
  dropLearned: dropLearnedUnsupportedReasoningEfforts,
  fromMetadata: reasoningEffortsFromMetadata,
  ensureSnapshot: ensureReasoningMetadataSnapshot,
};

/**
 * Provider/model configured reasoning levels for the Codex catalog. `undefined` means “no override”,
 * while an empty array means “intentionally expose no effort control for this model”.
 */
export function configuredReasoningEfforts(provider: OcxProviderConfig, modelId: string): string[] | undefined {
  return configuredReasoningEffortsWith(provider, modelId, DISK_METADATA);
}

/**
 * Translate Codex's reasoning label into the provider's real wire value. Prefer identity labels
 * (`xhigh` stays `xhigh`, `max` stays `max`); provider maps are only for real upstream aliases.
 */
export function mapReasoningEffort(provider: OcxProviderConfig, modelId: string, requested: string | undefined): string | undefined {
  return mapReasoningEffortWith(provider, modelId, requested, DISK_METADATA);
}
