// The reads reasoning-metadata.ts makes from its two caches (the models.dev snapshot and the rungs
// upstreams refused), over caches passed in. reasoning-metadata.ts passes its disk-backed memos; the
// Cloudflare Worker passes the copies ocx publishes to its Durable Object, so both map effort alike.
import { metadataProviderKeyForBaseUrl } from "./reasoning-metadata-destinations";
import type { OcxProviderConfig } from "../types";
import type { ReasoningMetadataAccess } from "../reasoning-effort-core";

export type ReasoningMetadataOption = { type: string; values?: string[] };
export type ReasoningMetadataModel = { reasoning: boolean; options: ReasoningMetadataOption[] };

export interface MetadataSnapshot {
  version: 1 | 2;
  fetchedAt: number;
  source: string;
  providers: Record<string, Record<string, ReasoningMetadataModel>>;
  /**
   * v2: models.dev provider key -> that provider's published api URL (normalised). v1 snapshots
   * predate the field and keep working through BASE_URL_TO_METADATA_PROVIDER.
   */
  apis?: Record<string, string>;
}

export interface SupportSnapshot {
  version: 2;
  rows: Record<string, { effort: string; at: number; evidence?: string }>;
}

/** Snapshot age that triggers a background refresh. Older snapshots still serve reads. */
export const CACHE_TTL_MS = 24 * 60 * 60 * 1000;
/** A learned "this rung is refused" fact expires: entitlements change. */
export const SUPPORT_TTL_MS = 30 * 24 * 60 * 60 * 1000;

/** Canonical Codex ladder order; mirrors reasoning-effort.ts CODEX_REASONING_LEVELS. */
const LADDER_ORDER = ["none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra"];
/** Mirror of registry.ts THINKING_TOGGLE_EFFORTS / THINKING_BUDGET_EFFORTS. */
export const CLASSIFIED_STYLE_EFFORTS = ["low", "medium", "high", "xhigh", "max"];

/** A parsed snapshot file, or null for anything reasoning-metadata.ts would not load. */
export function parseMetadataSnapshot(parsed: unknown): MetadataSnapshot | null {
  const value = parsed as MetadataSnapshot | null;
  return value && (value.version === 1 || value.version === 2) && value.providers && typeof value.providers === "object"
    ? value
    : null;
}

/** The unexpired rows of a parsed support file (version 1 rows carry no credential and are dropped). */
export function parseSupportRows(parsed: unknown, nowMs: number): Map<string, number> {
  const rows = new Map<string, number>();
  const value = parsed as SupportSnapshot | null;
  if (value && value.version === 2 && value.rows && typeof value.rows === "object") {
    for (const [key, row] of Object.entries(value.rows)) {
      if (!row || typeof row.at !== "number") continue;
      if (nowMs - row.at > SUPPORT_TTL_MS) continue;
      rows.set(key, row.at);
    }
  }
  return rows;
}

/** Canonical order + dedupe. Local mirror of sanitizeCodexReasoningEfforts (import cycle). */
export function sanitizeLadder(values: readonly string[] | undefined): string[] | undefined {
  if (!Array.isArray(values)) return undefined;
  const seen = new Set(values.filter((value): value is string => typeof value === "string"));
  const ordered = LADDER_ORDER.filter(effort => seen.has(effort));
  return ordered.length > 0 ? ordered : undefined;
}

export function metadataProviderKey(provider: OcxProviderConfig): string | undefined {
  return metadataProviderKeyForBaseUrl(typeof provider.baseUrl === "string" ? provider.baseUrl : undefined);
}

/** JSON encoding avoids delimiter ambiguity in provider, model, and effort identifiers. */
export function supportKey(providerKey: string, credential: string, modelId: string, effort: string): string {
  return JSON.stringify([providerKey, credential, modelId, effort]);
}

export function metadataModelIn(snapshot: MetadataSnapshot | null, provider: OcxProviderConfig, modelId: string): ReasoningMetadataModel | undefined {
  const key = metadataProviderKey(provider);
  if (!key) return undefined;
  const models = snapshot?.providers?.[key];
  if (!models) return undefined;
  const model = models[modelId];
  return model && typeof model === "object" ? model : undefined;
}

/** Raw models.dev effort values for a model, canonicalised; undefined when not published. */
export function metadataEffortValuesIn(snapshot: MetadataSnapshot | null, provider: OcxProviderConfig, modelId: string): string[] | undefined {
  const model = metadataModelIn(snapshot, provider, modelId);
  if (!model) return undefined;
  const options = Array.isArray(model.options) ? model.options : [];
  const effort = options.find(option => option && option.type === "effort");
  const ladder = sanitizeLadder(effort?.values);
  // none/minimal are sentinels, not picker rungs (mapReasoningEffort folds minimal to low), and
  // advertising them would trip the Codex runtime clamp for no user-visible gain.
  const rungs = ladder?.filter(value => value !== "none" && value !== "minimal");
  return rungs && rungs.length > 0 ? rungs : undefined;
}

export function learnedUnsupportedIn(
  support: ReadonlyMap<string, number>, credential: string | undefined, provider: OcxProviderConfig, modelId: string, effort: string,
): boolean {
  const key = metadataProviderKey(provider);
  if (!key || !credential) return false;
  return support.has(supportKey(key, credential, modelId, effort));
}

/**
 * After the ladder is chosen (registry config or models.dev metadata), remove the rungs this
 * account actually had refused. An all-refused ladder keeps the original list: turning "some
 * rungs" into "no effort control" would silently drop the picker.
 */
export function dropLearnedIn(
  support: ReadonlyMap<string, number>, credential: string | undefined, provider: OcxProviderConfig, modelId: string, efforts: readonly string[],
): string[] {
  if (efforts.length === 0) return [...efforts];
  const key = metadataProviderKey(provider);
  if (!key || !credential) return [...efforts];
  if (support.size === 0) return [...efforts];
  const kept = efforts.filter(effort => !support.has(supportKey(key, credential, modelId, effort)));
  return kept.length === 0 ? [...efforts] : kept;
}

/**
 * Metadata fallback ladder for a provider/model: published effort values win; a model the provider
 * classifies as thinking-toggle / thinking-budget keeps the provider's own list; refused rungs are
 * removed, and a ladder emptied by that learning returns undefined.
 */
export function reasoningEffortsFromMetadataIn(
  snapshot: MetadataSnapshot | null, support: () => ReadonlyMap<string, number>, credential: () => string | undefined,
  provider: OcxProviderConfig, modelId: string,
): string[] | undefined {
  let ladder = metadataEffortValuesIn(snapshot, provider, modelId);
  if (!ladder) {
    const classified = (provider.thinkingToggleModels ?? []).includes(modelId)
      || (provider.thinkingBudgetModels ?? []).includes(modelId);
    ladder = classified ? CLASSIFIED_STYLE_EFFORTS : undefined;
  }
  if (!ladder || ladder.length === 0) return undefined;
  // Read only once a ladder exists: the credential may come from the OS keychain.
  const refused = support();
  const identity = metadataProviderKey(provider) ? credential() : undefined;
  const kept = ladder.filter(effort => !learnedUnsupportedIn(refused, identity, provider, modelId, effort));
  return kept.length === 0 ? undefined : kept;
}

/**
 * reasoning-effort.ts's metadata access over given caches. `credential` digests the key the request
 * sends, as reasoning-metadata.ts's credentialIdentity does on the request path.
 */
export function metadataAccessFrom(
  snapshot: MetadataSnapshot | null, support: ReadonlyMap<string, number>, credential: (provider: OcxProviderConfig) => string | undefined,
): ReasoningMetadataAccess {
  return {
    dropLearned: (provider, modelId, efforts) =>
      metadataProviderKey(provider) ? dropLearnedIn(support, credential(provider), provider, modelId, efforts) : [...efforts],
    fromMetadata: (provider, modelId) =>
      reasoningEffortsFromMetadataIn(snapshot, () => support, () => credential(provider), provider, modelId),
    ensureSnapshot: () => {},
  };
}
