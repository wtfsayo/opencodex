/**
 * Data-driven reasoning ladders for routed providers.
 *
 * Routed providers rarely publish per-model effort ladders: OpenCode Zen Go answers /models
 * with ids only (id/object/created/owned_by), so opencodex had to hardcode ladders in
 * registry.ts and synthesise max/ultra for codex-rs catalog membership. The public models.dev
 * catalogue DOES publish them per model:
 *   reasoning: true
 *   reasoning_options: [{type:"effort",values:["low","high","max"]}, {type:"toggle"},
 *                       {type:"budget_tokens"}]
 * This module snapshots that catalogue to disk and hands configuredReasoningEfforts() a
 * fallback ladder, so the Codex catalog AND the wire clamp agree with the model instead of a
 * hand-written guess.
 *
 * Failure policy: a missing or corrupt snapshot yields undefined, and an expired snapshot still
 * serves its last ladder while a best-effort refresh runs in the background. The second
 * cache records rungs the upstream actually rejected (400/403 naming reasoning_effort), so an
 * entitlement gap (muse-spark max needs an active Muse Code subscription) costs one rejected
 * request instead of failing every turn that selects that rung.
 */
import { BASE_URL_TO_METADATA_PROVIDER, normalizeDestinationUrl } from "./reasoning-metadata-destinations";
import {
  CACHE_TTL_MS, dropLearnedIn, learnedUnsupportedIn, metadataEffortValuesIn, metadataModelIn, metadataProviderKey,
  parseMetadataSnapshot, parseSupportRows, reasoningEffortsFromMetadataIn, sanitizeLadder, supportKey, SUPPORT_TTL_MS,
  type MetadataSnapshot, type ReasoningMetadataModel, type ReasoningMetadataOption, type SupportSnapshot,
} from "./reasoning-metadata-core";
export type { ReasoningMetadataModel, ReasoningMetadataOption };
export { normalizeDestinationUrl };
import { createHash } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
// Leaf modules on purpose: this file is imported from reasoning-effort.ts, which combos/types.ts
// already imports. Going through the ../config barrel closes a cycle back into account-namespaces.ts
// and leaves COMBO_NAMESPACE in its temporal dead zone for entry points that start at combos/types.ts.
import { atomicWriteFile } from "../config/atomic-write";
import { durableMirrorEnabled, stateRequest } from "../lib/durable-mirror";
import { getConfigDir } from "../config/paths";
import type { OcxProviderConfig } from "../types";
import { resolveProviderApiKey } from "./api-key-resolve";

const FILENAME = "reasoning-metadata-cache.json";
const SUPPORT_FILENAME = "reasoning-support-cache.json";
const SOURCE_URL = "https://models.dev/api.json";
const USER_AGENT = "opencodex-reasoning-metadata/1.0 (+https://github.com/lidge-jun/opencodex)";
const PERSIST_DEBOUNCE_MS = 250;

/** Ranked rungs used for downgrade planning; ultra is client-only and folds to max. */
const RANKED = ["low", "medium", "high", "xhigh", "max"];

let snapshotMemo: MetadataSnapshot | null | undefined;
let supportMemo: Map<string, number> | undefined;
let persistTimer: ReturnType<typeof setTimeout> | null = null;
let refreshInFlight: Promise<unknown> | null = null;

/** Test seam: drop the memoised snapshot/support caches so a suite can drive the load paths. */
const PUBLISH_RETRY_MS = [1_000, 5_000, 30_000];
const publishRetries: Partial<Record<"snapshot" | "support", ReturnType<typeof setTimeout>>> = {};

export function resetReasoningMetadataCachesForTests(): void {
  snapshotMemo = undefined;
  supportMemo = undefined;
  if (persistTimer) {
    clearTimeout(persistTimer);
    persistTimer = null;
  }
  refreshInFlight = null;
  for (const kind of ["snapshot", "support"] as const) {
    clearTimeout(publishRetries[kind]);
    delete publishRetries[kind];
  }
}

let publishVersion = Date.now();

/**
 * On a Cloudflare deployment, hands the Durable Object what this process now holds, so the Worker
 * maps effort from the same caches (reasoning-metadata-core.ts). Refusal rows go without their
 * evidence text; the Worker needs only the keys and times. A failed publish is retried with
 * whatever the process holds by then, since the Worker otherwise keeps sending a refused rung.
 */
function publishForWorker(kind: "snapshot" | "support", attempt = 0): void {
  // Only where the Worker serves requests (the Worker sets it; see containerEnv).
  if (process.env.OCX_WORKER_NATIVE_STATE !== "1" || !durableMirrorEnabled()) return;
  const value = kind === "snapshot" ? snapshotMemo ?? null : supportMemo;
  if (kind === "support" && !value) return;
  const body = value instanceof Map
    ? { version: 2, rows: Object.fromEntries([...value].map(([key, at]) => [key, { effort: JSON.parse(key)[3] ?? "", at }])) }
    : value;
  const version = ++publishVersion;
  const retry = () => {
    if (attempt >= PUBLISH_RETRY_MS.length || publishRetries[kind]) return;
    const timer = setTimeout(() => { delete publishRetries[kind]; publishForWorker(kind, attempt + 1); }, PUBLISH_RETRY_MS[attempt]);
    timer.unref?.();
    publishRetries[kind] = timer;
  };
  const sent = stateRequest(`/reasoning-metadata/${kind}`, {
    method: "PUT",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ version, value: body }),
  });
  sent?.then(response => {
    void response.body?.cancel();
    // 409 means another process holds the lease; its own copy is the one that counts.
    if (!response.ok && response.status !== 409) retry();
  }, retry);
}

function readJsonFile<T>(filename: string): T | null {
  try {
    const path = join(getConfigDir(), filename);
    if (!existsSync(path)) return null;
    return JSON.parse(readFileSync(path, "utf8")) as T;
  } catch {
    // A corrupt cache must never break routing, the catalog, or the dashboard.
    return null;
  }
}

/** Whether catalog sync should bootstrap metadata for this destination. */
export function providerUsesReasoningMetadata(provider: OcxProviderConfig): boolean {
  return metadataProviderKey(provider) !== undefined;
}

/**
 * Local mirror of `modelRecordValue()` from `src/reasoning-effort.ts`, which imports this
 * module and so cannot be imported back. Exact id, then the `family:` prefix, then a
 * case-folded match — a configured ladder must resolve here exactly as it does there, or the
 * downgrade rung is chosen off a different ladder than the catalog advertises.
 */
function modelLadderValue(
  record: Record<string, string[]> | undefined,
  modelId: string,
): readonly string[] | undefined {
  if (!record) return undefined;
  if (Object.prototype.hasOwnProperty.call(record, modelId)) return record[modelId];
  const colon = modelId.indexOf(":");
  if (colon > 0) {
    const family = modelId.slice(0, colon);
    if (Object.prototype.hasOwnProperty.call(record, family)) return record[family];
  }
  const folded = modelId.toLowerCase();
  for (const [key, value] of Object.entries(record)) {
    if (key.toLowerCase() === folded) return value;
  }
  return undefined;
}

/**
 * Bind learned capability to the credential that supplied the evidence. A digest keeps the
 * credential itself out of the persisted cache while remaining stable across restarts and key
 * selection. Providers without key-auth identity may use metadata, but cannot teach the cache.
 *
 * The hash input is the resolved wire credential, not the configured expression: the catalog
 * path carries the raw config string in apiKey (a keychain:/env reference stays unresolved
 * there), while the request path carries the resolved secret in apiKey and the configured
 * expression in _apiKeyAttempt.reference. The request path hashes apiKey exactly as routed --
 * the reference is only provenance, and re-resolving it at record time could read a credential
 * rotated since the request was served. The catalog path resolves the configured expression,
 * so both sides still bind learned refusals to the same wire credential, and a rotation behind
 * a stable reference starts clean instead of inheriting the previous credential's refusals.
 */
function credentialIdentity(provider: OcxProviderConfig): string | undefined {
  const resolved = provider._apiKeyAttempt?.reference !== undefined
    ? provider.apiKey
    : resolveProviderApiKey(provider.apiKey);
  if (typeof resolved !== "string" || resolved.length === 0) return undefined;
  return createHash("sha256").update(resolved).digest("hex");
}

function loadSnapshot(): MetadataSnapshot | null {
  if (snapshotMemo !== undefined) return snapshotMemo;
  snapshotMemo = parseMetadataSnapshot(readJsonFile<MetadataSnapshot>(FILENAME));
  publishForWorker("snapshot");
  return snapshotMemo;
}

/**
 * Mapping report for diagnostics and tests: every gated destination, the models.dev provider it
 * resolves to, and whether the snapshot (v2) publishes an `api` URL that confirms it. The gate is
 * deliberate -- 36 of the registry's 83 destinations match a models.dev provider, so resolving by
 * URL alone would silently move ladders for providers this change has no evidence for.
 */
export function reasoningMetadataMapping(): Array<{
  destination: string;
  provider: string;
  publishedApi?: string;
  confirmed?: boolean;
  models: number;
}> {
  const snapshot = loadSnapshot();
  return Object.entries(BASE_URL_TO_METADATA_PROVIDER).map(([destination, provider]) => {
    const normalized = normalizeDestinationUrl(destination);
    const publishedApi = snapshot?.apis?.[provider];
    const row = {
      destination,
      provider,
      ...(publishedApi ? { publishedApi } : {}),
      ...(publishedApi ? { confirmed: publishedApi === normalized } : {}),
      models: Object.keys(snapshot?.providers?.[provider] ?? {}).length,
    };
    return row;
  });
}

function loadSupport(): Map<string, number> {
  const nowMs = Date.now();
  if (supportMemo) {
    // The memo lives for the process lifetime, so the TTL has to be re-applied on every read.
    // Checking it only on the disk load meant a long-running proxy kept clamping on a refusal
    // it recorded a month earlier, and `dropLearnedUnsupportedReasoningEfforts` inherited that
    // through the same map.
    for (const [key, at] of supportMemo) {
      if (nowMs - at > SUPPORT_TTL_MS) {
        supportMemo.delete(key);
        supportEvidence.delete(key);
      }
    }
    return supportMemo;
  }
  // Version 1 rows had no credential identity and are deliberately invalidated: accepting them
  // would preserve destination-wide refusals written by a lower-entitlement account.
  supportMemo = parseSupportRows(readJsonFile<SupportSnapshot>(SUPPORT_FILENAME), nowMs);
  publishForWorker("support");
  return supportMemo;
}

/** Snapshot health for ocx status / diagnostics. */
export function reasoningMetadataStatus(): { fetchedAt?: number; ageMs?: number; stale: boolean; models: number } {
  const snapshot = loadSnapshot();
  if (!snapshot) return { stale: false, models: 0 };
  const ageMs = Date.now() - snapshot.fetchedAt;
  let models = 0;
  for (const provider of Object.values(snapshot.providers)) models += Object.keys(provider).length;
  return { fetchedAt: snapshot.fetchedAt, ageMs, stale: ageMs > CACHE_TTL_MS, models };
}

export function reasoningMetadataModel(provider: OcxProviderConfig, modelId: string): ReasoningMetadataModel | undefined {
  return metadataModelIn(loadSnapshot(), provider, modelId);
}

/** Raw models.dev effort values for a model, canonicalised; undefined when not published. */
export function metadataEffortValues(provider: OcxProviderConfig, modelId: string): string[] | undefined {
  return metadataEffortValuesIn(loadSnapshot(), provider, modelId);
}

/** True when models.dev publishes the named option type (toggle / budget_tokens) for a model. */
export function metadataDeclaresType(provider: OcxProviderConfig, modelId: string, type: string): boolean {
  const model = reasoningMetadataModel(provider, modelId);
  if (!model) return false;
  const options = Array.isArray(model.options) ? model.options : [];
  return options.some(option => option?.type === type);
}

export function isReasoningEffortLearnedUnsupported(provider: OcxProviderConfig, modelId: string, effort: string): boolean {
  if (!metadataProviderKey(provider)) return false;
  return learnedUnsupportedIn(loadSupport(), credentialIdentity(provider), provider, modelId, effort);
}

/**
 * After the ladder is chosen (registry config or models.dev metadata), remove the rungs this
 * account actually had refused. Applied at the configuredReasoningEfforts() exit so a
 * registry-pinned ladder learns exactly like a metadata-derived one; without it a pinned rung
 * the upstream rejects would replay-and-fail on every request. An all-refused ladder keeps the
 * original list: turning "some rungs" into "no effort control" would silently drop the picker.
 */
export function dropLearnedUnsupportedReasoningEfforts(
  provider: OcxProviderConfig,
  modelId: string,
  efforts: readonly string[],
): string[] {
  if (efforts.length === 0 || !metadataProviderKey(provider)) return [...efforts];
  return dropLearnedIn(loadSupport(), credentialIdentity(provider), provider, modelId, efforts);
}

/**
 * Metadata fallback ladder for a provider/model.
 *
 * - Published effort values win.
 * - A model the provider already classifies as thinking-toggle / thinking-budget keeps the
 *   provider's own effort list; a toggle-only entry never invents wire semantics here.
 * - Rungs the upstream actually refused are removed; a ladder emptied by that learning
 *   returns undefined (status quo) rather than advertising "no effort control".
 */
export function reasoningEffortsFromMetadata(provider: OcxProviderConfig, modelId: string): string[] | undefined {
  return reasoningEffortsFromMetadataIn(loadSnapshot(), loadSupport, () => credentialIdentity(provider), provider, modelId);
}

const supportEvidence = new Map<string, string>();

/**
 * Record that the upstream refused a rung. Persisted (debounced) so the next catalog sync and
 * every later request clamp before dispatch. Returns true when this is new information.
 */
export function recordUnsupportedReasoningEffort(
  provider: OcxProviderConfig,
  modelId: string,
  effort: string,
  evidence?: string,
): boolean {
  const key = metadataProviderKey(provider);
  const credential = credentialIdentity(provider);
  if (!key || !credential || !effort) return false;
  const rowKey = supportKey(key, credential, modelId, effort);
  const rows = loadSupport();
  if (rows.has(rowKey)) return false;
  rows.set(rowKey, Date.now());
  if (evidence) supportEvidence.set(rowKey, evidence.slice(0, 240));
  // The Worker reads this process's view, not the file, so it learns the refusal before the write.
  publishForWorker("support");
  if (persistTimer) clearTimeout(persistTimer);
  persistTimer = setTimeout(() => {
    persistTimer = null;
    try {
      const out: SupportSnapshot["rows"] = {};
      for (const [rowKey, at] of rows) {
        const evidenceText = supportEvidence.get(rowKey);
        out[rowKey] = {
          effort: JSON.parse(rowKey)[3] ?? "",
          at,
          ...(evidenceText ? { evidence: evidenceText } : {}),
        };
      }
      atomicWriteFile(join(getConfigDir(), SUPPORT_FILENAME), JSON.stringify({ version: 2, rows: out }) + "\n");
      publishForWorker("support");
    } catch {
      // Best-effort persistence only.
    }
  }, PERSIST_DEBOUNCE_MS);
  return true;
}

/** Test seam: flush a pending support write so a script sees the snapshot immediately. */
export function flushReasoningSupportCache(): void {
  if (!persistTimer) return;
  clearTimeout(persistTimer);
  persistTimer = null;
  try {
    const rows = loadSupport();
    const out: SupportSnapshot["rows"] = {};
    for (const [rowKey, at] of rows) {
      const evidenceText = supportEvidence.get(rowKey);
      out[rowKey] = { effort: JSON.parse(rowKey)[3] ?? "", at, ...(evidenceText ? { evidence: evidenceText } : {}) };
    }
    atomicWriteFile(join(getConfigDir(), SUPPORT_FILENAME), JSON.stringify({ version: 2, rows: out }) + "\n");
    publishForWorker("support");
  } catch {
    // Best-effort persistence only.
  }
}

/**
 * Words an upstream uses when it is refusing the parameter it just named. Requiring one of
 * these beside the effort term is what separates "the gateway rejected reasoning effort" from
 * "the gateway rejected something else and echoed the request back".
 */
const REJECTION_LANGUAGE = /unsupported|not supported|does not support|invalid|unrecognized|unknown|not allowed|not permitted|must be|requires|required|cannot|can't|out of range/i;

/**
 * How far from the effort term the rejection language may sit and still be about it. Kept
 * deliberately short: an error body that echoes the request back puts unrelated field names and
 * their complaints within a hundred characters of each other, so a generous window classifies
 * every 400 that mentions effort as a refusal of it.
 */
const REJECTION_WINDOW = 48;

/**
 * The `invalid_request_error` type tag rides along on essentially every 400 an OpenAI-shaped
 * gateway emits, so it is evidence of nothing. Blanked before the language scan rather than
 * dropped from the pattern, because `invalid` is real evidence when it is the message.
 */
const GENERIC_ERROR_TYPE = /invalid_request_error/gi;

/**
 * Evidence test for a rejection body: does it blame reasoning effort?
 *
 * The parameter name on its own is not evidence. A 400 that refuses `max_tokens` may still
 * echo the whole request body back, `reasoning_effort` included, and treating that as a
 * refusal spends this request's one downgrade replay on a rung the upstream never objected to
 * — and persists a false refusal that clamps every later turn for thirty days.
 */
export function isReasoningEffortRejection(text: string | undefined): boolean {
  if (!text) return false;
  if (/unsupported.{0,24}effort/i.test(text)) return true;
  // An upstream that names the offending parameter has already said which one it means.
  if (/["']?param["']?\s*[:=]\s*["']?(?:reasoning[._ ]effort|reasoning)/i.test(text)) return true;
  const scanned = text.replace(GENERIC_ERROR_TYPE, " ");
  const term = /reasoning\.effort|reasoning_effort|reasoning effort|thinking budget|reasoning_parameters/gi;
  for (let match = term.exec(scanned); match; match = term.exec(scanned)) {
    const from = Math.max(0, match.index - REJECTION_WINDOW);
    const to = Math.min(scanned.length, match.index + match[0].length + REJECTION_WINDOW);
    if (REJECTION_LANGUAGE.test(scanned.slice(from, to))) return true;
  }
  return false;
}

/**
 * Plan a single-rung downgrade for a rejected request: records the refusal (so later turns
 * clamp before dispatch) and returns the next lower rung the model does publish.
 */
export function planReasoningEffortDowngrade(args: {
  provider: OcxProviderConfig;
  modelId: string;
  requested?: string;
  rejectionText?: string;
}): { effort: string; recorded: boolean } | undefined {
  const requested = args.requested === "ultra" ? "max" : args.requested;
  if (!requested || !RANKED.includes(requested)) return undefined;
  const recorded = recordUnsupportedReasoningEffort(args.provider, args.modelId, requested, args.rejectionText);
  // Same precedence as configuredReasoningEfforts(): a hand-written ladder is a contract and
  // models.dev is only consulted when nothing was configured for this model. Reading metadata
  // first would have picked the downgrade rung off the published ladder even where a pinned
  // one disagreed, so the replay could land on a rung the registry deliberately excludes.
  const effective = sanitizeLadder(modelLadderValue(args.provider.modelReasoningEfforts, args.modelId))
    ?? sanitizeLadder(args.provider.reasoningEfforts)
    ?? metadataEffortValues(args.provider, args.modelId);
  const ladder = (effective ?? []).filter(effort => RANKED.includes(effort));
  if (ladder.length === 0) return undefined;
  const candidates = ladder
    .filter(effort => RANKED.indexOf(effort) < RANKED.indexOf(requested))
    .filter(effort => !isReasoningEffortLearnedUnsupported(args.provider, args.modelId, effort));
  if (candidates.length === 0) return undefined;
  return { effort: candidates[candidates.length - 1], recorded };
}

/**
 * Refresh the models.dev snapshot. Best-effort and idempotent: never throws, never blocks a
 * request, keeps the previous snapshot on failure. Ladders are stored for the gated destinations
 * only (OpenCode Zen + Zen Go: about 130 models), while every published provider `api` URL is
 * kept so the gate can be checked against real data and widened without another format change.
 * Non-reasoning models carry no ladder and are dropped.
 */
type RefreshOutcome = {
  ok: boolean;
  reason: string;
  providers?: number;
  models?: number;
};

/** Bound a caller's wait without cancelling the shared refresh job. */
function waitForRefresh(work: Promise<RefreshOutcome>, waitMs: number | undefined): Promise<RefreshOutcome> {
  if (waitMs === undefined) return work;
  if (!Number.isSafeInteger(waitMs) || waitMs < 0) {
    return Promise.resolve({ ok: false, reason: "invalid wait budget" });
  }
  let timer: ReturnType<typeof setTimeout> | undefined;
  const deadline = new Promise<RefreshOutcome>(resolve => {
    timer = setTimeout(() => resolve({ ok: false, reason: "wait budget exceeded" }), waitMs);
    timer.unref?.();
  });
  return Promise.race([work, deadline]).finally(() => {
    if (timer) clearTimeout(timer);
  });
}

export async function refreshReasoningMetadata(options: { force?: boolean; waitMs?: number } = {}): Promise<RefreshOutcome> {
  const snapshot = loadSnapshot();
  if (!options.force && snapshot && Date.now() - snapshot.fetchedAt <= CACHE_TTL_MS) {
    return { ok: true, reason: "fresh" };
  }
  if (refreshInFlight) {
    return waitForRefresh(refreshInFlight.then(() => ({ ok: true, reason: "coalesced" })), options.waitMs);
  }
  const job = (async () => {
    const response = await fetch(SOURCE_URL, {
      headers: { "user-agent": USER_AGENT, accept: "application/json" },
      // A hanging connection must not pin refreshInFlight for the life of the process.
      signal: AbortSignal.timeout(15_000),
    });
    if (!response.ok) throw new Error("models.dev HTTP " + response.status);
    const raw = await response.json() as Record<string, { api?: unknown; models?: Record<string, unknown> }>;
    const providers: MetadataSnapshot["providers"] = {};
    const apis: Record<string, string> = {};
    const gated = new Set(Object.values(BASE_URL_TO_METADATA_PROVIDER));
    let models = 0;
    for (const [providerKey, entry] of Object.entries(raw ?? {})) {
      const api = normalizeDestinationUrl(typeof entry?.api === "string" ? entry.api : undefined);
      if (api) apis[providerKey] = api;
      if (!gated.has(providerKey)) continue;
      const out: Record<string, ReasoningMetadataModel> = {};
      for (const [modelId, value] of Object.entries(entry?.models ?? {})) {
        const model = value as { reasoning?: unknown; reasoning_options?: unknown };
        if (model?.reasoning !== true) continue;
        const options: ReasoningMetadataOption[] = [];
        if (Array.isArray(model?.reasoning_options)) {
          for (const option of model.reasoning_options) {
            if (!option || typeof option !== "object") continue;
            const type = (option as { type?: unknown }).type;
            if (typeof type !== "string") continue;
            const values = (option as { values?: unknown }).values;
            options.push({
              type,
              ...(Array.isArray(values)
                ? { values: values.filter((v): v is string => typeof v === "string").slice(0, 12) }
                : {}),
            });
          }
        }
        out[modelId] = { reasoning: model?.reasoning === true, options };
        models += 1;
      }
      if (Object.keys(out).length === 0) continue;
      providers[providerKey] = out;
    }
    const next: MetadataSnapshot = { version: 2, fetchedAt: Date.now(), source: SOURCE_URL, providers, apis };
    atomicWriteFile(join(getConfigDir(), FILENAME), JSON.stringify(next) + "\n");
    snapshotMemo = next;
    publishForWorker("snapshot");
    return { ok: true, reason: "refreshed", providers: Object.keys(providers).length, models };
  })();
  refreshInFlight = job.catch(() => undefined).finally(() => { refreshInFlight = null; });
  const settled = job.catch((error): RefreshOutcome => ({
    ok: false,
    reason: error instanceof Error ? error.message : String(error),
  }));
  return waitForRefresh(settled, options.waitMs);
}

/**
 * Optional background refresh for callers that do not wait for a snapshot. Catalog sync uses
 * refreshReasoningMetadata with a bounded wait; an expired ladder read can request this refresh.
 * A classified toggle/budget model can have a fallback ladder without any snapshot, so this
 * path must never bootstrap a missing snapshot. One refresh per process; failures are ignored.
 */
export function ensureReasoningMetadataSnapshot(): void {
  const snapshot = loadSnapshot();
  if (!snapshot || Date.now() - snapshot.fetchedAt <= CACHE_TTL_MS) return;
  if (refreshInFlight) return;
  void refreshReasoningMetadata().catch(() => undefined);
}
