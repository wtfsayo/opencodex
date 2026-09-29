// What a Cloudflare container hands its Worker at startup so the Worker can serve turns as this
// process would. Every step is a no-op off Cloudflare.
import { CLAUDE_CODE_HEADERS, CLAUDE_CODE_RUNTIME_HEADERS } from "../adapters/client-fingerprint";
import { durableMirrorEnabled, stateRequest } from "../lib/durable-mirror";
import { resolveProxyRoute } from "../lib/proxy-env";
import { startWorkerUsageInbox } from "../usage/worker-usage-inbox";
import { loadConfig } from "../config";
import { realpathSync, watch, type FSWatcher } from "node:fs";
import { basename, dirname, join } from "node:path";
import { listCodexAccountIds } from "../codex/account-store";
import { getMainChatgptAccountId } from "../codex/auth-collision";
import { isMainAccountCredentialUsable } from "../codex/main-account";
import { resolveCodexHomeDir } from "../codex/home";
import { NATIVE_OPENAI_CONTEXT_OVERRIDES, nativeContextLimits } from "../codex/catalog/metadata";
import { NATIVE_OPENAI_CAPABILITY_ALIAS_MODELS, NATIVE_OPENAI_MODELS } from "../codex/catalog/native-models";
import { listModelMetadata } from "../generated/model-metadata";
import { OPENAI_CODEX_PROVIDER_ID } from "../providers/openai-tiers";
import { resolveInputCeiling } from "./responses/input-admission";
import { requiresVisionPreprocessing } from "../vision/plan";
import { routedProviderConfig } from "../router";
import { contextRelayActivated } from "../codex/context-compat";
import { getObservedMainQuotaIdentityKey, mainQuotaCredentialObserved, onMainQuotaCredentialChange } from "../codex/main-account-cache";
import { isNativeMainTrafficBlocked } from "../codex/native-profile-startup";
import { CODEX_RESPONSES_HTTP_URL, CODEX_RESPONSES_WS_URL } from "./responses/codex-ws-request";
import { shouldUseCodexWsUpstream } from "./responses/ws-upstream";
import type { NativeOpenAiFacts } from "./cloudflare-native-chat-api";
import { prefetchDurableSpendJournal } from "../lib/durable-spend-ledger";

// A Durable Object reset or a lease that moved drops what was published; publishing again on this
// cadence restores it. A failed first publish is retried sooner.
const REPUBLISH_MS = 5 * 60 * 1000;
const FIRST_RETRY_MS = 10_000;
let republish: ReturnType<typeof setInterval> | undefined;

/**
 * The Claude Code fingerprint headers that name this process's runtime. A Claude subscription turn
 * the Worker serves sends these, not the Worker runtime's, so the upstream sees the same headers.
 */
export function publishClientRuntimeForWorker(retrySoon = false): Promise<void> | undefined {
  // Only where the Worker serves requests (the Worker sets it; see containerEnv).
  if (process.env.OCX_WORKER_NATIVE_STATE !== "1" || !durableMirrorEnabled()) return undefined;
  const headers = Object.fromEntries(CLAUDE_CODE_RUNTIME_HEADERS.map(name => [name, CLAUDE_CODE_HEADERS[name]!]));
  return stateRequest("/client-runtime", {
    method: "PUT",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(headers),
  })?.then(response => { void response.body?.cancel(); return response.ok; }, () => false)
    .then(ok => { if (!ok && retrySoon) setTimeout(() => void publishClientRuntimeForWorker(), FIRST_RETRY_MS).unref?.(); });
}

/** Every native model id ocx has a context table or metadata row for. */
function nativeModelIds(): string[] {
  const ids = new Set<string>([...NATIVE_OPENAI_MODELS, ...NATIVE_OPENAI_CAPABILITY_ALIAS_MODELS, ...Object.keys(NATIVE_OPENAI_CONTEXT_OVERRIDES)]);
  for (const catalog of ["openai-codex", "openai"]) for (const row of listModelMetadata(catalog)) ids.add(row.id);
  return [...ids].filter(id => /^gpt-/.test(id));
}

/** request-prepare.ts's ceiling for the canonical `openai` row, per native model id. */
function inputCeilings(): Record<string, number | null> {
  const config = loadConfig();
  const provider = config.providers?.[OPENAI_CODEX_PROVIDER_ID];
  const ceilings: Record<string, number | null> = {};
  if (!provider) return ceilings;
  const limits = nativeContextLimits(config);
  for (const id of nativeModelIds()) ceilings[id] = resolveInputCeiling(provider, OPENAI_CODEX_PROVIDER_ID, id, limits);
  return ceilings;
}

/** ws-upstream.ts's choice for a streamed turn to the ChatGPT backend, in this process. */
function upstreamTransport(): NativeOpenAiFacts["upstreamTransport"] {
  const websocket = shouldUseCodexWsUpstream(CODEX_RESPONSES_HTTP_URL, { method: "POST", body: "{\"stream\":true}" });
  const route = resolveProxyRoute(new URL(websocket ? CODEX_RESPONSES_WS_URL : CODEX_RESPONSES_HTTP_URL));
  if (route.kind !== "direct") return "proxied";
  return websocket ? "websocket" : "sse";
}

function mainAccountIdentityKey(): string | null {
  return mainQuotaCredentialObserved() ? getObservedMainQuotaIdentityKey() ?? null : null;
}

/** vision/plan.ts's answer for every model a configured provider lists, as request-prepare.ts asks it. */
function visionPreprocessed(): Record<string, boolean> {
  const config = loadConfig();
  const answers: Record<string, boolean> = {};
  for (const [name, row] of Object.entries(config.providers ?? {})) {
    if (!row || !Array.isArray(row.models)) continue;
    for (const id of row.models) {
      if (typeof id !== "string" || !id) continue;
      // As request-prepare.ts asks it: of the provider it routes with, registry policy merged in.
      let routed;
      try { routed = routedProviderConfig(name, row); } catch { continue; }
      answers[`${name}/${id}`] = requiresVisionPreprocessing(config, routed, id, name);
    }
  }
  return answers;
}

let factsVersion = Date.now();
// Computed on the publish cadence, off any request: the catalog tables and config read are not free.
let cachedCeilings: Record<string, number | null> | undefined;
let cachedVision: Record<string, boolean> | undefined;
let publishedMainKey: string | null | undefined;
let publishedMainLoginPresent: boolean | undefined;
let acknowledgedVersion = 0;
let publishScheduled = false;

export function nativeOpenAiFacts(refreshCeilings = false): NativeOpenAiFacts {
  if (refreshCeilings || !cachedCeilings) cachedCeilings = inputCeilings();
  if (refreshCeilings || !cachedVision) cachedVision = visionPreprocessed();
  return {
    version: ++factsVersion,
    mainAccountIdentityKey: mainAccountIdentityKey(),
    codexAccountsStored: listCodexAccountIds().length > 0,
    mainCodexLoginPresent: mainCodexLoginPresent(),
    nativeMainTrafficBlocked: isNativeMainTrafficBlocked(),
    contextRelayActive: contextRelayActivated(),
    upstreamTransport: upstreamTransport(),
    inputCeilings: cachedCeilings,
    visionPreprocessed: cachedVision,
  };
}

/**
 * What a ChatGPT passthrough turn reads from this process (NativeOpenAiFacts). Published at start,
 * when the observed main account changes, and on the republish cadence for the rest.
 */
export function publishNativeOpenAiFactsForWorker(retrySoon = false, refreshCeilings = false): Promise<void> | undefined {
  if (process.env.OCX_WORKER_NATIVE_STATE !== "1" || !durableMirrorEnabled()) return undefined;
  let facts: NativeOpenAiFacts;
  try {
    facts = nativeOpenAiFacts(refreshCeilings);
  } catch (error) {
    console.warn(`[opencodex] Worker facts not published: ${error instanceof Error ? error.name : "error"}`);
    return undefined;
  }
  return stateRequest("/native-openai-facts", {
    method: "PUT",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(facts),
  })?.then(response => { void response.body?.cancel(); return response.ok; }, () => false)
    .then(ok => {
      // Only a stored answer counts as published; a main account the Worker has not heard of yet
      // is retried until it has, since a caller holding it would otherwise skip ocx's limits.
      // The hub keeps only a newer version, so only a newer acknowledgement says what it holds.
      if (ok && facts.version > acknowledgedVersion) {
        acknowledgedVersion = facts.version;
        publishedMainKey = facts.mainAccountIdentityKey;
        publishedMainLoginPresent = facts.mainCodexLoginPresent;
      }
      else if (retrySoon || facts.mainAccountIdentityKey !== publishedMainKey) {
        setTimeout(() => void publishNativeOpenAiFactsForWorker(retrySoon), FIRST_RETRY_MS).unref?.();
      }
    });
}

/** Any main login ocx could search with, a refresh token alone included (main-account.ts). */
function mainCodexLoginPresent(): boolean {
  return isMainAccountCredentialUsable() || getMainChatgptAccountId() !== null;
}

/** Republish once, after whatever triggered it, when `changed` still says the answer moved. */
function scheduleRepublish(changed: () => boolean): void {
  if (publishScheduled || !changed()) return;
  publishScheduled = true;
  setTimeout(() => {
    publishScheduled = false;
    if (changed()) void publishNativeOpenAiFactsForWorker(true);
  }, 0).unref?.();
}

/** A main-credential observation: republish only if the observed account changed. */
function mainCredentialChanged(): void {
  scheduleRepublish(() => mainAccountIdentityKey() !== publishedMainKey);
}

/**
 * The main Codex login is written by several owners (login, refresh, profile switches), so its
 * file is watched rather than each writer: a login appearing must reach the Worker before it drops
 * a web search ocx would now run with it. That still leaves one publish round trip after a write.
 * A CODEX_HOME that does not exist yet is watched once it does (armed again on the cadence), and a
 * symlinked auth.json is watched where it lives.
 */
let loginWatchers: FSWatcher[] = [];
function watchMainCodexLogin(): void {
  if (loginWatchers.length > 0) return;
  const loginChanged = () => scheduleRepublish(() => mainCodexLoginPresent() !== publishedMainLoginPresent);
  const home = resolveCodexHomeDir();
  const directories = new Set([home]);
  try { directories.add(dirname(realpathSync(join(home, "auth.json")))); } catch { /* no login file yet */ }
  for (const directory of directories) {
    try {
      const watcher = watch(directory, (_event, file) => {
        if (file === null || file === "auth.json" || basename(String(file)) === "auth.json") loginChanged();
      });
      watcher.on("error", () => {
        watcher.close();
        loginWatchers = loginWatchers.filter(existing => existing !== watcher);
      });
      watcher.unref?.();
      loginWatchers.push(watcher);
    } catch {
      // Not there yet: the republish cadence arms it again.
    }
  }
}

export function startWorkerNativeState(): void {
  startWorkerUsageInbox();
  const published = publishClientRuntimeForWorker(true);
  void publishNativeOpenAiFactsForWorker(true, true);
  if (published && !republish) {
    onMainQuotaCredentialChange(mainCredentialChanged);
    watchMainCodexLogin();
    republish = setInterval(() => {
      watchMainCodexLogin();
      void publishClientRuntimeForWorker();
      void publishNativeOpenAiFactsForWorker(false, true);
    }, REPUBLISH_MS);
    republish.unref?.();
  }
}
