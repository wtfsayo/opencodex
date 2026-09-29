// Phase 4a: streamed Chat Completions answered in the Worker for the simplest routes, so the turn
// skips the Worker -> Durable Object -> container -> ocx hop. It reuses ocx's own request builder
// and SSE relay; everything around them (routing, eligibility) is a deliberately narrow subset of
// ocx's, and any request or config this does not fully understand returns null so the Worker
// forwards it untouched to the container, which stays the reference implementation.
//
// It runs in the Cloudflare Worker (deploy/cloudflare/src/index.ts, which reaches it through
// cloudflare-native.ts and typechecks against ./cloudflare-native-chat-api.ts). Its
// import graph is held free of Bun, node:fs and friends by tests/service/cloudflare-worker-native.test.ts.
import { createHash } from "node:crypto";
import type { NativeChatDeps, NativeOAuthCredential, ServeNativeChat, WorkerUsageRow } from "./cloudflare-native-chat-api";
import { isOAuthRefreshRejection } from "./cloudflare-native-chat-api";
import { workerSpendTurn, type WorkerSpendTurn } from "./cloudflare-native-spend";
import { buildOpenAIChatPassthroughRequest } from "../adapters/openai-chat/passthrough";
import { jsonCompletionSse, nativeChatSse, structuredError, usageFromChat } from "./chat-native-sse";
import { chatCompletionsErrorResponse, collectChatCompletion, isChatCompletionsStreamError } from "../chat/outbound";
import { redactSecretString } from "../lib/redact";
import { fastPolicyForModel } from "../providers/service-tier";
import { chatCollabSurface, isThreadSpawnRequest } from "./collab-surface";
import { createTranslatorBudget } from "../lib/translator-budget";
import { estimateTokens } from "../lib/token-estimate";
import type { OcxConfig, OcxProviderConfig, OcxUsage } from "../types";
import { PROVIDER_REGISTRY } from "../providers/registry";
import { resolveOpenCodeGoTransport } from "../providers/opencode-go-transport";
import { getOrAllocateRequestSessionLane } from "./request-log-conversation";
import { apiKeyAccountLogLabel } from "../codex/key-account-label";
import { stampOAuthAccountLabel } from "../providers/label";
import { routedProviderConfigWith } from "../providers/routed-provider-config";
import { deriveOAuthProviderConfig } from "../providers/derive";

type Rec = Record<string, unknown>;
const isRec = (value: unknown): value is Rec => !!value && typeof value === "object" && !Array.isArray(value);

// Provider fields whose meaning this path reproduces exactly. Any other field (headers, key pools,
// aliases, per-model wire overrides, reasoning or capability gates) sends the turn to the container.
const PROVIDER_FIELDS = new Set(["adapter", "baseUrl", "apiKey", "models", "authMode"]);
// Top-level config keys that cannot change how ocx admits, routes, shapes or sends a Chat
// Completions turn. Any other key present sends the turn to the container: a denylist would miss
// the next section that does (blockedModelRedirects, apiSurfaces and maxInboundBodyBytes were such).
const CONFIG_KEYS = new Set([
  "port", "hostname", "runtimeRole", "hub", "fastRows", "providers", "defaultProvider", "stallTimeoutSec", "usageLedgerMaxBytes",
  "managementUsageMaxReadBytes", "configRebaseProvenance", "oauthOpenBrowser",
  "codexAutoStart", "codexProviderDisplayName", "codexShimAutoRestore", "codexQuotaAutoRefresh",
  "codexAccountPickerEnabled", "catalogAutoRefresh", "quotaResetNotify", "remoteGui", "metricsExport",
  "openaiProviderTierVersion", "googleAntigravityStaticCatalogVersion", "subagentModelsVersion",
  "multiAgentSurfaceAdvisoryVersion", "apiKeys", "subagentModels",
  // Caps the retained state (logs, caches, continuations) of the container process; no module on
  // the native chat lane consults it, so it shapes the container's load, not this turn.
  "appOwnedMemoryBudgetMb",
  // Only opens the Responses WebSocket transport, whose frames run as these same turns.
  "websockets",
  // Read only by the anthropic adapter, which the turn paths build with it as ocx does.
  "cacheRetention",
  // The durable spend ceilings (spend-reservation-core.ts): with them the native paths reserve
  // against the hub Durable Object's ledger before a send, as request-spend.ts does locally.
  "spend",
  // On a request path only effort-policy.ts reads it, to exempt turns from effort caps, and no
  // config this path admits sets a cap.
  "multiAgentMode",
]);
// Keys `ocx init` writes (getDefaultConfig) at the value that behaves as if unset; any other value
// changes the turn (a retried empty completion, no multi-agent guidance) and declines it.
const CONFIG_DEFAULTS: Readonly<Record<string, unknown>> = {
  emptyCompletionRetry: false,
  dropCodexSafetyBuffering: false,
  multiAgentGuidanceEnabled: true,
};
export const configKeyAdmitted = (config: Record<string, unknown>, key: string) =>
  CONFIG_KEYS.has(key) || (Object.hasOwn(CONFIG_DEFAULTS, key) && config[key] === CONFIG_DEFAULTS[key]);
const RESERVED_NAMESPACES = new Set(["policy", "combo"]);
// chat-native.ts: config.connectTimeoutMs ?? 200_000; a config that sets it is declined.
const HEADER_TIMEOUT_MS = 200_000;
// ocx refuses provider destinations on private, loopback and metadata addresses. Its check resolves
// DNS, which the Worker cannot do the same way, so this path goes further: public https hosts by
// name only, plus the hosts the Worker answers itself.
const PRIVATE_HOST = /^(localhost|.*\.(localhost|local|lan|internal|home|corp))$/i;
const IP_LITERAL = /^(\d{1,3}(\.\d{1,3}){3}|\[[0-9a-f:.]+\])$/i;

function destinationAllowed(baseUrl: string, localHosts: ReadonlySet<string>): boolean {
  let url: URL;
  try { url = new URL(baseUrl); } catch { return false; }
  if (localHosts.has(url.host)) return true;
  // ocx strips trailing dots before judging a host name (src/lib/destination-policy.ts), and
  // `localhost.` is still localhost.
  const hostname = url.hostname.replace(/\.+$/, "");
  return url.protocol === "https:" && !url.username && !url.password && !PRIVATE_HOST.test(hostname) && !IP_LITERAL.test(hostname);
}

/**
 * ocx's resolveEnvValue (src/config/proxy-env.ts) over the environment the container would have:
 * `${NAME}` or `$NAME` names a variable, anything else is the key itself.
 */
function resolveKeyReference(value: string, secrets: Readonly<Record<string, string>>): string | undefined {
  const name = /^\$\{(\w+)\}$/.exec(value)?.[1] ?? (value.startsWith("$") ? value.slice(1) : undefined);
  if (name === undefined) return value;
  return Object.prototype.hasOwnProperty.call(secrets, name) ? secrets[name] : undefined;
}

const CHAT_ADAPTERS: ReadonlySet<string> = new Set(["openai-chat"]);
// Built-in providers whose OAuth login the Worker uses, and the saved-row fields it reproduces
// whatever their value. Any other field must hold what `ocx login` writes (upsertOAuthProvider);
// routed-provider-config.ts merges the registry's own values over those anyway.
const OAUTH_PROVIDERS = new Set(["anthropic"]);
const OAUTH_ROW_FIELDS = new Set(["adapter", "baseUrl", "authMode"]);
// The dashboard's model picker state, which no request path reads (ocx keeps a login's older
// defaultModel too while the catalog still lists it).
const OAUTH_PICKER_FIELDS = new Set(["selectedModels", "modelPreset", "newModelPolicy", "initialModelSelection"]);
// oauth/index.ts's REFRESH_SKEW_MS: ocx refreshes a token this close to expiry before sending. A
// token this old no longer sends as-is; the caller may refresh it (deps.oauthRefresh, through the
// hub's lease) or decline oauth-refresh-due for ocx to do it.
const OAUTH_REFRESH_SKEW_MS = 60_000;
// store.ts's credentialGeneration: the hash the hub's refresh CAS compares.
function oauthCredentialGeneration(credential: { refresh: string; access: string; expires: number }): string {
  return createHash("sha256").update(JSON.stringify([credential.refresh, credential.access, credential.expires])).digest("hex");
}
// Providers whose token endpoint a Worker can reach (chatgpt.com's backend is blocked, but
// auth.openai.com is not) and whose refresh shape the deploy layer implements.
const WORKER_REFRESHABLE_PROVIDERS: Record<string, true> = { anthropic: true, chatgpt: true };
export type NativeOAuthRefreshDue = {
  providerName: string;
  accountId: string;
  generation: string;
  credential: NativeOAuthCredential;
};
export type OAuthRefreshDueCallback = (due: NativeOAuthRefreshDue) => Promise<unknown>;
const PLAIN_MODEL_ID = /^[A-Za-z0-9][A-Za-z0-9._-]*$/;
const NO_LOCAL_HOSTS: ReadonlySet<string> = new Set();

/** JSON with object keys sorted, so two values compare equal whatever order they were written in. */
function canonicalJson(value: unknown): string {
  return JSON.stringify(value, (_key, inner: unknown) => isRec(inner)
    ? Object.fromEntries(Object.keys(inner).sort().map(key => [key, inner[key]]))
    : inner);
}

/**
 * A built-in OAuth provider's route as ocx takes it with its account pool off: the registry's
 * transport merged over the saved row (routed-provider-config.ts), the active account's access token
 * while it has more than a minute left, and that account's usage label. ocx turns on quota tracking
 * and failover once two accounts can serve (hasAnthropicFailoverQuorum), so exactly one is required.
 */
async function resolveOAuthRoute(
  providerName: string, row: unknown, model: string, modelId: string, localHosts: ReadonlySet<string>,
  authStore: unknown, no: (reason: string) => null,
  /** Called in place of the refresh-due decline when the caller can rotate the token. */
  oauthRefreshDue?: OAuthRefreshDueCallback,
): Promise<NativeChatRoute | null> {
  if (!isRec(row)) return no("provider-shape");
  const preset = deriveOAuthProviderConfig(providerName) as Record<string, unknown> | undefined;
  const unknownField = Object.keys(row).find(key => !OAUTH_ROW_FIELDS.has(key) && !OAUTH_PICKER_FIELDS.has(key)
    // An explicit `anthropic/<id>` routes past defaultModel, which only adds a known id; one with a
    // slash could match the whole selector (router.ts), so only a plain id is left out of it.
    && !(key === "defaultModel" && typeof row[key] === "string" && PLAIN_MODEL_ID.test(row[key]))
    && (!preset || !Object.hasOwn(preset, key) || canonicalJson(row[key]) !== canonicalJson(preset[key])));
  if (unknownField) return no(`provider-field:${unknownField}`);
  if (row.authMode !== "oauth") return no("auth-mode");
  // An explicit namespace routes the id verbatim; anything the slug codec would decode is ocx's.
  if (!PLAIN_MODEL_ID.test(modelId)) return no("model-id");
  let provider: OcxProviderConfig;
  try {
    provider = routedProviderConfigWith(providerName, row as unknown as OcxProviderConfig, {
      resolveApiKey: () => undefined,
      // Called with the final base URL. Never a host the Worker answers itself, whatever its name
      // looks like (sendUpstream answers those locally): the route carries the account's token.
      assertDestinationAllowed: (_name, target) => {
        if (typeof target.baseUrl !== "string" || !destinationAllowed(target.baseUrl, NO_LOCAL_HOSTS)
          || localHosts.has(new URL(target.baseUrl).host)) throw new Error("destination");
      },
      warnBaseUrlDiscarded: () => {},
    });
  } catch {
    return no("destination");
  }
  if (provider.adapter !== "anthropic" || provider.authMode !== "oauth") return no("adapter");
  const accountSet = isRec(authStore) ? authStore[providerName] : undefined;
  if (!isRec(accountSet) || !Array.isArray(accountSet.accounts)) return no("oauth-no-login");
  if (accountSet.accounts.length !== 1) return no("oauth-account-pool");
  const account = accountSet.accounts[0];
  // oauth/store.ts's normalizeAccount: an account it would drop is no login at all to ocx.
  if (!isRec(account) || typeof account.id !== "string" || account.id === "" || account.id !== accountSet.activeAccountId
    || !isRec(account.credential)) return no("oauth-account-shape");
  const { access, refresh, expires } = account.credential;
  if (typeof access !== "string" || access.trim() === "" || typeof refresh !== "string" || typeof expires !== "number") {
    return no("oauth-account-shape");
  }
  // ocx refuses an account marked for a fresh login when it commits the selection
  // (commitOAuthAccountSelection with requireUsableAccount, request-transport.ts).
  if (account.needsReauth === true) return no("oauth-needs-reauth");
  if (expires <= Date.now() + OAUTH_REFRESH_SKEW_MS) {
    if (!oauthRefreshDue || !WORKER_REFRESHABLE_PROVIDERS[providerName]) return no("oauth-refresh-due");
    const refreshed = await oauthRefreshDue({
      providerName,
      accountId: account.id as string,
      generation: oauthCredentialGeneration({ refresh, access, expires }),
      credential: { ...(account.credential as NativeOAuthCredential) },
    });
    if (refreshed === undefined) return no("oauth-refresh-due");
    return resolveOAuthRoute(providerName, row, model, modelId, localHosts, refreshed, no);
  }
  return { providerName, provider: { ...provider, apiKey: access }, modelId, requestedModel: model, apiKeyReference: "", oauthAccountId: account.id as string };
}
export const TURN_ADAPTERS: ReadonlySet<string> = new Set(["openai-chat", "anthropic"]);

export type NativeChatRoute = {
  providerName: string;
  provider: OcxProviderConfig;
  modelId: string;
  requestedModel: string;
  /** The key as configured (a literal or a `${NAME}` reference), which ocx's usage label digests. */
  apiKeyReference: string;
  /** Set for an OAuth login: the account whose token the route carries. */
  oauthAccountId?: string;
};

/**
 * The route ocx would pick for `model`, or null when this path cannot be sure it matches. Only
 * `<provider>/<model>` with an exact configured provider name and an exactly listed model qualifies.
 */
export async function resolveNativeChatRoute(
  config: unknown,
  model: unknown,
  localHosts: ReadonlySet<string> = new Set(),
  why: (reason: string) => void = () => {},
  secrets: Readonly<Record<string, string>> = {},
  // The chat lane forwards the body itself; turns run through an adapter also take anthropic.
  adapters: ReadonlySet<string> = CHAT_ADAPTERS,
  /** The Durable Object's copy of auth.json, for a provider logged in with OAuth. */
  authStore?: unknown,
  /**
   * Called when a login's token is inside its refresh window; the replacement auth store it
   * returns re-resolves the route, undefined keeps the oauth-refresh-due decline.
   */
  oauthRefreshDue?: OAuthRefreshDueCallback,
): Promise<NativeChatRoute | null> {
  const no = (reason: string) => { why(reason); return null; };
  if (!isRec(config) || !isRec(config.providers) || typeof model !== "string") return no("config-or-model-shape");
  const unknownKeys = Object.keys(config).filter(key => !configKeyAdmitted(config, key)).sort();
  if (unknownKeys.length > 0) return no(`config-keys:${unknownKeys.join(",")}`);
  const slash = model.indexOf("/");
  if (slash <= 0) return no("model-without-provider");
  const providerName = model.slice(0, slash);
  const modelId = model.slice(slash + 1);
  if (RESERVED_NAMESPACES.has(providerName) || !modelId) return no("reserved-namespace");
  // `--` is the separator of ocx's synthetic Fast and effort rows (src/server/fast-row.ts), which
  // it resolves before routing; a listed id containing it is left to ocx to disambiguate.
  if (modelId.includes("--")) return no("synthetic-row-grammar");
  if (!Object.prototype.hasOwnProperty.call(config.providers, providerName)) return no("unknown-provider");
  // ocx replaces a built-in provider's transport (its baseUrl among it) with the registry's, so a
  // configured URL on such a provider is not where ocx would send the key.
  if (PROVIDER_REGISTRY.some(entry => entry.id === providerName)) {
    if (!OAUTH_PROVIDERS.has(providerName) || !adapters.has("anthropic")) return no("built-in-provider");
    return resolveOAuthRoute(providerName, config.providers[providerName], model, modelId, localHosts, authStore, no, oauthRefreshDue);
  }
  const provider = config.providers[providerName];
  if (!isRec(provider)) return no("provider-shape");
  const unknownField = Object.keys(provider).find(key => !PROVIDER_FIELDS.has(key));
  if (unknownField) return no(`provider-field:${unknownField}`);
  if (typeof provider.adapter !== "string" || !adapters.has(provider.adapter)) return no("adapter");
  if (provider.authMode !== undefined && provider.authMode !== "key") return no("auth-mode");
  if (typeof provider.baseUrl !== "string" || !destinationAllowed(provider.baseUrl, localHosts)) return no("destination");
  if (typeof provider.apiKey !== "string" || provider.apiKey.startsWith("keychain:")) return no("key-reference");
  const apiKey = resolveKeyReference(provider.apiKey, secrets);
  if (!apiKey) return no("key-reference-unset");
  const models = provider.models;
  if (!Array.isArray(models) || !models.includes(modelId) || models.includes(model)) return no("model-not-listed");
  return { providerName, provider: { ...provider, apiKey } as unknown as OcxProviderConfig, modelId, requestedModel: model, apiKeyReference: provider.apiKey };
}

/**
 * The route with OpenCode Go's session header when its destination is Go, as ocx adds it
 * (opencode-go-transport.ts). `lane` defaults to ocx's own choice for a request with these
 * headers: the caller's session identity, else a value for this request alone.
 */
export function withOpenCodeGoSession(route: NativeChatRoute, headers: Headers, lane?: string): NativeChatRoute {
  const sessionLane = lane ?? getOrAllocateRequestSessionLane(new Request("http://worker.invalid/", { headers }));
  return { ...route, provider: resolveOpenCodeGoTransport(route.provider, sessionLane, route.provider) };
}

/** Whether a config has only the keys this path reproduces; any other key declines every turn. */
export function nativeConfigAdmitted(config: unknown): boolean {
  return isRec(config) && isRec(config.providers) && Object.keys(config).every(key => configKeyAdmitted(config, key));
}

/** The usage-row fields ocx fills from the route (providers/label.ts labels the key). */
export function routeUsageFields(route: NativeChatRoute): Pick<WorkerUsageRow, "provider" | "model" | "requestedModel" | "accountLogLabel"> {
  let accountLogLabel: string | undefined;
  if (route.oauthAccountId !== undefined) {
    // providers/label.ts: ocx labels OAuth accounts of some providers only.
    const stamp: { accountLogLabel?: string } = {};
    stampOAuthAccountLabel(stamp, route.providerName, route.provider, route.oauthAccountId);
    accountLogLabel = stamp.accountLogLabel;
  } else {
    accountLogLabel = apiKeyAccountLogLabel(route.providerName, { reference: route.apiKeyReference });
  }
  return {
    provider: route.providerName,
    model: route.modelId,
    requestedModel: route.requestedModel,
    ...(accountLogLabel ? { accountLogLabel } : {}),
  };
}

/** The request fields ocx's native Chat lane refuses or reroutes, plus anything carrying an image. */
export function nativeChatBodyEligible(body: Rec): boolean {
  if (body.stream !== undefined && typeof body.stream !== "boolean") return false;
  if (body.store === true || body.background === true) return false;
  if (body.previous_response_id !== undefined || body.compaction_trigger !== undefined) return false;
  // ocx answers an empty conversation itself with a 400 (src/chat/inbound.ts).
  if (!Array.isArray(body.messages) || body.messages.length === 0) return false;
  for (const message of body.messages) {
    if (!isRec(message)) return false;
    if (Array.isArray(message.content) && message.content.some(part => !isRec(part) || part.type !== "text")) return false;
  }
  if (body.tools !== undefined) {
    if (!Array.isArray(body.tools)) return false;
    if (body.tools.some(tool => !isRec(tool) || tool.type !== "function")) return false;
  }
  return true;
}

/** The auth store copy, read only when the model names a provider the Worker may use a login of. */
export async function oauthStoreForNativeChat(model: unknown, deps: Pick<NativeChatDeps, "readAuth">): Promise<unknown> {
  if (typeof model !== "string" || !model.startsWith("anthropic/") || !deps.readAuth) return undefined;
  const text = await deps.readAuth();
  try { return text ? JSON.parse(text) : undefined; } catch { return undefined; }
}

/**
 * Rotates an OAuth credential through the hub's arbiter (deps.oauthRefresh): lease the stored
 * generation, spend the refresh token, commit under CAS. Returns the auth store the caller must
 * re-resolve against — the commit itself on success, a re-read after a conflict (someone else
 * rotated first; retried at most once), or undefined when the credential should be left to ocx:
 * the lease was busy, the attempt lost its lease, the provider call failed, or the arbiter was
 * unreachable. Declining is always safe; spending a rotating token twice never is.
 */
export async function refreshNativeOAuthCredential(
  due: NativeOAuthRefreshDue,
  deps: Pick<NativeChatDeps, "oauthRefresh" | "readAuth">,
): Promise<unknown> {
  const arbiter = deps.oauthRefresh;
  if (!arbiter || !deps.readAuth) return undefined;
  const accountSetOf = (store: unknown): Rec | undefined => {
    const set = isRec(store) ? store[due.providerName] : undefined;
    const accounts = isRec(set) && Array.isArray(set.accounts) ? set.accounts as unknown[] : undefined;
    const account = accounts?.find(candidate => isRec(candidate) && candidate.id === due.accountId);
    return isRec(account) && isRec(account.credential) ? account : undefined;
  };
  let generation = due.generation;
  let refresh = due.credential.refresh;
  for (let attempt = 0; attempt < 2; attempt++) {
    const lease = await arbiter.acquire(due.providerName, due.accountId, generation).catch(() => null);
    if (lease === null) return undefined;
    let credential: NativeOAuthCredential;
    try {
      credential = await arbiter.refreshToken(due.providerName, refresh);
    } catch (error) {
      // An answered rejection is safe to abandon early; anything else may already have rotated
      // the token server-side, so the lease stays until its TTL as the ambiguity guard.
      if (isOAuthRefreshRejection(error)) await arbiter.release(due.providerName, due.accountId, lease.attemptId).catch(() => {});
      return undefined;
    }
    const outcome = await arbiter.commit(due.providerName, due.accountId, credential, generation, lease.attemptId)
      .catch(() => ({ conflict: "lease" as const }));
    if ("ok" in outcome) {
      // Callers re-resolve against the parsed store, like oauthStoreForNativeChat returns.
      const text = await deps.readAuth().catch(() => undefined);
      try { return text !== undefined ? JSON.parse(text) : undefined; } catch { return undefined; }
    }
    if (attempt > 0) return undefined;
    if (outcome.conflict === "lease") {
      // The lease row died or went to another attempt: whatever is stored now is not ours.
      return undefined;
    }
    // generation / committed-generation: someone rotated meanwhile. Adopt their credential by
    // re-reading the store; if it is still inside the window (committed-generation can race a
    // slower sibling rotation), loop once with its generation rather than spend again.
    const reRead = await deps.readAuth().catch(() => undefined);
    let parsed: unknown;
    try { parsed = reRead !== undefined ? JSON.parse(reRead) : undefined; } catch { return undefined; }
    const account = accountSetOf(parsed);
    if (!account) return undefined;
    const stored = account.credential;
    if (!isRec(stored)
      || typeof stored.refresh !== "string"
      || typeof stored.access !== "string"
      || typeof stored.expires !== "number") return undefined;
    const next = { refresh: stored.refresh, access: stored.access, expires: stored.expires };
    if (next.expires > Date.now() + OAUTH_REFRESH_SKEW_MS) return parsed;
    generation = oauthCredentialGeneration(next);
    refresh = next.refresh;
  }
  return undefined;
}

/**
 * Serves the turn, or returns null to hand it to the container. `bodyText` is the request body,
 * already read, so a decline can still forward it. An upstream error also declines, before any
 * byte reaches the client: the container's lane owns retries, key failover and error shaping.
 */
export const serveNativeChat: ServeNativeChat = async (bodyText, headers, signal, deps) => {
  const startedAt = Date.now();
  let body: unknown;
  const no = (reason: string) => { deps.onDecline?.(reason); return null; };
  try { body = JSON.parse(bodyText); } catch { return no("body-not-json"); }
  if (!isRec(body) || !nativeChatBodyEligible(body)) return no("body-ineligible");
  // ocx caps reasoning effort on collaboration turns (effortCapAppliesTo in effort-policy.ts).
  if (chatCollabSurface(body) !== null || isThreadSpawnRequest(headers)) return no("collaboration-turn");
  const configText = await deps.readConfig();
  if (!configText) return no("no-config-copy");
  let config: unknown;
  try { config = JSON.parse(configText); } catch { return no("config-not-json"); }
  const authStore = await oauthStoreForNativeChat(body.model, deps);
  const resolved = await resolveNativeChatRoute(
    config, body.model, new Set(Object.keys(deps.localHosts ?? {})), no, deps.secrets,
    CHAT_ADAPTERS, authStore, due => refreshNativeOAuthCredential(due, deps),
  );
  if (!resolved) return null;
  const route = withOpenCodeGoSession(resolved, headers);

  const requestedStream = body.stream === true;
  // The same Fast policy arguments chat-native.ts passes; a config with fastMode is declined.
  const request = buildOpenAIChatPassthroughRequest(
    route.provider, body, route.modelId, requestedStream,
    fastPolicyForModel(route.provider, route.modelId, route.providerName, "chat"),
  );
  const translatorBudget = createTranslatorBudget();
  let usage: OcxUsage | undefined;
  let firstOutputAt: number | undefined;
  let terminalStatus: number | undefined;
  // chat-completions.ts's spend reservation for a native Chat send: an estimate of the messages and
  // tools, and the caller's output ceiling.
  const spendParts = [JSON.stringify(body.messages ?? [])];
  if (body.tools !== undefined) spendParts.push(JSON.stringify(body.tools));
  const spendInputTokens = Math.max(1, estimateTokens(spendParts.join("\n"), body.model as string));
  const outputCeiling = body.max_completion_tokens ?? body.max_tokens;
  const spendOutputCeilingTokens = typeof outputCeiling === "number" && outputCeiling > 0 ? Math.trunc(outputCeiling) : 0;
  const usageFields = routeUsageFields(route);
  // request-spend.ts's tracker for this lane (cloudflare-native-spend.ts): reserve against the
  // hub ledger before the send, settle from the terminal usage row. Under a configured ceiling
  // a hub without the ledger declines rather than spend unbooked.
  const spend = workerSpendTurn(config, deps, {
    rootId: headers.get("x-codex-parent-thread-id")?.trim() || undefined,
    identityId: usageFields.accountLogLabel,
    poolId: usageFields.provider,
  });
  if (spend === "declined") return no("spend-ledger-unavailable");
  const record = (status: number) => {
    spend?.resolve(usage ? { inputTokens: usage.inputTokens, outputTokens: usage.outputTokens } : undefined);
    deps.recordUsage?.({
      spendInputTokens,
      ...(spend ? { spendLedger: "worker" as const } : {}),
      ...(spendOutputCeilingTokens > 0 ? { spendOutputCeilingTokens } : {}),
      requestId: crypto.randomUUID(),
      timestamp: startedAt,
      ...routeUsageFields(route),
      ...(typeof body.reasoning_effort === "string" ? { requestedEffort: body.reasoning_effort } : {}),
      inboundProtocol: "chat",
      admissionKind: "environment",
      status,
      durationMs: Date.now() - startedAt,
      ...(firstOutputAt !== undefined ? { firstOutputMs: firstOutputAt - startedAt } : {}),
      usageStatus: usage ? "reported" : "unreported",
      ...(usage ? { usage: usage as NonNullable<WorkerUsageRow["usage"]>, totalTokens: usage.totalTokens ?? usage.inputTokens + usage.outputTokens } : {}),
    });
  };
  if (spend) {
    const admission = await spend.charge(spendInputTokens, spendOutputCeilingTokens);
    if (!admission.ok) {
      record(429);
      return admission.refusal;
    }
  }
  let upstream: Response;
  try {
    upstream = await sendUpstream(request, signal, deps);
  } catch (error) {
    // The send may have left before it failed: unresolved spend, the same honesty the
    // container's tracker gives a lost usage frame.
    spend?.resolve(undefined);
    throw error;
  }
  if (!upstream.ok || !upstream.body) {
    spend?.resolve(undefined);
    await upstream.body?.cancel();
    return no(`upstream-${upstream.status}`);
  }
  const fail = (status: number, message: string, type?: string, code?: string | null) => {
    record(status);
    return chatCompletionsErrorResponse(status, redactSecretString(message), type, code);
  };

  // The rest follows chat-native.ts from the upstream response on. The request has been sent, so
  // from here an error is answered, not handed to the container, which would send it again.
  if ((upstream.headers.get("content-type") ?? "").includes("text/event-stream")) {
    const stream = nativeChatSse(upstream.body, {
      requestedModel: route.requestedModel,
      translatorBudget,
      signal,
      stallTimeoutSec: (config as Pick<OcxConfig, "stallTimeoutSec">).stallTimeoutSec,
      onFirstOutput: () => { firstOutputAt ??= Date.now(); },
      onUsage: reported => { usage = reported; },
      onTerminal: status => { terminalStatus = status; },
      // The relay closes normally when the client goes away, so the cancel is only visible here.
      onCancel: () => { terminalStatus ??= 499; },
    });
    // As chat-native.ts answers; Connection is hop-by-hop and the Workers runtime owns it.
    if (requestedStream) {
      return new Response(recordAtEnd(stream, end => record(end === "cancel" || signal.aborted ? 499 : end === "error" ? terminalStatus ?? 502 : terminalStatus ?? 200)), { headers: SSE_HEADERS });
    }
    try {
      const completion = await collectChatCompletion(stream, route.requestedModel, translatorBudget);
      record(200);
      return Response.json(completion);
    } catch (error) {
      if (signal.aborted) return fail(499, "Client cancelled request", "client_cancelled");
      if (isChatCompletionsStreamError(error)) return fail(error.status, error.message, error.type, error.code);
      return fail(502, error instanceof Error ? error.message : String(error), "upstream_error");
    }
  }
  const text = await readBounded(upstream.body, MAX_JSON_BYTES);
  if (text === null) return fail(502, "upstream response exceeded the safe limit", "upstream_error", "translation_buffer_limit");
  let parsed: unknown;
  try { parsed = JSON.parse(text); } catch { return fail(502, "upstream returned malformed Chat Completions JSON", "upstream_error"); }
  const error = structuredError(parsed);
  if (error) return fail(error.status ?? 502, error.message, error.type, error.code);
  if (!isRec(parsed) || !Array.isArray(parsed.choices) || parsed.choices.length === 0) {
    return fail(502, "upstream response contained no choices", "upstream_error");
  }
  usage = usageFromChat(parsed.usage);
  firstOutputAt = Date.now();
  record(200);
  return requestedStream
    ? new Response(jsonCompletionSse(parsed, route.requestedModel, translatorBudget), { headers: SSE_HEADERS })
    : new Response(JSON.stringify(parsed), { headers: { "content-type": "application/json" } });
};

/** Passes the stream through and calls `done` once, with how it ended. */
export function recordAtEnd(stream: ReadableStream<Uint8Array>, done: (end: "end" | "error" | "cancel") => void): ReadableStream<Uint8Array> {
  let called = false;
  const once = (end: "end" | "error" | "cancel") => { if (!called) { called = true; done(end); } };
  const reader = stream.getReader();
  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      try {
        const { done: finished, value } = await reader.read();
        if (finished) {
          once("end");
          controller.close();
        } else {
          controller.enqueue(value);
        }
      } catch (error) {
        once("error");
        controller.error(error);
      }
    },
    cancel(reason) {
      once("cancel");
      return reader.cancel(reason);
    },
  });
}

/**
 * Sends a built adapter request as ocx does (sendWithConnectionPolicy): a redirect is an error,
 * never followed with the key, and headers must arrive within ocx's default connect timeout. Hosts
 * the Worker answers itself (Workers AI) never leave it.
 */
export async function sendUpstream(
  request: { url: string; method: string; headers: Record<string, string>; body: string },
  signal: AbortSignal,
  deps: Pick<NativeChatDeps, "fetch" | "localHosts">,
): Promise<Response> {
  const headerDeadline = new AbortController();
  const timer = setTimeout(() => headerDeadline.abort(new DOMException("Timeout elapsed", "TimeoutError")), HEADER_TIMEOUT_MS);
  const upstreamRequest = new Request(request.url, {
    method: request.method, headers: request.headers, body: request.body, redirect: "manual",
    signal: AbortSignal.any([signal, headerDeadline.signal]),
  });
  const local = deps.localHosts?.[new URL(request.url).host];
  try {
    return local ? await local(upstreamRequest) : await deps.fetch(upstreamRequest);
  } finally {
    clearTimeout(timer);
  }
}

/** The hub config as the Worker routes with it, or a decline reason. */
export async function loadNativeConfig(deps: Pick<NativeChatDeps, "readConfig">): Promise<{ config: unknown } | { decline: string }> {
  const configText = await deps.readConfig();
  if (!configText) return { decline: "no-config-copy" };
  try { return { config: JSON.parse(configText) }; } catch { return { decline: "config-not-json" }; }
}

const SSE_HEADERS = { "content-type": "text/event-stream; charset=utf-8", "cache-control": "no-cache" };
// chat-native.ts's MAX_NATIVE_CHAT_JSON_BYTES.
const MAX_JSON_BYTES = 32 * 1024 * 1024;

async function readBounded(body: ReadableStream<Uint8Array>, maxBytes: number): Promise<string | null> {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let total = 0;
  let text = "";
  for (;;) {
    const { done, value } = await reader.read();
    if (done) return text + decoder.decode();
    total += value.byteLength;
    if (total > maxBytes) {
      await reader.cancel();
      return null;
    }
    text += decoder.decode(value, { stream: true });
  }
}
