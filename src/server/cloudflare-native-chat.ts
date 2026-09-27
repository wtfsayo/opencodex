// Phase 4a: streamed Chat Completions answered in the Worker for the simplest routes, so the turn
// skips the Worker -> Durable Object -> container -> ocx hop. It reuses ocx's own request builder
// and SSE relay; everything around them (routing, eligibility) is a deliberately narrow subset of
// ocx's, and any request or config this does not fully understand returns null so the Worker
// forwards it untouched to the container, which stays the reference implementation.
//
// It runs in the Cloudflare Worker (deploy/cloudflare/src/index.ts, which reaches it through the
// "ocx-worker-native" alias and typechecks against ./cloudflare-native-chat-api.ts). Its
// import graph is held free of Bun, node:fs and friends by tests/service/cloudflare-worker-native.test.ts.
import type { ServeNativeChat } from "./cloudflare-native-chat-api";
import { buildOpenAIChatPassthroughRequest } from "../adapters/openai-chat/passthrough";
import { nativeChatSse } from "./chat-native-sse";
import { chatCollabSurface, isThreadSpawnRequest } from "./collab-surface";
import { createTranslatorBudget } from "../lib/translator-budget";
import type { OcxConfig, OcxProviderConfig } from "../types";
import { PROVIDER_REGISTRY } from "../providers/registry";

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
  "managementUsageMaxReadBytes", "appOwnedMemoryBudgetMb", "configRebaseProvenance", "oauthOpenBrowser",
  "codexAutoStart", "codexProviderDisplayName", "codexShimAutoRestore", "codexQuotaAutoRefresh",
  "codexAccountPickerEnabled", "catalogAutoRefresh", "quotaResetNotify", "remoteGui", "metricsExport",
  "openaiProviderTierVersion", "googleAntigravityStaticCatalogVersion", "subagentModelsVersion",
  "multiAgentSurfaceAdvisoryVersion", "apiKeys", "subagentModels",
]);
const RESERVED_NAMESPACES = new Set(["policy", "combo"]);
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

export type NativeChatRoute = { providerName: string; provider: OcxProviderConfig; modelId: string; requestedModel: string };

/**
 * The route ocx would pick for `model`, or null when this path cannot be sure it matches. Only
 * `<provider>/<model>` with an exact configured provider name and an exactly listed model qualifies.
 */
export function resolveNativeChatRoute(
  config: unknown,
  model: unknown,
  localHosts: ReadonlySet<string> = new Set(),
  why: (reason: string) => void = () => {},
): NativeChatRoute | null {
  const no = (reason: string) => { why(reason); return null; };
  if (!isRec(config) || !isRec(config.providers) || typeof model !== "string") return no("config-or-model-shape");
  const unknownKeys = Object.keys(config).filter(key => !CONFIG_KEYS.has(key)).sort();
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
  if (PROVIDER_REGISTRY.some(entry => entry.id === providerName)) return no("built-in-provider");
  const provider = config.providers[providerName];
  if (!isRec(provider)) return no("provider-shape");
  const unknownField = Object.keys(provider).find(key => !PROVIDER_FIELDS.has(key));
  if (unknownField) return no(`provider-field:${unknownField}`);
  if (provider.adapter !== "openai-chat") return no("adapter");
  if (provider.authMode !== undefined && provider.authMode !== "key") return no("auth-mode");
  if (typeof provider.baseUrl !== "string" || !destinationAllowed(provider.baseUrl, localHosts)) return no("destination");
  // A literal key only: `keychain:` and `$NAME` / `${NAME}` references are resolved by ocx.
  if (typeof provider.apiKey !== "string" || provider.apiKey.startsWith("$") || provider.apiKey.startsWith("keychain:")) return no("key-reference");
  const models = provider.models;
  if (!Array.isArray(models) || !models.includes(modelId) || models.includes(model)) return no("model-not-listed");
  return { providerName, provider: provider as unknown as OcxProviderConfig, modelId, requestedModel: model };
}

/** The request fields ocx's native Chat lane refuses or reroutes, plus anything carrying an image. */
export function nativeChatBodyEligible(body: Rec): boolean {
  if (body.stream !== true) return false;
  if (body.store === true || body.background === true) return false;
  if (body.previous_response_id !== undefined || body.compaction_trigger !== undefined) return false;
  if (!Array.isArray(body.messages)) return false;
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

/**
 * Serves the turn, or returns null to hand it to the container. `bodyText` is the request body,
 * already read, so a decline can still forward it. An upstream error also declines, before any
 * byte reaches the client: the container's lane owns retries, key failover and error shaping.
 */
export const serveNativeChat: ServeNativeChat = async (bodyText, headers, signal, deps) => {
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
  const route = resolveNativeChatRoute(config, body.model, new Set(Object.keys(deps.localHosts ?? {})), no);
  if (!route) return null;

  const request = buildOpenAIChatPassthroughRequest(route.provider, body, route.modelId, true);
  const upstreamRequest = new Request(request.url, { method: request.method, headers: request.headers, body: request.body, signal });
  const local = deps.localHosts?.[new URL(request.url).host];
  const upstream = local ? await local(upstreamRequest) : await deps.fetch(upstreamRequest);
  if (!upstream.ok || !upstream.body || !(upstream.headers.get("content-type") ?? "").includes("text/event-stream")) {
    await upstream.body?.cancel();
    return no(`upstream-${upstream.status}`);
  }
  const stream = nativeChatSse(upstream.body, {
    requestedModel: route.requestedModel,
    translatorBudget: createTranslatorBudget(),
    signal,
    stallTimeoutSec: (config as Pick<OcxConfig, "stallTimeoutSec">).stallTimeoutSec,
    onUsage: () => {},
  });
  return new Response(stream, { headers: { "content-type": "text/event-stream", "cache-control": "no-cache" } });
};
