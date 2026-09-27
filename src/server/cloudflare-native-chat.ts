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
import { createTranslatorBudget } from "../lib/translator-budget";
import type { OcxConfig, OcxProviderConfig } from "../types";

type Rec = Record<string, unknown>;
const isRec = (value: unknown): value is Rec => !!value && typeof value === "object" && !Array.isArray(value);

// Provider fields whose meaning this path reproduces exactly. Any other field (headers, key pools,
// aliases, per-model wire overrides, reasoning or capability gates) sends the turn to the container.
const PROVIDER_FIELDS = new Set(["adapter", "baseUrl", "apiKey", "models", "authMode"]);
// Config sections that change how ocx routes or accounts for a turn before the explicit
// provider/model match this path relies on.
const ROUTING_SECTIONS = ["routingProfiles", "combos", "codexAccountNamespaces", "customModels", "spend"] as const;
const RESERVED_NAMESPACES = new Set(["policy", "combo"]);

export type NativeChatRoute = { providerName: string; provider: OcxProviderConfig; modelId: string; requestedModel: string };

/**
 * The route ocx would pick for `model`, or null when this path cannot be sure it matches. Only
 * `<provider>/<model>` with an exact configured provider name and an exactly listed model qualifies.
 */
export function resolveNativeChatRoute(config: unknown, model: unknown): NativeChatRoute | null {
  if (!isRec(config) || !isRec(config.providers) || typeof model !== "string") return null;
  for (const section of ROUTING_SECTIONS) {
    const value = config[section];
    if (value !== undefined && !(isRec(value) && Object.keys(value).length === 0) && !(Array.isArray(value) && value.length === 0)) return null;
  }
  const slash = model.indexOf("/");
  if (slash <= 0) return null;
  const providerName = model.slice(0, slash);
  const modelId = model.slice(slash + 1);
  if (RESERVED_NAMESPACES.has(providerName) || !modelId) return null;
  if (!Object.prototype.hasOwnProperty.call(config.providers, providerName)) return null;
  const provider = config.providers[providerName];
  if (!isRec(provider)) return null;
  if (Object.keys(provider).some(key => !PROVIDER_FIELDS.has(key))) return null;
  if (provider.adapter !== "openai-chat") return null;
  if (provider.authMode !== undefined && provider.authMode !== "key") return null;
  if (typeof provider.baseUrl !== "string" || !URL.canParse(provider.baseUrl)) return null;
  // A literal key only: `keychain:` and `$NAME` / `${NAME}` references are resolved by ocx.
  if (typeof provider.apiKey !== "string" || provider.apiKey.startsWith("$") || provider.apiKey.startsWith("keychain:")) return null;
  const models = provider.models;
  if (!Array.isArray(models) || !models.includes(modelId) || models.includes(model)) return null;
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
export const serveNativeChat: ServeNativeChat = async (bodyText, signal, deps) => {
  let body: unknown;
  try { body = JSON.parse(bodyText); } catch { return null; }
  if (!isRec(body) || !nativeChatBodyEligible(body)) return null;
  const configText = await deps.readConfig();
  if (!configText) return null;
  let config: unknown;
  try { config = JSON.parse(configText); } catch { return null; }
  const route = resolveNativeChatRoute(config, body.model);
  if (!route) return null;

  const request = buildOpenAIChatPassthroughRequest(route.provider, body, route.modelId, true);
  const upstreamRequest = new Request(request.url, { method: request.method, headers: request.headers, body: request.body, signal });
  const local = deps.localHosts?.[new URL(request.url).host];
  const upstream = local ? await local(upstreamRequest) : await deps.fetch(upstreamRequest);
  if (!upstream.ok || !upstream.body || !(upstream.headers.get("content-type") ?? "").includes("text/event-stream")) {
    await upstream.body?.cancel();
    return null;
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
