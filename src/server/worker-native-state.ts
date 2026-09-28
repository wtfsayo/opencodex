// What a Cloudflare container hands its Worker at startup so the Worker can serve turns as this
// process would. Every step is a no-op off Cloudflare.
import { CLAUDE_CODE_HEADERS, CLAUDE_CODE_RUNTIME_HEADERS } from "../adapters/client-fingerprint";
import { durableMirrorEnabled, stateRequest } from "../lib/durable-mirror";
import { resolveProxyRoute } from "../lib/proxy-env";
import { startWorkerUsageInbox } from "../usage/worker-usage-inbox";
import { catalogModelSupportsReasoningSummaries, readCatalog, readCodexCatalogPath } from "../codex/catalog";
import { activeCodexModelsCachePath } from "../codex/catalog/parsing";
import { contextRelayActivated } from "../codex/context-compat";
import { mainQuotaCredentialObserved, onMainQuotaCredentialChange } from "../codex/main-account-cache";
import { isNativeMainTrafficBlocked } from "../codex/native-profile-startup";
import { CODEX_RESPONSES_HTTP_URL, CODEX_RESPONSES_WS_URL } from "./responses/codex-ws-request";
import { shouldUseCodexWsUpstream } from "./responses/ws-upstream";
import type { NativeOpenAiFacts } from "./cloudflare-native-chat-api";

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

/**
 * The Codex catalog's reasoning-summary answer for every id it could give one for: each slug, and
 * the part after a provider prefix, which is how catalogModelSupportsReasoningSummaries matches a
 * bare id. An id outside the map gets no answer from ocx either.
 */
function reasoningSummarySupport(): Record<string, boolean> {
  const catalog = readCatalog(readCodexCatalogPath()) ?? readCatalog(activeCodexModelsCachePath());
  const candidates = new Set<string>();
  for (const entry of catalog?.models ?? []) {
    for (const id of [entry.slug, entry.id]) {
      if (typeof id !== "string" || id === "") continue;
      candidates.add(id);
      if (id.includes("/")) candidates.add(id.slice(id.indexOf("/") + 1));
    }
  }
  const support: Record<string, boolean> = {};
  for (const id of candidates) {
    const answer = catalogModelSupportsReasoningSummaries(id);
    if (typeof answer === "boolean") support[id] = answer;
  }
  return support;
}

/** ws-upstream.ts's choice for a streamed turn to the ChatGPT backend, in this process. */
function upstreamTransport(): NativeOpenAiFacts["upstreamTransport"] {
  const websocket = shouldUseCodexWsUpstream(CODEX_RESPONSES_HTTP_URL, { method: "POST", body: "{\"stream\":true}" });
  const route = resolveProxyRoute(new URL(websocket ? CODEX_RESPONSES_WS_URL : CODEX_RESPONSES_HTTP_URL));
  if (route.kind !== "direct") return "proxied";
  return websocket ? "websocket" : "sse";
}

export function nativeOpenAiFacts(): NativeOpenAiFacts {
  return {
    mainCredentialObserved: mainQuotaCredentialObserved(),
    nativeMainTrafficBlocked: isNativeMainTrafficBlocked(),
    contextRelayActive: contextRelayActivated(),
    upstreamTransport: upstreamTransport(),
    reasoningSummarySupport: reasoningSummarySupport(),
  };
}

/**
 * What a ChatGPT passthrough turn reads from this process (NativeOpenAiFacts). Published at start, on
 * every change of the observed main credential, and on the republish cadence for the rest (the
 * catalog, the ownership fence), so the Worker declines rather than serve on a stale answer.
 */
export function publishNativeOpenAiFactsForWorker(retrySoon = false): Promise<void> | undefined {
  if (process.env.OCX_WORKER_NATIVE_STATE !== "1" || !durableMirrorEnabled()) return undefined;
  let facts: NativeOpenAiFacts;
  try { facts = nativeOpenAiFacts(); } catch { return undefined; }
  return stateRequest("/native-openai-facts", {
    method: "PUT",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(facts),
  })?.then(response => { void response.body?.cancel(); return response.ok; }, () => false)
    .then(ok => { if (!ok && retrySoon) setTimeout(() => void publishNativeOpenAiFactsForWorker(), FIRST_RETRY_MS).unref?.(); });
}

export function startWorkerNativeState(): void {
  startWorkerUsageInbox();
  const published = publishClientRuntimeForWorker(true);
  void publishNativeOpenAiFactsForWorker(true);
  if (published && !republish) {
    onMainQuotaCredentialChange(() => void publishNativeOpenAiFactsForWorker(true));
    republish = setInterval(() => {
      void publishClientRuntimeForWorker();
      void publishNativeOpenAiFactsForWorker();
    }, REPUBLISH_MS);
    republish.unref?.();
  }
}
