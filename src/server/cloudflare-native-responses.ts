// Phase 5: streamed Responses API turns (what Codex CLI sends) answered in the Cloudflare Worker for
// openai-chat providers, reusing ocx's own parser, openai-chat adapter and Responses SSE bridge. It
// follows the container's path for exactly the turns where that path is stateless (the spec is in
// devlog/_plan/260927_cloudflare_native/000_goal.md, Phase 5); anything else returns null and the
// Worker forwards the untouched request to the container.
//
// The import graph is held Worker-safe by tests/service/cloudflare-worker-native.test.ts.
import type { NativeChatDeps, ServeNativeChat, WorkerUsageRow } from "./cloudflare-native-chat-api";
import { loadNativeConfig, recordAtEnd, resolveNativeChatRoute, routeUsageFields, sendUpstream, TURN_ADAPTERS, withOpenCodeGoSession, type NativeChatRoute } from "./cloudflare-native-chat";
import { createAnthropicAdapterWith, isLikelyRealAnthropicThinkingSignature, type AnthropicAdapterDeps } from "../adapters/anthropic/adapter";
import { CLAUDE_CODE_HEADERS } from "../adapters/client-fingerprint";
import { noStoredCodexAccounts, runNativeOpenAiTurn } from "./cloudflare-native-openai";
import { applyUpstreamRecoveryInit, fetchWithResetRetry, isNonReplayableResponse } from "../lib/upstream-retry";
import { readDisplaySafeErrorText } from "../lib/bounded-body";
import { formatErrorResponse } from "../bridge/errors";
import { describeUpstreamConnectFailure } from "./responses/upstream-error";
import { normalizeUpstreamErrorText } from "./responses/upstream-error-text";
import { routedUpstreamErrorResponse } from "./responses/routed-upstream-error";
import { hasShrinkableOpenAIChatImages } from "../adapters/openai-chat-image-budget";
import { isCanonicalOpenAiForwardProvider } from "../providers/openai-tiers";
import { providerCodexAccountMode } from "../providers/registry";
import { createInputAdmission } from "./responses/input-admission-core";
import { finishRegisteredAdapter, wrapOpenAIChatAdapter } from "../adapters/registered-adapter";
import { collabSurface, isThreadSpawnRequest } from "./collab-surface";
import { buildToolBridgeMaps } from "./responses/tool-bridge-maps";
import { replaceSkillsBlock, singleSkillsBlock } from "./responses/skills-catalog";
import { conversationIdFromResponsesRequest, reasoningReplayConversationIdFromResponsesRequest, sessionIdHeaderFromRequest } from "./request-log-conversation";
import { createOpenAIChatAdapterWith, type OpenAIChatAdapterDeps } from "../adapters/openai-chat/adapter";
import { renameRoutedIdentityInContext } from "../adapters/identity";
import { bridgeToResponsesSSE } from "../bridge/sse";
import { hasValidatedActiveReasoningEffort, parseRequest } from "../responses/parser";
import { mapReasoningEffortWith, NO_REASONING_METADATA, type ReasoningMetadataAccess } from "../reasoning-effort-core";
import { CACHE_TTL_MS, metadataAccessFrom, parseMetadataSnapshot, parseSupportRows } from "../providers/reasoning-metadata-core";
import { createHash } from "node:crypto";
import { metadataProviderKeyForBaseUrl } from "../providers/reasoning-metadata-destinations";
import { readResponseStreamWithInactivity, ResponseBodyInactivityError } from "../lib/response-body-inactivity";
import { createTranslatorBudget, type TranslatorBudget } from "../lib/translator-budget";
import { resolveStallTimeoutMs } from "../stall-timeout";
import type { AdapterEvent, OcxConfig, OcxProviderConfig, OcxUsage } from "../types";

type Rec = Record<string, unknown>;
const isRec = (value: unknown): value is Rec => !!value && typeof value === "object" && !Array.isArray(value);

// Body fields whose handling this path reproduces. Anything else (service_tier, conversation,
// background, previous_response_id, ...) is left to the container.
const BODY_FIELDS = new Set([
  "model", "instructions", "input", "tools", "tool_choice", "parallel_tool_calls", "reasoning", "store",
  "stream", "include", "prompt_cache_key", "text", "max_output_tokens", "temperature", "top_p", "metadata", "user",
  // Codex CLI sends it on every turn. Over HTTP ocx reads it only for compaction routing, which needs
  // a compactionRouting config section and a compaction_trigger item, both declined here.
  "client_metadata",
]);
const INPUT_ITEMS = new Set(["message", "function_call", "function_call_output", "reasoning"]);
const TEXT_PARTS = new Set(["input_text", "output_text"]);
// Multi-agent tools: ocx injects guidance and caps effort on these turns (collaboration.ts).
export const COLLABORATION_TOOLS = new Set([
  "spawn_agent", "send_input", "resume_agent", "close_agent", "send_message", "followup_task", "interrupt_agent", "list_agents",
]);
const SKILLS_BLOCK = "<skills_instructions>";
// What Codex CLI 0.157 sends: {"type":"web_search","external_web_access":false}. Without the sidecar
// ocx drops the tool whatever its options; other options stay with the container.
const WEB_SEARCH_FIELDS = new Set(["type", "external_web_access"]);

/** Every text in the body that ocx's catalog snapshot does not look at (user, assistant, tool text). */
function nonCatalogText(body: Rec): unknown[] {
  if (!Array.isArray(body.input)) return [];
  return body.input.filter(item => !(isRec(item) && (item.type === undefined || item.type === "message")
    && (item.role === "developer" || item.role === "system")));
}

async function sha256Hex(value: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(value));
  return [...new Uint8Array(digest)].map(byte => byte.toString(16).padStart(2, "0")).join("");
}

/**
 * skills-snapshot.ts for the Worker: the same scope (the session's own thread-id, else session-id;
 * the parent-thread case is declined earlier, and the data token is the principal), the same
 * single-block rule, and first block wins. A frozen block is substituted at once; a new one is only
 * returned for the caller to commit once the turn has been sent, as ocx stores only after admission.
 */
export async function freezeSkillsCatalog(body: Rec, headers: Headers, deps: NativeChatDeps): Promise<"decline" | (() => void) | undefined> {
  const found = singleSkillsBlock(body);
  if (!found) return undefined;
  const conversation = reasoningReplayConversationIdFromResponsesRequest({
    threadIdHeader: headers.get("thread-id")?.trim() || undefined,
    sessionIdHeader: sessionIdHeaderFromRequest(headers),
  });
  if (!conversation) return undefined;
  if (!deps.skills) return "decline";
  const skills = deps.skills;
  const scope = await sha256Hex(JSON.stringify(["skills_catalog_snapshot_v1", await sha256Hex(skills.principal), conversation]));
  const frozen = await skills.read(scope);
  if (frozen !== undefined) {
    if (frozen !== found.block) replaceSkillsBlock(found, frozen);
    return undefined;
  }
  return () => skills.commit(scope, found.block);
}

/**
 * web-search/index.ts's sidecar for a hosted web_search tool searches through the canonical `openai`
 * row (openai-sidecar.ts): in pool mode with a stored Codex login, in direct mode with the caller's.
 * Without one it drops the tool, as the Worker does; with one, its account selection and quota
 * records are ocx's alone.
 */
async function webSearchSidecarDecline(openai: unknown, deps: NativeChatDeps): Promise<string | undefined> {
  if (openai === undefined || (isRec(openai) && openai.disabled === true)) return undefined;
  // Only the row as saved is visible here, not what ocx's config loading makes of it; any row
  // but the canonical one is left to ocx.
  const row = (isRec(openai) && openai.authMode === undefined ? { ...openai, authMode: "forward" } : openai) as unknown as OcxProviderConfig;
  if (!isRec(openai) || !isCanonicalOpenAiForwardProvider(row)) return "web-search-sidecar";
  if (providerCodexAccountMode("openai", row) !== "pool") return "web-search-sidecar";
  if (!(await noStoredCodexAccounts(deps))) return "web-search-sidecar";
  const facts = await deps.nativeOpenAiFacts?.();
  if (!facts) return "web-search-facts-unpublished";
  return facts.codexAccountsStored || facts.mainCodexLoginPresent ? "web-search-sidecar" : undefined;
}

/** An image the client sent inline: no file id or remote URL for ocx to resolve. */
function isInlineImagePart(part: Rec): boolean {
  return part.type === "input_image" && typeof part.image_url === "string" && /^data:image\/[\w.+-]+;base64,/.test(part.image_url)
    && Object.keys(part).every(key => key === "type" || key === "image_url" || key === "detail");
}

function carriesImages(body: Rec): boolean {
  return Array.isArray(body.input) && body.input.some(item => isRec(item) && Array.isArray(item.content)
    && item.content.some(part => isRec(part) && part.type === "input_image"));
}

function hasHostedWebSearch(body: Rec): boolean {
  return Array.isArray(body.tools) && body.tools.some(tool => isRec(tool) && tool.type === "web_search");
}

function containsSkillsBlock(value: unknown): boolean {
  if (typeof value === "string") return value.includes(SKILLS_BLOCK);
  if (Array.isArray(value)) return value.some(containsSkillsBlock);
  if (isRec(value)) return Object.values(value).some(containsSkillsBlock);
  return false;
}

/** Why this turn is not one the Worker can serve exactly as ocx would, or undefined when it is. */
export function nativeResponsesDeclineReason(body: Rec, headers: Headers): string | undefined {
  const unknown = Object.keys(body).filter(key => !BODY_FIELDS.has(key)).sort();
  if (unknown.length > 0) return `body-fields:${unknown.join(",")}`;
  if (body.stream !== true) return "not-streamed";
  // Stored responses and their continuation live in ocx's response state.
  if (body.store !== false) return "stored-response";
  if (body.reasoning !== undefined) {
    if (!isRec(body.reasoning)) return "reasoning-shape";
  }
  if (body.tools !== undefined) {
    if (!Array.isArray(body.tools)) return "tools-shape";
    for (const tool of body.tools) {
      if (!isRec(tool)) return "tool-type";
      // Hosted web search is dropped from the upstream request unless ocx can run its search
      // sidecar; serveNativeResponses checks that against the config.
      if (tool.type === "web_search" && Object.keys(tool).every(key => WEB_SEARCH_FIELDS.has(key))) continue;
      // Namespaced function groups (Codex's multi_agent_v1, MCP servers) flatten and restore purely.
      const members = tool.type === "namespace" && typeof tool.name === "string" && Array.isArray(tool.tools) ? tool.tools : [tool];
      for (const member of members) {
        if (!isRec(member) || member.type !== "function" || typeof member.name !== "string" || member.namespace !== undefined) return "tool-type";
        // Codex's code-mode `exec` is parsed with Bun's transpiler.
        if (member.name === "exec") return "code-mode-exec";
      }
      // A bare collaboration tool: ocx's guidance and caps for it read process and catalog state.
      if (tool.type === "function" && COLLABORATION_TOOLS.has(tool.name as string)) return "collaboration-turn";
    }
  }
  if (typeof body.input !== "string") {
    if (!Array.isArray(body.input) || body.input.length === 0) return "input-shape";
    for (const item of body.input) {
      if (!isRec(item) || typeof item.type !== "string" || !INPUT_ITEMS.has(item.type)) return "input-item";
      if (item.type === "message") {
        if (typeof item.content !== "string" && !(Array.isArray(item.content)
          && item.content.every(part => isRec(part) && typeof part.type === "string" && (TEXT_PARTS.has(part.type) || isInlineImagePart(part))))) return "message-parts";
      }
      // ocx answers an unpaired tool result with its own 400.
      if ((item.type === "function_call" || item.type === "function_call_output")
        && (typeof item.call_id !== "string" || item.call_id === "")) return "tool-call-id";
      if (item.type === "function_call_output" && typeof item.output !== "string") return "tool-output-shape";
    }
  }
  // A catalog anywhere but instructions or developer/system text is not one ocx would freeze, and
  // not one this path has reasoned about.
  if (containsSkillsBlock(nonCatalogText(body))) return "skills-instructions";
  if (isThreadSpawnRequest(headers) || headers.has("x-codex-parent-thread-id")) return "collaboration-turn";
  if (headers.has("x-opencodex-grok")) return "grok-surface";
  return undefined;
}

// Effort is mapped with ocx's caches as it published them (or none, for a destination without
// models.dev metadata). Image-bearing turns are declined above, so the image hooks are never reached.
function workerAdapterDeps(metadata: ReasoningMetadataAccess, images: { normalizationNeeded: boolean }): OpenAIChatAdapterDeps {
  return {
    mapReasoningEffort: (provider, modelId, requested) => mapReasoningEffortWith(provider, modelId, requested, metadata),
    // ocx re-encodes images past the budget with Bun's codec; such a turn is left to it.
    hasShrinkableOpenAIChatImages,
    normalizeOpenAIChatImages: async () => { images.normalizationNeeded = true; },
  };
}

/** The auth store copy, read only when the model names a provider the Worker may use a login of. */
async function oauthStoreFor(model: unknown, deps: NativeChatDeps): Promise<unknown> {
  if (typeof model !== "string" || !model.startsWith("anthropic/") || !deps.readAuth) return undefined;
  const text = await deps.readAuth();
  try { return text ? JSON.parse(text) : undefined; } catch { return undefined; }
}

// The Worker never serves the native OpenAI provider, the only one these lookups answer for.
const WORKER_INPUT_ADMISSION = createInputAdmission({ contextWindow: () => undefined, maxInputTokens: () => undefined, maxOutputTokens: () => undefined });
// Image-bearing turns are declined, and ocx's normalization returns at once without images.
const NO_IMAGE_NORMALIZATION: AnthropicAdapterDeps = { normalizeAnthropicImages: async () => {} };

/** reasoning-metadata.ts's credentialIdentity on the request path: a digest of the key sent. */
function credentialDigest(provider: { apiKey?: string }): string | undefined {
  return typeof provider.apiKey === "string" && provider.apiKey.length > 0
    ? createHash("sha256").update(provider.apiKey).digest("hex")
    : undefined;
}

/**
 * The metadata access ocx would map this effort with, or a decline reason. ocx reads its caches from
 * memory; the Worker reads the copies ocx publishes, and declines until there are some.
 */
async function effortMetadataFor(deps: NativeChatDeps): Promise<ReasoningMetadataAccess | string> {
  const cached = await deps.reasoningMetadata?.();
  if (cached?.snapshot === undefined || cached.support === undefined) return "reasoning-metadata-unpublished";
  const now = Date.now();
  let snapshotJson: unknown;
  let supportJson: unknown;
  try {
    snapshotJson = JSON.parse(cached.snapshot);
    supportJson = JSON.parse(cached.support);
  } catch {
    return "reasoning-metadata-unreadable";
  }
  const snapshot = parseMetadataSnapshot(snapshotJson);
  // ocx starts a refresh when it reads a ladder from a snapshot this old; its next turns see the new one.
  if (snapshot && now - snapshot.fetchedAt > CACHE_TTL_MS) return "reasoning-metadata-stale";
  return metadataAccessFrom(snapshot, parseSupportRows(supportJson, now), credentialDigest);
}

/** A turn sent upstream: its Responses SSE, and the callback that writes its usage row when it ends. */
export type NativeResponsesTurn = {
  sse: ReadableStream<Uint8Array>;
  route: NativeChatRoute;
  finish(end: "end" | "error" | "cancel"): void;
} | {
  /** ocx's own answer for a send that failed after it left (adapter-dispatch.ts), already recorded. */
  failure: Response;
};

type TurnOptions = {
  /** The client's wire. ocx replays a Messages turn through its Responses pipeline as "anthropic". */
  inbound: "responses" | "anthropic";
  translatorBudget: TranslatorBudget;
  startedAt: number;
  /** Set by claude-messages.ts's replay: the Claude surface and its metadata conversation id. */
  surface?: "claude";
  conversationId?: string;
  /**
   * claude-messages.ts passes promptCacheKeyIsSharedCohort for a cache key taken from the system
   * prompt, and skills-snapshot.ts then keeps no snapshot for the turn.
   */
  sharedCacheCohort?: boolean;
  /** OpenCode Go's session lane when the caller derives it (claude-messages.ts does). */
  goSessionLane?: string;
  /** The input estimate ocx's path records for the spend ledger (claude-messages.ts's token floor). */
  spendInputTokens?: () => number;
  /** The WebSocket transport sends every response with an empty id (websocket-handler.ts). */
  responseId?: string;
  /** Runs once the route is known and before the send, so nothing after the send can throw. */
  beforeSend?(route: NativeChatRoute): void;
};

/**
 * The Responses turn as ocx runs it for `body`, or null (after `no`) when this path cannot match
 * it. Nothing reaches the client until the upstream has answered OK.
 */
export async function runNativeResponsesTurn(
  body: Rec, headers: Headers, signal: AbortSignal, deps: NativeChatDeps,
  no: (reason: string) => null, options: TurnOptions,
): Promise<NativeResponsesTurn | null> {
  const { startedAt } = options;
  const declined = nativeResponsesDeclineReason(body, headers);
  if (declined) return no(declined);
  const loaded = await loadNativeConfig(deps);
  if ("decline" in loaded) return no(loaded.decline);
  const authStore = await oauthStoreFor(body.model, deps);
  const resolved = resolveNativeChatRoute(loaded.config, body.model, new Set(Object.keys(deps.localHosts ?? {})), no, deps.secrets, TURN_ADAPTERS, authStore);
  if (!resolved) return null;
  // core-normalize.ts, once the route is final.
  const route = withOpenCodeGoSession(resolved, headers, options.goSessionLane);
  // request-prepare.ts: images reach a model as sent only when ocx would neither describe nor strip
  // them (vision/plan.ts, from catalogs only ocx reads) and the adapter would not re-encode them.
  const images = { normalizationNeeded: false };
  if (carriesImages(body)) {
    if (route.provider.adapter !== "openai-chat") return no("image-adapter");
    const published = await deps.nativeOpenAiFacts?.();
    if (published?.visionPreprocessed[`${route.providerName}/${route.modelId}`] !== false) return no("vision-preprocessing");
  }
  // ocx maps effort for these destinations from models.dev metadata and refusals it learned
  // (reasoning-metadata.ts); without an effort that state is never consulted.
  const effort = isRec(body.reasoning) ? body.reasoning.effort : undefined;
  let metadata = NO_REASONING_METADATA;
  if (effort !== undefined && effort !== null && metadataProviderKeyForBaseUrl(route.provider.baseUrl) !== undefined) {
    const access = await effortMetadataFor(deps);
    if (typeof access === "string") return no(access);
    metadata = access;
  }
  const config = loaded.config as Pick<OcxConfig, "stallTimeoutSec" | "cacheRetention">;

  if (hasHostedWebSearch(body)) {
    const sidecar = await webSearchSidecarDecline((loaded.config as { providers: Record<string, unknown> }).providers.openai, deps);
    if (sidecar) return no(sidecar);
  }

  // skills-snapshot.ts: a session's first catalog block is kept and substituted on later turns,
  // before the body is parsed. The Worker keeps its own copy; see skillsSnapshot.
  const frozen = options.sharedCacheCohort ? undefined : await freezeSkillsCatalog(body, headers, deps);
  if (frozen === "decline") return no("skills-snapshot-unavailable");
  const commitSkills = typeof frozen === "function" ? frozen : undefined;

  let parsed;
  try { parsed = parseRequest(body); } catch { return no("parse"); }
  // A v2 collaboration surface gets guidance built from the catalog on disk when subagent models
  // are configured (collaboration.ts); v1 gets guidance only at max effort, declined just below.
  const surface = collabSurface(parsed);
  const subagentModels = (loaded.config as { subagentModels?: unknown }).subagentModels;
  if (surface === "v2" && Array.isArray(subagentModels) && subagentModels.length > 0) return no("collaboration-v2-guidance");
  // collaboration.ts injects <multi_agent_mode> guidance on a v1 surface at max (ultra arrives as max).
  if (surface === "v1" && parsed.options.reasoning === "max") return no("collaboration-v1-guidance");
  // As core-normalize.ts: the upstream sees the routed id, and the identity sentence names it.
  if (parsed._rawBody && typeof parsed._rawBody === "object") (parsed._rawBody as { model?: string }).model = route.modelId;
  // core-normalize.ts: an Anthropic route answers with the selector the client sent.
  const responseModelId = route.providerName === "anthropic" || route.provider.adapter === "anthropic" ? parsed.modelId : route.modelId;
  parsed.modelId = route.modelId;
  parsed.context = renameRoutedIdentityInContext(parsed.context, route.modelId);
  // core-normalize.ts. Only for a Responses client: an Anthropic replay keeps what the parser decided.
  if (options.inbound === "responses") {
    const summary = isRec(body.reasoning) ? body.reasoning.summary : undefined;
    parsed.options.hideThinkingSummary = summary === "none"
      || (!summary && !hasValidatedActiveReasoningEffort(parsed.options) && route.provider.showThinkingSummary !== true);
  }

  // Whether ocx replays a signed thinking block turns on the serving identity it bound to the
  // thread in memory (core-replay.ts), which the Worker cannot see.
  if (route.provider.adapter === "anthropic" && parsed.context.messages.some(message => message.role === "assistant"
    && message.content.some(part => part.type === "thinking" && (!!part.redacted?.length || isLikelyRealAnthropicThinkingSignature(part.signature))))) {
    return no("reasoning-replay-state");
  }
  // request-prepare.ts refuses an input far past the context window, with its own error.
  if (parsed._compactionRequest !== true
    && !WORKER_INPUT_ADMISSION.checkInputAdmission(parsed, route.provider, route.providerName, parsed.modelId).admitted) {
    return no("input-admission");
  }

  const translatorBudget = options.translatorBudget;
  // As createRegisteredAdapter builds them (registered-adapter.ts), with the Worker's own hooks.
  // A subscription turn carries the Claude Code fingerprint of the process ocx runs in.
  let anthropicDeps = NO_IMAGE_NORMALIZATION;
  if (route.provider.adapter === "anthropic" && route.provider.authMode === "oauth") {
    const runtime = await deps.clientRuntime?.();
    if (!runtime) return no("oauth-client-runtime-unpublished");
    anthropicDeps = { ...NO_IMAGE_NORMALIZATION, claudeCodeHeaders: { ...CLAUDE_CODE_HEADERS, ...runtime } };
  }
  const adapter = route.provider.adapter === "anthropic"
    ? finishRegisteredAdapter(createAnthropicAdapterWith(route.provider, config.cacheRetention, anthropicDeps), "anthropic")
    : finishRegisteredAdapter(wrapOpenAIChatAdapter(createOpenAIChatAdapterWith(route.provider, workerAdapterDeps(metadata, images))), "openai-chat");
  const upstreamAbort = new AbortController();
  const upstreamSignal = AbortSignal.any([signal, upstreamAbort.signal]);
  const request = await adapter.buildRequest(parsed, { headers: new Headers(), translatorBudget, abortSignal: upstreamSignal });
  if (images.normalizationNeeded) return no("image-normalization");
  // Everything that can throw runs before the send: after it, a throw would resend via the container.
  const maps = buildToolBridgeMaps(parsed, translatorBudget);
  options.beforeSend?.(route);
  // request-prepare.ts: a conversation id already set (a routed Claude turn's) wins over headers.
  const conversationId = options.conversationId ?? conversationIdFromResponsesRequest({
    clientThreadId: parsed._clientThreadId,
    sessionIdHeader: sessionIdHeaderFromRequest(headers),
    threadIdHeader: headers.get("thread-id"),
    cursorConversationId: parsed._cursorConversationId,
  });
  const requestedEffort = parsed.options.reasoning;
  const failureRow = (status: number): WorkerUsageRow => ({
    requestId: crypto.randomUUID(),
    timestamp: startedAt,
    ...routeUsageFields(route),
    resolvedModel: route.modelId,
    ...(responseModelId !== route.modelId ? { wireModel: route.modelId } : {}),
    ...(requestedEffort ? { requestedEffort } : {}),
    ...(conversationId ? { conversationId } : {}),
    ...(options.surface ? { surface: options.surface } : {}),
    inboundProtocol: options.inbound === "anthropic" ? "messages" : "responses",
    admissionKind: "environment",
    status,
    durationMs: Date.now() - startedAt,
    usageStatus: "unreported",
  });
  const recordFailure = (status: number) => deps.recordUsage?.(failureRow(status));
  const maxOutput = parsed.options.maxOutputTokens;
  const spendOutputCeilingTokens = typeof maxOutput === "number" && maxOutput > 0 ? Math.trunc(maxOutput) : undefined;
  // adapter-dispatch.ts: reset-only retries for these providers, identity encoding for a stream,
  // and no ambiguous resend (ocx may buy one; the Worker never sends a possibly running turn twice).
  let upstream: Response;
  try {
    upstream = await fetchWithResetRetry(recovery => {
      const headers = applyUpstreamRecoveryInit({ headers: request.headers }, recovery).headers;
      if (parsed.stream && !headers.has("accept-encoding")) headers.set("accept-encoding", "identity");
      return sendUpstream({ ...request, headers: Object.fromEntries(headers) }, upstreamSignal, deps);
    }, { abortSignal: upstreamSignal, label: new URL(request.url).host, claimAmbiguousResend: () => false });
  } catch (error) {
    commitSkills?.();
    if (signal.aborted) {
      recordFailure(499);
      return { failure: formatErrorResponse(499, "client_cancelled", "Client cancelled request") };
    }
    recordFailure(502);
    return { failure: formatErrorResponse(502, "upstream_error", describeUpstreamConnectFailure(error, 200_000)) };
  }
  // A refused create generated nothing: ocx sends it itself and runs its recovery on the answer. The
  // retry ladder's own refusal is answered as ocx answers it.
  if (isNonReplayableResponse(upstream)) {
    commitSkills?.();
    recordFailure(upstream.status);
    return { failure: upstream };
  }
  if (upstream.status >= 400 && upstream.status < 500) {
    await upstream.body?.cancel().catch(() => {});
    return no(`upstream-${upstream.status}`);
  }
  if (!upstream.ok || !upstream.body) {
    commitSkills?.();
    const errorText = await readDisplaySafeErrorText(upstream, upstreamSignal, "unknown error");
    recordFailure(upstream.status);
    return { failure: routedUpstreamErrorResponse(upstream.status, normalizeUpstreamErrorText(errorText, "unknown error"), upstream.headers.get("retry-after"), route) };
  }
  commitSkills?.();

  let usage: OcxUsage | undefined;
  let firstOutputAt: number | undefined;
  let errorStatus: number | undefined;
  let completed = false;
  let recorded = false;
  const record = (status: number) => {
    if (recorded) return;
    recorded = true;
    deps.recordUsage?.({
      requestId: crypto.randomUUID(),
      timestamp: startedAt,
      ...routeUsageFields(route),
      resolvedModel: route.modelId,
      ...(responseModelId !== route.modelId ? { wireModel: route.modelId } : {}),
      ...(requestedEffort ? { requestedEffort } : {}),
      ...(conversationId ? { conversationId } : {}),
      ...(options.surface ? { surface: options.surface } : {}),
      inboundProtocol: options.inbound === "anthropic" ? "messages" : "responses",
      admissionKind: "environment",
      // request-prepare.ts reserves the caller's output ceiling, and the path's input estimate.
      ...(options.spendInputTokens ? { spendInputTokens: options.spendInputTokens() } : {}),
      ...(spendOutputCeilingTokens !== undefined ? { spendOutputCeilingTokens } : {}),
      status,
      durationMs: Date.now() - startedAt,
      ...(firstOutputAt !== undefined ? { firstOutputMs: firstOutputAt - startedAt } : {}),
      usageStatus: usage ? "reported" : "unreported",
      ...(usage ? { usage: usage as NonNullable<WorkerUsageRow["usage"]>, totalTokens: usage.totalTokens ?? usage.inputTokens + usage.outputTokens } : {}),
    });
  };

  // adapter-delivery.ts: the initial stream, with a stalled body reported in-stream as a 504.
  const inactivityMs = resolveStallTimeoutMs(config.stallTimeoutSec, { localUpstream: false });
  const events = (async function* (): AsyncGenerator<AdapterEvent> {
    try {
      for await (const event of readResponseStreamWithInactivity(upstream, upstreamSignal, inactivityMs,
        response => adapter.parseStream(response, translatorBudget, request.tierLog))) {
        if (event.type === "error") errorStatus ??= event.status ?? 502;
        yield event;
      }
    } catch (error) {
      if (error instanceof ResponseBodyInactivityError) {
        errorStatus ??= 504;
        yield { type: "error", message: "Upstream response body stalled before completing", status: 504, errorType: "upstream_error" };
        return;
      }
      errorStatus ??= signal.aborted ? 499 : 502;
      throw error;
    }
  })();
  const sse = bridgeToResponsesSSE(
    events, responseModelId, maps.toolNsMap, maps.freeformToolNames, maps.toolSearchToolNames,
    () => upstreamAbort.abort(), 2_000,
    {
      translatorBudget,
      ...(options.responseId !== undefined ? { responseId: options.responseId } : {}),
      stallTimeoutSec: config.stallTimeoutSec,
      localUpstream: false,
      hideThinkingSummary: parsed.options.hideThinkingSummary,
      declaredToolNames: maps.declaredToolNames,
      bareCustomToolNames: maps.bareCustomToolNames,
      // adapter-delivery.ts enforces declared tool names for Responses clients only.
      enforceDeclaredToolNames: options.inbound === "responses",
      toolParameterSchemas: maps.toolParameterSchemas,
      onFirstOutput: () => { firstOutputAt ??= Date.now(); },
      onUsage: reported => { usage = reported; },
      // Called before onUsage for the same final event, so it only marks the outcome; the row is
      // written when the stream ends, by which time usage has arrived.
      onCompletedResponse: () => { completed = true; },
    },
  );
  return {
    sse,
    route,
    // A stream that ends without response.completed failed (response.failed, a translator limit).
    finish: end => record(end === "cancel" || signal.aborted ? 499 : completed && errorStatus === undefined ? 200 : errorStatus ?? 502),
  };
}

export const serveNativeResponses: ServeNativeChat = async (bodyText, headers, signal, deps) => {
  const startedAt = Date.now();
  const no = (reason: string) => { deps.onDecline?.(`responses:${reason}`); return null; };
  let body: unknown;
  try { body = JSON.parse(bodyText); } catch { return no("body-not-json"); }
  if (!isRec(body)) return no("body-shape");
  // router.ts sends a bare native model to the `openai` provider; a routed id names its provider.
  if (typeof body.model === "string" && !body.model.includes("/")) {
    return (await runNativeOpenAiTurn(body, headers, signal, deps, no, startedAt))?.response ?? null;
  }
  const translatorBudget = createTranslatorBudget();
  const turn = await runNativeResponsesTurn(body, headers, signal, deps, no, { inbound: "responses", translatorBudget, startedAt });
  if (!turn) {
    translatorBudget.dispose();
    return null;
  }
  if ("failure" in turn) {
    translatorBudget.dispose();
    return turn.failure;
  }
  return new Response(recordAtEnd(turn.sse, end => { turn.finish(end); translatorBudget.dispose(); }), {
    headers: { "content-type": "text/event-stream", "cache-control": "no-cache", "x-accel-buffering": "no" },
  });
};


export type { NativeChatDeps };
