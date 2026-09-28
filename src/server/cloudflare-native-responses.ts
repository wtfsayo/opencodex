// Phase 5: streamed Responses API turns (what Codex CLI sends) answered in the Cloudflare Worker for
// openai-chat providers, reusing ocx's own parser, openai-chat adapter and Responses SSE bridge. It
// follows the container's path for exactly the turns where that path is stateless (the spec is in
// devlog/_plan/260927_cloudflare_native/000_goal.md, Phase 5); anything else returns null and the
// Worker forwards the untouched request to the container.
//
// The import graph is held Worker-safe by tests/service/cloudflare-worker-native.test.ts.
import type { NativeChatDeps, ServeNativeChat, WorkerUsageRow } from "./cloudflare-native-chat-api";
import { loadNativeConfig, recordAtEnd, resolveNativeChatRoute, routeUsageFields, sendUpstream, type NativeChatRoute } from "./cloudflare-native-chat";
import { collabSurface, isThreadSpawnRequest } from "./collab-surface";
import { buildToolBridgeMaps } from "./responses/tool-bridge-maps";
import { replaceSkillsBlock, singleSkillsBlock } from "./responses/skills-catalog";
import { conversationIdFromResponsesRequest, reasoningReplayConversationIdFromResponsesRequest, sessionIdHeaderFromRequest } from "./request-log-conversation";
import { createOpenAIChatAdapterWith, type OpenAIChatAdapterDeps } from "../adapters/openai-chat/adapter";
import { renameRoutedIdentityInContext } from "../adapters/identity";
import { bridgeToResponsesSSE } from "../bridge/sse";
import { hasValidatedActiveReasoningEffort, parseRequest } from "../responses/parser";
import { mapReasoningEffortWith, NO_REASONING_METADATA } from "../reasoning-effort-core";
import { metadataProviderKeyForBaseUrl } from "../providers/reasoning-metadata-destinations";
import { readResponseStreamWithInactivity, ResponseBodyInactivityError } from "../lib/response-body-inactivity";
import { createTranslatorBudget, type TranslatorBudget } from "../lib/translator-budget";
import { resolveStallTimeoutMs } from "../stall-timeout";
import type { AdapterEvent, OcxConfig, OcxUsage } from "../types";

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
const COLLABORATION_TOOLS = new Set([
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
async function freezeSkillsCatalog(body: Rec, headers: Headers, deps: NativeChatDeps): Promise<"decline" | (() => void) | undefined> {
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
          && item.content.every(part => isRec(part) && typeof part.type === "string" && TEXT_PARTS.has(part.type)))) return "message-parts";
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

// Effort is mapped as ocx maps it for a destination without models.dev metadata (the only kind
// resolveNativeChatRoute admits). Image-bearing turns are declined above, so those two are never reached.
const WORKER_ADAPTER_DEPS: OpenAIChatAdapterDeps = {
  mapReasoningEffort: (provider, modelId, requested) => mapReasoningEffortWith(provider, modelId, requested, NO_REASONING_METADATA),
  hasShrinkableOpenAIChatImages: () => false,
  normalizeOpenAIChatImages: async () => {},
};

/** A turn sent upstream: its Responses SSE, and the callback that writes its usage row when it ends. */
export type NativeResponsesTurn = {
  sse: ReadableStream<Uint8Array>;
  route: NativeChatRoute;
  finish(end: "end" | "error" | "cancel"): void;
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
  const route = resolveNativeChatRoute(loaded.config, body.model, new Set(Object.keys(deps.localHosts ?? {})), no, deps.secrets);
  if (!route) return null;
  // ocx maps effort for these destinations from models.dev metadata and refusals it learned, both
  // kept on disk (reasoning-metadata.ts); without an effort that state is never consulted.
  const effort = isRec(body.reasoning) ? body.reasoning.effort : undefined;
  if (effort !== undefined && effort !== null && metadataProviderKeyForBaseUrl(route.provider.baseUrl) !== undefined) {
    return no("reasoning-metadata-destination");
  }
  const config = loaded.config as Pick<OcxConfig, "stallTimeoutSec">;

  // ocx's web-search sidecar resolves through a configured OpenAI provider's accounts.
  const providers = (loaded.config as { providers: Record<string, unknown> }).providers;
  const openai = providers.openai;
  if (hasHostedWebSearch(body) && openai !== undefined && !(isRec(openai) && openai.disabled === true)) return no("web-search-sidecar");

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
  parsed.modelId = route.modelId;
  parsed.context = renameRoutedIdentityInContext(parsed.context, route.modelId);
  // core-normalize.ts; the provider's showThinkingSummary is outside the fields this path admits.
  // Only for a Responses client: an Anthropic replay keeps what the parser decided.
  if (options.inbound === "responses") {
    const summary = isRec(body.reasoning) ? body.reasoning.summary : undefined;
    parsed.options.hideThinkingSummary = summary === "none" || (!summary && !hasValidatedActiveReasoningEffort(parsed.options));
  }

  const translatorBudget = options.translatorBudget;
  const adapter = createOpenAIChatAdapterWith(route.provider, WORKER_ADAPTER_DEPS);
  const upstreamAbort = new AbortController();
  const upstreamSignal = AbortSignal.any([signal, upstreamAbort.signal]);
  const request = await adapter.buildRequest(parsed, { headers: new Headers(), translatorBudget, abortSignal: upstreamSignal });
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
  const upstream = await sendUpstream(request, upstreamSignal, deps);
  if (!upstream.ok || !upstream.body) {
    await upstream.body?.cancel();
    return no(`upstream-${upstream.status}`);
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
      ...(requestedEffort ? { requestedEffort } : {}),
      ...(conversationId ? { conversationId } : {}),
      ...(options.surface ? { surface: options.surface } : {}),
      inboundProtocol: options.inbound === "anthropic" ? "messages" : "responses",
      admissionKind: "environment",
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
    events, route.modelId, maps.toolNsMap, maps.freeformToolNames, maps.toolSearchToolNames,
    () => upstreamAbort.abort(), 2_000,
    {
      translatorBudget,
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
  const translatorBudget = createTranslatorBudget();
  const turn = await runNativeResponsesTurn(body, headers, signal, deps, no, { inbound: "responses", translatorBudget, startedAt });
  if (!turn) {
    translatorBudget.dispose();
    return null;
  }
  return new Response(recordAtEnd(turn.sse, end => { turn.finish(end); translatorBudget.dispose(); }), {
    headers: { "content-type": "text/event-stream", "cache-control": "no-cache", "x-accel-buffering": "no" },
  });
};


export type { NativeChatDeps };
