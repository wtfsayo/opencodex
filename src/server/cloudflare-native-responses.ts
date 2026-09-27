// Phase 5: streamed Responses API turns (what Codex CLI sends) answered in the Cloudflare Worker for
// openai-chat providers, reusing ocx's own parser, openai-chat adapter and Responses SSE bridge. It
// follows the container's path for exactly the turns where that path is stateless (the spec is in
// devlog/_plan/260927_cloudflare_native/000_goal.md, Phase 5); anything else returns null and the
// Worker forwards the untouched request to the container.
//
// The import graph is held Worker-safe by tests/service/cloudflare-worker-native.test.ts.
import type { NativeChatDeps, ServeNativeChat, WorkerUsageRow } from "./cloudflare-native-chat-api";
import { loadNativeConfig, recordAtEnd, resolveNativeChatRoute, sendUpstream } from "./cloudflare-native-chat";
import { isThreadSpawnRequest } from "./collab-surface";
import { buildToolBridgeMaps } from "./responses/tool-bridge-maps";
import { createOpenAIChatAdapterWith, type OpenAIChatAdapterDeps } from "../adapters/openai-chat/adapter";
import { renameRoutedIdentityInContext } from "../adapters/identity";
import { bridgeToResponsesSSE } from "../bridge/sse";
import { parseRequest } from "../responses/parser";
import { readResponseStreamWithInactivity, ResponseBodyInactivityError } from "../lib/response-body-inactivity";
import { createTranslatorBudget } from "../lib/translator-budget";
import { resolveStallTimeoutMs } from "../stall-timeout";
import type { AdapterEvent, OcxConfig, OcxUsage } from "../types";

type Rec = Record<string, unknown>;
const isRec = (value: unknown): value is Rec => !!value && typeof value === "object" && !Array.isArray(value);

// Body fields whose handling this path reproduces. Anything else (service_tier, conversation,
// background, previous_response_id, ...) is left to the container.
const BODY_FIELDS = new Set([
  "model", "instructions", "input", "tools", "tool_choice", "parallel_tool_calls", "reasoning", "store",
  "stream", "include", "prompt_cache_key", "text", "max_output_tokens", "temperature", "top_p", "metadata", "user",
]);
const INPUT_ITEMS = new Set(["message", "function_call", "function_call_output", "reasoning"]);
const TEXT_PARTS = new Set(["input_text", "output_text"]);
// Multi-agent tools: ocx injects guidance and caps effort on these turns (collaboration.ts).
const COLLABORATION_TOOLS = new Set([
  "spawn_agent", "send_input", "resume_agent", "close_agent", "send_message", "followup_task", "interrupt_agent", "list_agents",
]);
const SKILLS_BLOCK = "<skills_instructions>";

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
    // Effort ladders include what ocx learned about the model and keeps on disk.
    if (body.reasoning.effort !== undefined && body.reasoning.effort !== null) return "reasoning-effort";
  }
  if (body.tools !== undefined) {
    if (!Array.isArray(body.tools)) return "tools-shape";
    for (const tool of body.tools) {
      if (!isRec(tool) || tool.type !== "function" || typeof tool.name !== "string" || tool.namespace !== undefined) return "tool-type";
      // Codex's code-mode `exec` is parsed with Bun's transpiler.
      if (tool.name === "exec") return "code-mode-exec";
      if (COLLABORATION_TOOLS.has(tool.name)) return "collaboration-turn";
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
  // ocx substitutes a per-session skills snapshot for this block.
  if (containsSkillsBlock(body.instructions) || containsSkillsBlock(body.input)) return "skills-instructions";
  if (isThreadSpawnRequest(headers) || headers.has("x-codex-parent-thread-id")) return "collaboration-turn";
  if (headers.has("x-opencodex-grok")) return "grok-surface";
  return undefined;
}

// Effort-bearing and image-bearing turns are declined above, so these are never reached.
const WORKER_ADAPTER_DEPS: OpenAIChatAdapterDeps = {
  mapReasoningEffort: (_provider, _modelId, requested) => {
    if (requested !== undefined) throw new Error("reasoning effort is not mapped in the Worker");
    return undefined;
  },
  hasShrinkableOpenAIChatImages: () => false,
  normalizeOpenAIChatImages: async () => {},
};

export const serveNativeResponses: ServeNativeChat = async (bodyText, headers, signal, deps) => {
  const startedAt = Date.now();
  const no = (reason: string) => { deps.onDecline?.(`responses:${reason}`); return null; };
  let body: unknown;
  try { body = JSON.parse(bodyText); } catch { return no("body-not-json"); }
  if (!isRec(body)) return no("body-shape");
  const declined = nativeResponsesDeclineReason(body, headers);
  if (declined) return no(declined);
  const loaded = await loadNativeConfig(deps);
  if ("decline" in loaded) return no(loaded.decline);
  const route = resolveNativeChatRoute(loaded.config, body.model, new Set(Object.keys(deps.localHosts ?? {})), no, deps.secrets);
  if (!route) return null;
  const config = loaded.config as Pick<OcxConfig, "stallTimeoutSec">;

  let parsed;
  try { parsed = parseRequest(body); } catch { return no("parse"); }
  // As core-normalize.ts: the upstream sees the routed id, and the identity sentence names it.
  if (parsed._rawBody && typeof parsed._rawBody === "object") (parsed._rawBody as { model?: string }).model = route.modelId;
  parsed.modelId = route.modelId;
  parsed.context = renameRoutedIdentityInContext(parsed.context, route.modelId);
  const summary = isRec(body.reasoning) ? body.reasoning.summary : undefined;
  parsed.options.hideThinkingSummary = summary === "none" || !summary;

  const translatorBudget = createTranslatorBudget();
  const adapter = createOpenAIChatAdapterWith(route.provider, WORKER_ADAPTER_DEPS);
  const upstreamAbort = new AbortController();
  const upstreamSignal = AbortSignal.any([signal, upstreamAbort.signal]);
  const request = await adapter.buildRequest(parsed, { headers: new Headers(), translatorBudget, abortSignal: upstreamSignal });
  // Everything that can throw runs before the send: after it, a throw would resend via the container.
  const maps = buildToolBridgeMaps(parsed, translatorBudget);
  const upstream = await sendUpstream(request, upstreamSignal, deps);
  if (!upstream.ok || !upstream.body) {
    await upstream.body?.cancel();
    return no(`upstream-${upstream.status}`);
  }

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
      provider: route.providerName,
      model: route.modelId,
      requestedModel: route.requestedModel,
      inboundProtocol: "responses",
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
      enforceDeclaredToolNames: true,
      toolParameterSchemas: maps.toolParameterSchemas,
      onFirstOutput: () => { firstOutputAt ??= Date.now(); },
      onUsage: reported => { usage = reported; },
      // Called before onUsage for the same final event, so it only marks the outcome; the row is
      // written when the stream ends, by which time usage has arrived.
      onCompletedResponse: () => { completed = true; },
    },
  );
  // A stream that ends without response.completed failed (response.failed, a translator limit).
  return new Response(recordAtEnd(sse, end => record(
    end === "cancel" || signal.aborted ? 499 : completed && errorStatus === undefined ? 200 : errorStatus ?? 502,
  )), {
    headers: { "content-type": "text/event-stream", "cache-control": "no-cache", "x-accel-buffering": "no" },
  });
};


export type { NativeChatDeps };
