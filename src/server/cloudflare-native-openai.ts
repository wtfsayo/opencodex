// A Codex turn to a native OpenAI model on the caller's own ChatGPT login, served by the Cloudflare
// Worker the way ocx serves it: the canonical `openai` row forwards the caller's bearer to the
// ChatGPT backend (auth-context.ts's caller-owned "main" context), builds the body with the Responses
// passthrough adapter, dials the transport ocx would, and relays the stream through the same client
// rewrites (passthrough-dispatch.ts, passthrough-delivery.ts).
//
// Everything that reads or writes ocx's process state declines instead: stored Codex accounts (pool
// selection, quota and health memory), continuation state (previous_response_id), compaction,
// sub-agent and collaboration turns, Fast, and the models ocx gates by account. What ocx's process
// holds that even a plain turn depends on arrives as published facts (worker-native-state.ts).
//
// The import graph is held Worker-safe by tests/service/cloudflare-worker-native.test.ts.
import type { NativeChatDeps, NativeOpenAiFacts, WorkerUsageRow } from "./cloudflare-native-chat-api";
import { configKeyAdmitted, loadNativeConfig } from "./cloudflare-native-chat";
import { COLLABORATION_TOOLS, freezeSkillsCatalog } from "./cloudflare-native-responses";
import { createResponsesPassthroughAdapterWith, type ResponsesPassthroughAdapterDeps } from "../adapters/openai-responses/passthrough-adapter";
import { FORWARD_HEADERS } from "../adapters/openai-responses/forward-headers";
import { NO_REASONING_METADATA } from "../reasoning-effort-core";
import { parseRequest } from "../responses/parser";
import { collabSurface, isThreadSpawnRequest } from "./collab-surface";
import { buildToolBridgeMaps } from "./responses/tool-bridge-maps";
import { conversationIdFromResponsesRequest, sessionIdHeaderFromRequest } from "./request-log-conversation";
import { CODEX_RESPONSES_HTTP_URL, CODEX_RESPONSES_WS_URL, prepareCodexHttpInit, prepareCodexWsRequest } from "./responses/codex-ws-request";
import { codexWsExchange } from "./responses/codex-ws-exchange";
import { CodexWsSession } from "./responses/codex-ws-session";
import { codexWsCreateFrameExceedsLimit } from "./responses/codex-ws-wire";
import { relaySseEagerBounded } from "./relay-eager";
import { isFirstOutputSsePayload, sanitizePassthroughHeaders, terminalStatusFromParsed } from "./relay-frames";
import { BoundedSseFrameBuffer } from "./sse-frame-buffer";
import { composeSseBlockRewrites, composeSsePayloadRewrites, payloadRewriteAsBlockRewrite, sseDataPayload } from "./sse-payload-rewrite";
import { collectSelfNamedNamespaceScrubAuthorization, createSelfNamedToolCallNamespaceScrubRewrite } from "./responses-self-named-namespace-scrub";
import { currentTurnWireToolCatalogBody } from "./responses-undeclared-tool-guard";
import { createRoutedNamespaceCallRestoreRewrite, type RoutedNamespaceToolAliases } from "../responses/namespace-tool-compat";
import { createResponsesFieldBackfillBlockRewrite } from "./responses/responses-field-backfill";
import { usageFromResponsesPayload } from "../usage/responses-usage";
import { createTranslatorBudget } from "../lib/translator-budget";
import type { OcxProviderConfig, OcxUsage } from "../types";

type Rec = Record<string, unknown>;
const isRec = (value: unknown): value is Rec => !!value && typeof value === "object" && !Array.isArray(value);

/** The `openai` row `ocx init` writes (config/proxy-env.ts), the only native row this path reproduces. */
const CANONICAL_OPENAI_ROW = {
  adapter: "openai-responses",
  baseUrl: "https://chatgpt.com/backend-api/codex",
  codexAccountMode: "pool",
  authMode: "forward",
} as const;
// What Codex CLI sends on an ordinary native turn.
const BODY_FIELDS = new Set([
  "model", "instructions", "input", "tools", "tool_choice", "parallel_tool_calls", "reasoning", "store",
  "stream", "include", "prompt_cache_key", "text", "client_metadata",
]);
const INPUT_ITEMS = new Set(["message", "function_call", "function_call_output", "reasoning", "custom_tool_call", "custom_tool_call_output"]);
const TOOL_TYPES = new Set(["function", "custom", "namespace"]);
// router.ts's native family is wider; these are the plain release slugs. ocx rewrites or entitles
// account-gated models and the Reserve lane (core-codex-account.ts, catalog/native-models.ts).
const PLAIN_NATIVE_MODEL = /^gpt-\d+(?:\.\d+)?(?:-[a-z0-9]+)*$/;
const GATED_NATIVE_MODEL = /daybreak|astra|reserve/;

/** Why this turn is not one the Worker can serve exactly as ocx would, or undefined when it is. */
export function nativeOpenAiDeclineReason(body: Rec, headers: Headers): string | undefined {
  const unknown = Object.keys(body).filter(key => !BODY_FIELDS.has(key)).sort();
  if (unknown.length > 0) return `body-fields:${unknown.join(",")}`;
  if (typeof body.model !== "string" || !PLAIN_NATIVE_MODEL.test(body.model) || GATED_NATIVE_MODEL.test(body.model)) return "native-model";
  if (body.stream !== true) return "not-streamed";
  // core-normalize.ts defaults an omitted store to false for this backend; true is ocx's to refuse.
  if (body.store !== undefined && body.store !== false) return "stored-response";
  if (body.reasoning !== undefined) {
    if (!isRec(body.reasoning)) return "reasoning-shape";
    // catalog/effort.ts clamps these to the model's top rung.
    if (body.reasoning.effort === "max" || body.reasoning.effort === "ultra") return "native-effort-clamp";
  }
  if (body.tools !== undefined) {
    if (!Array.isArray(body.tools)) return "tools-shape";
    for (const tool of body.tools) {
      if (!isRec(tool) || typeof tool.type !== "string" || !TOOL_TYPES.has(tool.type)) return "tool-type";
      if (tool.type === "function" && COLLABORATION_TOOLS.has(tool.name as string)) return "collaboration-turn";
      if (tool.type === "namespace" && !(Array.isArray(tool.tools) && tool.tools.every(member => isRec(member) && member.type === "function"))) return "tool-type";
    }
  }
  if (!Array.isArray(body.input) || body.input.length === 0) return "input-shape";
  for (const item of body.input) {
    if (!isRec(item) || typeof item.type !== "string" || !INPUT_ITEMS.has(item.type)) return "input-item";
  }
  // request-prepare.ts rewrites plaintext spawn-message slots and recovers encrypted agent tasks.
  if (JSON.stringify(body.input).includes("\"encrypted_content\",")) return "encrypted-content-part";
  if (isThreadSpawnRequest(headers) || headers.has("x-codex-parent-thread-id")) return "collaboration-turn";
  if (headers.has("x-opencodex-grok")) return "grok-surface";
  // The caller's own login: its bearer and account, with the hub's key in the dedicated header
  // (auth-context.ts treats a bearer that is the admission key as a request for the stored main).
  if (!/^Bearer\s+\S/i.test(headers.get("authorization") ?? "") || !headers.get("chatgpt-account-id") || !headers.get("x-opencodex-api-key")) {
    return "caller-login";
  }
  return undefined;
}

/** The canonical row exactly, and a config this path reproduces otherwise. */
function canonicalOpenAiRow(config: unknown): OcxProviderConfig | undefined {
  if (!isRec(config) || !isRec(config.providers) || !Object.keys(config).every(key => configKeyAdmitted(config, key))) return undefined;
  const row = config.providers.openai;
  if (!isRec(row)) return undefined;
  const keys = Object.keys(row);
  if (keys.length !== Object.keys(CANONICAL_OPENAI_ROW).length) return undefined;
  for (const [key, value] of Object.entries(CANONICAL_OPENAI_ROW)) if (row[key] !== value) return undefined;
  return { ...CANONICAL_OPENAI_ROW };
}

/** No stored Codex account: codex-accounts.json absent, or holding none. */
async function noStoredCodexAccounts(deps: NativeChatDeps): Promise<boolean> {
  if (!deps.readCodexAccounts) return false;
  const text = await deps.readCodexAccounts();
  if (text === undefined) return true;
  try {
    const parsed = JSON.parse(text) as unknown;
    return isRec(parsed) && Array.isArray(parsed.accounts) && parsed.accounts.length === 0
      && Object.keys(parsed).every(key => key === "accounts" || key === "version");
  } catch {
    return false;
  }
}

/** auth-context.ts's materializeCodexUpstreamAuth for a caller-owned main context. */
function callerForwardHeaders(headers: Headers): Headers {
  const selected = new Headers();
  for (const name of FORWARD_HEADERS) {
    const value = headers.get(name);
    if (value) selected.set(name, value);
  }
  return selected;
}

/** ws-upstream.ts's codexWsUpstreamFetch, with the Worker's dialer and no socket pool. */
function workerCodexWsFetch(
  url: string, init: RequestInit, openSocket: NonNullable<NativeChatDeps["openUpstreamSocket"]>,
  sseFallback: (url: string, init: RequestInit) => Promise<Response>,
): Promise<Response> {
  const prepared = prepareCodexWsRequest(url, init);
  if (!prepared) return sseFallback(url, prepareCodexHttpInit(url, init));
  if (url !== CODEX_RESPONSES_HTTP_URL || codexWsCreateFrameExceedsLimit(prepared.frameText)) return sseFallback(url, prepared.httpInit);
  let session: CodexWsSession;
  try {
    session = new CodexWsSession(CODEX_RESPONSES_WS_URL, prepared.headers, false, undefined, undefined, openSocket);
    if (!session.reserve()) {
      session.dispose();
      return sseFallback(url, prepared.httpInit);
    }
  } catch {
    return sseFallback(url, prepared.httpInit);
  }
  return codexWsExchange({ session, url, init: prepared.httpInit, prepared, sseFallback: sseFallback as typeof fetch, bunVersion: "workerd" });
}

/**
 * passthrough-delivery.ts's createSseInspector, for what the usage row needs: the first output,
 * the terminal status, and the usage the terminal response reports.
 */
function createUsageInspector() {
  const framer = new BoundedSseFrameBuffer();
  const decoder = new TextDecoder();
  let terminal: "completed" | "failed" | "incomplete" | null = null;
  let usage: OcxUsage | undefined;
  let firstOutputAt: number | undefined;
  const handle = (block: string) => {
    const payload = sseDataPayload(block);
    if (payload === null) return;
    if (firstOutputAt === undefined && isFirstOutputSsePayload(payload)) firstOutputAt = Date.now();
    let parsed: unknown;
    try { parsed = JSON.parse(payload); } catch { return; }
    const status = terminalStatusFromParsed(parsed);
    if (status && !terminal) {
      terminal = status;
      const response = isRec(parsed) && isRec(parsed.response) ? parsed.response : undefined;
      usage = usageFromResponsesPayload(response?.usage);
    }
  };
  return {
    feed(chunk: Uint8Array) { for (const frame of framer.feed(chunk)) handle(decoder.decode(frame.block)); },
    finish() {
      const tail = framer.finish();
      if (tail.byteLength > 0) handle(decoder.decode(tail));
    },
    dispose() { framer.dispose(); },
    get terminal() { return terminal; },
    get usage() { return usage; },
    get firstOutputAt() { return firstOutputAt; },
  };
}

export type NativeOpenAiTurn = { response: Response };

/** The turn, or null (after `no`) when ocx would serve it differently or its state decides it. */
export async function runNativeOpenAiTurn(
  body: Rec, headers: Headers, signal: AbortSignal, deps: NativeChatDeps,
  no: (reason: string) => null, startedAt: number,
): Promise<NativeOpenAiTurn | null> {
  const declined = nativeOpenAiDeclineReason(body, headers);
  if (declined) return no(declined);
  const loaded = await loadNativeConfig(deps);
  if ("decline" in loaded) return no(loaded.decline);
  const provider = canonicalOpenAiRow(loaded.config);
  if (!provider) return no("openai-row");
  if (!(await noStoredCodexAccounts(deps))) return no("codex-accounts");
  const facts: NativeOpenAiFacts | undefined = await deps.nativeOpenAiFacts?.();
  if (!facts) return no("native-facts-unpublished");
  if (facts.mainCredentialObserved) return no("main-credential-observed");
  if (facts.nativeMainTrafficBlocked) return no("native-main-blocked");
  if (facts.contextRelayActive) return no("context-relay");
  if (facts.upstreamTransport === "proxied") return no("egress-proxy");
  if (facts.upstreamTransport === "websocket" && !deps.openUpstreamSocket) return no("upstream-websocket-unavailable");

  const frozen = await freezeSkillsCatalog(body, headers, deps);
  if (frozen === "decline") return no("skills-snapshot-unavailable");
  const commitSkills = typeof frozen === "function" ? frozen : undefined;

  let parsed;
  try { parsed = parseRequest(body); } catch { return no("parse"); }
  if (parsed.previousResponseId || parsed._compactionRequest === true) return no("continuation");
  // collaboration.ts: guidance for these surfaces reads the catalog and config on disk.
  if (collabSurface(parsed) !== null) return no("collaboration-turn");
  // core-normalize.ts: an omitted store is sent as false to this backend.
  if (isRec(parsed._rawBody) && parsed._rawBody.store === undefined) parsed._rawBody.store = false;

  const translatorBudget = createTranslatorBudget();
  const adapterDeps: ResponsesPassthroughAdapterDeps = {
    reasoningMetadata: NO_REASONING_METADATA,
    supportsReasoningSummaries: modelId => Object.hasOwn(facts.reasoningSummarySupport, modelId) ? facts.reasoningSummarySupport[modelId] : undefined,
    observeOutbound: () => {},
  };
  const adapter = createResponsesPassthroughAdapterWith(provider, adapterDeps);
  const toolBridgeMaps = buildToolBridgeMaps(parsed, translatorBudget);
  const clientToolAuthorizationBody = currentTurnWireToolCatalogBody(parsed._rawBody, parsed._replayPrefixLen ?? 0);
  const scrubAuthorization = collectSelfNamedNamespaceScrubAuthorization(
    clientToolAuthorizationBody, toolBridgeMaps.bareCustomToolNames, toolBridgeMaps.bareFunctionToolNames,
  );
  const bareNamespaceAliases: RoutedNamespaceToolAliases = new Map(
    [...toolBridgeMaps.toolNsMap].flatMap(([alias, identity]) => alias === identity.name
      ? [[alias, { namespace: identity.namespace, name: identity.name, kind: identity.freeform ? "custom" as const : "function" as const }] as const]
      : []),
  );
  let request;
  try {
    request = await adapter.buildRequest(parsed, { headers: callerForwardHeaders(headers), translatorBudget });
  } catch {
    translatorBudget.dispose();
    return no("build-request");
  }
  const conversationId = conversationIdFromResponsesRequest({
    clientThreadId: parsed._clientThreadId,
    sessionIdHeader: sessionIdHeaderFromRequest(headers),
    threadIdHeader: headers.get("thread-id"),
    cursorConversationId: parsed._cursorConversationId,
  });
  const requestedEffort = parsed.options.reasoning;

  const upstreamAbort = new AbortController();
  const upstreamSignal = AbortSignal.any([signal, upstreamAbort.signal]);
  const init: RequestInit = { method: request.method, headers: request.headers, body: request.body, signal: upstreamSignal };
  const sseFetch = (url: string, httpInit: RequestInit) => deps.fetch(new Request(url, httpInit));
  let upstream: Response;
  try {
    upstream = facts.upstreamTransport === "websocket"
      ? await workerCodexWsFetch(request.url, init, deps.openUpstreamSocket!, sseFetch)
      : await sseFetch(request.url, init);
  } catch {
    translatorBudget.dispose();
    return no("upstream-unreachable");
  }
  if (!upstream.ok || !upstream.body) {
    await upstream.body?.cancel().catch(() => {});
    translatorBudget.dispose();
    return no(`upstream-${upstream.status}`);
  }
  commitSkills?.();

  const payloadRewrites = [
    createSelfNamedToolCallNamespaceScrubRewrite(scrubAuthorization),
    bareNamespaceAliases.size > 0 ? createRoutedNamespaceCallRestoreRewrite(bareNamespaceAliases) : undefined,
  ].filter((rewrite): rewrite is NonNullable<typeof rewrite> => rewrite !== undefined);
  const rewriteBlocks = composeSseBlockRewrites(
    payloadRewriteAsBlockRewrite(composeSsePayloadRewrites(...payloadRewrites)),
    createResponsesFieldBackfillBlockRewrite(),
  );
  const inspector = createUsageInspector();
  const turnAc = new AbortController();
  turnAc.signal.addEventListener("abort", () => upstreamAbort.abort(), { once: true });
  let recorded = false;
  const record = (status: number) => {
    if (recorded) return;
    recorded = true;
    const usage = inspector.usage;
    const row: WorkerUsageRow = {
      requestId: crypto.randomUUID(),
      timestamp: startedAt,
      provider: "openai",
      model: parsed.modelId,
      requestedModel: parsed.modelId,
      resolvedModel: parsed.modelId,
      ...(requestedEffort ? { requestedEffort } : {}),
      ...(conversationId ? { conversationId } : {}),
      inboundProtocol: "responses",
      admissionKind: "environment",
      status,
      durationMs: Date.now() - startedAt,
      ...(inspector.firstOutputAt !== undefined ? { firstOutputMs: inspector.firstOutputAt - startedAt } : {}),
      usageStatus: usage ? "reported" : "unreported",
      ...(usage ? { usage: usage as NonNullable<WorkerUsageRow["usage"]>, totalTokens: usage.totalTokens ?? usage.inputTokens + usage.outputTokens } : {}),
    };
    deps.recordUsage?.(row);
  };
  let synthetic: "incomplete" | "failed" | undefined;
  const relayed = relaySseEagerBounded(upstream.body, turnAc, {
    inspectChunk: chunk => inspector.feed(chunk),
    finishInspection: () => inspector.finish(),
    disposeInspection: () => inspector.dispose(),
    sawTerminal: () => inspector.terminal !== null,
    rewriteBlocks,
    onSynthetic: kind => { synthetic ??= kind; },
    onClientCancel: () => record(499),
    onDone: () => {
      translatorBudget.dispose();
      record(signal.aborted ? 499 : inspector.terminal === "completed" && !synthetic ? 200 : inspector.terminal === "incomplete" ? 200 : 502);
    },
  }, { clientGoneSignal: signal, terminalBoundary: { dropCodexSafetyBuffering: false }, rewriteBudget: translatorBudget });
  const responseHeaders = sanitizePassthroughHeaders(upstream.headers);
  if (!responseHeaders.has("content-type")) responseHeaders.set("content-type", "text/event-stream");
  return { response: new Response(relayed, { status: upstream.status, headers: responseHeaders }) };
}
