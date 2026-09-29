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
import { isWorkerSpendRefusal, workerSpendTurn } from "./cloudflare-native-spend";
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
import { sanitizePassthroughHeaders } from "./relay-frames";
import { createSseInspectorCore } from "./sse-inspector";
import { composeSseBlockRewrites, composeSsePayloadRewrites, payloadRewriteAsBlockRewrite } from "./sse-payload-rewrite";
import { collectSelfNamedNamespaceScrubAuthorization, createSelfNamedToolCallNamespaceScrubRewrite } from "./responses-self-named-namespace-scrub";
import { currentTurnWireToolCatalogBody } from "./responses-undeclared-tool-guard";
import { createRoutedNamespaceCallRestoreRewrite, restoreRoutedNamespaceCalls, type RoutedNamespaceToolAliases } from "../responses/namespace-tool-compat";
import { createResponsesFieldBackfillBlockRewrite } from "./responses/responses-field-backfill";
import { usageFromResponsesPayload } from "../usage/responses-usage";
import { createTranslatorBudget } from "../lib/translator-budget";
import { formatErrorResponse } from "../bridge/errors";
import type { ResponsesTerminalStatus } from "../bridge";
import { readDisplaySafeErrorText } from "../lib/bounded-body";
import { applyUpstreamRecoveryInit, fetchWithTransientRetry, isNonReplayableResponse, isReplayRefusalResponse, replayRefusalResponse, TRANSIENT_RETRY_MAX_ATTEMPTS } from "../lib/upstream-retry";
import { classifyTransportFailureKind } from "../lib/upstream-reachability";
import { formatPassthroughUpstreamError } from "./responses/passthrough-error";
import { captureTerminalHttpStatus, httpStatusForRequestLogTerminal, type TerminalStatusContext } from "./terminal-status";
import { ADMISSION_TOLERANCE, estimateInputTokens } from "./responses/input-admission-core";
import { usageDisplayTotalTokens } from "../usage/totals";
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
const INPUT_ITEMS = new Set(["message", "function_call", "function_call_output", "reasoning", "custom_tool_call", "custom_tool_call_output", "web_search_call"]);
// The ChatGPT backend runs hosted web search itself; ocx's search sidecar is for routed providers.
const TOOL_TYPES = new Set(["function", "custom", "namespace", "web_search"]);
// router.ts's native family is wider; these are the plain release slugs. ocx rewrites or entitles
// account-gated models and the Reserve lane (core-codex-account.ts, catalog/native-models.ts).
const PLAIN_NATIVE_MODEL = /^gpt-\d+(?:\.\d+)?(?:-[a-z0-9]+)*$/;
const GATED_NATIVE_MODEL = /daybreak|astra|reserve/;

const CHATGPT_ACCESS_TOKEN = /^eyJ[\w-]*\.[\w-]+\.[\w-]+$/;

/** A `type: "encrypted_content"` part anywhere in the input, which request-prepare.ts rewrites. */
function hasEncryptedContentPart(value: unknown): boolean {
  const stack: unknown[] = [value];
  while (stack.length > 0) {
    const node = stack.pop();
    if (Array.isArray(node)) stack.push(...node);
    else if (isRec(node)) {
      if (node.type === "encrypted_content") return true;
      stack.push(...Object.values(node));
    }
  }
  return false;
}

/** Why this turn is not one the Worker can serve exactly as ocx would, or undefined when it is. */
export function nativeOpenAiDeclineReason(body: Rec, headers: Headers, continued = false): string | undefined {
  const unknown = Object.keys(body).filter(key => !BODY_FIELDS.has(key) && !(continued && key === "previous_response_id")).sort();
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
  if (hasEncryptedContentPart(body.input)) return "encrypted-content-part";
  if (isThreadSpawnRequest(headers) || headers.has("x-codex-parent-thread-id")) return "collaboration-turn";
  if (headers.has("x-opencodex-grok")) return "grok-surface";
  // The caller's own login: its bearer and account, with the hub's key in the dedicated header
  // (auth-context.ts treats a bearer that is the admission key as a request for the stored main).
  // A ChatGPT login's bearer is its access token, a JWT; anything else (an API key, one of the hub's
  // own keys) is not a login this path forwards.
  const bearer = (headers.get("authorization") ?? "").replace(/^Bearer\s+/i, "").trim();
  if (!CHATGPT_ACCESS_TOKEN.test(bearer) || !headers.get("chatgpt-account-id")?.trim() || !headers.get("x-opencodex-api-key")) {
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
export async function noStoredCodexAccounts(deps: NativeChatDeps): Promise<boolean> {
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
 * ocx's own inspector (passthrough-delivery.ts), with the request log's part reduced to what the
 * usage row takes (request-log.ts's applyResponseLogMetadata): the latest usage a payload reports
 * until the terminal, the first output, and the terminal status.
 */
function createTurnInspector(onCompletedResponse?: (response: Rec) => void) {
  let terminal: ResponsesTerminalStatus | null = null;
  let usage: OcxUsage | undefined;
  let firstOutputAt: number | undefined;
  const statusContext: TerminalStatusContext = {};
  const inspector = createSseInspectorCore({
    onTerminal: status => { terminal ??= status; },
    inspectLogPayload: (_payload, parsed) => {
      if (!isRec(parsed)) return;
      const source = isRec(parsed.response) ? parsed.response : parsed;
      usage = usageFromResponsesPayload(source.usage) ?? usage;
      // request-log.ts's captureUpstreamErrorParsed, for the status its row records.
      captureTerminalHttpStatus(statusContext, parsed);
      const reason = isRec(parsed.response) && isRec(parsed.response.incomplete_details) ? parsed.response.incomplete_details.reason : undefined;
      if (parsed.type === "response.incomplete" && statusContext.terminalIncompleteReason === undefined
        && typeof reason === "string" && reason.trim()) statusContext.terminalIncompleteReason = reason.trim();
    },
    onFirstOutput: () => { firstOutputAt ??= Date.now(); },
    ...(onCompletedResponse ? { onCompletedResponse: (response: Rec) => onCompletedResponse(response) } : {}),
  });
  return {
    feed: (chunk: Uint8Array) => inspector.feed(chunk),
    finish: () => inspector.finish(),
    dispose: () => inspector.dispose(),
    terminalSeen: () => inspector.terminalSeen(),
    get terminal() { return terminal; },
    get usage() { return usage; },
    get firstOutputAt() { return firstOutputAt; },
    statusContext,
  };
}

export type NativeOpenAiTurn = { response: Response };

export type NativeOpenAiTurnOptions = {
  /**
   * The body was expanded from a stored response (state.ts's expandPreviousResponseInput, which the
   * WebSocket session reproduces for its own responses) and keeps its previous_response_id.
   */
  continued?: boolean;
  /** passthrough-dispatch.ts's rememberPassthroughResponse: the request and its completed response. */
  onCompletedResponse?: (request: Record<string, unknown>, response: Rec) => void;
};

/** main-account-cache.ts's identity key for an account id. */
async function mainQuotaIdentityKey(accountId: string): Promise<string> {
  const bytes = new TextEncoder().encode(`opencodex-main-quota-v1\0${accountId}`);
  return [...new Uint8Array(await crypto.subtle.digest("SHA-256", bytes))].map(byte => byte.toString(16).padStart(2, "0")).join("");
}

// passthrough-dispatch.ts's header deadline (config.connectTimeoutMs, not admitted, defaults to it).
const CONNECT_TIMEOUT_MS = 200_000;

/** fetch-helpers.ts's fetchWithHeaderTimeout: identity encoding for a stream, no redirects, a deadline. */
async function sendWithHeaderDeadline(
  send: (init: RequestInit) => Promise<Response>, init: RequestInit, signal: AbortSignal,
): Promise<Response> {
  const deadline = new AbortController();
  const timer = setTimeout(() => deadline.abort(new DOMException("Timeout elapsed", "TimeoutError")), CONNECT_TIMEOUT_MS);
  try {
    const headers = new Headers(init.headers);
    if (!headers.has("accept-encoding")) headers.set("accept-encoding", "identity");
    return await send({ ...init, headers, redirect: "manual", signal: AbortSignal.any([signal, deadline.signal]) });
  } finally {
    clearTimeout(timer);
  }
}

/** The turn, or null (after `no`) when ocx would serve it differently or its state decides it. */
export async function runNativeOpenAiTurn(
  body: Rec, headers: Headers, signal: AbortSignal, deps: NativeChatDeps,
  no: (reason: string) => null, startedAt: number, options: NativeOpenAiTurnOptions = {},
): Promise<NativeOpenAiTurn | null> {
  const declined = nativeOpenAiDeclineReason(body, headers, options.continued);
  if (declined) return no(declined);
  // auth-cors.ts's isProxyAdmissionSecret: ocx never forwards one of its own keys upstream.
  const bearer = headers.get("authorization")!.replace(/^Bearer\s+/i, "").trim();
  if (!deps.isAdmissionSecret || (await deps.isAdmissionSecret(bearer))) return no("caller-login");
  const loaded = await loadNativeConfig(deps);
  if ("decline" in loaded) return no(loaded.decline);
  const provider = canonicalOpenAiRow(loaded.config);
  if (!provider) return no("openai-row");
  // Configured client keys are checked by ocx (auth-cors.ts), which the Worker does not repeat.
  if (isRec(loaded.config) && Array.isArray(loaded.config.apiKeys) && loaded.config.apiKeys.length > 0) return no("configured-api-keys");
  if (!(await noStoredCodexAccounts(deps))) return no("codex-accounts");
  const facts: NativeOpenAiFacts | undefined = await deps.nativeOpenAiFacts?.();
  if (!facts) return no("native-facts-unpublished");
  if (facts.codexAccountsStored) return no("codex-accounts");
  // auth-context.ts: a caller holding the main login ocx observed gets its hard lock and cooldowns.
  if (facts.mainAccountIdentityKey !== null
    && facts.mainAccountIdentityKey === await mainQuotaIdentityKey(headers.get("chatgpt-account-id")!.trim())) return no("main-account");
  if (facts.nativeMainTrafficBlocked) return no("native-main-blocked");
  if (facts.contextRelayActive) return no("context-relay");
  if (facts.upstreamTransport === "proxied") return no("egress-proxy");
  if (facts.upstreamTransport === "websocket" && !deps.openUpstreamSocket) return no("upstream-websocket-unavailable");
  const ceiling = Object.hasOwn(facts.inputCeilings, body.model as string) ? facts.inputCeilings[body.model as string] : undefined;
  if (ceiling === undefined) return no("input-ceiling-unknown");
  // request-spend.ts's tracker for this lane (cloudflare-native-spend.ts): every send in the
  // retry ladder below reserves against the hub ledger first, and the turn's terminal usage
  // settles the last of them. Under a configured ceiling a hub without the ledger declines
  // rather than spend unbooked.
  const spend = workerSpendTurn(loaded.config, deps, {
    rootId: headers.get("x-codex-parent-thread-id")?.trim() || undefined,
    poolId: "openai",
  });
  if (spend === "declined") return no("spend-ledger-unavailable");

  const frozen = await freezeSkillsCatalog(body, headers, deps);
  if (frozen === "decline") return no("skills-snapshot-unavailable");
  const commitSkills = typeof frozen === "function" ? frozen : undefined;

  let parsed;
  try { parsed = parseRequest(body); } catch { return no("parse"); }
  if ((parsed.previousResponseId && !options.continued) || parsed._compactionRequest === true) return no("continuation");
  if (options.continued) parsed._previousResponseInputExpanded = true;
  // collaboration.ts: guidance for these surfaces reads the catalog and config on disk.
  if (collabSurface(parsed) !== null) return no("collaboration-turn");
  // request-prepare.ts answers an input far past the model's window locally (checkInputAdmission).
  const estimatedInputTokens = estimateInputTokens(parsed, parsed.modelId, provider);
  if (ceiling !== null && estimatedInputTokens > ceiling * ADMISSION_TOLERANCE) return no("input-admission");
  // core-normalize.ts: an omitted store is sent as false to this backend.
  if (isRec(parsed._rawBody) && parsed._rawBody.store === undefined) parsed._rawBody.store = false;

  const translatorBudget = createTranslatorBudget();
  const adapterDeps: ResponsesPassthroughAdapterDeps = {
    reasoningMetadata: NO_REASONING_METADATA,
    // Only read for a body carrying stream_options, which is declined above.
    supportsReasoningSummaries: () => undefined,
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
  // The client and continuation restores passthrough-dispatch.ts applies for these are not
  // reproduced here; the canonical row converts none on an ordinary Codex turn.
  if ((request.convertedRoutedNamespaceToolAliases?.size ?? 0) > 0 || (request.convertedMuseToolNameAliases?.size ?? 0) > 0
    || (request.plaintextV2AgentMessageToolNames?.size ?? 0) > 0 || (request.convertedRoutedToolSearchNames?.size ?? 0) > 0) {
    translatorBudget.dispose();
    return no("converted-tools");
  }
  const conversationId = conversationIdFromResponsesRequest({
    clientThreadId: parsed._clientThreadId,
    sessionIdHeader: sessionIdHeaderFromRequest(headers),
    threadIdHeader: headers.get("thread-id"),
    cursorConversationId: parsed._cursorConversationId,
  });
  const requestedEffort = parsed.options.reasoning;
  const recordRow = (status: number, fields: Partial<WorkerUsageRow> = {}) => {
    deps.recordUsage?.({
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
      ...(typeof parsed.options.maxOutputTokens === "number" && parsed.options.maxOutputTokens > 0
        ? { spendOutputCeilingTokens: Math.trunc(parsed.options.maxOutputTokens) } : {}),
      ...(spend ? { spendLedger: "worker" as const } : {}),
      status,
      durationMs: Date.now() - startedAt,
      usageStatus: "unreported",
      ...fields,
    });
  };

  // passthrough-dispatch.ts: ocx's transient ladder, with no ambiguous resend (ocx may buy one; the
  // Worker never sends a turn a second time that may already be running).
  const upstreamAbort = new AbortController();
  const upstreamSignal = AbortSignal.any([signal, upstreamAbort.signal]);
  const sseFetch = (url: string, httpInit: RequestInit) => deps.fetch(new Request(url, httpInit));
  const baseInit: RequestInit = { method: request.method, headers: request.headers, body: request.body };
  let upstream: Response;
  try {
    upstream = await fetchWithTransientRetry(recovery => sendWithHeaderDeadline(async init => {
      // One reservation per physical send: a retry is a new send and books its own.
      const admission = spend
        ? await spend.charge(estimatedInputTokens, typeof parsed.options.maxOutputTokens === "number" && parsed.options.maxOutputTokens > 0 ? Math.trunc(parsed.options.maxOutputTokens) : 0)
        : ({ ok: true } as const);
      if (!admission.ok) return admission.refusal;
      return facts.upstreamTransport === "websocket"
        ? workerCodexWsFetch(request.url, init, deps.openUpstreamSocket!, sseFetch)
        : sseFetch(request.url, init);
    }, applyUpstreamRecoveryInit(baseInit, recovery), upstreamSignal),
    { abortSignal: upstreamSignal, label: "chatgpt.com", attempts: TRANSIENT_RETRY_MAX_ATTEMPTS, claimAmbiguousResend: () => false });
  } catch (error) {
    spend?.resolve(undefined);
    translatorBudget.dispose();
    commitSkills?.();
    if (signal.aborted) {
      recordRow(499);
      return { response: formatErrorResponse(499, "client_cancelled", "Client cancelled request") };
    }
    // As the routed path: a timeout never reached the upstream; any other rejection may have, and is
    // answered with the reset ladder's refusal rather than an error the client would retry.
    if (classifyTransportFailureKind(error) === "timeout") {
      recordRow(502);
      return { response: formatErrorResponse(502, "upstream_error", `Provider connect timeout after ${CONNECT_TIMEOUT_MS}ms`) };
    }
    recordRow(429);
    return { response: replayRefusalResponse() };
  }
  // A local spend refusal inside the ladder: answered as ocx answers it, never reshaped.
  if (isWorkerSpendRefusal(upstream)) {
    translatorBudget.dispose();
    commitSkills?.();
    recordRow(429);
    return { response: upstream };
  }
  const responseHeaders = sanitizePassthroughHeaders(upstream.headers);
  // A refused create generated nothing: ocx sends it itself and runs its own recovery on the answer.
  // Not so the retry ladder's own refusal to resend a turn that may already be running (a 429).
  const replayRefusal = isReplayRefusalResponse(upstream) || isNonReplayableResponse(upstream);
  if (upstream.status >= 400 && upstream.status < 500 && !replayRefusal) {
    spend?.resolve(undefined);
    await upstream.body?.cancel().catch(() => {});
    translatorBudget.dispose();
    return no(`upstream-${upstream.status}`);
  }
  commitSkills?.();
  if (upstream.status >= 300 && upstream.status < 400) {
    translatorBudget.dispose();
    spend?.resolve(undefined);
    recordRow(upstream.status);
    return { response: new Response(upstream.body, { status: upstream.status, statusText: upstream.statusText, headers: responseHeaders }) };
  }
  if (!upstream.ok) {
    const errorText = await readDisplaySafeErrorText(upstream, upstreamSignal, "");
    translatorBudget.dispose();
    spend?.resolve(undefined);
    recordRow(upstream.status);
    return { response: formatPassthroughUpstreamError(upstream.status, errorText, {
      statusText: upstream.statusText, headers: responseHeaders, replayRefusal,
    }) };
  }
  const servedModel = responseHeaders.get("openai-model")?.trim();
  const contentType = responseHeaders.get("content-type")?.toLowerCase();
  if (!upstream.body || !(contentType?.includes("text/event-stream") || (!contentType && parsed.stream))) {
    // passthrough-delivery.ts relays anything but an event stream as it came; the turn was sent,
    // so it is answered here rather than sent again through ocx.
    translatorBudget.dispose();
    spend?.resolve(undefined);
    recordRow(upstream.status, servedModel ? { resolvedModel: servedModel } : {});
    return { response: new Response(upstream.body, { status: upstream.status, statusText: upstream.statusText, headers: responseHeaders }) };
  }

  const payloadRewrites = [
    createSelfNamedToolCallNamespaceScrubRewrite(scrubAuthorization),
    bareNamespaceAliases.size > 0 ? createRoutedNamespaceCallRestoreRewrite(bareNamespaceAliases) : undefined,
  ].filter((rewrite): rewrite is NonNullable<typeof rewrite> => rewrite !== undefined);
  const rewriteBlocks = composeSseBlockRewrites(
    payloadRewriteAsBlockRewrite(composeSsePayloadRewrites(...payloadRewrites)),
    createResponsesFieldBackfillBlockRewrite(),
  );
  const rawBody = parsed._rawBody as Record<string, unknown>;
  // passthrough-dispatch.ts's rememberPassthroughResponseChecked: the continuation keeps the
  // client's own tool names (the bare-namespace restore), as the client sent them back.
  const remember = options.onCompletedResponse
    ? (response: Rec) => options.onCompletedResponse!(rawBody, restoreRoutedNamespaceCalls(response, bareNamespaceAliases).value as Rec)
    : undefined;
  const inspector = createTurnInspector(remember);
  const turnAc = new AbortController();
  turnAc.signal.addEventListener("abort", () => upstreamAbort.abort(), { once: true });
  let recorded = false;
  const record = (status: number) => {
    if (recorded) return;
    recorded = true;
    const usage = inspector.usage;
    spend?.resolve(usage ? { inputTokens: usage.inputTokens, outputTokens: usage.outputTokens } : undefined);
    recordRow(status, {
      ...(servedModel ? { resolvedModel: servedModel } : {}),
      ...(inspector.firstOutputAt !== undefined ? { firstOutputMs: inspector.firstOutputAt - startedAt } : {}),
      usageStatus: usage ? "reported" : "unreported",
      ...(usage ? { usage: usage as NonNullable<WorkerUsageRow["usage"]>, totalTokens: usageDisplayTotalTokens(usage) } : {}),
    });
  };
  const relayed = relaySseEagerBounded(upstream.body, turnAc, {
    inspectChunk: chunk => inspector.feed(chunk),
    finishInspection: () => inspector.finish(),
    disposeInspection: () => inspector.dispose(),
    sawTerminal: () => inspector.terminalSeen(),
    rewriteBlocks,
    // passthrough-delivery.ts's reportNativeTerminal for a synthetic end.
    onSynthetic: (kind, reason) => {
      if (kind === "incomplete") record(httpStatusForRequestLogTerminal("incomplete", inspector.statusContext));
      else if (reason === "upstream_error") record(inspector.statusContext.terminalHttpStatus ?? 502);
      else record(502);
    },
    onClientCancel: () => record(499),
    onDone: () => {
      translatorBudget.dispose();
      const terminal = inspector.terminal;
      if (terminal) record(httpStatusForRequestLogTerminal(terminal, inspector.statusContext));
      else record(signal.aborted ? 499 : 502);
    },
  }, { clientGoneSignal: signal, terminalBoundary: { dropCodexSafetyBuffering: false }, rewriteBudget: translatorBudget });
  if (!responseHeaders.has("content-type")) responseHeaders.set("content-type", "text/event-stream");
  return { response: new Response(relayed, { status: upstream.status, headers: responseHeaders }) };
}
