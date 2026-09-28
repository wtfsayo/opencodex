// Anthropic Messages turns (what Claude Code sends) answered in the Cloudflare Worker. ocx serves a
// routed Messages turn by translating it to a Responses request and replaying that through its
// Responses pipeline (claude-messages.ts); this does the same through the Worker's Responses turn,
// for exactly the turns where ocx's Messages handler does nothing that reads process state. Anything
// else returns null and the Worker forwards the untouched request to the container.
//
// The import graph is held Worker-safe by tests/service/cloudflare-worker-native.test.ts.
import type { ServeNativeChat } from "./cloudflare-native-chat-api";
import { recordAtEnd } from "./cloudflare-native-chat";
import { runNativeResponsesTurn } from "./cloudflare-native-responses";
import { FORWARD_HEADERS } from "../adapters/openai-responses/forward-headers";
import { openAIChatSerializesThinking } from "../adapters/openai-chat/messages";
import { resolveAlias } from "../claude/alias-codec";
import { extractOcxEffortDirective, extractOcxRouteDirective } from "../claude/inbound-model-options";
import { carriesMessageThread } from "../claude/message-threads";
import { stripOneMillionMarker } from "../claude/one-m-marker";
import { anthropicErrorResponse, collectAnthropicMessage, responsesSseToAnthropicSse } from "../claude/outbound";
import { anthropicErrorFromResponsesError } from "../claude/responses-error";
import { estimateClaudeRequestTokens } from "../claude/request-token-estimate";
import { CLAUDE_NATIVE_THINKING } from "../lib/claude-request-projection";
import { messagesToResponsesTranslation } from "../protocols/codecs/messages";
import type { ClaudeInboundTranslation } from "../claude/inbound";
import { conversationIdFromClaudeMetadata, normalizeLogConversationId, sessionLaneIdFromRequest } from "./request-log-conversation";
import { claudeNativeSessionId } from "../claude/native-session-id";
import { jsonUtf8Bytes } from "../lib/json-byte-size";
import { createTranslatorBudget, isTranslatorBudgetExceededError } from "../lib/translator-budget";

type Rec = Record<string, unknown>;
const isRec = (value: unknown): value is Rec => !!value && typeof value === "object" && !Array.isArray(value);

export const serveNativeMessages: ServeNativeChat = async (bodyText, headers, signal, deps) => {
  const startedAt = Date.now();
  const no = (reason: string) => { deps.onDecline?.(`messages:${reason}`); return null; };
  let body: unknown;
  try { body = JSON.parse(bodyText); } catch { return no("body-not-json"); }
  if (!isRec(body) || typeof body.model !== "string") return no("body-shape");
  const requestedModel = stripOneMillionMarker(body.model);
  body.model = requestedModel;
  // An injected agent pins its model and effort in the system prompt (devlog 072).
  if (extractOcxRouteDirective(body) !== null || extractOcxEffortDirective(body) !== null) return no("route-directive");
  // Only ocx's own routed-model aliases. A bare Claude id can be native passthrough on the
  // caller's credential, a Claude Desktop alias, a modelMap entry or an Auto Mode classifier
  // check, and each of those resolves against state this path does not have.
  if (!resolveAlias(requestedModel)) return no("model-not-alias");
  // ocx answers a message-thread turn with the error that makes Claude Code resend it in full.
  if (carriesMessageThread(body)) return no("message-thread");

  const translatorBudget = createTranslatorBudget();
  const decline = (reason: string) => { translatorBudget.dispose(); return no(reason); };
  let internal: Rec;
  let cacheKeySource: ClaudeInboundTranslation["cacheKeySource"];
  try {
    // No claudeCode section is admitted (CONFIG_KEYS), so ocx translates with none either.
    ({ body: internal, cacheKeySource } = messagesToResponsesTranslation(body, undefined, translatorBudget));
    // The charges claude-messages.ts makes for the translated body and then for its replay
    // request, so a turn near the translation limit fails the same way (ocx answers it with 413).
    translatorBudget.chargeRetained(jsonUtf8Bytes(internal), { kind: "request_copies" });
  } catch {
    // ocx answers with its own 400 or 413.
    return decline("translate");
  }
  const stream = internal.stream === true;
  internal.stream = true;
  try {
    const bytes = jsonUtf8Bytes(internal);
    translatorBudget.reserveTransient(3 * bytes, { kind: "request_copies" }).release();
    translatorBudget.chargeRetained(bytes, { kind: "request_copies" });
  } catch {
    return decline("translation-budget");
  }
  // ocx also drops `reasoning` when the route's ladder is definitively empty (supportedLadderFor).
  // No route the Worker takes has one: its own providers carry no ladder fields, and every model
  // of the Anthropic login has a non-empty ladder.

  // The replayed request carries only these caller headers, never the admission bearer.
  const internalHeaders = new Headers({ "content-type": "application/json" });
  for (const name of FORWARD_HEADERS) {
    if (name === "authorization") continue;
    const value = headers.get(name);
    if (value) internalHeaders.set(name, value);
  }
  // claude-messages.ts's Go lane: the forwarded session headers, the caller's own Go session, the
  // Claude Code metadata session, and only then a lane for this request alone.
  const goSessionLane = sessionLaneIdFromRequest(internalHeaders)
    ?? normalizeLogConversationId(headers.get("x-opencode-session"))
    ?? normalizeLogConversationId(claudeNativeSessionId(cacheKeySource, internal.prompt_cache_key, body.metadata));
  let inputTokenFloor = 0;
  const turn = await runNativeResponsesTurn(internal, internalHeaders, signal, deps, decline, {
    inbound: "anthropic",
    translatorBudget,
    startedAt,
    surface: "claude",
    conversationId: conversationIdFromClaudeMetadata(isRec(body.metadata) ? body.metadata : undefined),
    sharedCacheCohort: cacheKeySource === "system",
    ...(goSessionLane ? { goSessionLane } : {}),
    // claude-messages.ts records its token floor as the turn's input estimate.
    spendInputTokens: () => inputTokenFloor,
    beforeSend: route => {
      // claude-messages.ts's thinkingProjectionForRoute: only the openai-chat wire drops replayed thinking.
      inputTokenFloor = estimateClaudeRequestTokens(body, requestedModel, route.provider.adapter === "openai-chat"
        ? openAIChatSerializesThinking(route.provider, route.modelId)
        : CLAUDE_NATIVE_THINKING);
    },
  });
  if (!turn) return null;
  // claude-messages.ts re-shapes a failed Responses answer into the Anthropic envelope.
  if ("failure" in turn) {
    translatorBudget.dispose();
    return anthropicErrorFromResponsesError(turn.failure);
  }

  const anthropicSse = recordAtEnd(
    responsesSseToAnthropicSse(turn.sse, requestedModel, { translatorBudget, inputTokenFloor }),
    end => { turn.finish(end); translatorBudget.dispose(); },
  );
  if (stream) {
    return new Response(anthropicSse, {
      headers: { "content-type": "text/event-stream; charset=utf-8", "cache-control": "no-cache" },
    });
  }
  // claude-messages.ts folds the stream into one message for a client that did not ask to stream.
  let message: Rec;
  try {
    message = await collectAnthropicMessage(anthropicSse, requestedModel, translatorBudget);
  } catch (error) {
    // The fold stopped reading without cancelling: record the turn and release the upstream.
    turn.finish("error");
    await anthropicSse.cancel().catch(() => {});
    if (isTranslatorBudgetExceededError(error)) return anthropicErrorResponse(413, error.message, "request_too_large", error.code);
    return anthropicErrorResponse(502, error instanceof Error ? error.message : String(error), "api_error");
  }
  const isError = message.type === "error";
  const translatedError = isError && isRec(message.error) ? message.error : undefined;
  if (translatedError?.code === "translation_buffer_limit") {
    return anthropicErrorResponse(
      413,
      typeof translatedError.message === "string" ? translatedError.message : "upstream translation buffer exceeded the safe limit",
      "request_too_large",
      "translation_buffer_limit",
    );
  }
  return new Response(JSON.stringify(message), { status: isError ? 502 : 200, headers: { "content-type": "application/json" } });
};
