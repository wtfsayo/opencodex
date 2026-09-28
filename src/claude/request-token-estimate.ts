// ocx's estimate of an Anthropic Messages request's prompt tokens, used as the message_start input
// floor. Out of claude-messages.ts so the Cloudflare Worker can compute the same number.
import { sniffImageDimensions } from "../adapters/anthropic-image-guard";
import { estimateTokens } from "../lib/token-estimate";
import { CLAUDE_NATIVE_THINKING, projectClaudeRequest, type ClaudeThinkingProjection } from "../lib/claude-request-projection";

/** Per-attachment token estimate for a base64 payload: real image dimensions when the
 * header is sniffable (Anthropic prices images at ~pixels/750), else decoded bytes/512,
 * min 256 — the same shape as the Kiro usage estimator (estimateKiroImageTokens). */
function estimateBase64AttachmentTokens(data: string): number {
  const dims = sniffImageDimensions(data);
  if (dims) return Math.max(256, Math.ceil((dims.width * dims.height) / 750));
  const unpadded = data.endsWith("==") ? data.length - 2 : data.endsWith("=") ? data.length - 1 : data.length;
  return Math.max(256, Math.ceil(Math.floor((unpadded * 3) / 4) / 512));
}

/**
 * Char-based token estimate for an Anthropic-shaped request body. Base64 attachment
 * payloads (image/document blocks in message content, including blocks nested in
 * tool_result.content) are counted as a bounded per-attachment estimate instead of raw
 * characters: one 2MB screenshot is ~2.7M base64 chars, which the plain chars/token
 * divide reports as hundreds of thousands of tokens versus a real cost around 1.6k.
 * That breaks the >2x drift bound the estimator is held to (devlog 260711_claude_inbound
 * 040 §3); a live 260-message turn whose replayed thinking was 78.8% of the body breached
 * it at 3.28x, which is why the estimate is projected onto the settled route. Text and url
 * sources are left in place and counted as characters, as is
 * anything outside protocol content positions (tool_use.input, tool schemas).
 *
 * `thinking` selects which replayed thinking fields the SETTLED route serializes, so the measure
 * describes the prompt this proxy forwards rather than the one the caller typed. Omitted, the
 * whole body counts — correct for the Anthropic-native wire, where nothing is projected away.
 * See `claude-request-projection.ts` for why a routed wire must project it out.
 */
export function estimateClaudeRequestTokens(
  raw: { system?: unknown; messages?: unknown; tools?: unknown },
  modelId: string | undefined,
  thinking: ClaudeThinkingProjection = CLAUDE_NATIVE_THINKING,
): number {
  let attachmentTokens = 0;
  // Blank base64 payloads ONLY in protocol content positions: message content blocks and
  // blocks nested in tool_result.content. tool_use.input and tool schemas can legitimately
  // contain attachment-shaped JSON, and those bytes ARE serialized into function_call
  // arguments / tool definitions for routed providers, so they must keep counting as text.
  // system is text-only per the Anthropic protocol (no attachment sources), so it is
  // stringified as-is.
  const sanitizeBlock = (block: unknown): unknown => {
    if (!block || typeof block !== "object") return block;
    const b = block as Record<string, unknown>;
    if (b.type === "image" || b.type === "document") {
      const source = b.source as { type?: unknown; data?: unknown } | undefined;
      if (source && typeof source === "object" && source.type === "base64" && typeof source.data === "string") {
        attachmentTokens += estimateBase64AttachmentTokens(source.data);
        return { ...b, source: { ...(source as Record<string, unknown>), data: "" } };
      }
      return block;
    }
    if (b.type === "tool_result" && Array.isArray(b.content)) {
      return { ...b, content: (b.content as unknown[]).map(sanitizeBlock) };
    }
    return block;
  };
  const sanitizedMessages = (messages: unknown): unknown =>
    Array.isArray(messages)
      ? messages.map(message => {
          if (!message || typeof message !== "object") return message;
          const m = message as Record<string, unknown>;
          return Array.isArray(m.content) ? { ...m, content: (m.content as unknown[]).map(sanitizeBlock) } : message;
        })
      : messages;
  const parts: string[] = [];
  if (raw.system !== undefined) parts.push(typeof raw.system === "string" ? raw.system : JSON.stringify(raw.system));
  if (raw.messages !== undefined) {
    const projected = projectClaudeRequest(raw, thinking);
    parts.push(JSON.stringify(sanitizedMessages(projected.messages)));
  }
  if (raw.tools !== undefined) parts.push(JSON.stringify(raw.tools));
  return Math.max(1, estimateTokens(parts.join("\n"), modelId) + attachmentTokens);
}
