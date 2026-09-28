// A Responses-shaped error re-shaped into the Anthropic envelope Claude clients read
// (claude-messages.ts), apart from it so the Cloudflare Worker answers a Claude turn's failure alike.
import { anthropicErrorBody } from "./outbound";
import { CODEX_MAIN_PROFILE_MAINTENANCE_MESSAGE } from "../codex/main-profile-maintenance";
import { resolveClientRetryAfter } from "../lib/retry-after";
import {
  applyReplayRefusalClientHeaders,
  carryReplayRefusal,
  isReplayRefusalResponse,
  isTransientUpstreamStatus,
  REPLAY_REFUSED_STATUS,
  UPSTREAM_RESET_REPLAY_REFUSED_CODE,
} from "../lib/upstream-retry";

/** The Anthropic answer for a non-2xx Responses reply, with the status Claude clients retry on. */
export async function anthropicErrorFromResponsesError(response: Response): Promise<Response> {
  // Read the shared provenance verdict before consuming and re-wrapping the body. A refusal
  // and an ordinary provider rate limit are both 429, so the status cannot distinguish them.
  const replayRefusal = isReplayRefusalResponse(response);
  // Re-shape the OpenAI-style error envelope into the Anthropic one, preserving status.
  let message = `upstream error (${response.status})`;
  try {
    const text = await response.text();
    try {
      const parsed = JSON.parse(text) as { error?: { message?: string; type?: string } | string; message?: string };
      const nested = typeof parsed?.error === "object" && parsed.error ? parsed.error.message : undefined;
      const flat = typeof parsed?.error === "string" ? parsed.error : parsed?.message;
      message = nested || flat || (text ? `upstream error (${response.status}): ${text.slice(0, 400)}` : message);
    } catch {
      if (text) message = `upstream error (${response.status}): ${text.slice(0, 400)}`;
    }
  } catch { /* keep fallback message */ }
  const upstreamRetryAfter = response.headers.get("retry-after");
  const retryAfter = replayRefusal
    ? undefined
    : resolveClientRetryAfter({
        status: response.status,
        message,
        upstreamRetryAfter,
      })
      // Instant-retry "0" is a valid client directive but rejected by cooldown parsers.
      // Preserve it so it still wins over the transient "2" fallback (claude-529 mapping).
      ?? (upstreamRetryAfter?.trim() === "0" ? "0" : undefined);
  // Transient upstream 5xx (already retried pre-stream, 010): reclassify as Anthropic
  // 529 overloaded_error so the Claude Code client applies its built-in backoff retry
  // instead of dying on a fatal api_error (260716 sol-builder incident). The request
  // log keeps the upstream status (captured in the deferred-log closure before this
  // rewrite): log = upstream truth, client = retry signal.
  // Retryable 429s also get Retry-After (#507) so Codex-shaped clients and Claude Code
  // share a backoff hint when the upstream omitted the header.
  const nativeMainFence = response.status === 503
    && upstreamRetryAfter?.trim() === "1"
    && message === CODEX_MAIN_PROFILE_MAINTENANCE_MESSAGE;
  const transient = !replayRefusal && !nativeMainFence && isTransientUpstreamStatus(response.status);
  const outStatus = replayRefusal
    ? REPLAY_REFUSED_STATUS
    : nativeMainFence ? 503 : transient ? 529 : response.status;
  const outHeaders = new Headers({ "Content-Type": "application/json" });
  if (retryAfter) outHeaders.set("Retry-After", retryAfter);
  else if (transient) outHeaders.set("Retry-After", "2");
  if (replayRefusal) applyReplayRefusalClientHeaders(outHeaders);
  const out = new Response(JSON.stringify(anthropicErrorBody(
    outStatus,
    message,
    undefined,
    replayRefusal ? UPSTREAM_RESET_REPLAY_REFUSED_CODE : undefined,
  )), {
    status: outStatus,
    headers: outHeaders,
  });
  return carryReplayRefusal(response, out);
}
