// How ocx turns a Responses terminal event into the HTTP status its request log records. Apart from
// request-log.ts so the Cloudflare Worker records a turn it served with the same status.
import type { ResponsesTerminalStatus } from "../bridge";
import {
  CYBER_POLICY_ERROR_CODE,
  httpStatusFromTerminalError as httpStatusFromClassifiedTerminalError,
  isCyberPolicyCode,
  isCyberPolicyMessage,
  isRateLimitOrQuotaFailureMessage,
} from "../lib/errors";

/** The request-log fields a terminal decides (RequestLogContext's). */
export type TerminalStatusContext = {
  terminalHttpStatus?: number;
  terminalErrorCode?: typeof CYBER_POLICY_ERROR_CODE;
  terminalIncompleteReason?: string;
};

export function captureTerminalHttpStatus(
  logCtx: TerminalStatusContext,
  json: {
    type?: unknown;
    code?: unknown;
    message?: unknown;
    error?: { type?: unknown; code?: unknown; message?: unknown };
    last_error?: { type?: unknown; code?: unknown; message?: unknown };
    response?: {
      error?: { type?: unknown; code?: unknown; message?: unknown };
      incomplete_details?: { code?: unknown; message?: unknown; reason?: unknown };
    };
  },
): void {
  if (logCtx.terminalHttpStatus !== undefined) return;
  const type = json.type;
  if (type !== "response.failed" && type !== "response.incomplete" && type !== "error") return;
  const responseError = json.response?.error;
  const responseDetails = json.response?.incomplete_details;
  const candidates: Array<{ type?: unknown; code?: unknown; message?: unknown } | undefined> = [
    json.error, json.last_error, responseError, responseDetails, json,
  ];
  const policy = candidates.some(candidate => (
    candidate?.code === null || typeof candidate?.code === "string"
  ) && isCyberPolicyCode(candidate.code as string | null | undefined))
    || candidates.some(candidate => (
      typeof candidate?.message === "string"
      && candidate.message.trim().length > 0
      && isCyberPolicyMessage(candidate.message)
    ));
  if (policy) {
    logCtx.terminalErrorCode = CYBER_POLICY_ERROR_CODE;
    logCtx.terminalHttpStatus = 400;
    return;
  }
  // A quota terminal can carry only a structured reason, without an error message.
  // Keep this separate from normal output limits and from the policy precedence above.
  const quotaTag = (value: unknown): boolean => value === "usage_limit_reached"
    || value === "rate_limit_exceeded" || value === "insufficient_quota";
  const structuredRefusal = candidates.some(candidate => [400, 401, 403, 499].includes(
    httpStatusFromTerminalError({
      type: typeof candidate?.type === "string" ? candidate.type : undefined,
      code: typeof candidate?.code === "string" ? candidate.code : undefined,
    }),
  ));
  const ordinaryIncompleteReason = typeof responseDetails?.reason === "string"
    && ["max_output_tokens", "content_filter", "steered", "upstream_stall_timeout", "adapter_eof"].includes(responseDetails.reason);
  if (type === "response.incomplete" && !structuredRefusal && (quotaTag(responseDetails?.reason) || candidates.some(candidate =>
    quotaTag(candidate?.code)
    || quotaTag(candidate?.type) || candidate?.type === "rate_limit_error"
    || (!ordinaryIncompleteReason && typeof candidate?.message === "string" && isRateLimitOrQuotaFailureMessage(candidate.message))
  ))) {
    // The shared quota classifier also accepts a numeric HTTP status as its message.
    // Preserve explicit payment-required evidence rather than relabeling it as 429.
    logCtx.terminalHttpStatus = candidates.some(candidate => typeof candidate?.message === "string"
      && Number(candidate.message.trim()) === 402) ? 402 : 429;
    return;
  }
  if (type !== "response.failed" || !responseError || typeof responseError !== "object") return;
  const responseCode = responseError.code === null || typeof responseError.code === "string"
    ? responseError.code
    : undefined;
  logCtx.terminalHttpStatus = httpStatusFromTerminalError({
    type: typeof responseError.type === "string" ? responseError.type : undefined,
    code: responseCode,
    message: typeof responseError.message === "string" ? responseError.message : undefined,
  });
}

/** Map a terminal Responses error object to the HTTP status we record in /api/logs. */
export function httpStatusFromTerminalError(error: {
  type?: string;
  code?: string | null;
  message?: string;
} | undefined): number {
  return httpStatusFromClassifiedTerminalError(error);
}

export function httpStatusForTerminalStatus(status: ResponsesTerminalStatus): number {
  return status === "completed" ? 200 : 502;
}

export function httpStatusForRequestLogTerminal(
  status: ResponsesTerminalStatus,
  logCtx?: TerminalStatusContext,
): number {
  if (status === "incomplete" && (logCtx?.terminalHttpStatus === 429 || logCtx?.terminalHttpStatus === 402)) {
    return logCtx.terminalHttpStatus;
  }
  /**
   * [Decision Log]
   * - 목적과 의도: Keep request logs aligned with the successful HTTP/SSE contract.
   * - 기존 구현 및 제약 조건: All incomplete terminals were recorded as 502 even when the
   *   client-requested output limit was reached normally.
   * - 검토한 주요 대안: Treat every incomplete as success, or infer the reason from display text.
   * - 선택한 방식: Only structured max_output_tokens incompletes map to 200.
   * - 다른 대안 대신 이 방식을 선택한 이유: Stall, EOF, and unknown incompletes must remain
   *   visible failures, and display text is not a stable classification contract.
   * - 장점, 단점 및 영향: Logs stop reporting false upstream errors while retaining the
   *   incomplete terminal detail; native callers without a structured reason keep old behavior.
   */
  if (status === "incomplete" && logCtx?.terminalIncompleteReason === "max_output_tokens") {
    return 200;
  }
  if (status === "failed" && logCtx?.terminalHttpStatus !== undefined) {
    return logCtx.terminalHttpStatus;
  }
  return httpStatusForTerminalStatus(status);
}
