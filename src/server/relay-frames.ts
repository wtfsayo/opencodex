// The SSE frames, terminal boundary and passthrough header policy the relays share. Kept apart
// from relay.ts's request logging so the Cloudflare Worker can relay a passthrough turn as ocx does.
import type { ResponsesTerminalStatus } from "../bridge";
import {
  cyberPolicyErrorType,
  CYBER_POLICY_ERROR_CODE,
  CYBER_POLICY_FALLBACK_MESSAGE,
  isCyberPolicyCode,
  isCyberPolicyMessage,
  isTerminalRefusalCode,
  safetyRefusalCodeFromMessage,
  terminalRefusalFallbackMessage,
  upstreamErrorMessageFromPayload,
} from "../lib/errors";
import { redactSecretString } from "../lib/redact";
import { isTranslatorBudgetExceededError } from "../lib/translator-budget";
import { carryReplayRefusal } from "../lib/upstream-retry";
import {
  BoundedSseFrameBuffer,
  EMPTY_BYTES,
  joinSseFrameBytes,
  MAX_CLIENT_SSE_FRAME_BYTES,
} from "./sse-frame-buffer";
import { replaceSseDataPayload, sseDataPayload } from "./sse-payload-rewrite";

export const MAX_INSPECTION_SSE_FRAME_BYTES = MAX_CLIENT_SSE_FRAME_BYTES;
export const MAX_COMPLETED_OUTPUT_ITEMS = 256;
export const MAX_COMPLETED_OUTPUT_ITEM_SOURCE_BYTES = 8 * 1024 * 1024;
export const MAX_TAIL_ERROR_MESSAGE_CHARS = 512;
const ADAPTER_EOF_INCOMPLETE_PAYLOAD = JSON.stringify({
  type: "response.incomplete",
  response: {
    status: "incomplete",
    incomplete_details: { reason: "adapter_eof" },
  },
});
const DONE_SSE_FRAME_TEXT = "data: [DONE]\n\n";
const FAILED_TAIL_FALLBACK_PAYLOAD = JSON.stringify({
  type: "response.failed",
  response: {
    status: "failed",
    error: {
      type: "upstream_error",
      code: "upstream_reset",
      message: "Upstream stream terminated unexpectedly",
    },
    last_error: {
      type: "upstream_error",
      code: "upstream_reset",
      message: "Upstream stream terminated unexpectedly",
    },
  },
});

export type InspectionCounters = {
  frameBufferHighWaterBytes: number;
  completedItemsMaxCount: number;
  frameCapOverflows: number;
  itemCapEvictions: number;
  postCancelDrainStops: number;
};

export const inspectionCounters: InspectionCounters = {
  frameBufferHighWaterBytes: 0,
  completedItemsMaxCount: 0,
  frameCapOverflows: 0,
  itemCapEvictions: 0,
  postCancelDrainStops: 0,
};

export function getInspectionCounters(): InspectionCounters {
  return { ...inspectionCounters };
}

export function resetInspectionCountersForTest(): void {
  inspectionCounters.frameBufferHighWaterBytes = 0;
  inspectionCounters.completedItemsMaxCount = 0;
  inspectionCounters.frameCapOverflows = 0;
  inspectionCounters.itemCapEvictions = 0;
  inspectionCounters.postCancelDrainStops = 0;
}

export function relayWithAbort(
  body: ReadableStream<Uint8Array> | null,
  upstream: AbortController,
  onClientGone?: (reason?: unknown) => void,
): ReadableStream<Uint8Array> | null {
  if (!body) return null;
  const reader = body.getReader();
  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      try {
        const { done, value } = await reader.read();
        if (done) {
          controller.close();
          return;
        }
        controller.enqueue(value);
      } catch (err) {
        try { controller.error(err); } catch { /* already torn down */ }
      }
    },
    cancel(reason) {
      // A tee caller may transfer abort ownership to its bounded inspection pump.
      if (onClientGone) onClientGone(reason);
      else upstream.abort(reason);
      reader.cancel(reason).catch(() => {});
    },
  });
}

export function buildFailedTailPayload(err: unknown): string {
  const translatorOverflow = isTranslatorBudgetExceededError(err);
  const message = (translatorOverflow
    ? "upstream translation buffer exceeded the safe limit"
    : `Upstream stream terminated unexpectedly: ${err instanceof Error ? err.message : String(err)}`)
    .slice(0, MAX_TAIL_ERROR_MESSAGE_CHARS);
  const failure = {
    type: "upstream_error",
    code: translatorOverflow ? "translation_buffer_limit" : "upstream_reset",
    message,
  };
  return JSON.stringify({
    type: "response.failed",
    response: { status: "failed", error: failure, last_error: failure },
  });
}

function buildFailedTailPayloadOrFallback(err: unknown): string {
  try {
    return buildFailedTailPayload(err);
  } catch {
    // Error.message and String(error) may execute hostile accessors. Preserve a
    // bounded protocol terminal even when diagnostic serialization is unsafe.
    return FAILED_TAIL_FALLBACK_PAYLOAD;
  }
}

export function failedTailFrame(encoder: TextEncoder, err: unknown): Uint8Array {
  const payload = buildFailedTailPayloadOrFallback(err);
  return encoder.encode(`\n\nevent: response.failed\ndata: ${payload}\n\n${DONE_SSE_FRAME_TEXT}`);
}

/**
 * Close a turn the upstream ended without a Responses terminal.
 *
 * `refusalCode` carries the upstream's own verdict when it gave one. Codex
 * classifies this terminal by `error.code` alone and retries everything outside
 * its fatal set (codex-rs/codex-api/src/sse/responses.rs:423-450), so stamping
 * `upstream_server_error` on a refusal delivered it as a retryable disconnect
 * and drove the reconnect loop in #5176. Without a refusal code the terminal is
 * unchanged: a genuine transport failure stays retryable, which is what it is.
 */
export function upstreamErrorTailFrame(
  encoder: TextEncoder,
  message: string,
  refusalCode?: string,
): Uint8Array {
  return encoder.encode(
    `event: response.failed\ndata: ${upstreamErrorFailedPayload(message, refusalCode)}\n\n`,
  );
}

function upstreamErrorFailedPayload(message: string, refusalCode?: string): string {
  const error = {
    type: refusalCode === undefined ? "upstream_error" : "invalid_request_error",
    code: refusalCode ?? "upstream_server_error",
    message: redactSecretString(message).slice(0, MAX_TAIL_ERROR_MESSAGE_CHARS),
  };
  return JSON.stringify({
    type: "response.failed",
    response: {
      status: "failed",
      error,
      last_error: error,
      ...(refusalCode === undefined ? {} : { retryable: false }),
    },
  });
}

/**
 * Terminal for a read that failed after the upstream had already refused.
 *
 * Framed exactly like {@link failedTailFrame} — leading blank line to close a
 * partial block, then the sentinel — but carrying the refusal instead of the
 * generic reset. The refusal is the real outcome of the turn and the socket
 * teardown that followed it is not, so reporting `upstream_reset` here would
 * restart a turn the upstream has already ended (#5176).
 */
export function refusalFailedTailFrame(
  encoder: TextEncoder,
  message: string,
  refusalCode: string,
): Uint8Array {
  const payload = upstreamErrorFailedPayload(message, refusalCode);
  return encoder.encode(
    `\n\nevent: response.failed\ndata: ${payload}\n\n${DONE_SSE_FRAME_TEXT}`,
  );
}

export function boundedBareUpstreamErrorMessage(payload: unknown): string | undefined {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)
    || (payload as { type?: unknown }).type !== "error") return undefined;
  const message = upstreamErrorMessageFromPayload(payload);
  return message ? redactSecretString(message).slice(0, MAX_TAIL_ERROR_MESSAGE_CHARS) : undefined;
}

/**
 * A bare upstream `error` event reduced to what the synthesized terminal needs.
 *
 * The structured code is authoritative whenever the upstream sent one: a code
 * that is not a refusal means the upstream did not refuse, whatever its
 * diagnostic text happens to quote. Recognized refusal copy is read only when
 * no code was carried anywhere on the event, which is the shape #5176 reports.
 * A refusal code with no message still yields a terminal, because Codex accepts
 * that shape and supplies its own copy for it.
 *
 * The candidate topology mirrors {@link upstreamErrorMessageFromPayload}: code
 * and message must be read from the same places, or an event whose message is
 * nested under `response.error` would contribute text while its verdict went
 * unseen.
 */
function boundedBareUpstreamError(payload: unknown): {
  message: string;
  refusalCode: string | undefined;
} | undefined {
  const root = asJsonRecord(payload);
  if (!root || root.type !== "error") return undefined;
  const message = boundedBareUpstreamErrorMessage(payload);
  const response = asJsonRecord(root.response);
  // Precedence is {@link upstreamErrorMessageFromPayload}'s, so the envelope
  // that supplied the message also supplies the verdict. Taking the FIRST code
  // rather than searching for a refusal is what stops a refusal nested below a
  // transient one from overruling it.
  const code = [
    asJsonRecord(root.error),
    asJsonRecord(root.last_error),
    asJsonRecord(response?.error),
    asJsonRecord(response?.incomplete_details),
    root,
  ]
    .map(candidate => stringField(candidate, "code"))
    .find(candidate => candidate !== undefined);
  const refusalCode = code !== undefined
    ? (isTerminalRefusalCode(code) ? code : undefined)
    : message === undefined ? undefined : safetyRefusalCodeFromMessage(message);
  if (message !== undefined) return { message, refusalCode };
  if (refusalCode === undefined) return undefined;
  return { message: terminalRefusalFallbackMessage(refusalCode), refusalCode };
}

export type SseTerminalOutputBoundary = {
  feed(chunk: Uint8Array): Uint8Array;
  finish(): Uint8Array;
  terminalSeen(): boolean;
  doneSeen(): boolean;
  upstreamError(): string | undefined;
  upstreamRefusalCode(): string | undefined;
  dispose(): void;
};

/**
 * Frame-aware client output boundary shared by both native Responses relays.
 * It buffers only the current incomplete SSE block under the same hard byte
 * cap as inspection, forwards complete blocks through the first Responses
 * terminal, and drops every later block/byte. A premature [DONE] is held until
 * a terminal arrives so clean EOF can synthesize one terminal and one sentinel.
 */
export function createSseTerminalOutputBoundary(
  options?: CodexSafetyBufferingFilterOptions,
): SseTerminalOutputBoundary {
  const dropSafetyBuffering = options?.dropCodexSafetyBuffering === true;
  const decoder = new TextDecoder();
  const encoder = new TextEncoder();
  const framer = new BoundedSseFrameBuffer(MAX_INSPECTION_SSE_FRAME_BYTES);
  let terminal = false;
  let done = false;
  let pendingDone: { block: Uint8Array; delimiter: Uint8Array } | null = null;
  let disposed = false;
  let upstreamError: string | undefined;
  let upstreamRefusalCode: string | undefined;

  const processFrames = (
    frames: ReturnType<BoundedSseFrameBuffer["feed"]>,
  ): Uint8Array => {
    if (disposed || terminal || frames.length === 0) return EMPTY_BYTES;
    const output: Uint8Array[] = [];
    let responsesTerminal = false;
    for (const frame of frames) {
      const payload = sseDataPayload(decoder.decode(frame.block));
      const isDone = payload === "[DONE]";
      const parsed = payload === null ? undefined : parseSsePayload(payload);
      // Observe on the client reader itself: a tee inspection branch may lag
      // behind EOF, so its log context cannot determine the outgoing terminal.
      const bare = boundedBareUpstreamError(parsed);
      if (bare !== undefined) {
        upstreamError = bare.message;
        upstreamRefusalCode = bare.refusalCode;
      }
      const safetyBuffering = dropSafetyBuffering && parsed !== undefined
        ? codexSafetyBufferingBlockAction(parsed) : "keep";
      if (safetyBuffering === "drop") continue;
      const policyError = parsed !== undefined && isPolicyRewriteType(parsed)
        ? cyberPolicyTerminalError(parsed)
        : undefined;
      const policyPayload = policyError ? policyFailurePayload(policyError, parsed) : undefined;
      let outboundBlock = policyPayload !== undefined
        ? encoder.encode(rewritePolicyTerminalBlock(decoder.decode(frame.block), policyPayload))
        : frame.block;
      if (safetyBuffering === "strip") {
        outboundBlock = encoder.encode(stripCodexSafetyBufferingField(
          decoder.decode(outboundBlock),
          policyPayload !== undefined ? parseSsePayload(policyPayload) : parsed,
        ));
      }
      if (isDone) {
        done = true;
        if (responsesTerminal) {
          output.push(outboundBlock, frame.delimiter);
        } else if (!pendingDone) {
          // Do not expose a sentinel before a Responses terminal. If EOF
          // follows, the synthetic incomplete path owns the one sentinel;
          // if a terminal arrives later, this pending frame is emitted then.
          pendingDone = { block: outboundBlock, delimiter: frame.delimiter };
        }
        continue;
      }
      // Preserve every frame through the first Responses terminal. Every
      // later non-DONE frame is dropped.
      if (!responsesTerminal) output.push(outboundBlock, frame.delimiter);
      if (!responsesTerminal && payload && terminalStatusFromParsed(parsed)) {
        responsesTerminal = true;
        if (pendingDone) {
          output.push(pendingDone.block, pendingDone.delimiter);
          pendingDone = null;
        }
      }
    }
    if (responsesTerminal) {
      terminal = true;
      framer.dispose();
    }
    return joinSseFrameBytes(output);
  };

  return {
    feed(chunk) {
      if (disposed || terminal) return EMPTY_BYTES;
      return processFrames(framer.feed(chunk));
    },
    finish() {
      if (disposed || terminal) return EMPTY_BYTES;
      const tail = framer.finish();
      if (tail.byteLength === 0) return EMPTY_BYTES;
      // EOF may cut off the final SSE block before its blank-line delimiter.
      // Feed it through the exact same parser/rewrite/terminal path as a
      // complete frame, using a synthetic delimiter so the client receives a
      // dispatchable event rather than an unterminated tail.
      const tailText = decoder.decode(tail);
      const delimiter = encoder.encode(tailText.includes("\r\n") ? "\r\n\r\n" : "\n\n");
      return processFrames([{ block: tail, delimiter }]);
    },
    terminalSeen: () => terminal,
    doneSeen: () => done,
    upstreamError: () => upstreamError,
    upstreamRefusalCode: () => upstreamRefusalCode,
    dispose() {
      if (disposed) return;
      disposed = true;
      pendingDone = null;
      framer.dispose();
    },
  };
}

/**
 * Relay a passthrough SSE body like relayWithAbort, but convert a MID-STREAM failure (upstream
 * reset after headers) into a clean terminal: any partial block is closed off, then a synthetic
 * `response.failed` event and `data: [DONE]` are emitted and the stream closes. Without this the
 * client sees a raw socket teardown with no terminal SSE event. Deliberately NOT a resend: the
 * upstream already committed the request (duplicate-completion risk — same policy as cursor's
 * committed=non-replayable transport retry).
 */
export function relaySseWithFailedTail(
  body: ReadableStream<Uint8Array>,
  upstream: AbortController,
  onClientGone?: (reason?: unknown) => void,
  opts?: { upstreamError?: string; terminalBoundary?: CodexSafetyBufferingFilterOptions },
): ReadableStream<Uint8Array> {
  const reader = body.getReader();
  const encoder = new TextEncoder();
  const terminalBoundary = createSseTerminalOutputBoundary(opts?.terminalBoundary);
  let closed = false;
  const relayChunk = (
    controller: ReadableStreamDefaultController<Uint8Array>,
    value: Uint8Array,
  ): "terminal" | "output" | "buffered" => {
    const outbound = terminalBoundary.feed(value);
    if (outbound.byteLength > 0) controller.enqueue(outbound);
    if (!terminalBoundary.terminalSeen()) return outbound.byteLength > 0 ? "output" : "buffered";

    // A Responses terminal frame is the protocol boundary. Some compatible
    // gateways leave the HTTP connection open after response.completed, which
    // otherwise leaves Codex waiting forever even though the model turn is done.
    // Preserve through the terminal block only, add the conventional sentinel
    // when there was no real [DONE] data event, then stop reading upstream.
    if (!terminalBoundary.doneSeen()) {
      controller.enqueue(doneFrame(encoder));
    }
    closed = true;
    controller.close();
    const reason = "Responses terminal event received";
    // Notify the tee inspection branch as well. It has already received the
    // same terminal-bearing upstream chunk, so its bounded drain records the
    // real terminal and then releases the turn/upstream keep-alive connection.
    onClientGone?.(reason);
    reader.cancel(reason).catch(() => {});
    terminalBoundary.dispose();
    return "terminal";
  };
  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      try {
        for (;;) {
          const { done, value } = await reader.read();
          if (done) {
            const tail = terminalBoundary.finish();
            if (tail.byteLength > 0) controller.enqueue(tail);
            if (terminalBoundary.terminalSeen()) {
              if (!terminalBoundary.doneSeen()) controller.enqueue(doneFrame(encoder));
            } else {
              // A clean upstream EOF is still a failed Responses turn when no
              // protocol terminal arrived. Make that state explicit so Codex
              // does not treat HTTP 200 + bare EOF as a retryable disconnect.
              const upstreamError = terminalBoundary.upstreamError() ?? opts?.upstreamError;
              controller.enqueue(upstreamError === undefined
                ? adapterEofIncompleteFrame(encoder)
                : upstreamErrorTailFrame(
                  encoder,
                  upstreamError,
                  terminalBoundary.upstreamRefusalCode(),
                ));
              controller.enqueue(doneFrame(encoder));
            }
            terminalBoundary.dispose();
            controller.close();
            return;
          }
          const result = relayChunk(controller, value);
          if (result !== "buffered") return;
        }
      } catch (err) {
        let partial: Uint8Array = EMPTY_BYTES;
        let tailTerminal = false;
        try {
          partial = terminalBoundary.finish();
          tailTerminal = terminalBoundary.terminalSeen();
        } catch {
          // A near-cap ambiguous delimiter tail may itself overflow at EOF.
          // Preserve the original read/framing failure and continue emitting
          // the bounded failed tail instead of letting cleanup throw again.
        }
        terminalBoundary.dispose();
        if (closed) return;
        try {
          if (partial.byteLength > 0) controller.enqueue(partial);
          if (tailTerminal) {
            if (!terminalBoundary.doneSeen()) controller.enqueue(doneFrame(encoder));
          } else {
            // Leading blank line terminates a partial SSE block so the failed frame parses cleanly.
            const refusalCode = terminalBoundary.upstreamRefusalCode();
            const refusalMessage = terminalBoundary.upstreamError();
            controller.enqueue(refusalCode !== undefined && refusalMessage !== undefined
              ? refusalFailedTailFrame(encoder, refusalMessage, refusalCode)
              : failedTailFrame(encoder, err));
          }
          controller.close();
        } catch { /* client already torn down */ }
        upstream.abort();
      }
    },
    cancel(reason) {
      terminalBoundary.dispose();
      if (onClientGone) onClientGone(reason);
      else upstream.abort(reason);
      reader.cancel(reason).catch(() => {});
    },
  });
}

export function nextSseBlock(buffer: string): { block: string; delimiter: string; rest: string } | null {
  const match = buffer.match(/\r?\n\r?\n/);
  if (!match || match.index === undefined) return null;
  return {
    block: buffer.slice(0, match.index),
    delimiter: match[0],
    rest: buffer.slice(match.index + match[0].length),
  };
}

export { sseDataPayload } from "./sse-payload-rewrite";

type JsonRecord = Record<string, unknown>;

export function asJsonRecord(value: unknown): JsonRecord | null {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as JsonRecord
    : null;
}

function stringField(record: JsonRecord | null, key: string): string | undefined {
  const value = record?.[key];
  return typeof value === "string" && value.trim() ? value.trim() : undefined;
}

/**
 * Return a high-confidence policy error carried by an upstream terminal shape.
 * Deliberately inspect structured error fields and known refusal copy only — a
 * bare `cyber_policy` token in an unrelated payload is not sufficient.
 */
export function cyberPolicyTerminalError(parsed: unknown): { message: string; type?: string } | undefined {
  const root = asJsonRecord(parsed);
  if (!root) return undefined;
  const response = asJsonRecord(root.response);
  const candidates = [
    asJsonRecord(root.error),
    asJsonRecord(root.last_error),
    asJsonRecord(response?.error),
    asJsonRecord(response?.incomplete_details),
    root,
    response,
  ];
  for (const candidate of candidates) {
    if (!candidate) continue;
    if (isCyberPolicyCode(stringField(candidate, "code"))) {
      return {
        message: stringField(candidate, "message")
          ?? CYBER_POLICY_FALLBACK_MESSAGE,
        ...(stringField(candidate, "type") ? { type: stringField(candidate, "type") } : {}),
      };
    }
  }
  for (const candidate of candidates) {
    const message = stringField(candidate, "message");
    if (message && isCyberPolicyMessage(message)) {
      return {
        message,
        ...(stringField(candidate, "type") ? { type: stringField(candidate, "type") } : {}),
      };
    }
  }
  return undefined;
}

function parseSsePayload(payload: string): unknown | undefined {
  if (payload === "[DONE]") return undefined;
  try {
    return JSON.parse(payload);
  } catch {
    return undefined;
  }
}

export function isPolicyRewriteType(parsed: unknown): boolean {
  const type = asJsonRecord(parsed)?.type;
  return type === "response.failed" || type === "response.incomplete" || type === "error";
}

/**
 * Codex emits its safety-buffering hint in the SSE body as well as in headers:
 * a `response.metadata` event whose `metadata.type` is `safety_buffering`, or a
 * `safety_buffering` field on another event. The metadata event is dropped whole;
 * the field is stripped so the carrying event is otherwise relayed unchanged.
 */
function codexSafetyBufferingBlockAction(parsed: unknown): "keep" | "drop" | "strip" {
  const root = asJsonRecord(parsed);
  if (!root) return "keep";
  if (root.type === "response.metadata") {
    const metadata = asJsonRecord(root.metadata);
    if (metadata?.type === "safety_buffering") return "drop";
  }
  return Object.hasOwn(root, "safety_buffering") ? "strip" : "keep";
}

function stripCodexSafetyBufferingField(block: string, parsed: unknown): string {
  const root = asJsonRecord(parsed);
  if (!root) return block;
  const { safety_buffering: _safetyBuffering, ...rest } = root;
  return replaceSseDataPayload(block, JSON.stringify(rest));
}

function rewritePolicyTerminalBlock(block: string, payload: string): string {
  const newline = block.includes("\r\n") ? "\r\n" : "\n";
  const rewritten = replaceSseDataPayload(block, payload);
  const lines = rewritten.split(/\r?\n/);
  let eventRewritten = false;
  const withEvent = lines.map(line => {
    if (!eventRewritten && line.startsWith("event:")) {
      eventRewritten = true;
      return "event: response.failed";
    }
    return line;
  });
  if (!eventRewritten) withEvent.unshift("event: response.failed");
  return withEvent.join(newline);
}

function policyFailurePayload(policyError: { message: string; type?: string }, parsed: unknown): string {
  const error = {
    type: cyberPolicyErrorType(policyError.type),
    code: CYBER_POLICY_ERROR_CODE,
    message: redactSecretString(policyError.message).slice(0, MAX_TAIL_ERROR_MESSAGE_CHARS),
  };
  const root = asJsonRecord(parsed);
  const originalResponse = asJsonRecord(root?.response);
  const preservedResponse = Object.fromEntries(
    Object.entries(originalResponse ?? {}).filter(([key]) => key !== "incomplete_details"),
  );
  const response = {
    ...preservedResponse,
    status: "failed",
    error,
    last_error: error,
    retryable: false,
  };
  // Responses event metadata such as sequence_number is normally top-level.
  // Keep it (and any other non-error, non-response fields) while replacing only
  // the protocol type and error envelope.
  const preservedRoot = root
    ? Object.fromEntries(Object.entries(root).filter(([key]) => (
      key !== "type"
      && key !== "response"
      && key !== "error"
      && key !== "last_error"
      && key !== "retryable"
    )))
    : {};
  return JSON.stringify({
    ...preservedRoot,
    type: "response.failed",
    retryable: false,
    response,
  });
}

export function adapterEofIncompleteFrame(encoder: TextEncoder): Uint8Array {
  return encoder.encode(`event: response.incomplete\ndata: ${ADAPTER_EOF_INCOMPLETE_PAYLOAD}\n\n`);
}

export function doneFrame(encoder: TextEncoder): Uint8Array {
  return encoder.encode(DONE_SSE_FRAME_TEXT);
}

export function terminalStatusFromSsePayload(payload: string): ResponsesTerminalStatus | null {
  if (payload === "[DONE]") return null;
  return terminalStatusFromParsed(parseSsePayload(payload));
}

/** True when a native Responses SSE payload carries the FIRST kind of non-empty model output. */
export function isFirstOutputSsePayload(payload: string | null): boolean {
  if (!payload || payload === "[DONE]") return false;
  try {
    return firstOutputFromParsed(JSON.parse(payload));
  } catch {
    return false;
  }
}

export function firstOutputFromParsed(parsed: unknown): boolean {
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return false;
  const event = parsed as { type?: unknown; delta?: unknown };
  return (event.type === "response.output_text.delta"
    || event.type === "response.reasoning_summary_text.delta"
    || event.type === "response.reasoning_text.delta")
    && typeof event.delta === "string"
    && event.delta.length > 0;
}

/**
 * Bun's fetch auto-decompresses the response body but leaves the upstream `content-encoding`
 * (and a now-stale `content-length`) on `response.headers`. Relaying those with the already-decoded
 * body makes the caller (Codex) double-decode / truncate → "stream error" on every gpt passthrough.
 * Drop encoding + hop-by-hop headers; relay everything else (content-type, etc.) verbatim.
 */
export const CODEX_SAFETY_BUFFERING_HEADERS = [
  "x-codex-safety-buffering-enabled",
  "x-codex-safety-buffering-faster-model",
] as const;

const CODEX_SAFETY_BUFFERING_HEADER_SET: ReadonlySet<string> = new Set(CODEX_SAFETY_BUFFERING_HEADERS);

const PASSTHROUGH_DROP_HEADERS: ReadonlySet<string> = new Set([
  "content-encoding",
  "content-length",
  "transfer-encoding",
  "connection",
  "keep-alive",
  "proxy-authenticate",
  "proxy-authorization",
  "set-cookie",
  "set-cookie2",
  "te",
  "trailer",
  "upgrade",
]);

export interface CodexSafetyBufferingFilterOptions {
  /**
   * Drop Codex safety-buffering hints: the `x-codex-safety-buffering-*` response
   * headers and the `safety_buffering` SSE metadata event / field. Absent and
   * `false` relay everything unchanged.
   */
  dropCodexSafetyBuffering?: boolean;
}

/** Resolve the passthrough header policy from the loaded config (absent means "forward everything"). */
export function codexSafetyBufferingFilterOptions(
  config: { dropCodexSafetyBuffering?: boolean },
): CodexSafetyBufferingFilterOptions {
  return { dropCodexSafetyBuffering: config.dropCodexSafetyBuffering === true };
}

export function sanitizePassthroughHeaders(upstream: Headers, options?: CodexSafetyBufferingFilterOptions): Headers {
  const dropSafetyBuffering = options?.dropCodexSafetyBuffering === true;
  const out = new Headers();
  upstream.forEach((value, key) => {
    const lower = key.toLowerCase();
    if (PASSTHROUGH_DROP_HEADERS.has(lower)) return;
    if (dropSafetyBuffering && CODEX_SAFETY_BUFFERING_HEADER_SET.has(lower)) return;
    out.set(key, value);
  });
  return out;
}

export function terminalStatusFromParsed(parsed: unknown): ResponsesTerminalStatus | null {
  const type = asJsonRecord(parsed)?.type;
  switch (type) {
    case "response.completed":
      return "completed";
    case "response.failed":
      return "failed";
    case "response.incomplete":
      return cyberPolicyTerminalError(parsed) ? "failed" : "incomplete";
    case "error":
      return cyberPolicyTerminalError(parsed) ? "failed" : null;
    default:
      return null;
  }
}
