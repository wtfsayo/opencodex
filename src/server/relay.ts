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
import { isUsageDebugEnabled } from "../usage/debug";
import {
  addRequestLog,
  addFinalRequestLog,
  httpStatusForRequestLogTerminal,
  inspectResponseLogJson,
  inspectResponseLogSsePayloadParsed,
  recordFirstOutput,
  type RequestLogContext,
  type RequestLogEntry,
} from "./request-log";
import {
  BoundedSseFrameBuffer,
  EMPTY_BYTES,
  joinSseFrameBytes,
  MAX_CLIENT_SSE_FRAME_BYTES,
} from "./sse-frame-buffer";
import { replaceSseDataPayload, sseDataPayload } from "./sse-payload-rewrite";
import { createBoundedResponseLogBody } from "./response-log-body";
import { clientWireLogOf } from "./inference/client-wire";
import { recordClientWireRequestLog } from "./inference/client-wire-log";

import {
  asJsonRecord,
  boundedBareUpstreamErrorMessage,
  cyberPolicyTerminalError,
  firstOutputFromParsed,
  inspectionCounters,
  isFirstOutputSsePayload,
  isPolicyRewriteType,
  MAX_COMPLETED_OUTPUT_ITEM_SOURCE_BYTES,
  MAX_COMPLETED_OUTPUT_ITEMS,
  MAX_INSPECTION_SSE_FRAME_BYTES,
  terminalStatusFromParsed,
} from "./relay-frames";
export * from "./relay-frames";

import { createSseInspectorCore, type SseInspector, type SseInspectorHandlers } from "./sse-inspector";
export * from "./sse-inspector";

/** The inspector with the request log's per-payload inspection when `logCtx` is given. */
export function createSseInspector(handlers: Omit<SseInspectorHandlers, "logCtx"> & { logCtx?: RequestLogContext }): SseInspector {
  const logCtx = handlers.logCtx;
  return createSseInspectorCore(logCtx
    ? { ...handlers, inspectLogPayload: (payload, parsed) => inspectResponseLogSsePayloadParsed(logCtx, payload, parsed) }
    : handlers);
}

const nativePassthroughSseResponses = new WeakSet<Response>();
const eagerRelaySseResponses = new WeakSet<Response>();

export function trackSseForRequestLog(
  body: ReadableStream<Uint8Array>,
  onTerminal: (status: ResponsesTerminalStatus) => void,
  onCancel: () => void,
  logCtx?: RequestLogContext,
  onFirstOutput?: () => void,
): ReadableStream<Uint8Array> {
  const reader = body.getReader();
  let terminalReported = false;
  let cancelled = false;

  const reportTerminal = (status: ResponsesTerminalStatus) => {
    if (terminalReported) return;
    terminalReported = true;
    onTerminal(status);
  };
  // Reuse the byte-bounded inspector so translated responses cannot retain an
  // unterminated upstream frame or parse the same event once per observer.
  const inspector = createSseInspector({
    onTerminal: reportTerminal,
    logCtx,
    onFirstOutput,
  });

  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      try {
        const { done, value } = await reader.read();
        if (done) {
          if (!cancelled) {
            inspector.finish();
            if (!terminalReported) reportTerminal("incomplete");
          }
          inspector.dispose();
          controller.close();
          return;
        }
        inspector.feed(value);
        controller.enqueue(value);
      } catch (err) {
        // The upstream read rejected: the 200 body died mid-flight. Client
        // cancellation is the caller's separate 499 path, so a cancel-drained
        // pending read (cancelled=true) must not carry the truncation marker.
        if (!cancelled && !terminalReported && logCtx?.activeAttempt) {
          logCtx.activeAttempt.streamAborted = true;
        }
        if (!cancelled && !terminalReported) reportTerminal("incomplete");
        inspector.dispose();
        try { controller.error(err); } catch { /* already torn down */ }
      }
    },
    cancel(reason) {
      cancelled = true;
      inspector.dispose();
      onCancel();
      reader.cancel(reason).catch(() => {});
    },
  });
}

export function responseWithDeferredRequestLog(
  response: Response,
  requestId: string,
  start: number,
  logCtx: RequestLogContext,
  addLog: (entry: RequestLogEntry) => void = addRequestLog,
): Response {
  const contentType = response.headers.get("content-type")?.toLowerCase() ?? "";
  if (isUsageDebugEnabled() && !logCtx.usageDebugContentType && contentType) {
    logCtx.usageDebugContentType = contentType;
  }
  if (isNativePassthroughSseResponse(response)) {
    return response;
  }
  // A body already in the client's wire is not Responses SSE or JSON; its producer reports the
  // facts the tap below would read (PF-09 direct encoders).
  const clientWireLog = clientWireLogOf(response);
  if (clientWireLog) {
    recordClientWireRequestLog(clientWireLog, requestId, start, logCtx, addLog);
    return response;
  }
  if (!response.body || !contentType.includes("text/event-stream")) {
    if (response.body && (contentType.includes("application/json") || response.status >= 400)) {
      const body = createBoundedResponseLogBody(response.body, {
        json: contentType.includes("application/json"),
        inspect: text => inspectResponseLogJson(logCtx, text),
        finalize: reason => {
          // Preserve wire status; request history follows the adjacent SSE
          // convention for a client cancellation or upstream read failure.
          const status = reason === "cancel" ? 499 : reason === "read_error" ? 502 : response.status;
          addFinalRequestLog(requestId, start, logCtx, status, {
            ...(reason === "eof" && logCtx.observedTerminalStatus
              ? { terminalStatus: logCtx.observedTerminalStatus }
              : {}),
            closeReason: reason === "cancel" ? "client_cancel" : "non_stream",
          }, addLog);
        },
      });
      // Logging re-wraps the response, and an in-process verdict does not survive a re-wrap on
      // its own. A replay refusal that lost it here would read to a later quota recorder or
      // Retry-After synthesizer as a 429 some upstream produced.
      return carryReplayRefusal(response, new Response(body, {
        status: response.status,
        statusText: response.statusText,
        headers: response.headers,
      }));
    }
    if (isUsageDebugEnabled() && logCtx.usageDebugBodyKind === undefined) {
      logCtx.usageDebugBodyKind = response.body ? "other" : "none";
    }
    addFinalRequestLog(requestId, start, logCtx, response.status, { closeReason: "non_stream" }, addLog);
    return response;
  }

  let logged = false;
  const body = trackSseForRequestLog(
    response.body,
    status => {
      if (logged) return;
      logged = true;
      addFinalRequestLog(requestId, start, logCtx, httpStatusForRequestLogTerminal(status, logCtx), {
        terminalStatus: status,
        closeReason: "terminal",
      }, addLog);
    },
    () => {
      if (logged) return;
      logged = true;
      addFinalRequestLog(requestId, start, logCtx, 499, { closeReason: "client_cancel" }, addLog);
    },
    logCtx,
    () => recordFirstOutput(logCtx, start),
  );
  return new Response(body, {
    status: response.status,
    statusText: response.statusText,
    headers: response.headers,
  });
}

export function markNativePassthroughSseResponse(response: Response): Response {
  nativePassthroughSseResponses.add(response);
  return response;
}

export function isNativePassthroughSseResponse(response: Response): boolean {
  return nativePassthroughSseResponses.has(response);
}

export function markEagerRelaySseResponse(response: Response): Response {
  eagerRelaySseResponses.add(response);
  return response;
}

/** Test-only path identity seam; runtime behavior must not branch on this marker. */
export function isEagerRelaySseResponse(response: Response): boolean {
  return eagerRelaySseResponses.has(response);
}

export function relaySseWithHeartbeat(
  body: ReadableStream<Uint8Array> | null,
  upstream: AbortController,
  heartbeatMs = 15_000,
  onTerminal?: (status: ResponsesTerminalStatus) => void,
  options?: { onStart?: () => void; onDone?: () => void },
): ReadableStream<Uint8Array> | null {
  if (!body) return null;
  const reader = body.getReader();
  const heartbeat = new TextEncoder().encode(": opencodex keepalive\n\n");
  let timer: ReturnType<typeof setInterval> | undefined;
  let closed = false;
  let clientCancelled = false;
  let terminalReported = false;

  const reportTerminal = (status: ResponsesTerminalStatus) => {
    if (terminalReported || clientCancelled || closed) return;
    terminalReported = true;
    onTerminal?.(status);
  };
  const inspector = createSseInspector({ onTerminal: reportTerminal });

  const cleanup = () => {
    if (closed) return;
    closed = true;
    inspector.dispose();
    if (timer) clearInterval(timer);
    timer = undefined;
    options?.onDone?.();
  };

  return new ReadableStream<Uint8Array>({
    start(controller) {
      options?.onStart?.();
      timer = setInterval(() => {
        if (closed) return;
        try {
          controller.enqueue(heartbeat);
        } catch {
          cleanup();
        }
      }, heartbeatMs);
    },
    async pull(controller) {
      try {
        const { done, value } = await reader.read();
        if (done) {
          inspector.finish();
          if (!terminalReported && !clientCancelled) reportTerminal("incomplete");
          cleanup();
          controller.close();
          return;
        }
        inspector.feed(value);
        controller.enqueue(value);
      } catch (err) {
        if (!clientCancelled) reportTerminal("incomplete");
        cleanup();
        try { controller.error(err); } catch { /* already torn down */ }
      }
    },
    cancel(reason) {
      clientCancelled = true;
      cleanup();
      upstream.abort(reason);
      reader.cancel(reason).catch(() => {});
    },
  });
}

/**
 * Background-consume an SSE stream purely for terminal-outcome inspection (quota tracking).
 * Does not produce output; safe to ignore errors (the client-facing stream is separate).
 */
export type InspectionDrainBounds = { ms: number; bytes: number };

export type InspectionConsumerOptions = {
  clientGoneSignal?: AbortSignal;
  drainBounds?: Partial<InspectionDrainBounds>;
  upstream?: AbortController;
  now?: () => number;
  /** Forward provider-scoped response-id pinning to the owned inspector. */
  pinCompletedResponseIdToFirstSeen?: boolean;
  /** Observe every parsed SSE payload on the inspection side; see SseInspectorHandlers. */
  onParsedPayload?: (payload: unknown) => void;
  /** Test seam for proving both public consumers dispose their owned inspector. */
  inspectorFactory?: (handlers: SseInspectorHandlers) => SseInspector;
};

const DEFAULT_INSPECTION_DRAIN_MS = 15_000;
const DEFAULT_INSPECTION_DRAIN_BYTES = 32 * 1024 * 1024;
type InspectionPumpOptions = InspectionConsumerOptions & {
  reader: ReadableStreamDefaultReader<Uint8Array>;
  inspector: SseInspector;
  signal?: AbortSignal;
  onDone?: () => void;
  onCancel?: () => void;
  onCleanEof?: () => void;
  onReadError?: () => void;
};

function startBoundedInspectionPump(options: InspectionPumpOptions): void {
  const { reader, inspector, signal, clientGoneSignal } = options;
  let cancelled = false;
  let clientGone = false;
  let clientGoneReason: unknown;
  let drainedBytes = 0;
  let drainDeadline = Number.POSITIVE_INFINITY;
  let drainTimer: ReturnType<typeof setTimeout> | undefined;
  let drainStopped = false;
  const drainMs = options.drainBounds?.ms ?? DEFAULT_INSPECTION_DRAIN_MS;
  const drainBytes = options.drainBounds?.bytes ?? DEFAULT_INSPECTION_DRAIN_BYTES;
  const now = options.now ?? Date.now;
  let cancelFired = false;
  const fireCancel = () => {
    if (cancelFired) return;
    cancelFired = true;
    options.onCancel?.();
  };
  const markClientGone = () => {
    if (clientGone || cancelled) return;
    clientGone = true;
    clientGoneReason = clientGoneSignal?.reason;
    drainDeadline = now() + drainMs;
    if (inspector.terminalSeen() || drainMs <= 0 || drainBytes <= 0) {
      stopDrain();
      return;
    }
    // Do not unref: on Bun/Windows a pending `reader.read()` can be the only
    // wake source; an unref'd timer may never run, so a silent post-cancel
    // drain (time bound, no bytes) hangs the suite until the job timeout.
    drainTimer = setTimeout(stopDrain, drainMs);
  };
  // Ends the bounded drain by cancelling the reader: the pending read settles
  // and the pump loop observes `drainStopped`. Deliberately NOT a shared
  // Promise.race companion — racing every read against one pending promise
  // retains O(chunk-count) reactions on long streams (review C1-1), the exact
  // retention class this phase removes.
  const stopDrain = () => {
    if (drainStopped || cancelled) return;
    drainStopped = true;
    reader.cancel(clientGoneReason).catch(() => {});
  };
  const abortImmediately = () => {
    if (cancelled) return;
    cancelled = true;
    reader.cancel(signal?.reason).catch(() => {});
    fireCancel();
  };

  if (signal?.aborted) {
    cancelled = true;
    reader.cancel(signal.reason).catch(() => {});
    inspector.dispose();
    fireCancel();
    options.onDone?.();
    return;
  }
  signal?.addEventListener("abort", abortImmediately, { once: true });
  clientGoneSignal?.addEventListener("abort", markClientGone, { once: true });
  if (clientGoneSignal?.aborted) markClientGone();

  const pump = async () => {
    let clientGoneWithoutTerminal = false;
    let boundEndedDrain = false;
    try {
      for (;;) {
        const { done, value } = await reader.read();
        // Hard cancellation settles a pending read as EOF. Do not flush a
        // partial terminal after the owner already finalized cancellation.
        if (cancelled) break;
        if (clientGoneSignal?.aborted) markClientGone();
        if (drainStopped) {
          // stopDrain() cancelled the reader; the settled read is the wake-up.
          clientGoneWithoutTerminal = !inspector.terminalSeen();
          boundEndedDrain = clientGoneWithoutTerminal;
          break;
        }
        if (done) {
          inspector.finish();
          if (clientGone) clientGoneWithoutTerminal = !inspector.terminalSeen();
          else if (!cancelled) options.onCleanEof?.();
          break;
        }
        if (!clientGone) {
          inspector.feed(value);
          continue;
        }
        if (now() >= drainDeadline) {
          clientGoneWithoutTerminal = true;
          boundEndedDrain = true;
          break;
        }
        const remainingBytes = Math.max(0, drainBytes - drainedBytes);
        const inspectedValue = value.byteLength > remainingBytes
          ? value.subarray(0, remainingBytes)
          : value;
        if (inspectedValue.byteLength > 0) inspector.feed(inspectedValue);
        drainedBytes += inspectedValue.byteLength;
        if (inspector.terminalSeen()) break;
        if (value.byteLength > remainingBytes
          || drainedBytes >= drainBytes
          || now() >= drainDeadline) {
          clientGoneWithoutTerminal = true;
          boundEndedDrain = true;
          break;
        }
      }
    } catch {
      // Bun can settle a fetch body read before dispatching all abort listeners.
      // Observe the signal itself before classifying that rejection as upstream.
      if (clientGoneSignal?.aborted) markClientGone();
      // A read error can follow a final SSE block without its blank-line
      // delimiter. Flush that candidate before classifying the transport as a
      // synthetic reset; otherwise a real completed/failed/policy terminal is
      // downgraded to 502 on the inspection branch.
      if (!cancelled) {
        try { inspector.finish(); } catch { /* preserve the original read error */ }
      }
      if (clientGone) clientGoneWithoutTerminal = !inspector.terminalSeen();
      else if (!cancelled) options.onReadError?.();
    } finally {
      if (drainTimer) clearTimeout(drainTimer);
      signal?.removeEventListener("abort", abortImmediately);
      clientGoneSignal?.removeEventListener("abort", markClientGone);
      if (clientGone) {
        if (boundEndedDrain) inspectionCounters.postCancelDrainStops += 1;
        if (clientGoneWithoutTerminal) fireCancel();
        options.upstream?.abort(clientGoneReason);
        reader.cancel(clientGoneReason).catch(() => {});
      }
      inspector.dispose();
      options.onDone?.();
    }
  };
  void pump();
}

export function consumeForInspection(
  body: ReadableStream<Uint8Array>,
  onTerminal: (status: ResponsesTerminalStatus, httpStatusOverride?: number) => void,
  signal?: AbortSignal,
  onDone?: () => void,
  logCtx?: RequestLogContext,
  onCancel?: () => void,
  onCompletedResponse?: (response: { id?: unknown; output?: unknown; status?: unknown }) => void,
  onFirstOutput?: () => void,
  options?: InspectionConsumerOptions,
): void {
  const reader = body.getReader();
  let bareUpstreamError: string | undefined;
  const inspector = (options?.inspectorFactory ?? createSseInspector)({
    onTerminal,
    logCtx,
    onCompletedResponse,
    onParsedPayload: payload => {
      const message = boundedBareUpstreamErrorMessage(payload);
      if (message !== undefined) bareUpstreamError = message;
      options?.onParsedPayload?.(payload);
    },
    onFirstOutput,
    pinCompletedResponseIdToFirstSeen: options?.pinCompletedResponseIdToFirstSeen,
  });
  startBoundedInspectionPump({
    ...options,
    reader,
    inspector,
    signal,
    onDone,
    onCancel,
    onCleanEof: () => {
      if (!inspector.reported()) {
        if (logCtx) logCtx.terminalSource = "synthetic";
        if (bareUpstreamError !== undefined) {
          onTerminal("failed", httpStatusForRequestLogTerminal("failed", logCtx));
        } else {
          onTerminal("incomplete");
        }
      }
    },
    onReadError: () => {
      // Upstream read failure after HTTP 200 (mid-stream socket reset) is not a
      // protocol `response.incomplete` terminal. Report a synthetic 502 so account
      // health treats it as transient; abort-driven client cancellation still wins.
      if (!inspector.reported()) {
        if (logCtx) {
          logCtx.transportPhase = "mid_stream";
          logCtx.terminalSource = "synthetic";
          // A truncated 200 body must not meter as a success the client never
          // received; the router's equivalent turn carries 502 + streamAborted
          // (codex-router #139).
          if (logCtx.activeAttempt) logCtx.activeAttempt.streamAborted = true;
        }
        onTerminal("failed", 502);
      }
    },
  });
}

export function consumeForResponseLogMetadata(
  body: ReadableStream<Uint8Array>,
  logCtx: RequestLogContext,
  signal?: AbortSignal,
  onDone?: () => void,
  onCompletedResponse?: (response: { id?: unknown; output?: unknown; status?: unknown }) => void,
  onFirstOutput?: () => void,
  options?: InspectionConsumerOptions,
): void {
  const reader = body.getReader();
  // No onTerminal → the inspector's `reported` gate stays permanently false,
  // reproducing this consumer's unconditional logCtx inspection.
  const inspector = (options?.inspectorFactory ?? createSseInspector)({
    logCtx,
    onCompletedResponse,
    onParsedPayload: options?.onParsedPayload,
    onFirstOutput,
    pinCompletedResponseIdToFirstSeen: options?.pinCompletedResponseIdToFirstSeen,
  });
  startBoundedInspectionPump({ ...options, reader, inspector, signal, onDone });
}
