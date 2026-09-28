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

const nativePassthroughSseResponses = new WeakSet<Response>();
const eagerRelaySseResponses = new WeakSet<Response>();

function createFirstOutputReporter(onFirstOutput?: () => void): {
  payload: (payload: string | null) => void;
  parsed: (parsed: unknown) => void;
} {
  let reported = false;
  const report = (isFirst: boolean) => {
    if (reported || !isFirst) return;
    reported = true;
    try { onFirstOutput?.(); } catch { /* metrics must not break the stream */ }
  };
  return {
    payload: payload => report(isFirstOutputSsePayload(payload)),
    parsed: parsed => report(firstOutputFromParsed(parsed)),
  };
}


/** Extract the response object from a `response.completed` SSE payload, or null. */
export function completedResponseFromSsePayload(payload: string): { id?: unknown; output?: unknown; status?: unknown } | null {
  if (payload === "[DONE]") return null;
  try {
    const json = JSON.parse(payload) as { type?: unknown; response?: unknown };
    return completedResponseFromParsedEvent(json);
  } catch {
    return null;
  }
}

/** Extract the response object from an already-parsed `response.completed` event, or null. */
export function completedResponseFromParsedEvent(
  json: unknown,
): { id?: unknown; output?: unknown; status?: unknown } | null {
  if (!json || typeof json !== "object" || Array.isArray(json)
    || (json as { type?: unknown }).type !== "response.completed") return null;
  const response = (json as { response?: unknown }).response;
  if (!response || typeof response !== "object" || Array.isArray(response)) return null;
  return response as { id?: unknown; output?: unknown; status?: unknown };
}

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
export type SseInspector = {
  /** Feed one upstream chunk through the SSE scanning state machine. */
  feed(chunk: Uint8Array): void;
  /** Flush the decoder + trailing unterminated buffer (upstream cleanly done). */
  finish(): void;
  /** Drop every retained frame/item reference without parsing. Idempotent. */
  dispose(): void;
  /** True once a protocol terminal was detected and reported. */
  reported(): boolean;
  /** True once any protocol terminal was parsed, including metadata-only inspectors. */
  terminalSeen(): boolean;
};

export type SseInspectorHandlers = {
  onTerminal?: (status: ResponsesTerminalStatus, httpStatusOverride?: number) => void;
  logCtx?: RequestLogContext;
  onCompletedResponse?: (response: { id?: unknown; output?: unknown; status?: unknown }) => void;
  /**
   * Every parsed SSE payload, delivered BEFORE any onCompletedResponse derived from that same
   * payload. A caller that must decide on the whole turn -- not just its terminal snapshot --
   * needs to see the incremental events, because a stream can announce an item and then close
   * with an empty `output`.
   */
  onParsedPayload?: (payload: unknown) => void;
  /**
   * A complete data payload that did not parse as a JSON event, `[DONE]` included.
   *
   * An inspector that only hears about parsed events cannot tell "nothing has been emitted"
   * from "something was emitted that I could not read", and a replay decision needs that
   * difference: an unreadable payload is a payload the caller may already have seen.
   */
  onOpaquePayload?: () => void;
  onFirstOutput?: () => void;
  /**
   * Provider-scoped compatibility: persist the completed snapshot under the
   * first response id exposed to the client when an upstream changes ids
   * between `response.created` and `response.completed`.
   */
  pinCompletedResponseIdToFirstSeen?: boolean;
};

type CompletedOutputItem = { item: unknown; sourceBytes: number };

function delimiterLengthAt(
  index: number,
  length: number,
  byteAt: (index: number) => number,
): number | 0 | undefined {
  const first = byteAt(index);
  if (first === 10) {
    if (index + 1 >= length) return undefined;
    const second = byteAt(index + 1);
    if (second === 10) return 2;
    if (second !== 13) return 0;
    if (index + 2 >= length) return undefined;
    return byteAt(index + 2) === 10 ? 3 : 0;
  }
  if (first !== 13) return 0;
  if (index + 1 >= length) return undefined;
  if (byteAt(index + 1) !== 10) return 0;
  if (index + 2 >= length) return undefined;
  const third = byteAt(index + 2);
  if (third === 10) return 3;
  if (third !== 13) return 0;
  if (index + 3 >= length) return undefined;
  return byteAt(index + 3) === 10 ? 4 : 0;
}

/**
 * Per-chunk SSE inspection state machine shared by consumeForInspection,
 * consumeForResponseLogMetadata, and the eager bounded relay (relay-eager.ts).
 *
 * Extraction-fidelity invariants (devlog/_plan/260723_win_mem_safestream/020):
 * - logCtx SSE inspection is gated on !reported; in the metadata configuration
 *   (no onTerminal) `reported` stays permanently false, which reproduces the
 *   metadata consumer's unconditional inspection through the same gate.
 * - finish() skips the trailing-buffer scan once reported, while per-block
 *   onCompletedResponse continues firing after reported — an intentional
 *   asymmetry inherited from consumeForInspection.
 * - logCtx.transportPhase/terminalSource are mutated BEFORE onTerminal fires.
 * - Synthetic terminals (incomplete / failed-502) are the CALLER's decision:
 *   the caller owns `cancelled` state and reads `reported()` to decide.
 */
export function createSseInspector(handlers: SseInspectorHandlers): SseInspector {
  let decoder: TextDecoder | null = new TextDecoder();
  let reported = false;
  let sawTerminal = false;
  let disposed = false;
  let delimiterTail: Uint8Array = EMPTY_BYTES;
  let candidate: Uint8Array = EMPTY_BYTES;
  let candidateBytes = 0;
  let discardingOversizedFrame = false;
  const reportFirstOutput = createFirstOutputReporter(handlers.onFirstOutput);
  // Allocate reconstruction state only for persistence-capable inspectors.
  const completedItemsByOutputIndex = handlers.onCompletedResponse
    ? new Map<number, CompletedOutputItem>()
    : null;
  let aggregateItemBytes = 0;
  let reconstructionTainted = false;
  let firstResponseId: string | undefined;

  const clearFrameState = (): void => {
    delimiterTail = EMPTY_BYTES;
    candidate = EMPTY_BYTES;
    candidateBytes = 0;
    discardingOversizedFrame = false;
  };

  const clearCompletedItems = (): void => {
    completedItemsByOutputIndex?.clear();
    aggregateItemBytes = 0;
    reconstructionTainted = false;
  };

  const dispose = (): void => {
    if (disposed) return;
    disposed = true;
    decoder = null;
    clearFrameState();
    clearCompletedItems();
    firstResponseId = undefined;
  };

  const ensureCandidateCapacity = (requiredBytes: number): void => {
    if (candidate.byteLength >= requiredBytes) return;
    let capacity = candidate.byteLength === 0
      ? Math.min(MAX_INSPECTION_SSE_FRAME_BYTES, Math.max(requiredBytes, 4096))
      : candidate.byteLength;
    while (capacity < requiredBytes) {
      capacity = Math.min(
        MAX_INSPECTION_SSE_FRAME_BYTES,
        Math.max(requiredBytes, capacity * 2),
      );
    }
    const grown = new Uint8Array(capacity);
    if (candidateBytes > 0) grown.set(candidate.subarray(0, candidateBytes));
    candidate = grown;
  };

  const takeCandidate = (): Uint8Array => {
    if (candidateBytes === 0) return EMPTY_BYTES;
    const frame = candidate.slice(0, candidateBytes);
    candidate = EMPTY_BYTES;
    candidateBytes = 0;
    return frame;
  };

  const retainCandidateSlice = (slice: Uint8Array): void => {
    if (slice.byteLength === 0 || discardingOversizedFrame) return;
    const nextBytes = candidateBytes + slice.byteLength;
    inspectionCounters.frameBufferHighWaterBytes = Math.max(
      inspectionCounters.frameBufferHighWaterBytes,
      Math.min(nextBytes, MAX_INSPECTION_SSE_FRAME_BYTES),
    );
    if (nextBytes > MAX_INSPECTION_SSE_FRAME_BYTES) {
      candidate = EMPTY_BYTES;
      candidateBytes = 0;
      discardingOversizedFrame = true;
      inspectionCounters.frameCapOverflows += 1;
      // The rejected frame may have carried an output item we will never see;
      // any later empty-output terminal must not synthesize a partial replay
      // from the surviving map entries (same taint rule as item eviction).
      reconstructionTainted = true;
      return;
    }
    ensureCandidateCapacity(nextBytes);
    candidate.set(slice, candidateBytes);
    candidateBytes = nextBytes;
  };

  const retainCompletedItem = (index: number, item: unknown, sourceBytes: number): void => {
    const previous = completedItemsByOutputIndex!.get(index);
    if (previous) {
      aggregateItemBytes -= previous.sourceBytes;
      completedItemsByOutputIndex!.delete(index);
    }
    if (sourceBytes > MAX_COMPLETED_OUTPUT_ITEM_SOURCE_BYTES) {
      reconstructionTainted = true;
      inspectionCounters.itemCapEvictions += 1;
      return;
    }
    completedItemsByOutputIndex!.set(index, { item, sourceBytes });
    aggregateItemBytes += sourceBytes;
    while (completedItemsByOutputIndex!.size > MAX_COMPLETED_OUTPUT_ITEMS
      || aggregateItemBytes > MAX_COMPLETED_OUTPUT_ITEM_SOURCE_BYTES) {
      let highestIndex = -1;
      for (const retainedIndex of completedItemsByOutputIndex!.keys()) {
        if (retainedIndex > highestIndex) highestIndex = retainedIndex;
      }
      const evicted = completedItemsByOutputIndex!.get(highestIndex);
      if (!evicted) break;
      completedItemsByOutputIndex!.delete(highestIndex);
      aggregateItemBytes -= evicted.sourceBytes;
      reconstructionTainted = true;
      inspectionCounters.itemCapEvictions += 1;
    }
    inspectionCounters.completedItemsMaxCount = Math.max(
      inspectionCounters.completedItemsMaxCount,
      completedItemsByOutputIndex!.size,
    );
  };

  const scanPayload = (payload: string | null, sourceBytes: number): void => {
    if (!payload) return;
    let parsed: unknown | undefined;
    if (payload !== "[DONE]") {
      try {
        parsed = JSON.parse(payload);
      } catch {
        /* malformed SSE payloads remain best-effort/no-throw */
      }
    }
    if (!reported && handlers.logCtx) {
      inspectResponseLogSsePayloadParsed(handlers.logCtx, payload, parsed);
    }
    // Before any terminal handling: a consumer deciding on the whole turn must observe this
    // payload even when the terminal snapshot that follows no longer mentions it.
    if (handlers.onParsedPayload && parsed !== undefined) {
      try { handlers.onParsedPayload(parsed); } catch { /* inspection must never throw into the pump */ }
    }
    // The other half of the same observation. A payload that did not parse still reached the
    // caller, so a consumer deciding whether anything has been emitted has to hear about it.
    if (handlers.onOpaquePayload && parsed === undefined) {
      try { handlers.onOpaquePayload(); } catch { /* inspection must never throw into the pump */ }
    }
    reportFirstOutput.parsed(parsed);
    const status = terminalStatusFromParsed(parsed);
    const policyTerminal = status === "failed"
      && isPolicyRewriteType(parsed)
      && cyberPolicyTerminalError(parsed) !== undefined;
    if (status) sawTerminal = true;
    if (!reported && handlers.onTerminal && status) {
      try {
        reported = true;
        if (handlers.logCtx) {
          handlers.logCtx.transportPhase = "terminal_sse";
          handlers.logCtx.terminalSource = "upstream";
        }
        handlers.onTerminal(status, policyTerminal ? 400 : undefined);
      } finally {
        if (status === "failed" || status === "incomplete") clearCompletedItems();
      }
    } else if (status === "failed" || status === "incomplete") {
      clearCompletedItems();
    }
    if (handlers.onCompletedResponse) {
      type ParsedSseEvent = { type?: unknown; output_index?: unknown; item?: unknown; response?: unknown };
      const parsedEvent = parsed && typeof parsed === "object" && !Array.isArray(parsed)
        ? parsed as ParsedSseEvent
        : null;
      const responseRecord = parsedEvent
        && typeof parsedEvent.response === "object"
        && parsedEvent.response !== null
        && !Array.isArray(parsedEvent.response)
        ? parsedEvent.response as { id?: unknown }
        : null;
      if (handlers.pinCompletedResponseIdToFirstSeen
        && responseRecord
        && typeof responseRecord.id === "string") {
        firstResponseId ??= responseRecord.id;
      }
      const doneItem = parsedEvent?.type === "response.output_item.done" ? parsedEvent.item : undefined;
      if (parsedEvent
        && doneItem !== undefined
        && Number.isInteger(parsedEvent.output_index)
        && (parsedEvent.output_index as number) >= 0
        && typeof doneItem === "object"
        && doneItem !== null
        && !Array.isArray(doneItem)
        && typeof (doneItem as { type?: unknown }).type === "string") {
        retainCompletedItem(parsedEvent.output_index as number, doneItem, sourceBytes);
      }

      let response = completedResponseFromParsedEvent(parsedEvent);
      if (response) {
        if (handlers.pinCompletedResponseIdToFirstSeen
          && firstResponseId !== undefined
          && response.id !== firstResponseId) {
          response = { ...response, id: firstResponseId };
        }
        // Authoritative output is a NON-EMPTY ARRAY only. Anything else
        // (missing, null, scalar, object) keeps the historical backfill
        // behavior so a malformed terminal cannot reach rememberResponseState
        // and destroy continuation state (review C1-2).
        const hasAuthoritativeOutput = Array.isArray(response.output)
          && response.output.length > 0;
        if (!hasAuthoritativeOutput && reconstructionTainted) {
          clearCompletedItems();
          return;
        }
        if (!hasAuthoritativeOutput && completedItemsByOutputIndex!.size > 0) {
          response = {
            ...response,
            output: [...completedItemsByOutputIndex!.entries()]
              .sort(([left], [right]) => left - right)
              .map(([, retained]) => retained.item),
          };
        }
        try {
          handlers.onCompletedResponse(response);
        } finally {
          clearCompletedItems();
        }
      } else if (parsedEvent?.type === "response.completed") {
        clearCompletedItems();
      }
    }
  };

  const completeCandidate = (): void => {
    if (discardingOversizedFrame) {
      discardingOversizedFrame = false;
      return;
    }
    const sourceBytes = candidateBytes;
    const frame = takeCandidate();
    if (reported && !handlers.onCompletedResponse) return;
    const decoded = decoder!.decode(frame);
    scanPayload(sseDataPayload(decoded), sourceBytes);
  };

  const scanChunk = (chunk: Uint8Array): void => {
    const previousTail = delimiterTail;
    delimiterTail = EMPTY_BYTES;
    const tailLength = previousTail.byteLength;
    const totalLength = tailLength + chunk.byteLength;
    const byteAt = (index: number): number => index < tailLength
      ? previousTail[index]!
      : chunk[index - tailLength]!;
    const retainRange = (start: number, end: number): void => {
      if (end <= start || discardingOversizedFrame) return;
      if (start < tailLength) {
        retainCandidateSlice(previousTail.subarray(start, Math.min(end, tailLength)));
      }
      if (end > tailLength) {
        retainCandidateSlice(chunk.subarray(Math.max(0, start - tailLength), end - tailLength));
      }
    };
    let index = 0;
    let retainedThrough = 0;
    while (index < totalLength) {
      const delimiterLength = delimiterLengthAt(index, totalLength, byteAt);
      if (delimiterLength === undefined) break;
      if (delimiterLength > 0) {
        retainRange(retainedThrough, index);
        completeCandidate();
        index += delimiterLength;
        retainedThrough = index;
        continue;
      }
      index += 1;
    }
    retainRange(retainedThrough, index);
    if (index < totalLength) {
      delimiterTail = new Uint8Array(totalLength - index);
      for (let offset = 0; offset < delimiterTail.byteLength; offset += 1) {
        delimiterTail[offset] = byteAt(index + offset);
      }
    }
  };

  return {
    feed(chunk) {
      if (!disposed) scanChunk(chunk);
    },
    finish() {
      if (disposed) return;
      try {
        retainCandidateSlice(delimiterTail);
        delimiterTail = EMPTY_BYTES;
        if (!discardingOversizedFrame && candidateBytes > 0 && !reported) {
          const sourceBytes = candidateBytes;
          const decoded = decoder!.decode(takeCandidate());
          scanPayload(decoded.trim() ? sseDataPayload(decoded) : null, sourceBytes);
        }
      } finally {
        clearFrameState();
        clearCompletedItems();
      }
    },
    dispose,
    reported: () => reported,
    terminalSeen: () => sawTerminal,
  };
}

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
