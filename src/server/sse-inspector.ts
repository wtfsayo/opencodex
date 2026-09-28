// ocx's SSE inspection of a Responses stream (terminal, first output, the completed response rebuilt
// from its items), apart from relay.ts's request log so the Cloudflare Worker inspects a passthrough
// turn the same way. relay.ts's createSseInspector adds the log.
import type { ResponsesTerminalStatus } from "../bridge";
import {
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
import { EMPTY_BYTES } from "./sse-frame-buffer";
import { sseDataPayload } from "./sse-payload-rewrite";

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
  /** The request log's terminal fields, set before onTerminal fires. */
  logCtx?: { transportPhase?: unknown; terminalSource?: unknown };
  /** relay.ts's request-log inspection of each payload until a terminal is reported. */
  inspectLogPayload?: (payload: string | null, parsed: unknown) => void;
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
export function createSseInspectorCore(handlers: SseInspectorHandlers): SseInspector {
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
    if (!reported && handlers.inspectLogPayload) {
      handlers.inspectLogPayload(payload, parsed);
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
