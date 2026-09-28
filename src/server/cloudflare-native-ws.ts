// The Responses WebSocket transport (what Codex uses when ocx enables `websockets`) held by the
// Cloudflare Worker. ocx runs each response.create frame as a streamed Responses turn and sends
// its events back as text frames (index/websocket-handler.ts, ws-bridge.ts). The Worker does the
// same with its own Responses turn for each frame that turn can serve, until the first frame it
// cannot: from then on it relays every frame to ocx over a WebSocket of its own to the container
// and passes ocx's frames back, so ocx sees the socket's frames exactly as its own socket would
// and decides supersession, steering and injection itself.
//
// The import graph is held Worker-safe by tests/service/cloudflare-worker-native.test.ts.
import type { NativeChatDeps, NativeWsLink, NativeWsSession } from "./cloudflare-native-chat-api";
import { FORWARD_HEADERS } from "../adapters/openai-responses/forward-headers";
import { runNativeResponsesTurn } from "./cloudflare-native-responses";
import { nativeSteeringUnavailableReason } from "./responses/native-steering-availability";
import { resolveInboundBodyLimitBytes } from "./inbound-body-limit";
import { BoundedSseFrameBuffer } from "./sse-frame-buffer";
import { buildWarmupCompletionFrames, buildWsErrorFrame } from "./ws-frames";
import { createTranslatorBudget } from "../lib/translator-budget";
import { runNativeOpenAiTurn } from "./cloudflare-native-openai";
import { expandWithReplayEntry, replayEntryFor, type ReplayEntryItems } from "../responses/state/replay-expansion";
import { replayedInputPrefixLengths } from "../responses/replay-provenance";

type Rec = Record<string, unknown>;
const isRec = (value: unknown): value is Rec => !!value && typeof value === "object" && !Array.isArray(value);

/** live-sideband.ts's MAX_WS_FRAME_BYTES: the largest frame ocx's socket takes. */
export const MAX_WS_FRAME_BYTES = 50 * 1024 * 1024;
/**
 * Larger frames are relayed to ocx without being parsed here: a Worker has 128 MB, and a turn this
 * size is far past what its Responses path serves (the HTTP path forwards past the same bound).
 */
export const MAX_WORKER_FRAME_BYTES = 4 * 1024 * 1024;
const TERMINAL_TYPES = new Set(["response.completed", "response.failed", "response.incomplete"]);
// Codex chains each frame to the one before; a few are kept in case it reaches further back.
const MAX_SOCKET_CONTINUATIONS = 8;

/** ws-bridge.ts's selectForwardHeaders: the upgrade headers each frame's turn carries. */
function selectForwardHeaders(headers: Headers): Headers {
  const selected = new Headers();
  for (const name of FORWARD_HEADERS) {
    const value = headers.get(name);
    if (value) selected.set(name, value);
  }
  return selected;
}

function sseData(block: string): string | null {
  const data: string[] = [];
  for (const line of block.split(/\r?\n/)) {
    if (line.startsWith("data:")) {
      const value = line.slice(5);
      data.push(value.startsWith(" ") ? value.slice(1) : value);
    }
  }
  return data.length > 0 ? data.join("\n") : null;
}

function payloadType(payload: string): string | null {
  try {
    const json = JSON.parse(payload) as { type?: unknown };
    return typeof json.type === "string" ? json.type : null;
  } catch {
    return null;
  }
}

function withoutKey(record: Rec, key: string): Rec {
  const copy = { ...record };
  delete copy[key];
  return copy;
}

const protocolError = (message: string) => ({ type: "protocol_error", code: "websocket_protocol_error", message });

/** UTF-8 length, without encoding a copy when the string is short enough that no bound can apply. */
function frameBytes(data: string | ArrayBuffer): number {
  if (typeof data !== "string") return data.byteLength;
  if (data.length * 3 <= MAX_WORKER_FRAME_BYTES) return data.length;
  return new TextEncoder().encode(data).byteLength;
}

/**
 * ws-bridge.ts's pumpResponsesSseToWebSocket without native steering: each SSE payload becomes one
 * text frame, and the turn ends at its first terminal event.
 */
async function pumpSseToFrames(sse: ReadableStream<Uint8Array>, send: (text: string) => void, isCurrent: () => boolean): Promise<void> {
  const reader = sse.getReader();
  const decoder = new TextDecoder();
  const framer = new BoundedSseFrameBuffer();
  let terminalSeen = false;
  const handle = (payload: string): boolean => {
    if (!isCurrent()) return true;
    if (payload === "[DONE]") return false;
    const type = payloadType(payload);
    if (!type) {
      send(JSON.stringify(buildWsErrorFrame(502, protocolError("Invalid JSON payload in upstream SSE frame"))));
      terminalSeen = true;
      return true;
    }
    if (terminalSeen) return true;
    send(payload);
    if (TERMINAL_TYPES.has(type)) {
      terminalSeen = true;
      return true;
    }
    return false;
  };
  try {
    read: while (!terminalSeen) {
      const { done, value } = await reader.read();
      if (done) break;
      for (const frame of framer.feed(value)) {
        const payload = sseData(decoder.decode(frame.block));
        if (payload && handle(payload)) break read;
      }
    }
    const tail = framer.finish();
    if (!terminalSeen && tail.byteLength > 0) {
      const payload = sseData(decoder.decode(tail));
      if (payload) handle(payload);
    }
    if (!terminalSeen && isCurrent()) {
      send(JSON.stringify(buildWsErrorFrame(502, protocolError("Upstream stream ended before response terminal event"))));
    }
  } catch (error) {
    if (!terminalSeen && isCurrent()) {
      const code = error != null && typeof (error as { code?: unknown }).code === "string" ? (error as { code: string }).code : undefined;
      const message = error instanceof Error ? error.message : String(error);
      try {
        send(JSON.stringify(buildWsErrorFrame(502, code ? { type: "upstream_error", code, message } : protocolError(message))));
      } catch { /* the client is gone */ }
    }
  } finally {
    framer.dispose();
    void reader.cancel().catch(() => {});
  }
}

/**
 * One client socket. `upgradeHeaders` are the headers of the client's upgrade request; each turn
 * the Worker serves carries the same subset ocx's handler keeps.
 */
export function createNativeWsSession(link: NativeWsLink, upgradeHeaders: Headers, deps: NativeChatDeps): NativeWsSession {
  const headers = selectForwardHeaders(upgradeHeaders);
  // How the upgrade was admitted decides whose login a native turn uses (auth-context.ts): the hub key
  // in its dedicated header leaves the bearer the caller's own. Checked, never forwarded.
  const nativeHeaders = new Headers(headers);
  const dedicatedKey = upgradeHeaders.get("x-opencodex-api-key");
  if (dedicatedKey) nativeHeaders.set("x-opencodex-api-key", dedicatedKey);
  // Set by the first frame relayed to ocx; every later frame follows it.
  let container: ReturnType<NativeWsLink["openContainer"]> | undefined;
  let steeringUnavailable: string | undefined;
  let turnId = 0;
  let abortTurn: AbortController | undefined;
  let queue: Promise<void> = Promise.resolve();
  let closed = false;
  // state.ts's continuation cache for the native responses this socket's turns completed here: ocx
  // never saw them, so the Worker expands the frames that continue them as ocx would have.
  const continuations = new Map<string, ReplayEntryItems>();
  const rememberContinuation = (request: Record<string, unknown>, response: Rec) => {
    const entry = replayEntryFor(request, response);
    if (!entry) return;
    continuations.delete(entry.id);
    continuations.set(entry.id, entry);
    while (continuations.size > MAX_SOCKET_CONTINUATIONS) continuations.delete(continuations.keys().next().value!);
  };

  const sendJson = (payload: Rec) => link.send(JSON.stringify(payload));
  /** Runs `step` after every earlier frame's step, so frames reach ocx in arrival order. */
  const enqueue = (step: () => void | Promise<void>) => {
    queue = queue.then(step).catch(() => {
      // A relay that fails leaves the socket unusable, as a failed send on ocx's own socket would.
      if (!closed) link.close(1011, "relay to opencodex failed");
    });
  };
  const relay = (raw: string) => {
    if (closed) return;
    container ??= link.openContainer();
    container.send(raw);
  };

  /** A bare native model: the ChatGPT passthrough, continuing this socket's own responses. */
  const serveNativeOrRelay = async (payload: Rec, raw: string, signal: AbortSignal, isCurrent: () => boolean) => {
    const no = (reason: string) => { deps.onDecline?.(`responses-ws:${reason}`); return null; };
    const previous = typeof payload.previous_response_id === "string" ? continuations.get(payload.previous_response_id) : undefined;
    let body = payload;
    if (previous) {
      const expansion = expandWithReplayEntry(payload, previous);
      if (expansion.kind === "scope-mismatch") { relay(raw); return; }
      body = expansion.body;
      replayedInputPrefixLengths.set(body, expansion.prefixLength);
    }
    let turn;
    try {
      turn = await runNativeOpenAiTurn({ ...body, stream: true }, nativeHeaders, signal, deps, no, Date.now(), {
        continued: previous !== undefined,
        onCompletedResponse: rememberContinuation,
      });
    } catch {
      turn = null;
    }
    if (!isCurrent()) {
      await turn?.response.body?.cancel().catch(() => {});
      return;
    }
    if (!turn || !turn.response.body) {
      // ocx never saw the response this frame continues, so it gets the history already expanded.
      relay(previous ? JSON.stringify({ ...withoutKey(body, "previous_response_id"), type: "response.create" }) : raw);
      return;
    }
    await pumpSseToFrames(turn.response.body, link.send, isCurrent).catch(() => {});
  };

  const serveOrRelay = async (frame: Rec, raw: string, id: number, signal: AbortSignal) => {
    const isCurrent = () => turnId === id && !closed;
    if (!isCurrent()) return; // superseded before its turn came
    if (container) { relay(raw); return; }
    const payload: Rec = { ...frame };
    delete payload.type;
    if (typeof payload.model === "string" && !payload.model.includes("/")) {
      await serveNativeOrRelay(payload, raw, signal, isCurrent);
      return;
    }
    const startedAt = Date.now();
    const translatorBudget = createTranslatorBudget();
    const no = (reason: string) => { deps.onDecline?.(`responses-ws:${reason}`); return null; };
    let turn;
    try {
      turn = await runNativeResponsesTurn({ ...payload, stream: true }, headers, signal, deps, no, {
        inbound: "responses", translatorBudget, startedAt, responseId: "",
      });
    } catch {
      turn = null;
    }
    if (!isCurrent()) {
      translatorBudget.dispose();
      turn?.finish("cancel");
      await turn?.sse.cancel().catch(() => {});
      return;
    }
    if (!turn) {
      translatorBudget.dispose();
      relay(raw);
      return;
    }
    let end: "end" | "error" | "cancel" = "end";
    try {
      await pumpSseToFrames(turn.sse, link.send, isCurrent);
    } catch {
      end = "error";
    }
    if (!isCurrent()) end = "cancel";
    turn.finish(end);
    translatorBudget.dispose();
  };

  return {
    receive(data) {
      if (closed) return;
      const bytes = frameBytes(data);
      if (bytes > MAX_WS_FRAME_BYTES) {
        sendJson(buildWsErrorFrame(413, { type: "invalid_request_error", message: "WebSocket response.create frame is too large" }));
        link.close(1009, "message too large");
        return;
      }
      const raw = typeof data === "string" ? data : new TextDecoder().decode(data);
      if (container || bytes > MAX_WORKER_FRAME_BYTES) {
        // ocx never learns of a turn the Worker was running, so the Worker stops it here.
        abortTurn?.abort("websocket turn superseded or closed");
        turnId++;
        enqueue(() => relay(raw));
        return;
      }
      let frame: unknown;
      try { frame = JSON.parse(raw); } catch { return; } // text-only contract; ignore unparseable frames
      if (!isRec(frame)) return;
      if ((frame.type === "response.inject" || frame.type === "response.steer") && bytes > resolveInboundBodyLimitBytes(undefined)) {
        sendJson(buildWsErrorFrame(413, {
          type: "invalid_request_error", code: "inbound_body_too_large", message: "Native response control frame exceeds the configured inbound body limit.",
        }));
        return;
      }
      if (frame.type === "response.inject" || frame.type === "response.steer") {
        // No turn the Worker serves has a native control channel, as no routed turn of ocx's has.
        sendJson(buildWsErrorFrame(400, frame.type === "response.inject"
          ? { type: "invalid_request_error", code: "injection_not_supported", message: "Native injection is disabled or unavailable on this route." }
          : { type: "invalid_request_error", code: "steering_not_supported", message: steeringUnavailable ?? "Native steering transport is unavailable; the route may be unsupported or using HTTP fallback." }));
        return;
      }
      if (frame.type !== "response.create") return; // response.processed is an ack
      // A new frame supersedes whatever turn the socket has, as in ocx.
      abortTurn?.abort("websocket turn superseded or closed");
      const id = ++turnId;
      steeringUnavailable = nativeSteeringUnavailableReason(frame, undefined);
      if (frame.generate === false) {
        // ocx answers a warm-up itself.
        enqueue(() => {
          if (turnId !== id || closed) return;
          if (container) { relay(raw); return; }
          for (const payload of buildWarmupCompletionFrames(frame as Rec)) link.send(payload);
        });
        return;
      }
      const controller = new AbortController();
      abortTurn = controller;
      enqueue(() => serveOrRelay(frame as Rec, raw, id, controller.signal));
    },
    fromContainer(from, text) {
      if (closed || from !== container) return;
      link.send(text);
    },
    containerClosed(from, code, reason) {
      if (from !== container) return;
      container = undefined;
      // ocx closed the socket it holds for this client (a restart, an admission refusal): the client sees it.
      if (!closed) link.close(code, reason);
    },
    closed() {
      closed = true;
      abortTurn?.abort("websocket turn superseded or closed");
      container?.close();
      container = undefined;
    },
  };
}
