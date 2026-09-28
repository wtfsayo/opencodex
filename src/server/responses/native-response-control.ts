import type { OcxProviderConfig } from "../../types";
import { isCanonicalOpenAiForwardProvider } from "../../providers/openai-tiers";
import type { NativeSteeringReplayObserver } from "./native-steering-replay";
import type { ProviderExecutedCallType } from "../responses-undeclared-tool-guard";

import { isInjectionRequest } from "./native-injection-protocol";
import { nativeSteeringUnavailableReason } from "./native-steering-availability";

/** Shared transport ownership, not a shared steer/inject protocol state machine. */
export interface NativeResponseControl {
  readonly kind?: "steering" | "injection";
  relayActive: boolean;
  normalizeContinuation?: (frame: Record<string, unknown>) => Record<string, unknown>;
  replayFactory?: () => NativeSteeringReplayObserver;
  configureToolAuthorization?: (active: boolean, names: ReadonlySet<string>, bareNames: ReadonlySet<string>, namelessCallTypes: ReadonlySet<string>, providerExecuted: ReadonlySet<ProviderExecutedCallType>) => void;
  readonly attached: boolean;
  readonly ended: boolean;
  attach(send: (frame: Record<string, unknown>) => void, fail: (error: Error) => void): () => void;
  observe(frame: Record<string, unknown>): boolean;
  steer(frame: Record<string, unknown>): void;
  inject?(frame: Record<string, unknown>): void;
  continue(frame: Record<string, unknown>): boolean;
  /** Apply operator-configured limits to the exact reconstructed upstream frame. */
  assertOutboundFrame?(text: string): void;
}

export const OPENAI_API_RESPONSES_URL = "https://api.openai.com/v1/responses";

const nativeControlResponses = new WeakSet<Response>();
/** Mark the exact response for multi-response delivery without serializing a wire field. */
export function markNativeControlResponse(response: Response): Response { nativeControlResponses.add(response); return response; }
/** Recognize a marked native response by identity, not by caller-controlled content. */
export function isNativeControlResponse(response: Response): boolean { return nativeControlResponses.has(response); }

/** Canonical ChatGPT needs its upstream WS enabled; only injection may use the separately billed public API. */
export function nativeResponseControlEligible(provider: OcxProviderConfig, control?: NativeResponseControl): boolean {
  if (isCanonicalOpenAiForwardProvider(provider)) return provider.upstreamWebsocket !== false;
  return control?.kind === "injection" && provider.adapter === "openai-responses"
    && provider.upstreamWebsocket === true && provider.authMode !== "forward"
    && provider.baseUrl?.replace(/\/+$/, "") === "https://api.openai.com/v1";
}

/** Select by execution mode, never model name; a multi-agent request cannot acquire steering. */
export function nativeResponseControlMode(frame: Record<string, unknown>, flags: {
  codexNativeInjection?: boolean; codexNativeSteering?: boolean;
}): "injection" | "steering" | undefined {
  if (isInjectionRequest(frame)) return flags.codexNativeInjection === true ? "injection" : undefined;
  return nativeSteeringUnavailableReason(frame, flags.codexNativeSteering) === undefined ? "steering" : undefined;
}

export { nativeSteeringUnavailableReason };
