// Why a response.create frame cannot be steered natively, as the Responses WebSocket reports it.
// Its own module so the Cloudflare Worker, which holds that socket too, reports the same reason.
import { nativeResponseRecord as injectionRecord } from "./native-response-json";

/** Detect explicit multi-agent opt-in without inferring it from the model name. */
export function isInjectionRequest(frame: Record<string, unknown>): boolean {
  return injectionRecord(frame.multi_agent) && frame.multi_agent.enabled === true;
}

/** Explain documented execution-mode exclusions without claiming model entitlement. */
export function nativeSteeringUnavailableReason(frame: Record<string, unknown>, enabled?: boolean): string | undefined {
  if (enabled !== true) return "Native steering is disabled; enable codexNativeSteering and WebSockets for a supported route.";
  if (isInjectionRequest(frame)) return "Multi-agent execution does not support single-agent response.steer; use a later client request.";
  if (frame.conversation != null) return "Conversation-bound responses do not support native steering.";
  if (Array.isArray(frame.context_management) && frame.context_management.some(item =>
    item && typeof item === "object" && (item as Record<string, unknown>).type === "compaction")) {
    return "Automatic API compaction and native steering cannot share an active response.";
  }
  return undefined;
}
