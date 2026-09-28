// Frames the Responses WebSocket transport builds itself rather than relays. Its own module so the
// Cloudflare Worker, which serves that transport too, builds the same ones.
import { safeResponseHeaders } from "./safe-response-headers";

export function buildWarmupCompletionFrames(frame: Record<string, unknown>): string[] {
  const createdAt = Math.floor(Date.now() / 1000);
  const baseResponse: Record<string, unknown> = {
    id: "",
    object: "response",
    created_at: createdAt,
    model: typeof frame.model === "string" ? frame.model : undefined,
    output: [],
  };
  return [
    JSON.stringify({
      type: "response.created",
      sequence_number: 0,
      response: { ...baseResponse, status: "in_progress" },
    }),
    JSON.stringify({
      type: "response.completed",
      sequence_number: 1,
      response: { ...baseResponse, status: "completed" },
    }),
  ];
}

export function buildWsErrorFrame(
  status: number,
  error: Record<string, unknown>,
  headers?: Headers,
): Record<string, unknown> {
  return {
    type: "error",
    status,
    error,
    headers: headers ? safeResponseHeaders(headers) : {},
  };
}
