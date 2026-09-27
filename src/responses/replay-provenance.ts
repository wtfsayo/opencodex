// Which leading input items of a request body ocx restored from previous_response_id state.
// Import-free so the request parser can read it where response state is never stored (the
// Cloudflare Worker, which declines previous_response_id); state.ts writes it.
export const replayedInputPrefixLengths = new WeakMap<object, number>();

/** Number of leading input items restored from previous_response_id state for this exact body. */
export function previousResponseReplayPrefixLength(body: unknown): number {
  if (!body || typeof body !== "object" || Array.isArray(body)) return 0;
  return replayedInputPrefixLengths.get(body) ?? 0;
}
