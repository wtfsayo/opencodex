// The session identity claude-messages.ts derives from a Claude Code turn's metadata, for the
// native session header and OpenCode Go's lane. Its own module so the Cloudflare Worker derives the
// same value.
import { conversationIdFromClaudeMetadata } from "../server/request-log-conversation";

/** Format a 32-hex cache key as a uuid-shaped session id (version/variant nibbles forced). */
export function uuidFromHex(hex32: string): string {
  const h = (hex32 + "0".repeat(32)).slice(0, 32);
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-4${h.slice(13, 16)}-8${h.slice(17, 20)}-${h.slice(20, 32)}`;
}

/**
 * The session id of a turn whose prompt cache key came from Claude Code's metadata session, or
 * undefined for any other key (a shared system-prompt key must not become a session).
 */
export function claudeNativeSessionId(
  cacheKeySource: "metadata" | "system" | null,
  promptCacheKey: unknown,
  metadata: unknown,
): string | undefined {
  if (cacheKeySource !== "metadata" || typeof promptCacheKey !== "string") return undefined;
  const record = metadata && typeof metadata === "object" && !Array.isArray(metadata) ? metadata as { user_id?: unknown } : undefined;
  return conversationIdFromClaudeMetadata(record) !== undefined ? uuidFromHex(promptCacheKey) : undefined;
}
