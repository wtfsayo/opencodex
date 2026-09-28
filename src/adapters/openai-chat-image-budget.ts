// When a translated Chat request's images need normalizing, apart from the normalizer and its Bun
// image codec, so the Cloudflare Worker can tell a turn that needs none.
import { parseDataUrl } from "./image";

/**
 * Best-effort base64 image budget for translated Chat requests. This leaves room for
 * other request fields but is not a guarantee that the complete body fits an upstream
 * limit. Remote URLs are never fetched by request construction.
 */
export const OPENAI_CHAT_IMAGE_BASE64_BUDGET = 3_670_016; // 3.5MiB


/** Whether `value` is a plain object, so message and part shapes can be walked safely. */
function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

/**
 * Walk every well-formed `image_url` part in a Chat Completions message array, ignoring
 * malformed shapes rather than throwing on them. Returning false from `visit` stops the walk.
 */
export function forEachImagePart(
  messages: unknown,
  visit: (imageUrl: Record<string, unknown>, url: string) => boolean | void,
): void {
  if (!Array.isArray(messages)) return;
  for (const message of messages) {
    if (!isRecord(message) || !Array.isArray(message.content)) continue;
    for (const part of message.content) {
      if (!isRecord(part) || part.type !== "image_url" || !isRecord(part.image_url)) continue;
      const imageUrl = part.image_url;
      if (typeof imageUrl.url !== "string") continue;
      if (visit(imageUrl, imageUrl.url) === false) return;
    }
  }
}

/**
 * Whether this turn carries inline image bytes worth normalizing. The adapter uses this
 * to stay synchronous for text-only turns, which is every turn on most providers.
 */
export function hasShrinkableOpenAIChatImages(messages: unknown): boolean {
  let total = 0;
  let found = false;
  forEachImagePart(messages, (_imageUrl, url) => {
    const source = parseDataUrl(url);
    if (!source) return;
    total += source.base64.length;
    if (total > OPENAI_CHAT_IMAGE_BASE64_BUDGET) {
      found = true;
      return false;
    }
  });
  return found;
}
