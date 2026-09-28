import { parseDataUrl } from "./image";
import { forEachImagePart } from "./openai-chat-image-budget";
export { hasShrinkableOpenAIChatImages, OPENAI_CHAT_IMAGE_BASE64_BUDGET } from "./openai-chat-image-budget";
import { OPENAI_CHAT_IMAGE_BASE64_BUDGET } from "./openai-chat-image-budget";
import {
  normalizeImageTargets,
  type NormalizeOptions,
  type NormalizeTarget,
} from "./anthropic-image-normalize";

export interface NormalizeOpenAIChatImagesOptions
  extends Pick<NormalizeOptions, "encode" | "tierBias" | "validate"> {
  abortSignal?: AbortSignal;
}

/**
 * Normalize image_url parts in already-built Chat Completions messages, in place.
 *
 * The drop callback deliberately keeps the original URL. The shared normalizer calls
 * drop for corrupt or decode-bomb inputs, and this wire has no downstream guard that
 * would re-attach a dropped image, so dropping here would silently lose a user's
 * screenshot. Terminal-size overflow uses overflowAction "none" for the same reason:
 * an image floored at 320px stays attached rather than being removed.
 */
export async function normalizeOpenAIChatImages(
  messages: unknown,
  options: NormalizeOpenAIChatImagesOptions = {},
): Promise<void> {
  const targets: NormalizeTarget[] = [];
  forEachImagePart(messages, (imageUrl, url) => {
    const source = parseDataUrl(url);
    if (!source) return;
    targets.push({
      base64: source.base64,
      mediaType: source.mediaType,
      replace: (data: string, mediaType: string) => {
        imageUrl.url = `data:${mediaType};base64,${data}`;
      },
      drop: () => {
        // Preserve the original image URL when it cannot be normalized.
      },
      // The drop above is a no-op, so these bytes are still on the wire and must keep
      // counting against the budget. Without this the core would stop counting them and
      // the demotion loop could stop early, shipping a body that is still oversized.
      retainsBytesOnDrop: true,
    });
  });
  if (targets.length === 0) return;

  await normalizeImageTargets(targets, {
    budget: OPENAI_CHAT_IMAGE_BASE64_BUDGET,
    overflowAction: "none",
    ...options,
  });
}
