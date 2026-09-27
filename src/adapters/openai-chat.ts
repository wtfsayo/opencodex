import { hasShrinkableOpenAIChatImages, normalizeOpenAIChatImages } from "./openai-chat-images";
import type { ProviderAdapter } from "./base";
import type { OcxProviderConfig } from "../types";
import { mapReasoningEffort } from "../reasoning-effort";
import { createOpenAIChatAdapterWith } from "./openai-chat/adapter";

export { stripBracketedModelSuffix } from "./openai-chat/wire";
export { buildOpenAIChatPassthroughRequest } from "./openai-chat/passthrough";
export { formatOpenAIChatErrorBody } from "./openai-chat/errors";
export { createOpenAIChatAdapterWith, type OpenAIChatAdapterDeps } from "./openai-chat/adapter";

export function createOpenAIChatAdapter(provider: OcxProviderConfig): ProviderAdapter {
  return createOpenAIChatAdapterWith(provider, { mapReasoningEffort, hasShrinkableOpenAIChatImages, normalizeOpenAIChatImages });
}
