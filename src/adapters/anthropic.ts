// The Anthropic Messages adapter (anthropic/adapter.ts) with its image normalization bound. The
// Cloudflare Worker binds its own, since it serves no image-bearing turn.
import type { ProviderAdapter } from "./base";
import type { OcxProviderConfig } from "../types";
import { createAnthropicAdapterWith } from "./anthropic/adapter";
import { normalizeAnthropicImages } from "./anthropic-image-normalize";

export * from "./anthropic/adapter";

export function createAnthropicAdapter(provider: OcxProviderConfig, cacheRetention?: "none" | "short" | "long"): ProviderAdapter {
  return createAnthropicAdapterWith(provider, cacheRetention, { normalizeAnthropicImages });
}
