// The Responses passthrough adapter bound to ocx's disk-backed lookups; the adapter itself lives in
// passthrough-adapter.ts, which the Cloudflare Worker binds with its own.
import type { ProviderAdapter } from "../base";
import type { OcxProviderConfig } from "../../types";
import { catalogModelSupportsReasoningSummaries } from "../../codex/catalog";
import { DISK_METADATA } from "../../reasoning-effort";
import { observeOutbound } from "../../usage/cache-diagnostic";
import { createResponsesPassthroughAdapterWith, type ResponsesPassthroughAdapterDeps } from "./passthrough-adapter";

export * from "./passthrough-adapter";

const DISK_DEPS: ResponsesPassthroughAdapterDeps = {
  reasoningMetadata: DISK_METADATA,
  supportsReasoningSummaries: catalogModelSupportsReasoningSummaries,
  observeOutbound,
};

export function createResponsesPassthroughAdapter(provider: OcxProviderConfig): ProviderAdapter & { passthrough: true } {
  return createResponsesPassthroughAdapterWith(provider, DISK_DEPS);
}
