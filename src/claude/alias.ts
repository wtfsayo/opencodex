import { desktop3pAlias } from "./desktop-3p";
import { aliasForNative, aliasForRoute } from "./alias-codec";

export * from "./alias-codec";

/**
 * Claude Code (CLI) surface alias — devlog 050 + audit 051 #2.
 *
 * The readable `ocx-claude*` form when representable; otherwise the desktop-3p
 * hash so the model still appears in discovery (collisions follow the same
 * first-wins policy as the desktop registry — audit 051 #1). Real Anthropic
 * models pass through unchanged (they must keep hitting the sk-ant passthrough).
 * Old `claude-ocx*` ids keep decoding forever in resolveInboundModel, so ids persisted
 * in Claude Code's settings.json never break when the surface style changes.
 */
export function claudeCodeAlias(provider: string, modelId: string): string {
  if (provider === "anthropic" && modelId.startsWith("claude-")) return modelId;
  return aliasForRoute(provider, modelId) ?? desktop3pAlias(provider, modelId);
}

/** Claude Code (CLI) surface alias for a native OpenAI slug. */
export function claudeCodeNativeAlias(slug: string): string {
  return aliasForNative(slug) ?? desktop3pAlias("native", slug);
}
