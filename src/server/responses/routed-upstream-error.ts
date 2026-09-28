// The error a client gets for a routed provider's non-2xx answer (adapter-dispatch.ts), apart from
// the dispatch loop so the Cloudflare Worker answers a routed failure with the same body.
import { formatErrorResponse } from "../../bridge/errors";
import { CYBER_POLICY_ERROR_CODE, CYBER_POLICY_FALLBACK_MESSAGE, isCyberPolicyCode } from "../../lib/errors";
import { resolveClientRetryAfter } from "../../lib/retry-after";
import { enrichOpenCodeZenUpstreamMessage } from "../../providers/opencode-zen-rate-limit";
import type { OcxProviderConfig } from "../../types";
import type { NormalizedUpstreamErrorText } from "./upstream-error-text";

export function routedUpstreamErrorResponse(
  status: number,
  normalized: NormalizedUpstreamErrorText,
  upstreamRetryAfter: string | null,
  route: { providerName: string; provider: Pick<OcxProviderConfig, "baseUrl" | "adapter" | "authMode" | "apiKey"> },
): Response {
  const message = normalized.cyberPolicy
    ? normalized.message
      ?? (isCyberPolicyCode(normalized.code) ? CYBER_POLICY_FALLBACK_MESSAGE : normalized.safeText)
    : enrichOpenCodeZenUpstreamMessage(
      `Provider error ${status}: ${normalized.safeText}`,
      {
        status: status,
        providerName: route.providerName,
        baseUrl: route.provider.baseUrl,
        adapter: route.provider.adapter,
        authMode: route.provider.authMode,
        hasApiKey: Boolean(route.provider.apiKey?.trim()),
        upstreamRetryAfter,
        // This recovery path is the HTTP Responses wire; custom runTurn transports
        // never reach enrichOpenCodeZenUpstreamMessage here.
        supportsHttpSameKeyRetry: true,
      },
    );
  const retryAfter = normalized.cyberPolicy
    ? undefined
    : resolveClientRetryAfter({
      status: status,
      message,
      upstreamRetryAfter,
    });
  return formatErrorResponse(
    status,
    normalized.cyberPolicy ? (normalized.type ?? CYBER_POLICY_ERROR_CODE) : "upstream_error",
    message,
    {
      ...(normalized.cyberPolicy ? { code: CYBER_POLICY_ERROR_CODE } : {}),
      ...(retryAfter !== undefined ? { retryAfter } : {}),
    },
  );
}
