// An upstream error body reduced to what ocx shows a client, apart from core-errors.ts so the
// Cloudflare Worker answers an upstream failure with the same text.
import { redactSecretString } from "../../lib/redact";
import { isCyberPolicyCode, isCyberPolicyMessage } from "../../lib/errors";

export interface NormalizedUpstreamErrorText {
  safeText: string;
  message?: string;
  type?: string;
  code?: string;
  cyberPolicy: boolean;
}


/**
 * Extract the structured provider error envelope without making `error.type` authoritative.
 * Policy identity comes from the dedicated code (or the legacy message fallback); a credible
 * upstream type is only carried through so callers do not erase provider diagnostics.
 */
export function normalizeUpstreamErrorText(text: string, fallback: string): NormalizedUpstreamErrorText {
  const safeText = redactSecretString(text).slice(0, 500).trim() || fallback;
  let message: string | undefined;
  let type: string | undefined;
  let code: string | undefined;
  try {
    const parsed = JSON.parse(text) as Record<string, unknown>;
    const response = parsed.response && typeof parsed.response === "object" && !Array.isArray(parsed.response)
      ? parsed.response as Record<string, unknown>
      : undefined;
    const candidates = [parsed.error, response?.error, response?.last_error, parsed.last_error, parsed];
    const source = candidates.find((candidate): candidate is Record<string, unknown> => {
      if (candidate === null || typeof candidate !== "object" || Array.isArray(candidate)) return false;
      const record = candidate as Record<string, unknown>;
      return [record.message, record.type, record.code].some(value => typeof value === "string");
    });
    if (!source) return { safeText, cyberPolicy: isCyberPolicyMessage(safeText) };
    if (typeof source.message === "string" && source.message.trim()) {
      message = redactSecretString(source.message.trim()).slice(0, 500);
    }
    if (typeof source.type === "string" && source.type.trim()) type = source.type.trim();
    if (typeof source.code === "string" && source.code.trim()) code = source.code.trim();
  } catch {
    /* non-JSON upstream body — retain the bounded display-safe text */
  }
  const cyberPolicy = isCyberPolicyCode(code) || isCyberPolicyMessage(message ?? safeText);
  return { safeText, message, type, code, cyberPolicy };
}
