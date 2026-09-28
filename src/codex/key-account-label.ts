// Its own module so the Cloudflare Worker labels the usage rows of turns it serves as ocx does:
// `k<hex32>` for a configured key, `o<hex6>` for a non-Codex OAuth account.
import { createHash } from "node:crypto";
import type { ProviderApiKeySelection } from "../types/provider";

/**
 * Account log labels come in three families:
 *
 * - `p<hex6>` (plus the literal `main`) — a Codex pool account.
 * - `o<hex6>` — a non-Codex OAuth provider account (xai, cursor, and siblings).
 * - `k<hex32>` — a request-owned API-key selection, scoped to provider and reference.
 *
 * Labels never contain an email, raw key/reference, or raw provider account id. That is
 * a privacy requirement, not a formatting preference: these labels are written to the usage log
 * and served over the management API.
 *
 * hex6 is 16.7M values, so two accounts CAN collide and merge into one reported row. That is a
 * reporting inaccuracy at operator scale, not a correctness or privacy failure, and it is the
 * accepted cost of keeping the existing `p` format byte-compatible.
 */
export const CODEX_ACCOUNT_LOG_LABEL_RE = /^p[a-f0-9]{6}$/;
export const OAUTH_ACCOUNT_LOG_LABEL_RE = /^o[a-f0-9]{6}$/;
export const KEY_ACCOUNT_LOG_LABEL_RE = /^k[a-f0-9]{32}$/;
export const ACCOUNT_LOG_LABEL_RE = /^(?:main|[po][a-f0-9]{6}|k[a-f0-9]{32})$/;

/** Digest the request-owned configured selection, never serialize its key/reference. */
export function apiKeyAccountLogLabel(provider: string, selection: ProviderApiKeySelection | undefined): `k${string}` | undefined {
  if (!selection || typeof selection.reference !== "string" || !selection.reference.length) return undefined;
  return `k${createHash("sha256").update(JSON.stringify([
    "ocx-key-account-v1", provider, selection.entryId ?? null, selection.reference,
  ])).digest("hex").slice(0, 32)}`;
}

export function oauthAccountLogLabel(accountId: string, provider = ""): string {
  return `o${createHash("sha256").update(`${provider}\0${accountId}`).digest("hex").slice(0, 6)}`;
}
