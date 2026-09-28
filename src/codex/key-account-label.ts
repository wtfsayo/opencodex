// Its own module so the Cloudflare Worker labels the usage rows of turns it serves as ocx does.
import { createHash } from "node:crypto";
import type { ProviderApiKeySelection } from "../types/provider";

/** Digest the request-owned configured selection, never serialize its key/reference. */
export function apiKeyAccountLogLabel(provider: string, selection: ProviderApiKeySelection | undefined): `k${string}` | undefined {
  if (!selection || typeof selection.reference !== "string" || !selection.reference.length) return undefined;
  return `k${createHash("sha256").update(JSON.stringify([
    "ocx-key-account-v1", provider, selection.entryId ?? null, selection.reference,
  ])).digest("hex").slice(0, 32)}`;
}
