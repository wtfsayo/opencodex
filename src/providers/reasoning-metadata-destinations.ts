// The destinations whose reasoning ladders come from models.dev metadata and learned refusals
// (reasoning-metadata.ts). Import-free so the Cloudflare Worker can decline exactly these.

export const BASE_URL_TO_METADATA_PROVIDER: Record<string, string> = {
  "https://opencode.ai/zen/go/v1": "opencode-go",
  "https://opencode.ai/zen/v1": "opencode",
};

/**
 * Both sides of the mapping are compared after this normalisation, so a trailing slash or a
 * `/v1` suffix never decides whether a destination resolves. models.dev publishes each
 * provider's own `api` URL; the snapshot keeps it (v2) so the mapping can be checked against
 * published data instead of trusted blindly.
 */
export function normalizeDestinationUrl(url: string | undefined): string | undefined {
  if (typeof url !== "string" || url.trim() === "") return undefined;
  try {
    const parsed = new URL(url.trim());
    const path = parsed.pathname.replace(/\/+$/, "").replace(/\/v1$/i, "");
    return (parsed.protocol + "//" + parsed.host + path).toLowerCase();
  } catch {
    return undefined;
  }
}

/** The models.dev provider key for a base URL, or undefined for every other destination. */
export function metadataProviderKeyForBaseUrl(baseUrl: string | undefined): string | undefined {
  const normalized = normalizeDestinationUrl(baseUrl);
  if (!normalized) return undefined;
  for (const [destination, key] of Object.entries(BASE_URL_TO_METADATA_PROVIDER)) {
    if (normalizeDestinationUrl(destination) === normalized) return key;
  }
  return undefined;
}
