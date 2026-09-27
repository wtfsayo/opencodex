import type { OcxConfig } from "../types";
import type { GuiSessionRequestContext } from "./gui-session";

/**
 * Cloudflare Access identity for the dashboard (remoteGui.cloudflareAccess).
 *
 * Access puts a signed JWT in `Cf-Access-Jwt-Assertion` on every request it lets through. The
 * header itself proves nothing: anyone can send one to a listener. Only a token signed by the
 * configured team's keys, issued for the configured application (`aud`), and naming an
 * allowlisted email counts. Every failure is a null identity; nothing here throws to the caller.
 */

export const CLOUDFLARE_ACCESS_JWT_HEADER = "Cf-Access-Jwt-Assertion";

// Keys are refreshed after this, but a set that cannot be refreshed keeps working until
// STALE_KEY_MAX_AGE_MS: an Access outage should not lock operators out, and a retired key should
// not stay trusted forever.
const KEY_CACHE_TTL_MS = 60 * 60_000;
const STALE_KEY_MAX_AGE_MS = 24 * 60 * 60_000;
// Every fetch, whatever triggered it, is at least this far apart per team, and concurrent callers
// share one in-flight fetch. Unauthenticated requests carrying forged tokens reach this code, so
// neither a cold cache nor a failing endpoint may turn the listener into a fetch amplifier.
const KEY_FETCH_MIN_INTERVAL_MS = 60_000;
const KEY_FETCH_TIMEOUT_MS = 5_000;
const MIN_RSA_MODULUS_BITS = 2048;
const CLOCK_SKEW_MS = 60_000;
const MAX_TOKEN_BYTES = 16 * 1024;

type Jwk = JsonWebKey & { kid?: string };
type KeySet = { keys: Map<string, CryptoKey>; fetchedAt: number };
type FetchLike = (input: string, init?: RequestInit) => Promise<Response>;

const keySets = new Map<string, KeySet>();
const lastFetchAt = new Map<string, number>();
const inFlight = new Map<string, Promise<KeySet | null>>();

export function resetCloudflareAccessKeyCacheForTests(): void {
  keySets.clear();
  lastFetchAt.clear();
  inFlight.clear();
}

function base64UrlDecode(segment: string): Uint8Array<ArrayBuffer> | null {
  if (!/^[A-Za-z0-9_-]*$/.test(segment)) return null;
  try {
    return Uint8Array.from(Buffer.from(segment, "base64url"));
  } catch {
    return null;
  }
}

function decodeJson(segment: string): Record<string, unknown> | null {
  const bytes = base64UrlDecode(segment);
  if (!bytes) return null;
  try {
    const value: unknown = JSON.parse(new TextDecoder().decode(bytes));
    return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : null;
  } catch {
    return null;
  }
}

async function importSigningKey(jwk: Jwk): Promise<CryptoKey | null> {
  if (jwk.kty !== "RSA" || (jwk.alg !== undefined && jwk.alg !== "RS256") || (jwk.use !== undefined && jwk.use !== "sig")) return null;
  // 65537 only; a small or odd exponent has no legitimate use here.
  if (jwk.e !== "AQAB") return null;
  try {
    const key = await crypto.subtle.importKey(
      "jwk",
      { kty: "RSA", n: jwk.n, e: jwk.e, alg: "RS256", ext: true },
      { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" },
      false,
      ["verify"],
    );
    return (key.algorithm as RsaHashedKeyAlgorithm).modulusLength >= MIN_RSA_MODULUS_BITS ? key : null;
  } catch {
    return null;
  }
}

async function fetchKeySet(teamDomain: string, fetchImpl: FetchLike, now: number): Promise<KeySet | null> {
  try {
    const response = await fetchImpl(`https://${teamDomain}/cdn-cgi/access/certs`, {
      signal: AbortSignal.timeout(KEY_FETCH_TIMEOUT_MS),
      redirect: "error",
    });
    if (!response.ok) return null;
    const body = await response.json() as { keys?: unknown };
    if (!Array.isArray(body.keys)) return null;
    const keys = new Map<string, CryptoKey>();
    for (const jwk of body.keys as Jwk[]) {
      if (!jwk || typeof jwk.kid !== "string") continue;
      const key = await importSigningKey(jwk);
      if (key) keys.set(jwk.kid, key);
    }
    const set = { keys, fetchedAt: now };
    keySets.set(teamDomain, set);
    return set;
  } catch {
    return null;
  }
}

/** Starts a fetch unless one ran within the minimum interval; concurrent callers share it. */
function refreshKeySet(teamDomain: string, fetchImpl: FetchLike, now: number): Promise<KeySet | null> | null {
  const running = inFlight.get(teamDomain);
  if (running) return running;
  if (now - (lastFetchAt.get(teamDomain) ?? -Infinity) < KEY_FETCH_MIN_INTERVAL_MS) return null;
  lastFetchAt.set(teamDomain, now);
  const fetching = fetchKeySet(teamDomain, fetchImpl, now).finally(() => inFlight.delete(teamDomain));
  inFlight.set(teamDomain, fetching);
  return fetching;
}

async function signingKey(teamDomain: string, kid: string, fetchImpl: FetchLike, now: number): Promise<CryptoKey | null> {
  const cached = keySets.get(teamDomain);
  const usable = cached && now - cached.fetchedAt <= STALE_KEY_MAX_AGE_MS ? cached : undefined;
  if (usable && now - usable.fetchedAt > KEY_CACHE_TTL_MS) {
    // Stale but usable: refresh in the background rather than holding this request.
    void refreshKeySet(teamDomain, fetchImpl, now);
  }
  const known = usable?.keys.get(kid);
  if (known) return known;
  // No usable set yet, or an unknown kid (Access rotates keys): wait for a fetch if one is allowed.
  const fetched = await refreshKeySet(teamDomain, fetchImpl, now);
  return fetched?.keys.get(kid) ?? null;
}

export interface CloudflareAccessIdentity {
  email: string;
  /** When the verified token stops being valid; a session minted from it must not outlive it. */
  expiresAt: number;
}

/** Lowercases ASCII only; anything else is refused so Unicode case folding cannot widen a match. */
export function normalizeAccessEmail(value: string): string | null {
  const trimmed = value.trim();
  return /^[\x21-\x7e]+$/.test(trimmed) ? trimmed.toLowerCase() : null;
}

/**
 * Returns the verified, allowlisted identity for this request, or null. Only meaningful when
 * `remoteGui.cloudflareAccess` is configured; otherwise always null.
 */
export async function verifyCloudflareAccessIdentity(
  req: Request,
  config: OcxConfig,
  options: { now?: number; fetchImpl?: FetchLike } = {},
): Promise<CloudflareAccessIdentity | null> {
  const access = config.remoteGui?.cloudflareAccess;
  if (!access) return null;
  const token = req.headers.get(CLOUDFLARE_ACCESS_JWT_HEADER)?.trim();
  if (!token || token.length > MAX_TOKEN_BYTES) return null;
  const parts = token.split(".");
  if (parts.length !== 3) return null;
  const [headerSegment, payloadSegment, signatureSegment] = parts as [string, string, string];
  const header = decodeJson(headerSegment);
  const payload = decodeJson(payloadSegment);
  const signature = base64UrlDecode(signatureSegment);
  // RS256 only: accepting the token's own `alg` is how "none" and HMAC-with-public-key forgeries work.
  if (!header || !payload || !signature || header.alg !== "RS256" || typeof header.kid !== "string") return null;

  const now = options.now ?? Date.now();
  const key = await signingKey(access.teamDomain, header.kid, options.fetchImpl ?? fetch, now);
  if (!key) return null;
  const signed = new TextEncoder().encode(`${headerSegment}.${payloadSegment}`);
  let valid = false;
  try {
    valid = await crypto.subtle.verify("RSASSA-PKCS1-v1_5", key, signature, signed);
  } catch {
    return null;
  }
  if (!valid) return null;

  if (payload.iss !== `https://${access.teamDomain}`) return null;
  const audiences = Array.isArray(payload.aud) ? payload.aud : [payload.aud];
  if (!audiences.includes(access.audience)) return null;
  if (typeof payload.exp !== "number" || payload.exp * 1000 + CLOCK_SKEW_MS <= now) return null;
  if (payload.nbf !== undefined && (typeof payload.nbf !== "number" || payload.nbf * 1000 - CLOCK_SKEW_MS > now)) return null;
  if (typeof payload.email !== "string") return null;
  const email = normalizeAccessEmail(payload.email);
  return email && access.allowedEmails.includes(email) ? { email, expiresAt: payload.exp * 1000 } : null;
}

/**
 * Headers Cloudflare's edge adds to every proxied request, tunnels included. A request carrying
 * any of them came through Cloudflare, not Tailscale Serve, so its `Tailscale-User-Login` header
 * was not set by Tailscale and must not be trusted even on the management ingress.
 */
const CLOUDFLARE_EDGE_HEADERS = [CLOUDFLARE_ACCESS_JWT_HEADER, "Cf-Ray", "Cf-Connecting-Ip"];

export function cameThroughCloudflare(req: Request): boolean {
  return CLOUDFLARE_EDGE_HEADERS.some(name => req.headers.has(name));
}

/** The GUI session context for a request on `ingress`, with any Access identity already verified. */
export async function guiSessionRequestContext(
  req: Request,
  config: OcxConfig,
  ingress: string,
): Promise<GuiSessionRequestContext> {
  const managementIngress = ingress === "hub-management";
  // Off the management ingress, a leaked token would skip Access's own checks (device posture,
  // IP rules, revocation) by going straight to a listener Access does not front. Deployments
  // where Access fronts every listener (a Worker in front of a container) opt in explicitly.
  const accessHere = managementIngress || config.remoteGui?.cloudflareAccess?.anyListener === true;
  return {
    trustedTailscaleIngress: managementIngress && !cameThroughCloudflare(req),
    cloudflareAccess: accessHere ? await verifyCloudflareAccessIdentity(req, config) : null,
  };
}
