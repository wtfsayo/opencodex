import { beforeEach, describe, expect, test } from "bun:test";

import { getDefaultConfig, validateConfigCandidate } from "../../src/config";
import {
  guiSessionRequestContext,
  resetCloudflareAccessKeyCacheForTests,
  verifyCloudflareAccessIdentity,
} from "../../src/server/cloudflare-access";
import {
  authorizeGuiSessionRequest,
  issueGuiSession,
  REMOTE_GUI_SESSION_TTL_MS,
  type GuiSessionState,
} from "../../src/server/gui-session";
import type { OcxConfig } from "../../src/types";

const TEAM = "acme.cloudflareaccess.com";
const AUD = "a".repeat(64);
const NOW = 1_800_000_000_000;

type Signer = { kid: string; privateKey: CryptoKey; jwk: JsonWebKey & { kid: string } };

async function signer(kid: string, modulusLength = 2048): Promise<Signer> {
  const pair = await crypto.subtle.generateKey(
    { name: "RSASSA-PKCS1-v1_5", modulusLength, publicExponent: new Uint8Array([1, 0, 1]), hash: "SHA-256" },
    true,
    ["sign", "verify"],
  );
  const jwk = await crypto.subtle.exportKey("jwk", pair.publicKey);
  return { kid, privateKey: pair.privateKey, jwk: { ...jwk, kid, alg: "RS256" } };
}

const b64 = (value: string | Uint8Array) => Buffer.from(value).toString("base64url");

async function token(by: Signer, claims: Record<string, unknown> = {}, header: Record<string, unknown> = {}): Promise<string> {
  const head = b64(JSON.stringify({ alg: "RS256", kid: by.kid, typ: "JWT", ...header }));
  const body = b64(JSON.stringify({
    iss: `https://${TEAM}`, aud: [AUD], email: "Alice@Example.test",
    iat: NOW / 1000, exp: NOW / 1000 + 3600, ...claims,
  }));
  const signature = await crypto.subtle.sign("RSASSA-PKCS1-v1_5", by.privateKey, new TextEncoder().encode(`${head}.${body}`));
  return `${head}.${body}.${b64(new Uint8Array(signature))}`;
}

function keyServer(keys: () => Signer[]) {
  const calls: string[] = [];
  const fetchImpl = async (url: string) => {
    calls.push(url);
    return Response.json({ keys: keys().map(k => k.jwk) });
  };
  return { calls, fetchImpl };
}

function config(overrides: Partial<OcxConfig> = {}): OcxConfig {
  return {
    port: 0,
    hostname: "0.0.0.0",
    runtimeRole: "hub",
    hub: { managementPublicOrigin: "https://hub.example.test" },
    remoteGui: { cloudflareAccess: { teamDomain: TEAM, audience: AUD, allowedEmails: ["alice@example.test"] } },
    providers: {},
    ...overrides,
  } as OcxConfig;
}

const request = (jwt?: string, extra: Record<string, string> = {}) => new Request("https://hub.example.test/", {
  headers: { Host: "hub.example.test", ...(jwt ? { "Cf-Access-Jwt-Assertion": jwt } : {}), ...extra },
});

const newState = (): GuiSessionState => ({ sessions: new Map(), pairingGrants: new Map() });

beforeEach(() => resetCloudflareAccessKeyCacheForTests());

describe("Cloudflare Access token verification", () => {
  test("accepts a token signed by the team key for the configured application and allowlisted email", async () => {
    const key = await signer("k1");
    const { fetchImpl, calls } = keyServer(() => [key]);
    expect(await verifyCloudflareAccessIdentity(request(await token(key)), config(), { now: NOW, fetchImpl })).toEqual({
      email: "alice@example.test",
      expiresAt: NOW + 3600_000,
    });
    expect(calls).toEqual([`https://${TEAM}/cdn-cgi/access/certs`]);
    // Cached: a second request does not refetch the keys.
    await verifyCloudflareAccessIdentity(request(await token(key)), config(), { now: NOW + 1000, fetchImpl });
    expect(calls).toHaveLength(1);
  });

  test("rejects every forgery and mismatch", async () => {
    const key = await signer("k1");
    const stranger = await signer("k1");
    const { fetchImpl } = keyServer(() => [key]);
    const verify = async (jwt: string | undefined, cfg = config()) =>
      verifyCloudflareAccessIdentity(request(jwt), cfg, { now: NOW, fetchImpl });
    const [head, body] = (await token(key)).split(".");

    expect(await verify(undefined)).toBeNull();
    expect(await verify("not.a.jwt.at-all")).toBeNull();
    expect(await verify(`${b64(JSON.stringify({ alg: "none", kid: "k1" }))}.${body}.`)).toBeNull();
    expect(await verify(await token(key, {}, { alg: "HS256" }))).toBeNull();
    expect(await verify(await token(stranger))).toBeNull();
    expect(await verify(`${head}.${b64(JSON.stringify({ iss: `https://${TEAM}`, aud: [AUD], email: "alice@example.test", exp: NOW / 1000 + 3600 }))}.${(await token(key)).split(".")[2]}`)).toBeNull();
    expect(await verify(await token(key, { iss: "https://other.cloudflareaccess.com" }))).toBeNull();
    expect(await verify(await token(key, { aud: ["b".repeat(64)] }))).toBeNull();
    expect(await verify(await token(key, { exp: NOW / 1000 - 120 }))).toBeNull();
    expect(await verify(await token(key, { nbf: NOW / 1000 + 600 }))).toBeNull();
    expect(await verify(await token(key, { email: "mallory@example.test" }))).toBeNull();
    expect(await verify(await token(key, { email: undefined }))).toBeNull();
    expect(await verify(await token(key), config({ remoteGui: {} }))).toBeNull();
  });

  test("a failed key fetch fails closed", async () => {
    const key = await signer("k1");
    const down = async () => new Response("unavailable", { status: 503 });
    expect(await verifyCloudflareAccessIdentity(request(await token(key)), config(), { now: NOW, fetchImpl: down })).toBeNull();
    const broken = async () => { throw new Error("network"); };
    resetCloudflareAccessKeyCacheForTests();
    expect(await verifyCloudflareAccessIdentity(request(await token(key)), config(), { now: NOW, fetchImpl: broken })).toBeNull();
  });

  test("forged tokens cannot amplify key fetches: cold, failing, and concurrent", async () => {
    const forged = `${b64(JSON.stringify({ alg: "RS256", kid: "x" }))}.${b64("{}")}.AAAA`;
    let fetches = 0;
    const down = async () => { fetches++; return new Response("unavailable", { status: 503 }); };
    for (let i = 0; i < 20; i++) {
      await verifyCloudflareAccessIdentity(request(forged), config(), { now: NOW + i * 1000, fetchImpl: down });
    }
    expect(fetches).toBe(1);

    resetCloudflareAccessKeyCacheForTests();
    let slow = 0;
    const key = await signer("k1");
    const gated = async () => { slow++; await Bun.sleep(50); return Response.json({ keys: [key.jwk] }); };
    await Promise.all(Array.from({ length: 50 }, () =>
      verifyCloudflareAccessIdentity(request(forged), config(), { now: NOW, fetchImpl: gated })));
    expect(slow).toBe(1);
  });

  test("an expired cache keeps serving its keys while one background refresh runs, up to a hard limit", async () => {
    const key = await signer("k1");
    let fetches = 0;
    let up = true;
    const flaky = async () => { fetches++; return up ? Response.json({ keys: [key.jwk] }) : new Response("down", { status: 503 }); };
    const verify = async (at: number) => verifyCloudflareAccessIdentity(request(await token(key, { exp: at / 1000 + 3600 })), config(), { now: at, fetchImpl: flaky });
    expect(await verify(NOW)).not.toBeNull();
    up = false;
    const twoHours = NOW + 2 * 3600_000;
    expect(await verify(twoHours)).not.toBeNull();
    expect(await verify(twoHours + 1000)).not.toBeNull();
    expect(fetches).toBe(2);
    // Past the stale limit a key that cannot be refreshed is no longer trusted.
    expect(await verify(NOW + 25 * 3600_000)).toBeNull();
  });

  test("weak keys and non-ASCII emails are refused", async () => {
    const weak = await signer("weak", 1024);
    const { fetchImpl } = keyServer(() => [weak]);
    expect(await verifyCloudflareAccessIdentity(request(await token(weak)), config(), { now: NOW, fetchImpl })).toBeNull();
    resetCloudflareAccessKeyCacheForTests();
    const key = await signer("k1");
    const good = keyServer(() => [key]);
    // U+212A KELVIN SIGN lowercases to "k"; it must not match kate@example.test.
    const kate = config({ remoteGui: { cloudflareAccess: { teamDomain: TEAM, audience: AUD, allowedEmails: ["kate@example.test"] } } });
    expect(await verifyCloudflareAccessIdentity(request(await token(key, { email: "\u212Aate@example.test" })), kate, { now: NOW, fetchImpl: good.fetchImpl })).toBeNull();
    expect(await verifyCloudflareAccessIdentity(request(await token(key, { email: "Kate@Example.test" })), kate, { now: NOW, fetchImpl: good.fetchImpl }))
      .toMatchObject({ email: "kate@example.test" });
  });

  test("an unknown kid refetches for key rotation, at most once a minute", async () => {
    const old = await signer("old");
    const rotated = await signer("new");
    let published = [old];
    const { fetchImpl, calls } = keyServer(() => published);
    await verifyCloudflareAccessIdentity(request(await token(old)), config(), { now: NOW, fetchImpl });
    published = [old, rotated];
    // Within the minute: an unknown kid does not refetch, so forged kids cannot amplify fetches.
    expect(await verifyCloudflareAccessIdentity(request(await token(rotated)), config(), { now: NOW + 30_000, fetchImpl })).toBeNull();
    expect(calls).toHaveLength(1);
    expect(await verifyCloudflareAccessIdentity(request(await token(rotated)), config(), { now: NOW + 61_000, fetchImpl }))
      .toMatchObject({ email: "alice@example.test" });
    expect(calls).toHaveLength(2);
  });
});

describe("Cloudflare Access GUI sessions", () => {
  const access = { email: "alice@example.test", expiresAt: NOW + 3600_000 };

  test("a verified identity mints a session capped at the token's expiry, on any ingress", () => {
    const session = issueGuiSession(request(), config(), newState(), { trustedTailscaleIngress: false, cloudflareAccess: access, now: NOW });
    expect(session).toMatchObject({
      issuance: "cloudflare-access",
      serverOrigin: "https://hub.example.test",
      browserOrigin: "https://hub.example.test",
      expiresAt: NOW + 3600_000,
      notAfter: NOW + 3600_000,
    });
  });

  test("the remote-session rules still apply", () => {
    const state = newState();
    const issue = (cfg: OcxConfig, req = request(), identity: typeof access | null = access) =>
      issueGuiSession(req, cfg, state, { trustedTailscaleIngress: false, cloudflareAccess: identity, now: NOW });
    expect(issue(config({ runtimeRole: "client" }))).toBeNull();
    expect(issue(config({ hub: { managementPublicOrigin: "http://hub.example.test" } }))).toBeNull();
    expect(issue(config(), request(undefined, { Origin: "https://evil.example.test" }))).toBeNull();
    expect(issue(config(), request(), { ...access, expiresAt: NOW })).toBeNull();
    expect(issue(config(), request(), null)).toBeNull();
    // Without a verified identity, a Tailscale header on the public ingress still grants nothing.
    expect(issueGuiSession(request(undefined, { "Tailscale-User-Login": "alice@example.test" }),
      config({ remoteGui: { allowedTailscaleUsers: ["alice@example.test"] } }), state, { trustedTailscaleIngress: false, now: NOW })).toBeNull();
  });

  test("activity renews the session but never past the token's expiry", () => {
    const state = newState();
    const shortLived = { email: "alice@example.test", expiresAt: NOW + 10 * 60_000 };
    const session = issueGuiSession(request(), config(), state, { trustedTailscaleIngress: false, cloudflareAccess: shortLived, now: NOW })!;
    const use = (at: number) => authorizeGuiSessionRequest(new Request("https://hub.example.test/api/config", {
      headers: { Host: "hub.example.test", Authorization: `Bearer ${session.token}`, "x-opencodex-gui-origin": session.browserOrigin },
    }), config(), state, at);
    expect(use(NOW + 5 * 60_000)).toMatchObject({ ok: true });
    expect(state.sessions.get(session.token)!.expiresAt).toBe(NOW + 10 * 60_000);
    expect(state.sessions.get(session.token)!.expiresAt).toBeLessThan(NOW + REMOTE_GUI_SESSION_TTL_MS);
    expect(use(NOW + 10 * 60_000)).toEqual({ ok: false, reason: "expired" });
  });

  test("the request context carries only a verified identity, and only where Access fronts the listener", async () => {
    const key = await signer("k1");
    const { fetchImpl } = keyServer(() => [key]);
    // Warm the key cache so the context helper (which uses the global fetch) needs no network.
    const jwt = await token(key, { exp: Date.now() / 1000 + 3600 });
    await verifyCloudflareAccessIdentity(request(jwt), config(), { now: Date.now(), fetchImpl });
    expect((await guiSessionRequestContext(request("forged.token.value"), config(), "hub-management")).cloudflareAccess).toBeNull();
    expect((await guiSessionRequestContext(request(jwt), config(), "hub-management")).cloudflareAccess).toMatchObject({ email: "alice@example.test" });
    // A token replayed straight at another listener skips Access's own checks, so it counts only on opt-in.
    expect((await guiSessionRequestContext(request(jwt), config(), "public")).cloudflareAccess).toBeNull();
    const everywhere = config({ remoteGui: { cloudflareAccess: { teamDomain: TEAM, audience: AUD, allowedEmails: ["alice@example.test"], anyListener: true } } });
    expect((await guiSessionRequestContext(request(jwt), everywhere, "public")).cloudflareAccess).toMatchObject({ email: "alice@example.test" });
  });

  test("a Tailscale header that came through Cloudflare is not trusted, even on the management ingress", async () => {
    const tailscale = { "Tailscale-User-Login": "alice@example.test" };
    expect((await guiSessionRequestContext(request(undefined, tailscale), config(), "hub-management")).trustedTailscaleIngress).toBe(true);
    for (const header of ["Cf-Ray", "Cf-Connecting-Ip", "Cf-Access-Jwt-Assertion"]) {
      const context = await guiSessionRequestContext(request(undefined, { ...tailscale, [header]: "x" }), config(), "hub-management");
      expect(context.trustedTailscaleIngress).toBe(false);
    }
  });

  test("removing an email or the whole block revokes live sessions immediately", () => {
    const state = newState();
    const session = issueGuiSession(request(), config(), state, { trustedTailscaleIngress: false, cloudflareAccess: access, now: NOW })!;
    const use = (cfg: OcxConfig) => authorizeGuiSessionRequest(new Request("https://hub.example.test/api/config", {
      headers: { Host: "hub.example.test", Authorization: `Bearer ${session.token}`, "x-opencodex-gui-origin": session.browserOrigin },
    }), cfg, state, NOW + 1000);
    expect(use(config())).toMatchObject({ ok: true });
    expect(use(config({ remoteGui: { cloudflareAccess: { teamDomain: TEAM, audience: AUD, allowedEmails: ["bob@example.test"] } } })))
      .toEqual({ ok: false, reason: "revoked" });
    expect(state.sessions.has(session.token)).toBe(false);
    const again = issueGuiSession(request(), config(), state, { trustedTailscaleIngress: false, cloudflareAccess: access, now: NOW })!;
    expect(authorizeGuiSessionRequest(new Request("https://hub.example.test/api/config", {
      headers: { Host: "hub.example.test", Authorization: `Bearer ${again.token}`, "x-opencodex-gui-origin": again.browserOrigin },
    }), config({ remoteGui: {} }), state, NOW + 1000)).toEqual({ ok: false, reason: "revoked" });
  });
});

describe("remoteGui.cloudflareAccess config", () => {
  // Through the real config validation path, as tests/server/config.test.ts does for remoteGui.
  const parse = (cloudflareAccess: unknown) => {
    const result = validateConfigCandidate({ ...getDefaultConfig(), remoteGui: { cloudflareAccess } });
    return { success: result.ok, data: result.ok ? result.config.remoteGui : undefined };
  };
  const valid = { teamDomain: TEAM, audience: AUD, allowedEmails: ["alice@example.test"] };

  test("accepts a well-formed block and normalizes case", () => {
    const parsed = parse({ ...valid, teamDomain: "ACME.cloudflareaccess.com", allowedEmails: ["Alice@Example.test"] });
    expect(parsed.success).toBe(true);
    expect(parsed.data?.cloudflareAccess).toEqual({ ...valid, allowedEmails: ["alice@example.test"] });
  });

  test("rejects anything that could widen what is trusted", () => {
    expect(parse({ ...valid, teamDomain: "https://acme.cloudflareaccess.com" }).success).toBe(false);
    expect(parse({ ...valid, teamDomain: "acme.cloudflareaccess.com.evil.test" }).success).toBe(false);
    expect(parse({ ...valid, teamDomain: "evil.test" }).success).toBe(false);
    expect(parse({ ...valid, audience: "short" }).success).toBe(false);
    expect(parse({ ...valid, allowedEmails: [] }).success).toBe(false);
    expect(parse({ ...valid, allowedEmails: ["alice@example.test", "ALICE@example.test"] }).success).toBe(false);
    expect(parse({ ...valid, extra: true }).success).toBe(false);
    expect(parse({ ...valid, allowedEmails: ["\u212Aate@example.test"] }).success).toBe(false);
    expect(parse({ ...valid, anyListener: "yes" }).success).toBe(false);
    expect(parse({ ...valid, anyListener: true }).success).toBe(true);
  });
});
