// The hub's OAuth refresh arbiter. Refresh tokens for Anthropic and ChatGPT logins rotate on every
// spend: exactly one of the Worker and the container may spend a generation, so this object holds
// the lease and the compare-and-swap on the Durable Object's auth.json copy. The container-side
// intent lockfile (src/oauth/store.ts) carries {generation, attemptId}; the row here carries the
// same identity, plus acquiredAt so a Worker isolate dying mid-refresh frees the key on a TTL.
//
// Synchronous storage only (ctx.storage.sql + ctx.storage.kv inside transactionSync): a lease
// check, a generation check and a document write either all land or none do. The DO's input gate
// already serializes requests; the transaction additionally closes the await gaps.
import { createHash } from "node:crypto";
import { DOCUMENT_KEY_PREFIX, DOCUMENT_SEQ_KEY_PREFIX, MAX_DOCUMENT_BYTES, type StoredDocument } from "./lease";

/** Long enough for a provider round trip, short enough that a dead isolate frees the key quickly. */
export const OAUTH_REFRESH_LEASE_TTL_MS = 30_000;

const AUTH_DOCUMENT_KEY = `${DOCUMENT_KEY_PREFIX}auth`;
const AUTH_SEQ_KEY = `${DOCUMENT_SEQ_KEY_PREFIX}auth`;

type SqlRows = { toArray(): unknown[] };

/** The synchronous slice of DurableObjectStorage this module needs (tests drive it with fakes). */
export interface OAuthRefreshStorage {
  sql: { exec(query: string, ...params: unknown[]): SqlRows };
  kv: {
    get<T>(key: string): T | undefined;
    put(key: string, value: unknown): void;
    delete(key: string): boolean;
  };
  transactionSync<T>(closure: () => T): T;
}

interface RefreshLeaseRow {
  provider: string;
  accountId: string;
  generation: string;
  attemptId: string;
  acquiredAt: number;
}

export type OAuthRefreshAcquireResult = { attemptId: string } | { busy: true };
export type OAuthRefreshCommitResult =
  | { ok: true; generation: string }
  | { conflict: "lease" | "generation" | "committed-generation"; generation?: string };
export type OAuthRefreshLeaseCheck = { live: boolean; generation?: string };

/** A credential as auth.json stores it; unknown fields are kept verbatim on merge. */
export interface RefreshedOAuthCredential {
  access: string;
  refresh: string;
  expires: number;
  accountId?: string;
  email?: string;
  source?: string;
  projectId?: string;
  apiBaseUrl?: string;
  kiro?: unknown;
}

type Rec = Record<string, unknown>;
const isRec = (value: unknown): value is Rec => !!value && typeof value === "object" && !Array.isArray(value);

/** src/oauth/store.ts's credentialGeneration: sha256 of the mutable credential fields. */
export function credentialGenerationOf(credential: { refresh: string; access: string; expires: number }): string {
  return createHash("sha256").update(JSON.stringify([credential.refresh, credential.access, credential.expires])).digest("hex");
}

function validCredential(credential: unknown): credential is RefreshedOAuthCredential {
  return isRec(credential)
    && typeof credential.access === "string"
    && typeof credential.refresh === "string"
    && typeof credential.expires === "number";
}

export class OAuthRefreshCoordinator {
  constructor(
    private readonly storage: OAuthRefreshStorage,
    private readonly now: () => number = Date.now,
  ) {
    this.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS refresh_lease (" +
      "provider TEXT NOT NULL, accountId TEXT NOT NULL, generation TEXT NOT NULL, " +
      "attemptId TEXT NOT NULL, acquiredAt INTEGER NOT NULL, PRIMARY KEY (provider, accountId))",
    );
    // A monotonic count of Worker-committed rotations per account: the hash alone proves sameness,
    // not order, so the counter is the durable record of which commit came after which.
    this.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS credential_gen (" +
      "provider TEXT NOT NULL, accountId TEXT NOT NULL, counter INTEGER NOT NULL, generation TEXT NOT NULL, " +
      "PRIMARY KEY (provider, accountId))",
    );
  }

  private leaseRow(provider: string, accountId: string): RefreshLeaseRow | undefined {
    return this.storage.sql
      .exec("SELECT provider, accountId, generation, attemptId, acquiredAt FROM refresh_lease WHERE provider = ? AND accountId = ?", provider, accountId)
      .toArray()[0] as RefreshLeaseRow | undefined;
  }

  private liveLease(provider: string, accountId: string): RefreshLeaseRow | undefined {
    const row = this.leaseRow(provider, accountId);
    return row && this.now() - row.acquiredAt < OAUTH_REFRESH_LEASE_TTL_MS ? row : undefined;
  }

  /** The generation the DO's auth.json copy currently carries for this account, if any. */
  private storedGeneration(provider: string, accountId: string): string | undefined {
    const stored = this.storage.kv.get<StoredDocument>(AUTH_DOCUMENT_KEY);
    if (typeof stored?.body !== "string") return undefined;
    let parsed: unknown;
    try { parsed = JSON.parse(stored.body); } catch { return undefined; }
    const set = isRec(parsed) ? parsed[provider] : undefined;
    const accounts = isRec(set) && Array.isArray(set.accounts) ? set.accounts as unknown[] : undefined;
    const account = accounts?.find(candidate => isRec(candidate) && candidate.id === accountId);
    return isRec(account) && validCredential(account.credential) ? credentialGenerationOf(account.credential) : undefined;
  }

  /**
   * Takes the refresh lease for (provider, accountId). Busy only while a live lease guards the
   * generation the store still carries: once a commit lands, the stored generation moves on and the
   * old row can never again fence the same spend, so it is overwritten rather than blocking.
   */
  acquire(provider: string, accountId: string, generation: string): OAuthRefreshAcquireResult {
    return this.storage.transactionSync(() => {
      const lease = this.liveLease(provider, accountId);
      if (lease && lease.generation === this.storedGeneration(provider, accountId)) return { busy: true as const };
      const attemptId = crypto.randomUUID();
      this.storage.sql.exec(
        "INSERT OR REPLACE INTO refresh_lease (provider, accountId, generation, attemptId, acquiredAt) VALUES (?, ?, ?, ?, ?)",
        provider, accountId, generation, attemptId, this.now(),
      );
      return { attemptId };
    });
  }

  /** The container's pre-spend check: true while a Worker is refreshing THIS stored generation. */
  leaseCheck(provider: string, accountId: string): OAuthRefreshLeaseCheck {
    return this.storage.transactionSync(() => {
      const lease = this.liveLease(provider, accountId);
      if (!lease) return { live: false };
      return { live: lease.generation === this.storedGeneration(provider, accountId), generation: lease.generation };
    });
  }

  /**
   * Commits a rotated credential into the DO's auth.json copy. Three checks in one transaction:
   * the caller's attemptId still owns a live lease, the stored credential still carries the
   * generation the caller refreshed, and the credential being written is not the one already
   * stored (a retried commit must succeed without rotating anything again). Any of them failing
   * leaves the store untouched; a conflict means someone else rotated and the caller re-reads.
   */
  commit(
    provider: string,
    accountId: string,
    credential: RefreshedOAuthCredential,
    expectedGeneration: string,
    attemptId: string,
  ): OAuthRefreshCommitResult {
    if (!validCredential(credential)) throw new Error("oauth refresh commit: malformed credential");
    return this.storage.transactionSync<OAuthRefreshCommitResult>(() => {
      const lease = this.leaseRow(provider, accountId);
      if (!lease || lease.attemptId !== attemptId || this.now() - lease.acquiredAt >= OAUTH_REFRESH_LEASE_TTL_MS) {
        return { conflict: "lease" };
      }
      const stored = this.storage.kv.get<StoredDocument>(AUTH_DOCUMENT_KEY);
      let store: unknown;
      try { store = typeof stored?.body === "string" ? JSON.parse(stored.body) : undefined; } catch { store = undefined; }
      const set = isRec(store) ? store[provider] : undefined;
      const accounts = isRec(set) && Array.isArray(set.accounts) ? set.accounts as unknown[] : undefined;
      const account = accounts?.find((candidate): candidate is Rec => isRec(candidate) && candidate.id === accountId);
      if (!account || !validCredential(account.credential)) return { conflict: "generation" };
      const previous = account.credential;
      const storedGeneration = credentialGenerationOf(previous);
      const generation = credentialGenerationOf(credential);
      // This attempt's commit already landed (e.g. a retried request): report it without rewriting.
      if (storedGeneration === generation) {
        this.storage.sql.exec("DELETE FROM refresh_lease WHERE provider = ? AND accountId = ?", provider, accountId);
        return { conflict: "committed-generation", generation: storedGeneration };
      }
      if (storedGeneration !== expectedGeneration) return { conflict: "generation" };
      // oauth/index.ts's merged(): provider-returned fields win, stored identity metadata survives.
      account.credential = {
        ...credential,
        source: previous.source === "local-cli" ? "oauth" : (credential.source ?? previous.source ?? "oauth"),
        ...(credential.projectId === undefined && previous.projectId ? { projectId: previous.projectId } : {}),
        ...(credential.apiBaseUrl === undefined && previous.apiBaseUrl ? { apiBaseUrl: previous.apiBaseUrl } : {}),
        ...(credential.email === undefined && previous.email ? { email: previous.email } : {}),
        ...(credential.accountId === undefined && previous.accountId ? { accountId: previous.accountId } : {}),
        ...(credential.kiro === undefined && previous.kiro ? { kiro: previous.kiro } : {}),
      };
      // A successful rotation proves the grant is alive, as mergeAccountCredential does.
      delete account.needsReauth;
      const body = `${JSON.stringify(store, null, 2)}\n`;
      if (new TextEncoder().encode(body).byteLength > MAX_DOCUMENT_BYTES) return { conflict: "generation" };
      const seq = (stored?.seq ?? 0) + 1;
      this.storage.kv.put(AUTH_DOCUMENT_KEY, { body, seq } satisfies StoredDocument);
      this.storage.kv.put(AUTH_SEQ_KEY, seq);
      this.storage.sql.exec(
        "INSERT OR REPLACE INTO credential_gen (provider, accountId, counter, generation) " +
        "VALUES (?, ?, COALESCE((SELECT counter FROM credential_gen WHERE provider = ? AND accountId = ?), 0) + 1, ?)",
        provider, accountId, provider, accountId, generation,
      );
      this.storage.sql.exec("DELETE FROM refresh_lease WHERE provider = ? AND accountId = ?", provider, accountId);
      return { ok: true, generation };
    });
  }


  /** Discards all arbitration state with the rest of the hub's saved state (a reset runs while no container is up). */
  discard(): void {
    this.storage.sql.exec("DELETE FROM refresh_lease");
    this.storage.sql.exec("DELETE FROM credential_gen");
  }
  /** Abandons a lease whose refresh definitively failed; the TTL covers every other exit. */
  release(provider: string, accountId: string, attemptId: string): void {
    this.storage.sql.exec(
      "DELETE FROM refresh_lease WHERE provider = ? AND accountId = ? AND attemptId = ?",
      provider, accountId, attemptId,
    );
  }
}

// --- Provider refresh endpoints -----------------------------------------------------------
// Both request shapes copy src/oauth/* because those modules drag node: dependencies the Worker
// bundle cannot take. The deploy layer owns the HTTP so src stays transport-free.

/** Raised when the token endpoint answered with a non-success status: the request reached the
 * provider and was rejected without ambiguity, so the lease is safe to drop early. The marker
 * field is OAUTH_REFRESH_REJECTED_MARK in src/server/cloudflare-native-chat-api.ts. */
export class OAuthRefreshRejectedError extends Error {
  readonly oauthRefreshRejected = true;
  constructor(
    message: string,
    readonly httpStatus: number | undefined,
  ) {
    super(message);
    this.name = "OAuthRefreshRejectedError";
  }
}

async function postForJson(url: string, headers: Record<string, string>, body: string, what: string): Promise<Record<string, unknown>> {
  const response = await fetch(url, {
    method: "POST",
    headers,
    body,
    signal: AbortSignal.timeout(30_000),
  });
  const text = await response.text();
  if (!response.ok) {
    // The status and any OAuth error code travel, the body does not: it can carry request echoes.
    let oauthError = "";
    try {
      const parsed = JSON.parse(text) as { error?: unknown };
      if (typeof parsed.error === "string") oauthError = `: ${parsed.error}`;
    } catch { /* unstructured error bodies stay a bare status */ }
    throw new OAuthRefreshRejectedError(`${what} refresh failed: ${response.status}${oauthError}`, response.status);
  }
  try {
    return JSON.parse(text) as Record<string, unknown>;
  } catch {
    throw new Error(`${what} refresh returned invalid JSON`);
  }
}

// src/oauth/anthropic.ts: refreshAnthropicToken posts JSON grant fields to api.anthropic.com and
// keeps the sent refresh token when the response omits one.
const ANTHROPIC_CLIENT_ID = atob("OWQxYzI1MGEtZTYxYi00NGQ5LTg4ZWQtNTk0NGQxOTYyZjVl");
const ANTHROPIC_TOKEN_URL = "https://api.anthropic.com/v1/oauth/token";

async function refreshAnthropic(refreshToken: string): Promise<RefreshedOAuthCredential> {
  const data = await postForJson(ANTHROPIC_TOKEN_URL, { "Content-Type": "application/json", Accept: "application/json" },
    JSON.stringify({ grant_type: "refresh_token", client_id: ANTHROPIC_CLIENT_ID, refresh_token: refreshToken }),
    "Anthropic OAuth");
  if (typeof data.access_token !== "string" || data.access_token === "") {
    throw new Error("Anthropic OAuth refresh returned no access token");
  }
  // credsFrom's expiry guard: a missing/negative/overflowing expires_in must never stamp a past
  // or non-finite expiry, and Anthropic refresh lands 5 minutes early.
  const expiresIn = typeof data.expires_in === "number" && Number.isFinite(data.expires_in) && data.expires_in >= 0
    ? data.expires_in : 3600;
  const computed = Date.now() + expiresIn * 1000 - 5 * 60 * 1000;
  const account = isRec(data.account) ? data.account : {};
  return {
    access: data.access_token,
    refresh: typeof data.refresh_token === "string" && data.refresh_token !== "" ? data.refresh_token : refreshToken,
    expires: Number.isFinite(computed) ? computed : Date.now() + 3600 * 1000 - 5 * 60 * 1000,
    ...(typeof account.uuid === "string" && account.uuid !== "" ? { accountId: account.uuid } : {}),
    ...(typeof account.email_address === "string" && account.email_address !== "" ? { email: account.email_address } : {}),
  };
}

// src/oauth/chatgpt.ts: refreshChatGPTToken posts form-urlencoded fields to auth.openai.com (the
// chatgpt.com backend is fingerprint-blocked for Workers; the auth host is not).
const CHATGPT_CLIENT_ID = "app_EMoamEEZ73f0CkXaXp7hrann";
const CHATGPT_TOKEN_URL = "https://auth.openai.com/oauth/token";

function decodeJwtPayload(token: string): Rec | undefined {
  const parts = token.split(".");
  if (parts.length !== 3 || !parts[1]) return undefined;
  try {
    return JSON.parse(atob(parts[1].replace(/-/g, "+").replace(/_/g, "/"))) as Rec;
  } catch {
    return undefined;
  }
}

// extractAccountId's precedence: top-level claim, namespaced claim, then organizations[0].
function chatgptAccountId(idToken: unknown, accessToken: string): string | undefined {
  for (const token of [typeof idToken === "string" ? idToken : undefined, accessToken]) {
    if (!token) continue;
    const payload = decodeJwtPayload(token);
    if (!payload) continue;
    if (typeof payload.chatgpt_account_id === "string") return payload.chatgpt_account_id;
    const ns = payload["https://api.openai.com/auth"];
    if (isRec(ns) && typeof ns.chatgpt_account_id === "string") return ns.chatgpt_account_id;
    const orgs = payload.organizations;
    if (Array.isArray(orgs) && isRec(orgs[0]) && typeof orgs[0].id === "string") return orgs[0].id;
  }
  return undefined;
}

async function refreshChatGpt(refreshToken: string): Promise<RefreshedOAuthCredential> {
  const data = await postForJson(CHATGPT_TOKEN_URL, { "Content-Type": "application/x-www-form-urlencoded" },
    new URLSearchParams({ grant_type: "refresh_token", client_id: CHATGPT_CLIENT_ID, refresh_token: refreshToken }).toString(),
    "ChatGPT");
  if (typeof data.access_token !== "string" || data.access_token === "") {
    throw new Error("ChatGPT token response missing access token");
  }
  const expiresIn = typeof data.expires_in === "number" && Number.isFinite(data.expires_in) && data.expires_in >= 0
    ? data.expires_in : 3600;
  const computed = Date.now() + expiresIn * 1000;
  const credential: RefreshedOAuthCredential = {
    access: data.access_token,
    refresh: typeof data.refresh_token === "string" ? data.refresh_token : "",
    expires: Number.isFinite(computed) ? computed : Date.now() + 3600 * 1000,
  };
  const accountId = chatgptAccountId(data.id_token, credential.access);
  if (accountId) credential.accountId = accountId;
  return credential;
}

/** Dispatches on the providers whose refresh endpoints a Worker can reach. */
export function refreshOAuthTokenForProvider(provider: string, refreshToken: string): Promise<RefreshedOAuthCredential> {
  if (provider === "anthropic") return refreshAnthropic(refreshToken);
  if (provider === "chatgpt") return refreshChatGpt(refreshToken);
  return Promise.reject(new OAuthRefreshRejectedError(`oauth refresh is not supported for ${provider} here`, undefined));
}
