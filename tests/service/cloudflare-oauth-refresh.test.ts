import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { upsertOAuthProvider } from "../../src/oauth";
import { saveCredential } from "../../src/oauth/store";
import { gateDurableOAuthRefresh } from "../../src/oauth/durable-refresh-gate";
import { serveNativeResponses } from "../../src/server/cloudflare-native-responses";
import type { NativeOAuthCredential } from "../../src/server/cloudflare-native-chat-api";
import { CLAUDE_CODE_HEADERS, CLAUDE_CODE_RUNTIME_HEADERS } from "../../src/adapters/client-fingerprint";
import { DURABLE_STATE_BOOT_ID_ENV, setDurableMirrorTransportForTests, writeSequenceState } from "../../src/lib/durable-mirror";
import { getConfigDir } from "../../src/config";
import type { OcxConfig } from "../../src/types";
import {
  credentialGenerationOf,
  OAuthRefreshCoordinator,
  OAuthRefreshRejectedError,
  type OAuthRefreshCommitResult,
  type OAuthRefreshStorage,
} from "../../deploy/cloudflare/src/oauth-refresh";
import { DOCUMENT_KEY_PREFIX, type StoredDocument } from "../../deploy/cloudflare/src/lease";
import { installIsolatedCodexHome, type IsolatedCodexHome } from "../helpers/isolated-codex-home";
import { removeTreeWithRetry } from "../helpers/remove-tree";

type Rec = Record<string, unknown>;
const AUTH_DOC_KEY = `${DOCUMENT_KEY_PREFIX}auth`;

/** bun:sqlite + a Map stand in for ctx.storage's sql/kv pair; transactionSync runs unbracketed. */
function syncStorage(): OAuthRefreshStorage & { kvMap: Map<string, unknown>; db: Database } {
  const db = new Database(":memory:");
  const kvMap = new Map<string, unknown>();
  return {
    db,
    kvMap,
    sql: {
      exec: (query: string, ...params: unknown[]) => {
        const statement = db.prepare(query);
        const rows = /^\s*select\b/i.test(query) ? statement.all(...params as never[]) : (statement.run(...params as never[]), [] as unknown[]);
        return { toArray: () => rows as unknown[] };
      },
    },
    kv: {
      get: <T,>(key: string) => kvMap.get(key) as T | undefined,
      put: (key: string, value: unknown) => { kvMap.set(key, value); },
      delete: (key: string) => kvMap.delete(key),
    },
    transactionSync: <T,>(closure: () => T) => closure(),
  };
}

const CREDENTIAL = { access: "access-1", refresh: "refresh-1", expires: 1_000 };
const ROTATED = { access: "access-2", refresh: "refresh-2", expires: 2_000 };

function authDocument(credential: Rec = CREDENTIAL, extra: Rec = {}): StoredDocument {
  return {
    body: JSON.stringify({ anthropic: { activeAccountId: "acct-1", accounts: [{ id: "acct-1", credential, ...extra }] } }, null, 2),
    seq: 7,
  };
}

function acquire(arbiter: OAuthRefreshCoordinator, generation: string): string {
  const lease = arbiter.acquire("anthropic", "acct-1", generation);
  if (!("attemptId" in lease)) throw new Error("expected a lease");
  return lease.attemptId;
}

describe("the Durable Object's OAuth refresh arbiter", () => {
  test("one live lease per stored generation, freed by TTL and by a commit elsewhere", () => {
    let now = 10_000;
    const storage = syncStorage();
    storage.kvMap.set(AUTH_DOC_KEY, authDocument());
    const arbiter = new OAuthRefreshCoordinator(storage, () => now);
    const generation = credentialGenerationOf(CREDENTIAL);

    acquire(arbiter, generation);
    // A second caller for the same stored generation loses: spending would be a second spend.
    expect(arbiter.acquire("anthropic", "acct-1", generation)).toEqual({ busy: true });
    // A TTL-expired lease frees the key even though the row was never released.
    now += 31_000;
    expect(arbiter.acquire("anthropic", "acct-1", generation)).toHaveProperty("attemptId");
    // The store moving past the lease's generation frees the row: that lease can no longer
    // fence the credential it was guarding.
    now = 10_000;
    arbiter.acquire("anthropic", "acct-1", generation);
    storage.kvMap.set(AUTH_DOC_KEY, authDocument(ROTATED));
    expect(arbiter.acquire("anthropic", "acct-1", credentialGenerationOf(ROTATED))).toHaveProperty("attemptId");
  });

  test("commit rotates the stored credential under CAS, bumps the seq, frees the lease", () => {
    const storage = syncStorage();
    storage.kvMap.set(AUTH_DOC_KEY, authDocument({ ...CREDENTIAL, email: "user@example.test" }, { needsReauth: true }));
    const arbiter = new OAuthRefreshCoordinator(storage);
    const generation = credentialGenerationOf(CREDENTIAL);
    const attemptId = acquire(arbiter, generation);

    // The lease's owner check comes before the generation check: a wrong attemptId writes nothing.
    expect(arbiter.commit("anthropic", "acct-1", ROTATED, generation, "not-the-attempt")).toEqual({ conflict: "lease" });
    expect(arbiter.commit("anthropic", "acct-1", ROTATED, "some-other-generation", attemptId)).toEqual({ conflict: "generation" });

    const committed = arbiter.commit("anthropic", "acct-1", ROTATED, generation, attemptId);
    expect(committed).toEqual({ ok: true, generation: credentialGenerationOf(ROTATED) });
    const stored = storage.kvMap.get(AUTH_DOC_KEY) as StoredDocument;
    expect(stored.seq).toBe(8);
    const accounts = ((JSON.parse(stored.body) as Rec).anthropic as Rec).accounts as Rec[];
    // The provider fields win, stored identity survives (oauth/index.ts's merged()), and a proven
    // rotation clears needsReauth.
    expect(accounts[0]!.credential).toMatchObject({ ...ROTATED, email: "user@example.test", source: "oauth" });
    expect(accounts[0]!.needsReauth).toBeUndefined();
    // The lease row is gone, the generation counter advanced, and a second commit is a conflict.
    expect(arbiter.leaseCheck("anthropic", "acct-1")).toEqual({ live: false });
    expect(storage.db.prepare("SELECT counter, generation FROM credential_gen").get()).toMatchObject({ counter: 1 });
    expect(arbiter.commit("anthropic", "acct-1", ROTATED, generation, attemptId)).toEqual({ conflict: "lease" });
  });

  test("leaseCheck reports a live lease only while it guards the stored generation", () => {
    const storage = syncStorage();
    storage.kvMap.set(AUTH_DOC_KEY, authDocument());
    const arbiter = new OAuthRefreshCoordinator(storage);
    expect(arbiter.leaseCheck("anthropic", "acct-1")).toEqual({ live: false });
    acquire(arbiter, credentialGenerationOf(CREDENTIAL));
    expect(arbiter.leaseCheck("anthropic", "acct-1")).toMatchObject({ live: true });
    storage.kvMap.set(AUTH_DOC_KEY, authDocument(ROTATED));
    expect(arbiter.leaseCheck("anthropic", "acct-1")).toMatchObject({ live: false });
  });

  test("release abandons only the owning attempt", () => {
    const storage = syncStorage();
    storage.kvMap.set(AUTH_DOC_KEY, authDocument());
    const arbiter = new OAuthRefreshCoordinator(storage);
    const generation = credentialGenerationOf(CREDENTIAL);
    const attemptId = acquire(arbiter, generation);
    arbiter.release("anthropic", "acct-1", "somebody-else");
    expect(arbiter.leaseCheck("anthropic", "acct-1").live).toBe(true);
    arbiter.release("anthropic", "acct-1", attemptId);
    expect(arbiter.leaseCheck("anthropic", "acct-1")).toEqual({ live: false });
  });
});

// --- Worker lane: a token inside its refresh window rotates through the arbiter --------------

const sse = (events: [string, unknown][]) => new Response(
  events.map(([event, data]) => `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`).join(""),
  { headers: { "content-type": "text/event-stream" } },
);
const reply = () => sse([
  ["message_start", { type: "message_start", message: { id: "msg_1", type: "message", role: "assistant", model: "claude-opus-5-5", content: [], usage: { input_tokens: 30, output_tokens: 1 } } }],
  ["content_block_start", { type: "content_block_start", index: 0, content_block: { type: "tool_use", id: "toolu_1", name: "custom_shell", input: {} } }],
  ["content_block_delta", { type: "content_block_delta", index: 0, delta: { type: "input_json_delta", partial_json: "{\"cmd\":\"ls\"}" } }],
  ["content_block_stop", { type: "content_block_stop", index: 0 }],
  ["message_delta", { type: "message_delta", delta: { stop_reason: "tool_use" }, usage: { output_tokens: 7 } }],
  ["message_stop", { type: "message_stop" }],
]);
const shell = { type: "function", name: "shell", description: "Run a command", parameters: { type: "object", properties: { cmd: { type: "string" } }, required: ["cmd"] } };
const responsesTurn = (): Rec => ({
  model: "anthropic/claude-opus-5-5", instructions: "You are a coding agent.", store: false, stream: true, tools: [shell],
  input: [{ type: "message", role: "user", content: [{ type: "input_text", text: "List files." }] }], reasoning: { effort: "high" },
});
const containerRuntime = Object.fromEntries(CLAUDE_CODE_RUNTIME_HEADERS.map(name => [name, CLAUDE_CODE_HEADERS[name]!]));

type Arbiter = {
  acquire(provider: string, accountId: string, generation: string): Promise<{ attemptId: string } | null>;
  commit(provider: string, accountId: string, credential: NativeOAuthCredential, expectedGeneration: string, attemptId: string): Promise<OAuthRefreshCommitResult>;
  release(provider: string, accountId: string, attemptId: string): Promise<void>;
  refreshToken(provider: string, refreshToken: string): Promise<NativeOAuthCredential>;
};

describe("a Worker-native turn on a token inside its refresh window", () => {
  let home = "";
  let codexHome: IsolatedCodexHome | null = null;
  const previousHome = process.env.OPENCODEX_HOME;
  // The row `ocx login anthropic` writes.
  const row = (() => {
    const config = { providers: {} } as unknown as OcxConfig;
    upsertOAuthProvider(config, "anthropic");
    return config.providers.anthropic as unknown as Rec;
  })();

  beforeEach(() => {
    codexHome = installIsolatedCodexHome("ocx-oauth-refresh-");
    home = mkdtempSync(join(tmpdir(), "ocx-oauth-refresh-"));
    process.env.OPENCODEX_HOME = home;
  });
  afterEach(() => {
    if (previousHome === undefined) delete process.env.OPENCODEX_HOME;
    else process.env.OPENCODEX_HOME = previousHome;
    codexHome?.restore();
    codexHome = null;
    removeTreeWithRetry(home);
  });

  /** auth.json holding one Anthropic login whose access token is inside the refresh window. */
  async function seedExpiringLogin(): Promise<{ authText: string; accountId: string }> {
    await saveCredential("anthropic", {
      access: "stale-access", refresh: "refresh-1", expires: Date.now() + 30_000,
    } as never);
    const authText = readFileSync(join(home, "auth.json"), "utf8");
    const set = (JSON.parse(authText) as Rec).anthropic as Rec;
    return { authText, accountId: set.activeAccountId as string };
  }

  /** The dep over the real coordinator, so the CAS semantics under test are the arbiter's own. */
  function arbiterDeps(authText: string, refreshToken: (provider: string, token: string) => Promise<NativeOAuthCredential>) {
    const storage = syncStorage();
    storage.kvMap.set(AUTH_DOC_KEY, { body: authText, seq: 3 });
    const coordinator = new OAuthRefreshCoordinator(storage);
    const oauthRefresh: Arbiter = {
      acquire: async (provider, accountId, generation) => {
        const result = coordinator.acquire(provider, accountId, generation);
        return "busy" in result ? null : result;
      },
      commit: async (provider, accountId, credential, expectedGeneration, attemptId) =>
        coordinator.commit(provider, accountId, credential, expectedGeneration, attemptId),
      release: async (provider, accountId, attemptId) => { coordinator.release(provider, accountId, attemptId); },
      refreshToken,
    };
    return { storage, coordinator, oauthRefresh };
  }

  async function throughWorker(authText: string, arbiter: { storage?: ReturnType<typeof syncStorage>; oauthRefresh?: Arbiter }) {
    const declines: string[] = [];
    let authorization = "";
    const response = await serveNativeResponses(JSON.stringify(responsesTurn()), new Headers({ "user-agent": "test/1" }), new AbortController().signal, {
      readConfig: async () => JSON.stringify({ providers: { anthropic: { ...row, baseUrl: "https://claude.example.test" } } }),
      // A re-read sees the arbiter's copy, so a committed or externally rotated credential wins.
      readAuth: async () => (arbiter.storage?.kvMap.get(AUTH_DOC_KEY) as StoredDocument | undefined)?.body ?? authText,
      clientRuntime: async () => containerRuntime,
      fetch: async request => { authorization = request.headers.get("authorization") ?? ""; return reply(); },
      oauthRefresh: arbiter.oauthRefresh,
      onDecline: reason => declines.push(reason),
    });
    return { declines, authorization, status: response?.status };
  }

  test("rotates through the lease and serves with the new access token", async () => {
    const { authText } = await seedExpiringLogin();
    const spends: string[] = [];
    const arbiter = arbiterDeps(authText, async (_provider, token) => {
      spends.push(token);
      return { access: "fresh-access", refresh: "refresh-2", expires: Date.now() + 3_600_000 };
    });
    const turn = await throughWorker(authText, arbiter);
    expect(turn.declines).toEqual([]);
    expect(spends).toEqual(["refresh-1"]);
    expect(turn.authorization).toBe("Bearer fresh-access");
    const stored = arbiter.storage.kvMap.get(AUTH_DOC_KEY) as StoredDocument;
    expect(JSON.parse(stored.body)).toMatchObject({ anthropic: { accounts: [{ credential: { refresh: "refresh-2", access: "fresh-access" } }] } });
    expect(stored.seq).toBe(4);
    expect(arbiter.storage.db.prepare("SELECT counter FROM credential_gen").get()).toMatchObject({ counter: 1 });
  });

  test("one generation is spent once: a racing turn declines instead of refreshing again", async () => {
    const { authText } = await seedExpiringLogin();
    let releaseFetch: (() => void) | undefined;
    const pending = new Promise<void>(resolve => { releaseFetch = resolve; });
    const spends: string[] = [];
    const arbiter = arbiterDeps(authText, async (_provider, token) => {
      spends.push(token);
      await pending; // hold the lease while the second turn arrives
      return { access: "fresh-access", refresh: "refresh-2", expires: Date.now() + 3_600_000 };
    });
    const first = throughWorker(authText, arbiter);
    const second = await throughWorker(authText, arbiter);
    releaseFetch!();
    const firstResult = await first;
    expect(firstResult.authorization).toBe("Bearer fresh-access");
    expect(second.declines).toEqual(["responses:oauth-refresh-due"]);
    expect(spends).toEqual(["refresh-1"]);
  });

  test("a commit conflict adopts whoever rotated instead of spending again", async () => {
    const { authText, accountId } = await seedExpiringLogin();
    const spends: string[] = [];
    const arbiter = arbiterDeps(authText, async (_provider, token) => {
      spends.push(token);
      return { access: "fresh-access", refresh: "refresh-2", expires: Date.now() + 3_600_000 };
    });
    // The container rotated between our lease and our commit: the CAS fails and the re-read
    // adopts its credential rather than overwrite it.
    const realCommit = arbiter.oauthRefresh.commit;
    let rotatedElsewhere = false;
    arbiter.oauthRefresh.commit = (provider, account, credential, expectedGeneration, attemptId) => {
      if (rotatedElsewhere) return realCommit(provider, account, credential, expectedGeneration, attemptId);
      rotatedElsewhere = true;
      arbiter.storage.kvMap.set(AUTH_DOC_KEY, {
        body: JSON.stringify({ anthropic: { activeAccountId: accountId, accounts: [{ id: accountId, credential: { access: "adopted-access", refresh: "refresh-3", expires: Date.now() + 3_600_000 } }] } }),
        seq: 4,
      });
      return Promise.resolve({ conflict: "generation" as const });
    };
    const turn = await throughWorker(authText, arbiter);
    expect(turn.declines).toEqual([]);
    expect(turn.authorization).toBe("Bearer adopted-access");
    expect(spends).toEqual(["refresh-1"]);
  });

  test("a busy lease leaves the turn to ocx without spending", async () => {
    const { authText } = await seedExpiringLogin();
    const spends: string[] = [];
    const turn = await throughWorker(authText, {
      oauthRefresh: {
        acquire: async () => null,
        commit: async () => ({ ok: true, generation: "unreached" }),
        release: async () => {},
        refreshToken: async (_provider, token) => { spends.push(token); throw new Error("must not spend"); },
      },
    });
    expect(turn.declines).toEqual(["responses:oauth-refresh-due"]);
    expect(spends).toEqual([]);
  });

  test("a definitive provider rejection releases the lease and still declines", async () => {
    const { authText } = await seedExpiringLogin();
    const released: string[] = [];
    const arbiter = arbiterDeps(authText, async () => { throw new OAuthRefreshRejectedError("invalid_grant", 400); });
    const wrapped: Arbiter = {
      ...arbiter.oauthRefresh,
      release: async (provider, accountId, attemptId) => {
        released.push(attemptId);
        await arbiter.oauthRefresh.release(provider, accountId, attemptId);
      },
    };
    const turn = await throughWorker(authText, { storage: arbiter.storage, oauthRefresh: wrapped });
    expect(turn.declines).toEqual(["responses:oauth-refresh-due"]);
    expect(released).toHaveLength(1);
  });
});

// --- Container side: the gate in front of its own refresh spend ------------------------------

describe("the container's durable refresh gate", () => {
  const BOOT_ID = "0123456789abcdef0123456789abcdef";
  let home = "";
  let codexHome: IsolatedCodexHome | null = null;
  const previousHome = process.env.OPENCODEX_HOME;
  const savedBoot = process.env[DURABLE_STATE_BOOT_ID_ENV];
  const credential = { access: "stale-access", refresh: "refresh-1", expires: Date.now() + 30_000 };
  const generation = credentialGenerationOf(credential);

  beforeEach(async () => {
    codexHome = installIsolatedCodexHome("ocx-oauth-gate-");
    home = mkdtempSync(join(tmpdir(), "ocx-oauth-gate-"));
    process.env.OPENCODEX_HOME = home;
    process.env[DURABLE_STATE_BOOT_ID_ENV] = BOOT_ID;
    await saveCredential("anthropic", credential as never);
  });
  afterEach(() => {
    setDurableMirrorTransportForTests(null);
    if (previousHome === undefined) delete process.env.OPENCODEX_HOME;
    else process.env.OPENCODEX_HOME = previousHome;
    if (savedBoot === undefined) delete process.env[DURABLE_STATE_BOOT_ID_ENV];
    else process.env[DURABLE_STATE_BOOT_ID_ENV] = savedBoot;
    codexHome?.restore();
    codexHome = null;
    removeTreeWithRetry(home);
  });

  function transport(leaseLive: boolean, document?: { body: string; seq: number }) {
    setDurableMirrorTransportForTests({
      origin: "http://state.ocx.internal", sleep: async () => {}, schedule: () => ({ cancel: () => {} }),
      fetch: async url => {
        if (url.endsWith("/oauth-refresh/lease-check")) return Response.json({ live: leaseLive });
        if (url.endsWith("/documents/auth")) {
          return document
            ? new Response(document.body, { headers: { "content-type": "application/json", "x-ocx-document-seq": String(document.seq) } })
            : new Response("none", { status: 404 });
        }
        return new Response("unexpected", { status: 500 });
      },
    });
  }

  test("is local without a live lease", async () => {
    transport(false);
    expect(await gateDurableOAuthRefresh("anthropic", "acct", generation)).toEqual({ kind: "local" });
  });

  test("adopts the hub's rotated document instead of spending the token again", async () => {
    const rotated = { access: "fresh-access", refresh: "refresh-2", expires: Date.now() + 3_600_000 };
    const body = JSON.stringify({ anthropic: { activeAccountId: "acct", accounts: [{ id: "acct", credential: rotated }] } });
    transport(true, { body, seq: 11 });
    const outcome = await gateDurableOAuthRefresh("anthropic", "acct", generation, async () => {});
    expect(outcome).toEqual({ kind: "adopted", credential: rotated });
    // The local file now holds the hub's bytes, so the next store read sees the rotation, and the
    // sequence file continues numbering above the adopted document.
    expect(JSON.parse(readFileSync(join(home, "auth.json"), "utf8"))).toMatchObject({ anthropic: { accounts: [{ credential: rotated }] } });
    expect(readFileSync(join(home, "auth.json.seq"), "utf8")).toContain('"seq":11');
  });

  test("blocks rather than double-spend when a live lease never commits", async () => {
    const document = { body: readFileSync(join(home, "auth.json"), "utf8"), seq: 5 };
    transport(true, document);
    const accountId = (JSON.parse(document.body) as Rec).anthropic as Rec;
    const id = accountId.activeAccountId as string;
    let now = 50_000;
    const outcome = await gateDurableOAuthRefresh("anthropic", id, generation, async () => {}, () => (now += 2_000));
    expect(outcome).toEqual({ kind: "blocked" });
  });

  test("adopts a committed rotation even after the lease is gone (the post-commit window)", async () => {
    const rotated = { access: "fresh-access", refresh: "refresh-2", expires: Date.now() + 3_600_000 };
    const body = JSON.stringify({ anthropic: { activeAccountId: "acct", accounts: [{ id: "acct", credential: rotated }] } });
    transport(false, { body, seq: 11 });
    const outcome = await gateDurableOAuthRefresh("anthropic", "acct", generation, async () => {});
    expect(outcome).toEqual({ kind: "adopted", credential: rotated });
  });

  test("does not adopt a document behind the local sequence (the DO is the stale side)", async () => {
    // The container's own commit is already mirrored at seq 12; an older DO copy must not regress it.
    const local = readFileSync(join(home, "auth.json"), "utf8");
    writeSequenceState(join(getConfigDir(), "auth.json.seq"), { seq: 12, mirrored: true });
    transport(false, { body: JSON.stringify({ anthropic: { activeAccountId: "acct", accounts: [{ id: "acct", credential: { access: "old", refresh: "old", expires: 9 } }] } }), seq: 11 });
    const outcome = await gateDurableOAuthRefresh("anthropic", "acct", generation, async () => {});
    expect(outcome).toEqual({ kind: "local" });
    expect(readFileSync(join(home, "auth.json"), "utf8")).toBe(local);
  });
});
