import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { saveConfig } from "../../src/config";
import { startServer } from "../../src/server";
import { upsertOAuthProvider } from "../../src/oauth";
import { saveCredential } from "../../src/oauth/store";
import { CLAUDE_CODE_HEADERS, CLAUDE_CODE_RUNTIME_HEADERS } from "../../src/adapters/client-fingerprint";
import { readRecentUsageEntries } from "../../src/usage/log";
import { serveNativeMessages } from "../../src/server/cloudflare-native-messages";
import { serveNativeResponses } from "../../src/server/cloudflare-native-responses";
import type { OcxConfig } from "../../src/types";
import { DURABLE_STATE_BOOT_ID_ENV, setDurableMirrorTransportForTests } from "../../src/lib/durable-mirror";
import { publishClientRuntimeForWorker } from "../../src/server/worker-native-state";
import { LeaseState, type LeaseStorage } from "../../deploy/cloudflare/src/lease";
import { handleStateRequest, type StateBucket } from "../../deploy/cloudflare/src/state-routes";
import { installIsolatedCodexHome, type IsolatedCodexHome } from "../helpers/isolated-codex-home";
import { removeTreeWithRetry } from "../helpers/remove-tree";

type Rec = Record<string, unknown>;
const ACCESS = ["claude", "oauth", "access"].join("-");
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
const responsesTurn = (extra: Rec = {}): Rec => ({
  model: "anthropic/claude-opus-5-5", instructions: "You are a coding agent.", store: false, stream: true, tools: [shell],
  input: [{ type: "message", role: "user", content: [{ type: "input_text", text: "List files." }] }], reasoning: { effort: "high" }, ...extra,
});
const messagesTurn = (): Rec => ({
  model: "ocx-claude-anthropic--claude-opus-5-5", max_tokens: 4000, stream: true,
  metadata: { user_id: JSON.stringify({ device_id: "d", account_uuid: "", session_id: "0f0e0d0c-0b0a-4908-8706-050403020100" }) },
  system: [{ type: "text", text: "You are Claude Code." }], thinking: { type: "adaptive" },
  tools: [{ name: "shell", description: "Run a command", input_schema: shell.parameters }],
  messages: [{ role: "user", content: "List files." }],
});
const normalize = (text: string) => text
  .replace(/"(resp|msg|fc|rs|item)_[A-Za-z0-9_-]+"/g, "\"$1_ID\"")
  .replace(/"(created_at|completed_at)":\d+/g, "\"$1\":0");
// Transport headers, Bun's fetch defaults, and the one header ocx mints at random per request.
const sentHeaders = (headers: Headers) => Object.fromEntries([...headers]
  .filter(([name, value]) => !["host", "content-length", "accept-encoding", "connection", "x-client-request-id"].includes(name)
    && !(name === "accept" && value === "*/*") && !(name === "user-agent" && value.startsWith("Bun/")))
  .sort());

function memoryStorage(): LeaseStorage {
  const map = new Map<string, unknown>();
  return {
    get: async <T>(key: string) => map.get(key) as T | undefined,
    put: async (key, value) => { map.set(key, value); },
    delete: async key => map.delete(key),
    list: async <T>({ prefix, limit }: { prefix: string; limit: number }) =>
      new Map([...map].filter(([key]) => key.startsWith(prefix)).slice(0, limit)) as Map<string, T>,
  };
}

describe("the container's Claude Code runtime, as the Worker reads it", () => {
  const BOOT_ID = "0123456789abcdef0123456789abcdef";
  const noBucket: StateBucket = { get: async () => null, put: async () => {}, delete: async () => {}, list: async () => [] };
  const saved = { bootId: process.env[DURABLE_STATE_BOOT_ID_ENV], flag: process.env.OCX_WORKER_NATIVE_STATE };
  afterEach(() => {
    setDurableMirrorTransportForTests(null);
    for (const [name, value] of [[DURABLE_STATE_BOOT_ID_ENV, saved.bootId], ["OCX_WORKER_NATIVE_STATE", saved.flag]] as const) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
  });

  async function publish(flag: string | undefined) {
    const hub = new LeaseState(memoryStorage());
    await hub.acquireLease(BOOT_ID);
    const puts: Promise<Response>[] = [];
    process.env[DURABLE_STATE_BOOT_ID_ENV] = BOOT_ID;
    if (flag === undefined) delete process.env.OCX_WORKER_NATIVE_STATE;
    else process.env.OCX_WORKER_NATIVE_STATE = flag;
    setDurableMirrorTransportForTests({
      origin: "http://state.ocx.internal", sleep: async () => {}, schedule: () => ({ cancel: () => {} }),
      fetch: async (url, init) => {
        const response = handleStateRequest(new Request(url, init), hub, noBucket, "ns", "stamp-1");
        puts.push(response);
        return response;
      },
    });
    await publishClientRuntimeForWorker();
    await Promise.all(puts);
    return { hub, puts };
  }

  test("is this process's runtime, under the stamp of the deployment it was published to", async () => {
    const { hub } = await publish("1");
    const expected = Object.fromEntries(CLAUDE_CODE_RUNTIME_HEADERS.map(name => [name, CLAUDE_CODE_HEADERS[name]!]));
    expect(await hub.clientRuntimeRead("stamp-1")).toEqual(expected);
    // A new Worker version or container environment may run another runtime.
    expect(await hub.clientRuntimeRead("stamp-2")).toBeUndefined();
    // Only the newest publish counts: a Worker version other than the hub's finds none of its own.
    await hub.clientRuntimeCommit(BOOT_ID, expected, "stamp-2");
    expect(await hub.clientRuntimeRead("stamp-1")).toBeUndefined();
    expect(await hub.clientRuntimeRead("stamp-2")).toEqual(expected);
  });

  test("is not published where the Worker does not serve requests, and the route takes only those headers", async () => {
    expect((await publish(undefined)).puts).toHaveLength(0);
    const hub = new LeaseState(memoryStorage());
    await hub.acquireLease(BOOT_ID);
    const put = (body: unknown) => handleStateRequest(new Request("http://state.ocx.internal/client-runtime", {
      method: "PUT", headers: { "x-ocx-boot-id": BOOT_ID }, body: JSON.stringify(body),
    }), hub, noBucket, "ns", "stamp-1");
    expect((await put({ "X-Stainless-Arch": "x64", "X-Stainless-OS": "linux", "X-Stainless-Runtime-Version": "24.3.0", Authorization: "x" })).status).toBe(400);
    expect((await put({ "X-Stainless-Arch": "x64", "X-Stainless-OS": "linux", "X-Stainless-Runtime-Version": "24.3.0\r\nX: y" })).status).toBe(400);
    expect((await put({ "X-Stainless-Arch": "x64", "X-Stainless-OS": "linux", "X-Stainless-Runtime-Version": "24.3.0" })).status).toBe(204);
  });
});

describe("Worker-native turns on an Anthropic OAuth login", () => {
  let home = "";
  let codexHome: IsolatedCodexHome | null = null;
  const previousHome = process.env.OPENCODEX_HOME;
  beforeEach(async () => {
    codexHome = installIsolatedCodexHome("ocx-worker-oauth-");
    home = mkdtempSync(join(tmpdir(), "ocx-worker-oauth-"));
    process.env.OPENCODEX_HOME = home;
    await saveCredential("anthropic", { access: ACCESS, refresh: "refresh-token", expires: Date.now() + 3_600_000 } as never);
  });
  afterEach(() => {
    if (previousHome === undefined) delete process.env.OPENCODEX_HOME;
    else process.env.OPENCODEX_HOME = previousHome;
    codexHome?.restore();
    codexHome = null;
    removeTreeWithRetry(home);
  });

  // The row `ocx login anthropic` writes.
  const row = (() => {
    const config = { providers: {} } as unknown as OcxConfig;
    upsertOAuthProvider(config, "anthropic");
    return config.providers.anthropic as unknown as Rec;
  })();
  // What the container's ocx publishes: its own runtime, the same process here.
  const containerRuntime = Object.fromEntries(CLAUDE_CODE_RUNTIME_HEADERS.map(name => [name, CLAUDE_CODE_HEADERS[name]!]));
  const authText = () => readFileSync(join(home, "auth.json"), "utf8");

  async function throughOcx(path: string, body: Rec) {
    let sent: { body: Rec; headers: Record<string, string> } | undefined;
    const upstream = Bun.serve({ port: 0, async fetch(req) { sent = { body: await req.json() as Rec, headers: sentHeaders(req.headers) }; return reply(); } });
    saveConfig({ port: 0, providers: { anthropic: { ...row, baseUrl: upstream.url.toString().replace(/\/$/, ""), allowPrivateNetwork: true } } } as unknown as OcxConfig);
    const server = startServer(0);
    try {
      const response = await fetch(`http://127.0.0.1:${server.port}${path}`, {
        method: "POST", headers: { "content-type": "application/json", "user-agent": "test/1" }, body: JSON.stringify(body),
      });
      const text = await response.text();
      await Bun.sleep(100);
      return { sent: sent!, status: response.status, text, row: readRecentUsageEntries(5).at(-1) as unknown as Rec };
    } finally {
      await server.stop(true);
      await upstream.stop(true);
    }
  }

  async function throughWorker(path: string, body: Rec, auth: string, providerRow: Rec = row, clientRuntime: Record<string, string> | null = containerRuntime) {
    let sent: { body: Rec; headers: Record<string, string> } | undefined;
    const declines: string[] = [];
    const rows: Rec[] = [];
    const serve = path === "/v1/messages" ? serveNativeMessages : serveNativeResponses;
    const response = await serve(JSON.stringify(body), new Headers({ "user-agent": "test/1" }), new AbortController().signal, {
      // A host other than api.anthropic.com, as ocx's local upstream is: the adapter adds automatic
      // cache control only for Anthropic's own host.
      readConfig: async () => JSON.stringify({ providers: { anthropic: { ...providerRow, baseUrl: "https://claude.example.test" } } }),
      readAuth: async () => auth,
      clientRuntime: async () => clientRuntime ?? undefined,
      fetch: async request => { sent = { body: await request.json() as Rec, headers: sentHeaders(request.headers) }; return reply(); },
      onDecline: reason => declines.push(reason),
      recordUsage: usageRow => rows.push(usageRow as Rec),
    });
    return { sent, declines, rows, status: response?.status, text: await response?.text() };
  }

  for (const [name, path, body] of [
    ["a Codex turn", "/v1/responses", responsesTurn()],
    ["a Claude Code turn", "/v1/messages", messagesTurn()],
  ] as const) {
    test(`${name}: the same upstream request, client bytes and usage label as ocx`, async () => {
      const proxy = await throughOcx(path, body as Rec);
      const worker = await throughWorker(path, body as Rec, authText());
      expect(worker.declines).toEqual([]);
      expect(worker.sent!.body).toEqual(proxy.sent.body);
      expect(worker.sent!.headers).toEqual(proxy.sent.headers);
      expect(proxy.sent.headers.authorization).toBe(`Bearer ${ACCESS}`);
      expect(worker.status).toBe(proxy.status);
      expect(normalize(worker.text!)).toBe(normalize(proxy.text));
      // ocx labels no Anthropic OAuth account (providers/label.ts), and names the wire model.
      for (const field of ["provider", "model", "requestedModel", "resolvedModel", "wireModel", "requestedEffort", "accountLogLabel", "surface", "status", "usageStatus"]) {
        expect([field, worker.rows[0]![field]]).toEqual([field, proxy.row[field]]);
      }
    });
  }

  test("leaves to ocx a token ocx would refresh first, a second account, and a row it does not reproduce", async () => {
    const auth = JSON.parse(authText()) as { anthropic: { accounts: Rec[]; activeAccountId: string } };
    const account = auth.anthropic.accounts[0]!;
    const soon = structuredClone(auth);
    (soon.anthropic.accounts[0]!.credential as Rec).expires = Date.now() + 30_000;
    expect((await throughWorker("/v1/responses", responsesTurn(), JSON.stringify(soon))).declines).toEqual(["responses:oauth-refresh-due"]);
    const two = structuredClone(auth);
    two.anthropic.accounts.push({ ...account, id: "second" });
    expect((await throughWorker("/v1/responses", responsesTurn(), JSON.stringify(two))).declines).toEqual(["responses:oauth-account-pool"]);
    const reauth = structuredClone(auth);
    reauth.anthropic.accounts[0]!.needsReauth = true;
    expect((await throughWorker("/v1/responses", responsesTurn(), JSON.stringify(reauth))).declines).toEqual(["responses:oauth-needs-reauth"]);
    // oauth/store.ts drops an account without a refresh token, so ocx has no login at all.
    const noRefresh = structuredClone(auth);
    delete (noRefresh.anthropic.accounts[0]!.credential as Rec).refresh;
    expect((await throughWorker("/v1/responses", responsesTurn(), JSON.stringify(noRefresh))).declines).toEqual(["responses:oauth-account-shape"]);
    expect((await throughWorker("/v1/responses", responsesTurn(), authText(), { ...row, fastEnabled: true })).declines).toEqual(["responses:provider-field:fastEnabled"]);
    // A login field the operator changed is no longer the preset the Worker reproduces.
    expect((await throughWorker("/v1/responses", responsesTurn(), authText(), { ...row, defaultMaxOutputTokens: 1000 })).declines).toEqual(["responses:provider-field:defaultMaxOutputTokens"]);
    expect((await throughWorker("/v1/responses", responsesTurn(), authText(), { ...row, models: ["claude-opus-5-5"] })).declines).toEqual(["responses:provider-field:models"]);
    expect((await throughWorker("/v1/responses", responsesTurn(), "")).declines).toEqual(["responses:oauth-no-login"]);
    // Until this deployment's ocx has said what runtime it reports, the Worker cannot send as it.
    expect((await throughWorker("/v1/responses", responsesTurn(), authText(), row, null)).declines).toEqual(["responses:oauth-client-runtime-unpublished"]);
    // The HTTP chat lane is a passthrough of the caller's body; OAuth stays ocx's there.
    const { serveNativeChat } = await import("../../src/server/cloudflare-native-chat");
    const declines: string[] = [];
    await serveNativeChat(JSON.stringify({ model: "anthropic/claude-opus-5-5", messages: [{ role: "user", content: "hi" }] }), new Headers(), new AbortController().signal, {
      readConfig: async () => JSON.stringify({ providers: { anthropic: row } }), readAuth: async () => authText(), fetch: async () => reply(), onDecline: reason => declines.push(reason),
    });
    expect(declines).toEqual(["built-in-provider"]);
  });

  test("sends the Claude Code runtime headers the container published, not its own", async () => {
    const published = { "X-Stainless-Arch": "x64", "X-Stainless-OS": "linux", "X-Stainless-Runtime-Version": "24.3.0" };
    const worker = await throughWorker("/v1/responses", responsesTurn(), authText(), row, published);
    expect(worker.declines).toEqual([]);
    for (const [name, value] of Object.entries(published)) expect(worker.sent!.headers[name.toLowerCase()]).toBe(value);
  });

  test("leaves to ocx a turn replaying signed thinking, whose fate turns on ocx's serving identity", async () => {
    const signed = responsesTurn({ input: [
      { type: "message", role: "user", content: [{ type: "input_text", text: "List files." }] },
      { type: "reasoning", id: "rs_1", summary: [{ type: "summary_text", text: "Thinking." }], encrypted_content: `ocxr1:${Buffer.from(JSON.stringify({ sig: "A".repeat(64), txt: "Thinking." })).toString("base64")}` },
      { type: "message", role: "assistant", content: [{ type: "output_text", text: "Sure." }] },
      { type: "message", role: "user", content: [{ type: "input_text", text: "Again." }] },
    ] });
    expect((await throughWorker("/v1/responses", signed, authText())).declines).toEqual(["responses:reasoning-replay-state"]);
  });

  test("an input ocx refuses as far past the login's context window is left to ocx", async () => {
    // claude-haiku-4-5 has a 200k window in the login row; ocx refuses past 2.5 times it.
    const huge = responsesTurn({ model: "anthropic/claude-haiku-4-5", input: [{ type: "message", role: "user", content: [{ type: "input_text", text: "lorem ipsum dolor ".repeat(200_000) }] }] });
    const proxy = await throughOcx("/v1/responses", huge);
    // ocx refuses before sending, in the stream the client asked for.
    expect(proxy.sent).toBeUndefined();
    expect(proxy.text).toContain("\"code\":\"context_length_exceeded\"");
    expect((await throughWorker("/v1/responses", huge, authText())).declines).toEqual(["responses:input-admission"]);
  });

  test("serves a login row as ocx persists it after startup, with an older default and picker state", async () => {
    // Logins before the claude-sonnet-5 default kept claude-sonnet-4-6, which startup leaves while it is listed.
    saveConfig({ port: 0, providers: { anthropic: { ...row, defaultModel: "claude-sonnet-4-6", selectedModels: ["claude-opus-5-5"] } } } as unknown as OcxConfig);
    const server = startServer(0);
    await server.stop(true);
    const persisted = JSON.parse(readFileSync(join(home, "config.json"), "utf8")) as Rec;
    expect((persisted.providers as Rec).anthropic).toMatchObject({ defaultModel: "claude-sonnet-4-6" });
    const declines: string[] = [];
    await serveNativeResponses(JSON.stringify(responsesTurn()), new Headers(), new AbortController().signal, {
      readConfig: async () => JSON.stringify(persisted), readAuth: async () => authText(), clientRuntime: async () => containerRuntime,
      fetch: async () => reply(), onDecline: reason => declines.push(reason),
    });
    expect(declines).toEqual([]);
  });

  test("a field the login does not write is declined, whatever its name", async () => {
    const proto = JSON.parse(`{"__proto__":{},${JSON.stringify(row).slice(1)}`) as Rec;
    expect(Object.hasOwn(proto, "__proto__")).toBe(true);
    expect((await throughWorker("/v1/responses", responsesTurn(), authText(), proto)).declines).toEqual(["responses:provider-field:__proto__"]);
    // A default with a slash could name the whole selector in ocx's router.
    expect((await throughWorker("/v1/responses", responsesTurn(), authText(), { ...row, defaultModel: "anthropic/claude-opus-5-5" })).declines).toEqual(["responses:provider-field:defaultModel"]);
  });

  test("a Claude Code turn replaying signed thinking is left to ocx too", async () => {
    const body = messagesTurn();
    body.messages = [
      { role: "user", content: "List files." },
      { role: "assistant", content: [{ type: "thinking", thinking: "Listing.", signature: "A".repeat(64) }, { type: "text", text: "Sure." }] },
      { role: "user", content: "Again." },
    ];
    expect((await throughWorker("/v1/messages", body, authText())).declines).toEqual(["messages:reasoning-replay-state"]);
  });

  test("never sends the account's token to a host the Worker answers itself", async () => {
    const declines: string[] = [];
    await serveNativeResponses(JSON.stringify(responsesTurn()), new Headers(), new AbortController().signal, {
      readConfig: async () => JSON.stringify({ providers: { anthropic: { ...row, baseUrl: "https://local.worker.test" } } }),
      readAuth: async () => authText(),
      clientRuntime: async () => containerRuntime,
      localHosts: { "local.worker.test": async () => reply() },
      fetch: async () => reply(),
      onDecline: reason => declines.push(reason),
    });
    expect(declines).toEqual(["responses:destination"]);
  });
});
