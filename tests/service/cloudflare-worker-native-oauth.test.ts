import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { saveConfig } from "../../src/config";
import { startServer } from "../../src/server";
import { saveCredential } from "../../src/oauth/store";
import { readRecentUsageEntries } from "../../src/usage/log";
import { serveNativeMessages } from "../../src/server/cloudflare-native-messages";
import { serveNativeResponses } from "../../src/server/cloudflare-native-responses";
import type { OcxConfig } from "../../src/types";
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

  const row = { adapter: "anthropic", authMode: "oauth" };
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

  async function throughWorker(path: string, body: Rec, auth: string, providerRow: Rec = row) {
    let sent: { body: Rec; headers: Record<string, string> } | undefined;
    const declines: string[] = [];
    const rows: Rec[] = [];
    const serve = path === "/v1/messages" ? serveNativeMessages : serveNativeResponses;
    const response = await serve(JSON.stringify(body), new Headers({ "user-agent": "test/1" }), new AbortController().signal, {
      // A host other than api.anthropic.com, as ocx's local upstream is: the adapter adds automatic
      // cache control only for Anthropic's own host.
      readConfig: async () => JSON.stringify({ providers: { anthropic: { ...providerRow, baseUrl: "https://claude.example.test" } } }),
      readAuth: async () => auth,
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
    expect((await throughWorker("/v1/responses", responsesTurn(), authText(), { ...row, fastEnabled: true })).declines).toEqual(["responses:provider-field:fastEnabled"]);
    expect((await throughWorker("/v1/responses", responsesTurn(), "")).declines).toEqual(["responses:oauth-no-login"]);
    // The HTTP chat lane is a passthrough of the caller's body; OAuth stays ocx's there.
    const { serveNativeChat } = await import("../../src/server/cloudflare-native-chat");
    const declines: string[] = [];
    await serveNativeChat(JSON.stringify({ model: "anthropic/claude-opus-5-5", messages: [{ role: "user", content: "hi" }] }), new Headers(), new AbortController().signal, {
      readConfig: async () => JSON.stringify({ providers: { anthropic: row } }), readAuth: async () => authText(), fetch: async () => reply(), onDecline: reason => declines.push(reason),
    });
    expect(declines).toEqual(["built-in-provider"]);
  });
});
