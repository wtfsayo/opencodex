import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { getDefaultConfig, saveConfig } from "../../src/config";
import { startServer } from "../../src/server";
import { serveNativeMessages } from "../../src/server/cloudflare-native-messages";
import { serveNativeResponses } from "../../src/server/cloudflare-native-responses";
import type { OcxConfig } from "../../src/types";
import { installIsolatedCodexHome, type IsolatedCodexHome } from "../helpers/isolated-codex-home";
import { removeTreeWithRetry } from "../helpers/remove-tree";

type Rec = Record<string, unknown>;
const KEY = ["anthropic", "test", "key"].join("-");
const sse = (events: [string, unknown][]) => new Response(
  events.map(([event, data]) => `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`).join(""),
  { headers: { "content-type": "text/event-stream" } },
);
const anthropicText = () => sse([
  ["message_start", { type: "message_start", message: { id: "msg_1", type: "message", role: "assistant", model: "claude-x", content: [], usage: { input_tokens: 12, output_tokens: 1 } } }],
  ["content_block_start", { type: "content_block_start", index: 0, content_block: { type: "text", text: "" } }],
  ["content_block_delta", { type: "content_block_delta", index: 0, delta: { type: "text_delta", text: "Po" } }],
  ["content_block_delta", { type: "content_block_delta", index: 0, delta: { type: "text_delta", text: "ng" } }],
  ["content_block_stop", { type: "content_block_stop", index: 0 }],
  ["message_delta", { type: "message_delta", delta: { stop_reason: "end_turn" }, usage: { output_tokens: 3 } }],
  ["message_stop", { type: "message_stop" }],
]);
const anthropicTool = () => sse([
  ["message_start", { type: "message_start", message: { id: "msg_2", type: "message", role: "assistant", model: "claude-x", content: [], usage: { input_tokens: 20, output_tokens: 1 } } }],
  ["content_block_start", { type: "content_block_start", index: 0, content_block: { type: "tool_use", id: "toolu_9", name: "shell", input: {} } }],
  ["content_block_delta", { type: "content_block_delta", index: 0, delta: { type: "input_json_delta", partial_json: "{\"cmd\":\"ls\"}" } }],
  ["content_block_stop", { type: "content_block_stop", index: 0 }],
  ["message_delta", { type: "message_delta", delta: { stop_reason: "tool_use" }, usage: { output_tokens: 9 } }],
  ["message_stop", { type: "message_stop" }],
]);
// An upstream that numbers its tool calls by position, repeating an id the client already holds.
const chatReusedId = () => new Response([
  `data: ${JSON.stringify({ choices: [{ index: 0, delta: { role: "assistant", tool_calls: [{ index: 0, id: "call-0-0", type: "function", function: { name: "shell", arguments: "{\"cmd\":\"pwd\"}" } }] } }] })}\n\n`,
  `data: ${JSON.stringify({ choices: [{ index: 0, delta: {}, finish_reason: "tool_calls" }] })}\n\n`,
  "data: [DONE]\n\n",
].join(""), { headers: { "content-type": "text/event-stream" } });

const shell = { type: "function", name: "shell", description: "Run a command", parameters: { type: "object", properties: { cmd: { type: "string" } }, required: ["cmd"] } };
const responsesTurn = (model: string, extra: Rec = {}): Rec => ({
  model, instructions: "You are a coding agent.", store: false, stream: true, tools: [shell], tool_choice: "auto",
  input: [{ type: "message", role: "user", content: [{ type: "input_text", text: "List files." }] }],
  ...extra,
});
const withHistory = (model: string) => responsesTurn(model, {
  input: [
    { type: "message", role: "user", content: [{ type: "input_text", text: "List files." }] },
    { type: "function_call", call_id: "call-0-0", name: "shell", arguments: "{\"cmd\":\"ls\"}" },
    { type: "function_call_output", call_id: "call-0-0", output: "a\nb" },
  ],
});
const messagesTurn = (model: string, extra: Rec = {}): Rec => ({
  model, max_tokens: 4000, stream: true,
  metadata: { user_id: JSON.stringify({ device_id: "d", account_uuid: "", session_id: "0f0e0d0c-0b0a-4908-8706-050403020100" }) },
  system: [{ type: "text", text: "You are Claude Code." }],
  thinking: { type: "adaptive" }, output_config: { effort: "high" },
  tools: [{ name: "shell", description: "Run a command", input_schema: shell.parameters }],
  messages: [{ role: "user", content: "List files." }],
  ...extra,
});

// Ids and times are minted per response; everything else must match.
const normalize = (text: string) => text
  .replace(/"(resp|msg|fc|rs|item)_[A-Za-z0-9_-]+"/g, "\"$1_ID\"")
  .replace(/"(created_at|completed_at)":\d+/g, "\"$1\":0");
// The transport's own headers, and the defaults Bun's fetch adds when ocx sets none (a Worker's
// fetch adds its own), are not ocx's to decide.
const sentHeaders = (headers: Headers) => Object.fromEntries([...headers]
  .filter(([name, value]) => !["host", "content-length", "accept-encoding", "connection"].includes(name)
    && !(name === "accept" && value === "*/*") && !(name === "user-agent" && value.startsWith("Bun/")))
  .sort());

describe("Worker-native turns on the anthropic adapter", () => {
  let home = "";
  let codexHome: IsolatedCodexHome | null = null;
  const previousHome = process.env.OPENCODEX_HOME;
  beforeEach(() => {
    codexHome = installIsolatedCodexHome("ocx-worker-anthropic-");
    home = mkdtempSync(join(tmpdir(), "ocx-worker-anthropic-"));
    process.env.OPENCODEX_HOME = home;
  });
  afterEach(() => {
    if (previousHome === undefined) delete process.env.OPENCODEX_HOME;
    else process.env.OPENCODEX_HOME = previousHome;
    codexHome?.restore();
    codexHome = null;
    removeTreeWithRetry(home);
  });

  /** One turn through ocx, whose provider `name` points at a local upstream answering with `reply`. */
  async function throughOcx(path: string, body: Rec, provider: Rec, reply: () => Response, extraConfig: Rec = {}) {
    let sent: { body: Rec; headers: Record<string, string> } | undefined;
    const upstream = Bun.serve({ port: 0, async fetch(req) { sent = { body: await req.json() as Rec, headers: sentHeaders(req.headers) }; return reply(); } });
    const base = `${upstream.url.toString().replace(/\/$/, "")}/v1`;
    saveConfig({ port: 0, providers: { z: { ...provider, baseUrl: base, allowPrivateNetwork: true } }, ...extraConfig } as unknown as OcxConfig);
    const server = startServer(0);
    try {
      const response = await fetch(`http://127.0.0.1:${server.port}${path}`, {
        method: "POST", headers: { "content-type": "application/json", "user-agent": "test/1" }, body: JSON.stringify(body),
      });
      return { sent: sent!, status: response.status, text: await response.text(), base };
    } finally {
      await server.stop(true);
      await upstream.stop(true);
    }
  }

  async function throughWorker(path: string, body: Rec, provider: Rec, reply: () => Response, base: string, extraConfig: Rec = {}) {
    let sent: { body: Rec; headers: Record<string, string> } | undefined;
    const declines: string[] = [];
    const serve = path === "/v1/messages" ? serveNativeMessages : serveNativeResponses;
    const response = await serve(JSON.stringify(body), new Headers({ "user-agent": "test/1" }), new AbortController().signal, {
      // The same base URL ocx used, so both send to the same place; the Worker only checks it is public.
      readConfig: async () => JSON.stringify({ providers: { z: { ...provider, baseUrl: "https://api.example.test/v1" } }, ...extraConfig }),
      fetch: async request => {
        sent = { body: await request.json() as Rec, headers: sentHeaders(request.headers) };
        return reply();
      },
      onDecline: reason => declines.push(reason),
    });
    return { sent, declines, status: response?.status, text: await response?.text(), base };
  }

  const anthropic = { adapter: "anthropic", apiKey: KEY, models: ["claude-x"] };
  const chat = { adapter: "openai-chat", apiKey: KEY, models: ["m-1"] };
  for (const [name, path, body, provider, reply, extraConfig] of [
    ["an anthropic Responses text turn", "/v1/responses", responsesTurn("z/claude-x"), anthropic, anthropicText, {}],
    ["an anthropic Responses tool call, at high effort", "/v1/responses", responsesTurn("z/claude-x", { reasoning: { effort: "high" } }), anthropic, anthropicTool, {}],
    ["an anthropic Responses turn with long cache retention", "/v1/responses", responsesTurn("z/claude-x"), anthropic, anthropicText, { cacheRetention: "long" }],
    ["an anthropic Claude Code turn", "/v1/messages", messagesTurn("ocx-claude-z--claude-x"), anthropic, anthropicText, {}],
    ["an openai-chat upstream reusing a tool call id the client holds", "/v1/responses", withHistory("z/m-1"), chat, chatReusedId, {}],
  ] as const) {
    test(`${name}: the same upstream request and client bytes as ocx`, async () => {
      const proxy = await throughOcx(path, body as Rec, provider, reply, extraConfig);
      const worker = await throughWorker(path, body as Rec, provider, reply, proxy.base, extraConfig);
      expect(worker.declines).toEqual([]);
      expect(worker.sent!.body).toEqual(proxy.sent.body);
      // Every header the upstream sees, apart from the transport's own.
      expect(worker.sent!.headers).toEqual(proxy.sent.headers);
      expect(worker.status).toBe(proxy.status);
      expect(normalize(worker.text!)).toBe(normalize(proxy.text));
    });
  }

  test("a config `ocx init` wrote is served, and one changing what its defaults leave unset is not", async () => {
    const init = getDefaultConfig() as unknown as Rec;
    const withProvider = (extra: Rec) => ({ ...init, ...extra, providers: { ...(init.providers as Rec), z: { ...anthropic, baseUrl: "https://api.example.test/v1" } } });
    const run = async (config: Rec) => {
      const declines: string[] = [];
      await serveNativeResponses(JSON.stringify(responsesTurn("z/claude-x")), new Headers(), new AbortController().signal, {
        readConfig: async () => JSON.stringify(config), fetch: async () => anthropicText(), onDecline: reason => declines.push(reason),
      });
      return declines;
    };
    expect(await run(withProvider({}))).toEqual([]);
    expect(await run(withProvider({ emptyCompletionRetry: true }))).toEqual(["responses:config-keys:emptyCompletionRetry"]);
    expect(await run(withProvider({ multiAgentGuidanceEnabled: false }))).toEqual(["responses:config-keys:multiAgentGuidanceEnabled"]);
  });

  test("an upstream failure after the send gets ocx's answer, not a second send through ocx", async () => {
    const failing = (status: number) => () => new Response(JSON.stringify({ error: { message: "boom", type: "server_error" } }), {
      status, headers: { "content-type": "application/json" },
    });
    for (const [path, body] of [
      ["/v1/responses", responsesTurn("z/m-1")],
      ["/v1/messages", messagesTurn("ocx-claude-z--m-1")],
    ] as const) {
      const proxy = await throughOcx(path, body as Rec, chat, failing(503));
      const worker = await throughWorker(path, body as Rec, chat, failing(503), proxy.base);
      expect([path, worker.declines]).toEqual([path, []]);
      expect([path, worker.status]).toEqual([path, proxy.status]);
      expect([path, worker.text]).toEqual([path, proxy.text]);
    }
    // A 4xx generated nothing: ocx sends it itself, with its own recovery.
    const refused = await throughWorker("/v1/responses", responsesTurn("z/m-1"), chat, failing(400), "");
    expect(refused.sent).toBeDefined();
    expect(refused.declines).toEqual(["responses:upstream-400"]);
  });

  test("a lost connection is never answered with an error the client would retry", async () => {
    const run = async (fetchImpl: () => Promise<Response>) => {
      const declines: string[] = [];
      const rows: Rec[] = [];
      const res = await serveNativeResponses(JSON.stringify(responsesTurn("z/m-1", { max_output_tokens: 300 })), new Headers({ "user-agent": "test/1" }), new AbortController().signal, {
        readConfig: async () => JSON.stringify({ providers: { z: { ...chat, baseUrl: "https://api.example.test/v1" } } }),
        fetch: fetchImpl,
        onDecline: reason => declines.push(reason),
        recordUsage: row => rows.push(row as Rec),
      });
      return { declines, rows, status: res?.status, text: await res?.text(), retry: res?.headers.get("x-should-retry") };
    };
    // How a Worker's fetch reports a connection dropped under the request.
    const lost = await run(async () => { throw new Error("Network connection lost."); });
    expect(lost.declines).toEqual([]);
    expect(lost.status).toBe(429);
    expect(lost.retry).toBe("false");
    // The failed send is still booked at what ocx reserves for it.
    expect(lost.rows.map(row => [row.status, row.spendOutputCeilingTokens])).toEqual([[429, 300]]);
    // A header deadline never reached the upstream, and reads as ocx's timeout.
    const timedOut = await run(async () => { throw new DOMException("Timeout elapsed", "TimeoutError"); });
    expect(timedOut.status).toBe(502);
    expect(timedOut.text).toContain("Provider connect timeout after 200000ms");
  });

  test("an anthropic provider with a setting the Worker does not reproduce is left to ocx", async () => {
    const worker = await throughWorker("/v1/responses", responsesTurn("z/claude-x"), { ...anthropic, apiKeyTransport: "bearer" }, anthropicText, "");
    expect(worker.declines).toEqual(["responses:provider-field:apiKeyTransport"]);
  });
});
