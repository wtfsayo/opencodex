import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { serveNativeMessages } from "../../src/server/cloudflare-native-messages";
import { handleClaudeMessages } from "../../src/server/claude-messages";
import type { RequestLogContext } from "../../src/server/request-log";
import type { OcxConfig } from "../../src/types";
import { apiAuthAdmitsDataToken } from "../../deploy/cloudflare/src/container-env";
import { installIsolatedCodexHome, type IsolatedCodexHome } from "../helpers/isolated-codex-home";
import { removeTreeWithRetry } from "../helpers/remove-tree";

type Rec = Record<string, unknown>;
const provider = { adapter: "openai-chat", baseUrl: "https://api.example.test/v1", apiKey: "sk-literal", models: ["m-1"] };
const workerConfig = JSON.stringify({ providers: { p: provider } });
const ALIAS = "ocx-claude-p--m-1";

// The shape Claude Code 2.1.283 sends: system blocks with cache_control, adaptive thinking, an
// effort, a session in metadata, client tools without a `type`, and a mid-conversation system turn.
const claudeTurn = (extra: Rec = {}): Rec => ({
  model: ALIAS,
  max_tokens: 32000,
  stream: true,
  metadata: { user_id: JSON.stringify({ device_id: "d", account_uuid: "", session_id: "0f0e0d0c-0b0a-4908-8706-050403020100" }) },
  system: [
    { type: "text", text: "You are Claude Code.", cache_control: { type: "ephemeral" } },
    { type: "text", text: "Environment: macOS", cache_control: { type: "ephemeral" } },
  ],
  thinking: { type: "adaptive", display: "omitted" },
  output_config: { effort: "high" },
  context_management: { edits: [{ type: "clear_thinking_20251015", keep: "all" }] },
  tools: [{ name: "Bash", description: "Run a command", input_schema: { type: "object", properties: { command: { type: "string" } }, required: ["command"] } }],
  messages: [
    { role: "user", content: [{ type: "text", text: "List the files.", cache_control: { type: "ephemeral" } }] },
    { role: "assistant", content: [{ type: "thinking", thinking: "Use ls.", signature: "sig" }, { type: "tool_use", id: "toolu_1", name: "Bash", input: { command: "ls" } }] },
    { role: "user", content: [{ type: "tool_result", tool_use_id: "toolu_1", content: "a\nb" }] },
    { role: "system", content: "Reminder: be brief." },
  ],
  ...extra,
});

const textReply = [
  `data: ${JSON.stringify({ choices: [{ index: 0, delta: { role: "assistant", content: "Two" } }] })}\n\n`,
  `data: ${JSON.stringify({ choices: [{ index: 0, delta: { content: " files." } }] })}\n\n`,
  `data: ${JSON.stringify({ choices: [{ index: 0, delta: {}, finish_reason: "stop" }], usage: { prompt_tokens: 40, completion_tokens: 3 } })}\n\n`,
  "data: [DONE]\n\n",
];
const toolReply = [
  `data: ${JSON.stringify({ choices: [{ index: 0, delta: { role: "assistant", tool_calls: [{ index: 0, id: "call_9", type: "function", function: { name: "Bash", arguments: "{\"command\":\"pwd\"}" } }] } }] })}\n\n`,
  `data: ${JSON.stringify({ choices: [{ index: 0, delta: {}, finish_reason: "tool_calls" }], usage: { prompt_tokens: 40, completion_tokens: 7 } })}\n\n`,
  "data: [DONE]\n\n",
];
const sse = (frames: string[]) => new Response(frames.join(""), { headers: { "content-type": "text/event-stream" } });

// Message ids are minted per response; everything else must match byte for byte.
const normalize = (text: string) => text.replace(/"msg_[A-Za-z0-9_-]+"/g, "\"msg_ID\"");

let home = "";
let previousHome: string | undefined;
let codexHome: IsolatedCodexHome | null = null;
beforeEach(() => {
  previousHome = process.env.OPENCODEX_HOME;
  codexHome = installIsolatedCodexHome("ocx-worker-messages-");
  home = mkdtempSync(join(tmpdir(), "ocx-worker-messages-"));
  process.env.OPENCODEX_HOME = home;
});
afterEach(() => {
  if (previousHome === undefined) delete process.env.OPENCODEX_HOME;
  else process.env.OPENCODEX_HOME = previousHome;
  codexHome?.restore();
  codexHome = null;
  if (home) removeTreeWithRetry(home);
});

/** The same turn through ocx's own /v1/messages handler, against a local upstream. */
async function throughProxy(body: Rec, reply: string[]): Promise<{ sent: Rec; status: number; text: string; type: string | null }> {
  let sent: Rec = {};
  const upstream = Bun.serve({ port: 0, async fetch(req) { sent = await req.json() as Rec; return sse(reply); } });
  try {
    const config = {
      port: 0,
      providers: { p: { ...provider, baseUrl: `${upstream.url.toString().replace(/\/$/, "")}/v1`, allowPrivateNetwork: true } },
    } as unknown as OcxConfig;
    const response = await handleClaudeMessages(new Request("http://localhost/v1/messages?beta=true", {
      method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body),
    }), config, { model: "", provider: "" } as RequestLogContext);
    return { sent, status: response.status, text: await response.text(), type: response.headers.get("content-type") };
  } finally {
    await upstream.stop(true);
  }
}

async function throughWorker(body: Rec, reply: string[], headers = new Headers()) {
  let sent: Rec = {};
  const rows: Rec[] = [];
  const declines: string[] = [];
  const response = await serveNativeMessages(JSON.stringify(body), headers, new AbortController().signal, {
    readConfig: async () => workerConfig,
    fetch: async request => { sent = await request.json() as Rec; return sse(reply); },
    recordUsage: row => rows.push(row as Rec),
    onDecline: reason => declines.push(reason),
  });
  return { response, sent, rows, declines };
}

describe("Worker-native Messages", () => {
  for (const [name, body, reply] of [
    ["a streamed Claude Code tool-loop turn", claudeTurn(), textReply],
    ["a turn the model answers with a tool call", claudeTurn(), toolReply],
    ["a turn that did not ask to stream", claudeTurn({ stream: false }), textReply],
    ["a first turn without thinking or effort", claudeTurn({ thinking: undefined, output_config: undefined, messages: [{ role: "user", content: "hi" }] }), textReply],
  ] as const) {
    test(`${name}: same upstream request and same client bytes as ocx`, async () => {
      const proxy = await throughProxy(structuredClone(body), [...reply]);
      const worker = await throughWorker(structuredClone(body), [...reply]);
      expect(worker.declines).toEqual([]);
      expect(worker.response).not.toBeNull();
      expect(worker.sent).toEqual(proxy.sent);
      expect(worker.response!.status).toBe(proxy.status);
      expect(worker.response!.headers.get("content-type")).toBe(proxy.type);
      expect(normalize(await worker.response!.text())).toBe(normalize(proxy.text));
    });
  }

  test("usage rows carry the fields ocx's own rows have, for chat, Responses and Messages", async () => {
    const chatBody = { model: "p/m-1", stream: true, reasoning_effort: "high", messages: [{ role: "user", content: "hi" }] };
    const responsesBody = { model: "p/m-1", input: "hi", store: false, stream: true, reasoning: { effort: "low" } };
    const responsesHeaders = { "thread-id": "t-1", "session-id": "s-1" };
    // ocx's rows, from its usage log.
    const upstream = Bun.serve({ port: 0, fetch: () => sse(textReply) });
    const config = { port: 0, providers: { p: { ...provider, baseUrl: `${upstream.url.toString().replace(/\/$/, "")}/v1`, allowPrivateNetwork: true } } };
    const { saveConfig } = await import("../../src/config");
    const { startServer } = await import("../../src/server");
    const { readRecentUsageEntries } = await import("../../src/usage/log");
    saveConfig(config as unknown as OcxConfig);
    const server = startServer(0);
    try {
      const post = (path: string, body: unknown, headers: Record<string, string> = {}) => fetch(`http://127.0.0.1:${server.port}${path}`, {
        method: "POST", headers: { "content-type": "application/json", ...headers }, body: JSON.stringify(body),
      }).then(response => response.text());
      await post("/v1/chat/completions", chatBody);
      await post("/v1/responses", responsesBody, responsesHeaders);
      await post("/v1/messages", claudeTurn());
      await Bun.sleep(200);
    } finally {
      await server.stop(true);
      await upstream.stop(true);
    }
    const proxyRows = readRecentUsageEntries(10).sort((a, b) => a.timestamp - b.timestamp);
    // The Worker's rows for the same three turns.
    const workerRows: Rec[] = [];
    const deps = {
      readConfig: async () => workerConfig,
      fetch: async () => sse(textReply),
      recordUsage: (row: unknown) => workerRows.push(row as Rec),
    };
    const { serveNativeChat } = await import("../../src/server/cloudflare-native-chat");
    const { serveNativeResponses } = await import("../../src/server/cloudflare-native-responses");
    await (await serveNativeChat(JSON.stringify(chatBody), new Headers(), new AbortController().signal, deps))!.text();
    await (await serveNativeResponses(JSON.stringify(responsesBody), new Headers(responsesHeaders), new AbortController().signal, deps))!.text();
    await (await serveNativeMessages(JSON.stringify(claudeTurn()), new Headers(), new AbortController().signal, deps))!.text();
    const fields = ["inboundProtocol", "provider", "model", "requestedModel", "resolvedModel", "requestedEffort", "accountLogLabel", "conversationId", "surface", "status", "usageStatus"];
    const pick = (row: Rec) => Object.fromEntries(fields.filter(field => row[field] !== undefined).map(field => [field, row[field]]));
    expect(proxyRows.map(row => row.inboundProtocol)).toEqual(["chat", "responses", "messages"]);
    expect(workerRows.map(pick)).toEqual(proxyRows.map(row => pick(row as unknown as Rec)));
  });

  test("a cache key taken from the system prompt keeps no skills snapshot, as in ocx", async () => {
    // claude-messages.ts marks such a key a shared cohort, and skills-snapshot.ts skips the turn.
    const turn = (block: string) => claudeTurn({ metadata: undefined, system: `<skills_instructions>${block}</skills_instructions>`, messages: [{ role: "user", content: "hi" }] });
    const store = new Map<string, string>();
    const sent: string[] = [];
    for (const block of ["ONE", "TWO"]) {
      const response = await serveNativeMessages(JSON.stringify(turn(block)), new Headers({ session_id: "s1" }), new AbortController().signal, {
        readConfig: async () => workerConfig,
        fetch: async request => { sent.push(JSON.stringify(await request.json())); return sse(textReply); },
        skills: { principal: "token", read: async scope => store.get(scope), commit: (scope, value) => { store.set(scope, value); } },
      });
      await response!.text();
    }
    expect(sent[1]).toContain("TWO");
    expect(store.size).toBe(0);
  });

  test("sends OpenCode Go the session header ocx sends, under any provider name", async () => {
    const go = { ...provider, baseUrl: "https://opencode.ai/zen/go/v1" };
    const goConfig = JSON.stringify({ providers: { g: go } });
    const sessionHeader = (headers: Headers) => headers.get("x-opencode-session");
    // What ocx sends, from its own handlers in this process, with fetch answered locally.
    const proxySends: (string | null)[] = [];
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
      const request = new Request(input, init);
      if (new URL(request.url).hostname !== "opencode.ai") return originalFetch(input, init);
      proxySends.push(sessionHeader(request.headers));
      return sse(textReply);
    }) as typeof fetch;
    const { saveConfig } = await import("../../src/config");
    const { startServer } = await import("../../src/server");
    saveConfig({ port: 0, providers: { g: go } } as unknown as OcxConfig);
    const chat = { model: "g/m-1", stream: true, messages: [{ role: "user", content: "hi" }] };
    const responses = { model: "g/m-1", input: "hi", store: false, stream: true };
    // No effort: Go's effort ladder comes from models.dev, which the Worker declines.
    const goTurn = (extra: Rec = {}) => claudeTurn({ model: "ocx-claude-g--m-1", output_config: undefined, thinking: undefined, ...extra });
    const cases: [string, unknown, Record<string, string>][] = [
      ["/v1/chat/completions", chat, { "session-id": "s-chat" }],
      ["/v1/responses", responses, { "thread-id": "t-resp" }],
      ["/v1/messages", goTurn(), {}],
      ["/v1/messages", goTurn({ metadata: undefined }), { "session-id": "s-msg" }],
      ["/v1/messages", goTurn({ metadata: undefined }), { "x-opencode-session": "caller-go" }],
    ];
    const server = startServer(0);
    try {
      for (const [path, body, headers] of cases) {
        await originalFetch(`http://127.0.0.1:${server.port}${path}`, {
          method: "POST", headers: { "content-type": "application/json", ...headers }, body: JSON.stringify(body),
        }).then(response => response.text());
      }
    } finally {
      await server.stop(true);
      globalThis.fetch = originalFetch;
    }
    const workerSends: (string | null)[] = [];
    const deps = { readConfig: async () => goConfig, fetch: async (request: Request) => { workerSends.push(sessionHeader(request.headers)); return sse(textReply); } };
    const { serveNativeChat } = await import("../../src/server/cloudflare-native-chat");
    const { serveNativeResponses } = await import("../../src/server/cloudflare-native-responses");
    for (const [path, body, headers] of cases) {
      const serve = path === "/v1/chat/completions" ? serveNativeChat : path === "/v1/responses" ? serveNativeResponses : serveNativeMessages;
      await (await serve(JSON.stringify(body), new Headers(headers), new AbortController().signal, deps))!.text();
    }
    expect(proxySends).toHaveLength(cases.length);
    expect(workerSends).toEqual(proxySends);
    expect(proxySends.every(value => /^ocx_[0-9a-f]{32}$/.test(value ?? ""))).toBe(true);
  });

  test("a Go turn with no session identity gets a lane of its own, as in ocx", async () => {
    const sends: (string | null)[] = [];
    const deps = {
      readConfig: async () => JSON.stringify({ providers: { g: { ...provider, baseUrl: "https://opencode.ai/zen/go/v1" } } }),
      fetch: async (request: Request) => { sends.push(request.headers.get("x-opencode-session")); return sse(textReply); },
    };
    for (let i = 0; i < 2; i++) {
      const turn = claudeTurn({ model: "ocx-claude-g--m-1", metadata: undefined, output_config: undefined, thinking: undefined });
      await (await serveNativeMessages(JSON.stringify(turn), new Headers(), new AbortController().signal, deps))!.text();
    }
    expect(sends.every(value => /^ocx_[0-9a-f]{32}$/.test(value ?? ""))).toBe(true);
    expect(sends[0]).not.toBe(sends[1]);
  });

  test("releases its translation budgets whether it serves or declines", async () => {
    const { translatorLiveBudgetCountForTests } = await import("../../src/lib/translator-budget");
    const before = translatorLiveBudgetCountForTests();
    await (await throughWorker(claudeTurn(), textReply)).response!.text();
    await (await throughWorker(claudeTurn({ stream: false }), textReply)).response!.text();
    await throughWorker(claudeTurn({ thread: { previous_message_id: "m" } }), textReply);
    await throughWorker(claudeTurn({ messages: [{ role: "user", content: [{ type: "image", source: { type: "base64", media_type: "image/png", data: "AAAA" } }] }] }), textReply);
    expect(translatorLiveBudgetCountForTests()).toBe(before);
  });

  test("the message_start usage floor is ocx's own estimate", async () => {
    const { response } = await throughWorker(claudeTurn(), textReply);
    const start = JSON.parse(/^data: (.+)$/m.exec(await response!.text())![1]!) as { message: { usage: { input_tokens: number } } };
    const proxy = await throughProxy(claudeTurn(), textReply);
    const proxyStart = JSON.parse(/^data: (.+)$/m.exec(proxy.text)![1]!) as { message: { usage: { input_tokens: number } } };
    expect(start.message.usage.input_tokens).toBeGreaterThan(0);
    expect(start.message.usage.input_tokens).toBe(proxyStart.message.usage.input_tokens);
  });

  for (const [name, body, reason] of [
    ["a Claude model id, which may be native passthrough or a Desktop alias", claudeTurn({ model: "claude-opus-5-5" }), "messages:model-not-alias"],
    ["a synthetic Fast row", claudeTurn({ model: `${ALIAS}--fast` }), "messages:synthetic-row-grammar"],
    ["a message-thread continuation", claudeTurn({ thread: { previous_message_id: "msg_1" } }), "messages:message-thread"],
    ["an injected agent's route directive", claudeTurn({ system: [{ type: "text", text: "<!-- ocx-route: p/m-1 -->" }] }), "messages:route-directive"],
    ["an image", claudeTurn({ messages: [{ role: "user", content: [{ type: "image", source: { type: "base64", media_type: "image/png", data: "AAAA" } }] }] }), "messages:vision-preprocessing"],
  ] as const) {
    test(`declines ${name}`, async () => {
      const { response, declines } = await throughWorker(structuredClone(body), textReply);
      expect(response).toBeNull();
      expect(declines).toEqual([reason]);
    });
  }

  test("declines when the config has a claudeCode section", async () => {
    const declines: string[] = [];
    const response = await serveNativeMessages(JSON.stringify(claudeTurn()), new Headers(), new AbortController().signal, {
      readConfig: async () => JSON.stringify({ providers: { p: provider }, claudeCode: { modelMap: {} } }),
      fetch: async () => { throw new Error("must not send"); },
      onDecline: reason => declines.push(reason),
    });
    expect(response).toBeNull();
    expect(declines).toEqual(["messages:config-keys:claudeCode"]);
  });

  test("admission follows ocx's /v1/messages header order", async () => {
    const env = { OPENCODEX_API_AUTH_TOKEN: "data-token" } as never;
    const req = (headers: Record<string, string>) => new Request("https://hub.example/v1/messages", { method: "POST", headers });
    expect(await apiAuthAdmitsDataToken(req({ "x-api-key": "data-token" }), env)).toBe(true);
    expect(await apiAuthAdmitsDataToken(req({ authorization: "Bearer data-token" }), env)).toBe(true);
    expect(await apiAuthAdmitsDataToken(req({ "x-opencodex-api-key": "data-token" }), env)).toBe(true);
    // The first header present decides, as in resolveApiAuth.
    expect(await apiAuthAdmitsDataToken(req({ authorization: "Bearer other", "x-api-key": "data-token" }), env)).toBe(false);
    expect(await apiAuthAdmitsDataToken(req({}), env)).toBe(false);
  });
});
