import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { serveNativeMessages } from "../../src/server/cloudflare-native-messages";
import { handleClaudeMessages } from "../../src/server/claude-messages";
import type { RequestLogContext } from "../../src/server/request-log";
import type { OcxConfig } from "../../src/types";
import { messagesAdmitsDataToken } from "../../deploy/cloudflare/src/container-env";
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

  test("the usage row is written as a Messages turn with the reported tokens", async () => {
    const { response, rows } = await throughWorker(claudeTurn(), textReply);
    await response!.text();
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      inboundProtocol: "messages", provider: "p", model: "m-1", requestedModel: ALIAS, status: 200, usageStatus: "reported",
    });
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
    ["an image", claudeTurn({ messages: [{ role: "user", content: [{ type: "image", source: { type: "base64", media_type: "image/png", data: "AAAA" } }] }] }), "messages:message-parts"],
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
    expect(await messagesAdmitsDataToken(req({ "x-api-key": "data-token" }), env)).toBe(true);
    expect(await messagesAdmitsDataToken(req({ authorization: "Bearer data-token" }), env)).toBe(true);
    expect(await messagesAdmitsDataToken(req({ "x-opencodex-api-key": "data-token" }), env)).toBe(true);
    // The first header present decides, as in resolveApiAuth.
    expect(await messagesAdmitsDataToken(req({ authorization: "Bearer other", "x-api-key": "data-token" }), env)).toBe(false);
    expect(await messagesAdmitsDataToken(req({}), env)).toBe(false);
  });
});
