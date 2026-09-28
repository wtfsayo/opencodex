import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { getDefaultConfig, saveConfig } from "../../src/config";
import { startServer } from "../../src/server";
import { readRecentUsageEntries } from "../../src/usage/log";
import { serveNativeResponses } from "../../src/server/cloudflare-native-responses";
import { createNativeWsSession } from "../../src/server/cloudflare-native-ws";
import { nativeOpenAiDeclineReason } from "../../src/server/cloudflare-native-openai";
import { ACCOUNT_GATED_NATIVE_OPENAI_MODELS } from "../../src/codex/catalog/native-models";
import { CODEX_ACCOUNT_GATED_CANONICAL_WIRE_MODELS } from "../../src/server/responses/core-codex-account";
import { bunSupportsBoundedCodexWsRelay } from "../../src/server/responses/ws-upstream";
import type { NativeOpenAiFacts, WorkerUsageRow } from "../../src/server/cloudflare-native-chat-api";
import { DURABLE_STATE_BOOT_ID_ENV, setDurableMirrorTransportForTests } from "../../src/lib/durable-mirror";
import { nativeOpenAiFacts, publishNativeOpenAiFactsForWorker } from "../../src/server/worker-native-state";
import { observeMainQuotaCredential, observeMainQuotaIdentity, onMainQuotaCredentialChange, clearMainAccountInfoCache } from "../../src/codex/main-account-cache";
import { resolveCodexHomeDir } from "../../src/codex/home";
import { LeaseState, type LeaseStorage } from "../../deploy/cloudflare/src/lease";
import { handleStateRequest, type StateBucket } from "../../deploy/cloudflare/src/state-routes";
import { installIsolatedCodexHome, type IsolatedCodexHome } from "../helpers/isolated-codex-home";
import { removeTreeWithRetry } from "../helpers/remove-tree";

type Rec = Record<string, unknown>;
type Listener = (event: unknown) => void;

// The ChatGPT backend over its WebSocket: each response.create frame is answered with the next
// queued reply, else `events`.
let events: Rec[] = [];
let replyQueue: Rec[][] = [];
const nextReply = () => replyQueue.shift() ?? events;
class FakeWebSocket {
  static dials: { url: string; headers: Rec; frames: string[] }[] = [];
  readyState = 0;
  private readonly listeners = new Map<string, Listener[]>();
  private readonly dial: { url: string; headers: Rec; frames: string[] };
  constructor(url: string, options?: { headers?: Rec }) {
    this.dial = { url, headers: { ...(options?.headers ?? {}) }, frames: [] };
    FakeWebSocket.dials.push(this.dial);
    queueMicrotask(() => { this.readyState = 1; this.emit("open"); });
  }
  addEventListener(type: string, listener: Listener) { this.listeners.set(type, [...(this.listeners.get(type) ?? []), listener]); }
  removeEventListener(type: string, listener: Listener) { this.listeners.set(type, (this.listeners.get(type) ?? []).filter(l => l !== listener)); }
  private emit(type: string, event: unknown = {}) { for (const l of this.listeners.get(type) ?? []) l(event); }
  send(data: string) {
    this.dial.frames.push(data);
    const answer = nextReply();
    queueMicrotask(() => { for (const event of answer) this.emit("message", { data: JSON.stringify(event) }); });
  }
  close() { this.readyState = 3; }
}
// The same backend over HTTP SSE, for a runtime ocx does not dial the WebSocket from.
// When set, the HTTP backend answers every send with this status instead.
let failStatus: number | undefined;
const sseReply = () => failStatus !== undefined
  ? new Response(JSON.stringify({ error: { type: "server_error", code: "server_error", message: "The server had an error." } }), { status: failStatus, headers: { "content-type": "application/json" } })
  : new Response(nextReply().map(e => `event: ${e.type}\ndata: ${JSON.stringify(e)}\n\n`).join(""), {
  headers: { "content-type": "text/event-stream" },
});

const response = (status: string, output: Rec[], usage?: Rec) => ({ id: "resp_up1", object: "response", status, model: "gpt-5.5", output, ...(usage ? { usage } : {}) });
const message = (text: string) => ({ id: "msg_up1", type: "message", role: "assistant", status: "completed", content: [{ type: "output_text", text, annotations: [] }] });
const textTurn = (): Rec[] => [
  { type: "response.created", sequence_number: 0, response: response("in_progress", []) },
  { type: "response.output_item.added", sequence_number: 1, output_index: 0, item: { id: "msg_up1", type: "message", role: "assistant", status: "in_progress", content: [] } },
  { type: "response.output_text.delta", sequence_number: 2, item_id: "msg_up1", output_index: 0, content_index: 0, delta: "pong" },
  { type: "response.output_item.done", sequence_number: 3, output_index: 0, item: message("pong") },
  { type: "response.completed", sequence_number: 4, response: response("completed", [message("pong")], { input_tokens: 11, input_tokens_details: { cached_tokens: 4 }, output_tokens: 2, output_tokens_details: { reasoning_tokens: 1 }, total_tokens: 13 }) },
];
const call = { id: "fc_up1", type: "function_call", status: "completed", call_id: "call_1", name: "shell", arguments: "{\"cmd\":\"ls\"}" };
const reasoning = { id: "rs_up1", type: "reasoning", summary: [{ type: "summary_text", text: "Listing." }], encrypted_content: "gAAAAAB-opaque" };
const toolTurn = (): Rec[] => [
  { type: "response.created", sequence_number: 0, response: response("in_progress", []) },
  { type: "response.output_item.added", sequence_number: 1, output_index: 0, item: { ...reasoning, summary: [] } },
  { type: "response.output_item.done", sequence_number: 2, output_index: 0, item: reasoning },
  { type: "response.output_item.added", sequence_number: 3, output_index: 1, item: { ...call, status: "in_progress", arguments: "" } },
  { type: "response.function_call_arguments.delta", sequence_number: 4, item_id: "fc_up1", output_index: 1, delta: "{\"cmd\":\"ls\"}" },
  { type: "response.output_item.done", sequence_number: 5, output_index: 1, item: call },
  { type: "response.completed", sequence_number: 6, response: response("completed", [reasoning, call], { input_tokens: 40, output_tokens: 9, total_tokens: 49 }) },
];
const failedTurn = (): Rec[] => [
  { type: "response.created", sequence_number: 0, response: response("in_progress", []) },
  { type: "response.failed", sequence_number: 1, response: { ...response("failed", []), error: { code: "server_error", message: "The model had an error." } } },
];

const applyPatch = { type: "custom", name: "apply_patch", description: "Apply a patch", format: { type: "grammar", syntax: "lark", definition: "start: /.+/" } };
const mcpNamespace = { type: "namespace", name: "mcp__docs", description: "Docs server", tools: [{ type: "function", name: "search", description: "Search docs", strict: false, parameters: { type: "object", properties: { q: { type: "string" } } } }] };
const mcpCall = { id: "fc_up2", type: "function_call", status: "completed", call_id: "call_2", namespace: "mcp__docs", name: "search", arguments: "{\"q\":\"x\"}" };
const mcpTurn = (): Rec[] => [
  { type: "response.created", sequence_number: 0, response: response("in_progress", []) },
  { type: "response.output_item.added", sequence_number: 1, output_index: 0, item: { ...mcpCall, status: "in_progress", arguments: "" } },
  { type: "response.output_item.done", sequence_number: 2, output_index: 0, item: mcpCall },
  { type: "response.completed", sequence_number: 3, response: response("completed", [mcpCall], { input_tokens: 30, output_tokens: 5, total_tokens: 35 }) },
];
const searchItem = { id: "ws_up1", type: "web_search_call", status: "completed", action: { type: "search", query: "bun 1.4" } };
const searchTurn = (): Rec[] => [
  { type: "response.created", sequence_number: 0, response: response("in_progress", []) },
  { type: "response.output_item.added", sequence_number: 1, output_index: 0, item: { ...searchItem, status: "in_progress" } },
  { type: "response.web_search_call.searching", sequence_number: 2, output_index: 0, item_id: "ws_up1" },
  { type: "response.web_search_call.completed", sequence_number: 3, output_index: 0, item_id: "ws_up1" },
  { type: "response.output_item.done", sequence_number: 4, output_index: 0, item: searchItem },
  { type: "response.output_item.added", sequence_number: 5, output_index: 1, item: { id: "msg_up1", type: "message", role: "assistant", status: "in_progress", content: [] } },
  { type: "response.output_text.delta", sequence_number: 6, item_id: "msg_up1", output_index: 1, content_index: 0, delta: "Found." },
  { type: "response.output_item.done", sequence_number: 7, output_index: 1, item: message("Found.") },
  { type: "response.completed", sequence_number: 8, response: response("completed", [searchItem, message("Found.")], { input_tokens: 50, output_tokens: 4, total_tokens: 54 }) },
];
const shell = { type: "function", name: "shell", description: "Run a command", strict: false, parameters: { type: "object", properties: { cmd: { type: "string" } }, required: ["cmd"] } };
const codexTurn = (extra: Rec = {}): Rec => ({
  model: "gpt-5.5", instructions: "You are Codex.", store: false, stream: true, tools: [shell], tool_choice: "auto", parallel_tool_calls: false,
  reasoning: { effort: "medium", summary: "auto" }, include: ["reasoning.encrypted_content"], prompt_cache_key: "0199aa00-thread",
  input: [{ type: "message", role: "user", content: [{ type: "input_text", text: "List files." }] }],
  ...extra,
});
const withHistory = () => codexTurn({
  input: [
    { type: "message", role: "user", content: [{ type: "input_text", text: "List files." }] },
    reasoning,
    { type: "function_call", call_id: "call_1", name: "shell", arguments: "{\"cmd\":\"ls\"}" },
    { type: "function_call_output", call_id: "call_1", output: "a\nb" },
  ],
});
// A ChatGPT access token is a JWT; this one is shaped like one and signs nothing.
const CALLER_TOKEN = ["eyJhbGciOiJub25lIn0", "eyJzdWIiOiJjYWxsZXIifQ", "c2lnbmF0dXJl"].join(".");
// What Codex CLI sends on its own ChatGPT login, with the hub's key in the dedicated header.
const callerHeaders = () => ({
  "content-type": "application/json", authorization: `Bearer ${CALLER_TOKEN}`, "chatgpt-account-id": "acct-caller",
  originator: "codex_cli_rs", "user-agent": "codex_cli_rs/0.157.1", "x-opencodex-api-key": "hub-data-token",
  session_id: "0199aa00-thread", "x-codex-installation-id": "install-1",
});

const transport: NativeOpenAiFacts["upstreamTransport"] = bunSupportsBoundedCodexWsRelay() ? "websocket" : "sse";
// This process's own ceilings, as the container would publish them.
const facts = (extra: Partial<NativeOpenAiFacts> = {}): NativeOpenAiFacts => ({
  version: 1, mainAccountIdentityKey: null, codexAccountsStored: false, mainCodexLoginPresent: false, nativeMainTrafficBlocked: false, contextRelayActive: false,
  upstreamTransport: transport, inputCeilings: nativeOpenAiFacts(true).inputCeilings, ...extra,
});
const isAdmissionSecret = async (value: string) => value === "hub-data-token";

/** What the backend received: the create frame and handshake headers, or the POST body and headers. */
type Sent = { body: Rec; headers: Record<string, string> };
const lowerKeys = (headers: Rec) => Object.fromEntries(Object.entries(headers).map(([k, v]) => [k.toLowerCase(), String(v)]).sort());
function upstreamSent(httpSent: Sent | undefined): Sent | undefined {
  const dial = FakeWebSocket.dials.at(-1);
  if (dial) return { body: JSON.parse(dial.frames[0]!) as Rec, headers: lowerKeys(dial.headers) };
  return httpSent;
}
// Transport headers Bun's fetch or a Worker's own fetch adds, which are not ocx's to decide.
const fetchHeaders = (headers: Headers) => Object.fromEntries([...headers]
  .filter(([name, value]) => !["host", "content-length", "accept-encoding", "connection"].includes(name) && !(name === "accept" && value === "*/*") && !(name === "user-agent" && value.startsWith("Bun/")))
  .sort());
const normalize = (text: string) => text.replace(/"(created_at|completed_at)":\d+/g, "\"$1\":0");

describe("Worker-native Codex turns on the caller's ChatGPT login", () => {
  let home = "";
  let codexHome: IsolatedCodexHome | null = null;
  const saved = { home: process.env.OPENCODEX_HOME, ws: globalThis.WebSocket, fetch: globalThis.fetch };
  beforeEach(() => {
    codexHome = installIsolatedCodexHome("ocx-worker-openai-");
    home = mkdtempSync(join(tmpdir(), "ocx-worker-openai-"));
    process.env.OPENCODEX_HOME = home;
    FakeWebSocket.dials = [];
  });
  afterEach(() => {
    globalThis.WebSocket = saved.ws;
    globalThis.fetch = saved.fetch;
    if (saved.home === undefined) delete process.env.OPENCODEX_HOME; else process.env.OPENCODEX_HOME = saved.home;
    codexHome?.restore();
    codexHome = null;
    removeTreeWithRetry(home);
  });

  async function throughOcx(body: Rec) {
    FakeWebSocket.dials = [];
    let httpSent: Sent | undefined;
    globalThis.WebSocket = FakeWebSocket as unknown as typeof WebSocket;
    globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
      const request = new Request(input as RequestInfo, init);
      if (request.url.startsWith("https://chatgpt.com/")) {
        httpSent = { body: await request.json() as Rec, headers: fetchHeaders(request.headers) };
        return sseReply();
      }
      return saved.fetch(input, init);
    }) as typeof fetch;
    saveConfig({ ...(getDefaultConfig() as unknown as Rec), port: 0 } as never);
    const server = startServer(0);
    try {
      const res = await saved.fetch(`http://127.0.0.1:${server.port}/v1/responses`, { method: "POST", headers: callerHeaders(), body: JSON.stringify(body) });
      const text = await res.text();
      await Bun.sleep(100);
      return {
        sent: upstreamSent(httpSent), status: res.status, contentType: res.headers.get("content-type"), text,
        row: readRecentUsageEntries(5).at(-1) as unknown as Rec, config: readFileSync(join(home, "config.json"), "utf8"),
      };
    } finally {
      await server.stop(true);
      globalThis.WebSocket = saved.ws;
      globalThis.fetch = saved.fetch;
    }
  }

  async function throughWorker(body: Rec, config: string, extra: { facts?: NativeOpenAiFacts | undefined; accounts?: string; headers?: Rec; secret?: (value: string) => boolean } = {}) {
    FakeWebSocket.dials = [];
    let httpSent: Sent | undefined;
    const declines: string[] = [];
    const rows: WorkerUsageRow[] = [];
    const res = await serveNativeResponses(JSON.stringify(body), new Headers({ ...callerHeaders(), ...(extra.headers ?? {}) } as Record<string, string>), new AbortController().signal, {
      readConfig: async () => config,
      readCodexAccounts: async () => extra.accounts,
      isAdmissionSecret: async value => extra.secret?.(value) ?? isAdmissionSecret(value),
      nativeOpenAiFacts: async () => ("facts" in extra ? extra.facts : facts()),
      openUpstreamSocket: (url, headers) => new FakeWebSocket(url, { headers }) as unknown as WebSocket,
      fetch: async request => { httpSent = { body: await request.json() as Rec, headers: fetchHeaders(request.headers) }; return sseReply(); },
      onDecline: reason => declines.push(reason),
      recordUsage: row => rows.push(row),
    });
    const text = await res?.text();
    await Bun.sleep(20);
    return { sent: upstreamSent(httpSent), status: res?.status, contentType: res?.headers.get("content-type"), text, declines, row: rows[0] as unknown as Rec };
  }

  for (const [name, body, reply] of [
    ["a text turn", codexTurn(), textTurn],
    ["a tool call with reasoning", codexTurn(), toolTurn],
    ["a turn replaying reasoning and a tool result", withHistory(), textTurn],
    ["a turn that fails upstream", codexTurn(), failedTurn],
    ["a turn without store", (() => { const b = codexTurn(); delete b.store; return b; })(), textTurn],
    ["a turn with Codex's freeform apply_patch, an MCP namespace and verbosity", codexTurn({
      tools: [shell, applyPatch, mcpNamespace], text: { verbosity: "low" },
    }), mcpTurn],
    ["a turn with Codex's hosted web search, replaying a search", codexTurn({
      tools: [shell, { type: "web_search", external_web_access: true }],
      input: [
        { type: "message", role: "user", content: [{ type: "input_text", text: "Search it." }] },
        { type: "web_search_call", id: "ws_1", status: "completed", action: { type: "search", query: "bun 1.4" } },
        { type: "message", role: "assistant", content: [{ type: "output_text", text: "Found it." }] },
        { type: "message", role: "user", content: [{ type: "input_text", text: "Again." }] },
      ],
    }), textTurn],
    ["a turn whose answer searches the web", codexTurn({ tools: [shell, { type: "web_search", external_web_access: true }] }), searchTurn],
    ["a turn replaying a custom tool call", codexTurn({
      tools: [shell, applyPatch],
      input: [
        { type: "message", role: "user", content: [{ type: "input_text", text: "Patch it." }] },
        { type: "custom_tool_call", call_id: "call_p", name: "apply_patch", input: "*** Begin Patch\n*** End Patch" },
        { type: "custom_tool_call_output", call_id: "call_p", output: "Done" },
      ],
    }), textTurn],
  ] as const) {
    test(`${name}: the same upstream request, client bytes and usage row as ocx`, async () => {
      events = reply();
      const proxy = await throughOcx(body as Rec);
      const worker = await throughWorker(body as Rec, proxy.config);
      expect(worker.declines).toEqual([]);
      expect(worker.sent).toEqual(proxy.sent);
      expect(worker.status).toBe(proxy.status);
      expect(worker.contentType).toBe(proxy.contentType);
      expect(normalize(worker.text!)).toBe(normalize(proxy.text));
      for (const field of ["provider", "model", "requestedModel", "resolvedModel", "requestedEffort", "conversationId", "status", "usageStatus", "usage", "totalTokens", "inboundProtocol"]) {
        expect([field, worker.row?.[field]]).toEqual([field, proxy.row?.[field]]);
      }
    });
  }

  test("over Codex's WebSocket: a chained turn expanded from the socket's own response, as ocx does", async () => {
    const second = (): Rec[] => textTurn().map(event => JSON.parse(JSON.stringify(event).replaceAll("resp_up1", "resp_up2").replaceAll("msg_up1", "msg_up2")) as Rec);
    const frames: Rec[] = [
      { type: "response.create", ...codexTurn() },
      { type: "response.create", ...codexTurn({ previous_response_id: "resp_up1", input: [{ type: "function_call_output", call_id: "call_1", output: "a\nb" }] }) },
    ];
    const terminal = (text: string) => /"type":"response\.(completed|failed|incomplete)"/.test(text);
    const turnsDone = (received: string[]) => received.filter(terminal).length;

    // ocx's own socket.
    replyQueue = [toolTurn(), second()];
    FakeWebSocket.dials = [];
    const ocxHttp: Rec[] = [];
    globalThis.WebSocket = FakeWebSocket as unknown as typeof WebSocket;
    globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
      const request = new Request(input as RequestInfo, init);
      if (request.url.startsWith("https://chatgpt.com/")) { ocxHttp.push(await request.json() as Rec); return sseReply(); }
      return saved.fetch(input, init);
    }) as typeof fetch;
    saveConfig({ ...(getDefaultConfig() as unknown as Rec), port: 0, websockets: true } as never);
    const config = readFileSync(join(home, "config.json"), "utf8");
    const server = startServer(0);
    const proxyReceived: string[] = [];
    try {
      const socket = new saved.ws(`ws://127.0.0.1:${server.port}/v1/responses`, { headers: callerHeaders() } as never);
      socket.addEventListener("message", event => proxyReceived.push(String(event.data)));
      await new Promise((resolve, reject) => { socket.addEventListener("open", resolve); socket.addEventListener("error", reject); });
      for (const [index, frame] of frames.entries()) {
        socket.send(JSON.stringify(frame));
        const end = Date.now() + 3_000;
        while (turnsDone(proxyReceived) <= index && Date.now() < end) await Bun.sleep(5);
      }
      socket.close();
    } finally {
      await server.stop(true);
      globalThis.WebSocket = saved.ws;
      globalThis.fetch = saved.fetch;
    }
    const proxyUpstream = FakeWebSocket.dials.length > 0 ? FakeWebSocket.dials.map(dial => JSON.parse(dial.frames[0]!) as Rec) : ocxHttp;

    // The Worker's session.
    replyQueue = [toolTurn(), second()];
    FakeWebSocket.dials = [];
    const workerHttp: Rec[] = [];
    const workerReceived: string[] = [];
    const relayed: string[] = [];
    const declines: string[] = [];
    const session = createNativeWsSession({
      send: text => workerReceived.push(text),
      close: () => {},
      openContainer: () => ({ send: text => relayed.push(text), close: () => {} }),
    }, new Headers(callerHeaders()), {
      readConfig: async () => config,
      readCodexAccounts: async () => undefined,
      isAdmissionSecret,
      nativeOpenAiFacts: async () => facts(),
      openUpstreamSocket: (url, headers) => new FakeWebSocket(url, { headers }) as unknown as WebSocket,
      fetch: async request => { workerHttp.push(await request.json() as Rec); return sseReply(); },
      onDecline: reason => declines.push(reason),
    });
    for (const [index, frame] of frames.entries()) {
      session.receive(JSON.stringify(frame));
      const end = Date.now() + 3_000;
      while (turnsDone(workerReceived) <= index && Date.now() < end) await Bun.sleep(5);
    }
    const workerUpstream = FakeWebSocket.dials.length > 0 ? FakeWebSocket.dials.map(dial => JSON.parse(dial.frames[0]!) as Rec) : workerHttp;

    expect(declines).toEqual([]);
    expect(relayed).toEqual([]);
    expect(proxyUpstream).toHaveLength(2);
    // The second request carries the first turn's input and output ahead of the new tool result.
    expect((proxyUpstream[1]!.input as unknown[]).length).toBe(4);
    expect(workerUpstream).toEqual(proxyUpstream);
    expect(workerReceived.map(normalize)).toEqual(proxyReceived.map(normalize));
  }, 20_000);

  test("a chained frame the Worker cannot serve reaches ocx with the history ocx never saw", async () => {
    replyQueue = [textTurn()];
    const relayed: string[] = [];
    const received: string[] = [];
    const session = createNativeWsSession({
      send: text => received.push(text),
      close: () => {},
      openContainer: () => ({ send: text => relayed.push(text), close: () => {} }),
    }, new Headers(callerHeaders()), {
      readConfig: async () => JSON.stringify({ ...(getDefaultConfig() as unknown as Rec), port: 0, websockets: true }),
      readCodexAccounts: async () => undefined,
      isAdmissionSecret,
      nativeOpenAiFacts: async () => facts(),
      openUpstreamSocket: (url, headers) => new FakeWebSocket(url, { headers }) as unknown as WebSocket,
      fetch: async () => sseReply(),
    });
    session.receive(JSON.stringify({ type: "response.create", ...codexTurn() }));
    const end = Date.now() + 3_000;
    while (!received.some(frame => frame.includes("\"response.completed\"")) && Date.now() < end) await Bun.sleep(5);
    // Max effort is clamped by ocx's catalog, so this frame goes to ocx.
    const next = { type: "response.create", ...codexTurn({ previous_response_id: "resp_up1", reasoning: { effort: "max" }, input: [{ type: "message", role: "user", content: [{ type: "input_text", text: "Again." }] }] }) };
    session.receive(JSON.stringify(next));
    while (relayed.length === 0 && Date.now() < end) await Bun.sleep(5);
    const sent = JSON.parse(relayed[0]!) as Rec;
    expect(sent.previous_response_id).toBeUndefined();
    expect(sent.type).toBe("response.create");
    const input = sent.input as Rec[];
    // The first turn's input and output, then the new message.
    expect(input.map(item => item.type ?? item.role)).toEqual(["message", "message", "message"]);
    expect((input[1] as { id?: string }).id).toBe("msg_up1");
  });

  test.if(transport === "sse")("an upstream 5xx after the send: ocx's answer and row, and no second send through ocx", async () => {
    events = textTurn();
    failStatus = 503;
    try {
      const proxy = await throughOcx(codexTurn());
      const worker = await throughWorker(codexTurn(), proxy.config);
      expect(worker.declines).toEqual([]);
      expect(worker.status).toBe(proxy.status);
      expect(worker.text).toBe(proxy.text);
      expect(worker.row?.status).toBe(proxy.row?.status);
      // A refused create generated nothing, so ocx sends it itself and runs its own recovery.
      failStatus = 400;
      const refused = await throughWorker(codexTurn(), proxy.config);
      expect(refused.sent).toBeDefined();
      expect(refused.declines).toEqual(["responses:upstream-400"]);
    } finally {
      failStatus = undefined;
    }
  });

  test.if(transport === "sse")("after the send, a turn that may already be running is never handed to ocx to send again", async () => {
    const config = JSON.stringify({ ...(getDefaultConfig() as unknown as Rec), port: 0 });
    const run = async (fetchImpl: (request: Request) => Promise<Response>) => {
      const declines: string[] = [];
      let sends = 0;
      const rows: WorkerUsageRow[] = [];
      const res = await serveNativeResponses(JSON.stringify(codexTurn()), new Headers(callerHeaders()), new AbortController().signal, {
        readConfig: async () => config, readCodexAccounts: async () => undefined, isAdmissionSecret,
        nativeOpenAiFacts: async () => facts(),
        fetch: async request => { sends++; return fetchImpl(request); },
        onDecline: reason => declines.push(reason), recordUsage: row => rows.push(row),
      });
      return { declines, sends, status: res?.status, text: await res?.text(), rows };
    };
    // A connection reset under the request: the retry ladder refuses to resend it, and so does the Worker.
    const reset = await run(async () => { throw Object.assign(new Error("socket hang up"), { code: "ECONNRESET" }); });
    expect(reset.declines).toEqual([]);
    expect(reset.sends).toBe(1);
    expect(reset.status).toBe(429);
    expect(reset.rows.map(row => row.status)).toEqual([429]);
    // A 2xx that is not an event stream is relayed as it came, as ocx does.
    const json = await run(async () => new Response(JSON.stringify({ id: "resp_x", status: "completed" }), { headers: { "content-type": "application/json" } }));
    expect(json.declines).toEqual([]);
    expect(json.status).toBe(200);
    expect(JSON.parse(json.text!)).toEqual({ id: "resp_x", status: "completed" });
  });

  test("a routed turn with hosted web search: ocx drops the tool without a stored login, and so does the Worker", async () => {
    const chatReply = () => new Response([
      `data: ${JSON.stringify({ choices: [{ index: 0, delta: { role: "assistant", content: "ok" } }] })}\n\n`,
      `data: ${JSON.stringify({ choices: [{ index: 0, delta: {}, finish_reason: "stop" }], usage: { prompt_tokens: 9, completion_tokens: 1 } })}\n\n`,
      "data: [DONE]\n\n",
    ].join(""), { headers: { "content-type": "text/event-stream" } });
    const routedTurn = codexTurn({ model: "z/m-1", tools: [shell, { type: "web_search", external_web_access: false }], reasoning: undefined, include: undefined });
    delete routedTurn.reasoning;
    delete routedTurn.include;
    const chat = { adapter: "openai-chat", apiKey: "sk-z", models: ["m-1"] };
    // ocx, with the openai row `ocx init` writes and no Codex login in its CODEX_HOME.
    let sent: Rec | undefined;
    const upstream = Bun.serve({ port: 0, async fetch(req) { sent = await req.json() as Rec; return chatReply(); } });
    const init = getDefaultConfig() as unknown as Rec;
    saveConfig({ ...init, port: 0, providers: { ...(init.providers as Rec), z: { ...chat, baseUrl: `${upstream.url.toString().replace(/\/$/, "")}/v1`, allowPrivateNetwork: true } } } as never);
    const server = startServer(0);
    let proxyText = "";
    try {
      const res = await saved.fetch(`http://127.0.0.1:${server.port}/v1/responses`, { method: "POST", headers: callerHeaders(), body: JSON.stringify(routedTurn) });
      proxyText = await res.text();
    } finally {
      await server.stop(true);
      await upstream.stop(true);
    }
    const workerConfig = JSON.stringify({ ...init, port: 0, providers: { ...(init.providers as Rec), z: { ...chat, baseUrl: "https://api.example.test/v1" } } });
    const run = async (published: NativeOpenAiFacts | undefined, accounts?: string) => {
      let workerSent: Rec | undefined;
      const declines: string[] = [];
      const res = await serveNativeResponses(JSON.stringify(routedTurn), new Headers(callerHeaders()), new AbortController().signal, {
        readConfig: async () => workerConfig, readCodexAccounts: async () => accounts, nativeOpenAiFacts: async () => published,
        fetch: async request => { workerSent = await request.json() as Rec; return chatReply(); },
        onDecline: reason => declines.push(reason),
      });
      return { declines, sent: workerSent, text: await res?.text() };
    };
    const worker = await run(facts());
    expect(worker.declines).toEqual([]);
    expect(worker.sent).toEqual(sent);
    expect((sent!.tools as Rec[]).map(tool => (tool.function as Rec).name)).toEqual(["shell"]);
    expect(normalize(worker.text!).replace(/"(resp|msg|fc|rs|item)_[A-Za-z0-9_-]+"/g, "\"$1_ID\"")).toBe(normalize(proxyText).replace(/"(resp|msg|fc|rs|item)_[A-Za-z0-9_-]+"/g, "\"$1_ID\""));
    // With a login to search with, ocx runs the search on an account it selects.
    expect((await run(facts({ mainCodexLoginPresent: true }))).declines).toEqual(["responses:web-search-sidecar"]);
    expect((await run(facts(), JSON.stringify({ accounts: [{ id: "a1" }] }))).declines).toEqual(["responses:web-search-sidecar"]);
    expect((await run(undefined)).declines).toEqual(["responses:web-search-facts-unpublished"]);
  });

  test("every model ocx gates by account is declined by name", () => {
    for (const model of [...ACCOUNT_GATED_NATIVE_OPENAI_MODELS, ...CODEX_ACCOUNT_GATED_CANONICAL_WIRE_MODELS.keys()]) {
      expect([model, nativeOpenAiDeclineReason(codexTurn({ model }), new Headers(callerHeaders()))]).toEqual([model, "native-model"]);
    }
  });

  test("leaves to ocx what its own state decides, before anything is sent", async () => {
    events = textTurn();
    const config = JSON.stringify({ ...(getDefaultConfig() as unknown as Rec), port: 0 });
    const declines = async (body: Rec, extra: Parameters<typeof throughWorker>[2] = {}) => {
      const worker = await throughWorker(body, config, extra);
      expect(worker.sent).toBeUndefined();
      return worker.declines;
    };
    expect(await declines(codexTurn(), { accounts: JSON.stringify({ accounts: [{ id: "a1" }] }) })).toEqual(["responses:codex-accounts"]);
    expect(await declines(codexTurn(), { facts: undefined })).toEqual(["responses:native-facts-unpublished"]);
    // Only a caller holding the main login ocx observed; another account is still served.
    const mainKey = createHash("sha256").update("opencodex-main-quota-v1\0").update("acct-caller").digest("hex");
    expect(await declines(codexTurn(), { facts: facts({ mainAccountIdentityKey: mainKey }) })).toEqual(["responses:main-account"]);
    expect(await declines(codexTurn(), { facts: facts({ codexAccountsStored: true }) })).toEqual(["responses:codex-accounts"]);
    expect(await declines(codexTurn(), { facts: facts({ inputCeilings: {} }) })).toEqual(["responses:input-ceiling-unknown"]);
    expect(await declines(codexTurn(), { facts: facts({ inputCeilings: { "gpt-5.5": 10 } }) })).toEqual(["responses:input-admission"]);
    // A bearer that is one of the hub's own keys never leaves for chatgpt.com.
    expect(await declines(codexTurn(), { secret: () => true })).toEqual(["responses:caller-login"]);
    expect(await declines(codexTurn(), { headers: { authorization: "Bearer sk-an-api-key" } })).toEqual(["responses:caller-login"]);
    expect((await throughWorker(codexTurn(), JSON.stringify({ ...JSON.parse(config), apiKeys: [{ id: "k1", key: "x" }] }))).declines).toEqual(["responses:configured-api-keys"]);
    // A part keyed in any order is found.
    expect(await declines(codexTurn({ input: [{ type: "message", role: "user", content: [{ encrypted_content: "x", type: "encrypted_content" }] }] }))).toEqual(["responses:encrypted-content-part"]);
    expect(await declines(codexTurn(), { facts: facts({ nativeMainTrafficBlocked: true }) })).toEqual(["responses:native-main-blocked"]);
    expect(await declines(codexTurn(), { facts: facts({ contextRelayActive: true }) })).toEqual(["responses:context-relay"]);
    expect(await declines(codexTurn({ previous_response_id: "resp_1" }))).toEqual(["responses:body-fields:previous_response_id"]);
    expect(await declines(codexTurn({ reasoning: { effort: "max" } }))).toEqual(["responses:native-effort-clamp"]);
    expect(await declines(codexTurn({ service_tier: "priority" }))).toEqual(["responses:body-fields:service_tier"]);
    expect(await declines(codexTurn({ model: "gpt-daybreak-blue-latest" }))).toEqual(["responses:native-model"]);
    // The hub's key as the bearer asks ocx for the stored main login instead of the caller's.
    expect(await declines(codexTurn(), { headers: { authorization: "Bearer hub-data-token", "x-opencodex-api-key": "" } })).toEqual(["responses:caller-login"]);
    expect(await declines(codexTurn(), { headers: { "x-openai-subagent": "collab_spawn" } })).toEqual(["responses:collaboration-turn"]);
    const pooled = JSON.parse(config) as { providers: Record<string, Rec> };
    pooled.providers.openai = { ...pooled.providers.openai, codexAccountMode: "direct" };
    expect((await throughWorker(codexTurn(), JSON.stringify(pooled))).declines).toEqual(["responses:openai-row"]);
  });
});

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

describe("what a ChatGPT passthrough turn reads from ocx's process", () => {
  const BOOT_ID = "0123456789abcdef0123456789abcdef";
  const noBucket: StateBucket = { get: async () => null, put: async () => {}, delete: async () => {}, list: async () => [] };
  let codexHome: IsolatedCodexHome | null = null;
  const saved = { bootId: process.env[DURABLE_STATE_BOOT_ID_ENV], flag: process.env.OCX_WORKER_NATIVE_STATE };
  beforeEach(() => { codexHome = installIsolatedCodexHome("ocx-worker-openai-facts-"); });
  afterEach(() => {
    setDurableMirrorTransportForTests(null);
    onMainQuotaCredentialChange(undefined);
    clearMainAccountInfoCache();
    codexHome?.restore();
    for (const [name, value] of [[DURABLE_STATE_BOOT_ID_ENV, saved.bootId], ["OCX_WORKER_NATIVE_STATE", saved.flag]] as const) {
      if (value === undefined) delete process.env[name]; else process.env[name] = value;
    }
  });

  test("this process's answers, published under the stamp and read only while its boot holds the lease", async () => {
    expect(nativeOpenAiFacts()).toMatchObject({ mainAccountIdentityKey: null, codexAccountsStored: false, contextRelayActive: false, upstreamTransport: transport });
    expect(nativeOpenAiFacts().inputCeilings["gpt-5.5"]).toBeGreaterThan(0);
    // A Codex login in CODEX_HOME is what ocx's web-search sidecar would search with.
    expect(nativeOpenAiFacts().mainCodexLoginPresent).toBe(false);
    // A refresh token alone is a login ocx refreshes and searches with.
    writeFileSync(join(resolveCodexHomeDir(), "auth.json"), JSON.stringify({ tokens: { refresh_token: "r" } }));
    expect(nativeOpenAiFacts().mainCodexLoginPresent).toBe(true);
    writeFileSync(join(resolveCodexHomeDir(), "auth.json"), JSON.stringify({ tokens: { access_token: CALLER_TOKEN, refresh_token: "r", account_id: "acct-main" } }));
    expect(nativeOpenAiFacts().mainCodexLoginPresent).toBe(true);
    const hub = new LeaseState(memoryStorage());
    await hub.acquireLease(BOOT_ID);
    process.env[DURABLE_STATE_BOOT_ID_ENV] = BOOT_ID;
    process.env.OCX_WORKER_NATIVE_STATE = "1";
    setDurableMirrorTransportForTests({
      origin: "http://state.ocx.internal", sleep: async () => {}, schedule: () => ({ cancel: () => {} }),
      fetch: async (url, init) => handleStateRequest(new Request(url, init), hub, noBucket, "ns", "stamp-1"),
    });
    await publishNativeOpenAiFactsForWorker();
    const published = await hub.nativeOpenAiFactsRead("stamp-1") as NativeOpenAiFacts;
    expect({ ...published, version: 0 }).toEqual({ ...nativeOpenAiFacts(), version: 0 });
    expect(await hub.nativeOpenAiFactsRead("stamp-2")).toBeUndefined();
    // Asleep, the next boot restores the same state: the answers stand. Another boot holding the
    // lease has not said yet what it holds.
    await hub.releaseLease(BOOT_ID);
    expect(await hub.nativeOpenAiFactsRead("stamp-1")).toBeDefined();
    await hub.acquireLease("fedcba9876543210fedcba9876543210");
    expect(await hub.nativeOpenAiFactsRead("stamp-1")).toBeUndefined();
  });

  test("observing a main credential republishes at once, and the route takes only the facts' own shape", async () => {
    let changes = 0;
    onMainQuotaCredentialChange(() => { changes++; });
    observeMainQuotaIdentity("acct-main");
    observeMainQuotaCredential("main-token", "acct-main");
    expect(changes).toBe(2);
    expect(nativeOpenAiFacts().mainAccountIdentityKey).toBe(createHash("sha256").update("opencodex-main-quota-v1\0").update("acct-main").digest("hex"));
    const hub = new LeaseState(memoryStorage());
    await hub.acquireLease(BOOT_ID);
    const put = (body: unknown) => handleStateRequest(new Request("http://state.ocx.internal/native-openai-facts", {
      method: "PUT", headers: { "x-ocx-boot-id": BOOT_ID }, body: JSON.stringify(body),
    }), hub, noBucket, "ns", "stamp-1");
    expect((await put({ ...facts(), extra: true })).status).toBe(400);
    expect((await put({ ...facts(), upstreamTransport: "carrier-pigeon" })).status).toBe(400);
    expect((await put({ ...facts(), inputCeilings: { "gpt-5.5": "big" } })).status).toBe(400);
    expect((await put({ ...facts(), mainAccountIdentityKey: "acct-main" })).status).toBe(400);
    expect((await put(facts({ version: 5 }))).status).toBe(204);
    // An older publish from the same process arriving late does not replace a newer one.
    expect((await put(facts({ version: 4, codexAccountsStored: true }))).status).toBe(204);
    expect(((await hub.nativeOpenAiFactsRead("stamp-1")) as NativeOpenAiFacts).codexAccountsStored).toBe(false);
  });
});
