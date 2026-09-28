import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { getDefaultConfig, saveConfig } from "../../src/config";
import { startServer } from "../../src/server";
import { readRecentUsageEntries } from "../../src/usage/log";
import { serveNativeResponses } from "../../src/server/cloudflare-native-responses";
import { bunSupportsBoundedCodexWsRelay } from "../../src/server/responses/ws-upstream";
import type { NativeOpenAiFacts, WorkerUsageRow } from "../../src/server/cloudflare-native-chat-api";
import { DURABLE_STATE_BOOT_ID_ENV, setDurableMirrorTransportForTests } from "../../src/lib/durable-mirror";
import { nativeOpenAiFacts, publishNativeOpenAiFactsForWorker } from "../../src/server/worker-native-state";
import { observeMainQuotaCredential, observeMainQuotaIdentity, onMainQuotaCredentialChange, clearMainAccountInfoCache } from "../../src/codex/main-account-cache";
import { LeaseState, type LeaseStorage } from "../../deploy/cloudflare/src/lease";
import { handleStateRequest, type StateBucket } from "../../deploy/cloudflare/src/state-routes";
import { installIsolatedCodexHome, type IsolatedCodexHome } from "../helpers/isolated-codex-home";
import { removeTreeWithRetry } from "../helpers/remove-tree";

type Rec = Record<string, unknown>;
type Listener = (event: unknown) => void;

// The ChatGPT backend over its WebSocket: each response.create frame is answered with `events`.
let events: Rec[] = [];
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
    const answer = events;
    queueMicrotask(() => { for (const event of answer) this.emit("message", { data: JSON.stringify(event) }); });
  }
  close() { this.readyState = 3; }
}
// The same backend over HTTP SSE, for a runtime ocx does not dial the WebSocket from.
const sseReply = () => new Response(events.map(e => `event: ${e.type}\ndata: ${JSON.stringify(e)}\n\n`).join(""), {
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
// What Codex CLI sends on its own ChatGPT login, with the hub's key in the dedicated header.
const callerHeaders = () => ({
  "content-type": "application/json", authorization: "Bearer caller-chatgpt-token", "chatgpt-account-id": "acct-caller",
  originator: "codex_cli_rs", "user-agent": "codex_cli_rs/0.157.1", "x-opencodex-api-key": "hub-data-token",
  session_id: "0199aa00-thread", "x-codex-installation-id": "install-1",
});

const transport: NativeOpenAiFacts["upstreamTransport"] = bunSupportsBoundedCodexWsRelay() ? "websocket" : "sse";
const facts = (extra: Partial<NativeOpenAiFacts> = {}): NativeOpenAiFacts => ({
  mainCredentialObserved: false, nativeMainTrafficBlocked: false, contextRelayActive: false, upstreamTransport: transport, reasoningSummarySupport: {}, ...extra,
});

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

  async function throughWorker(body: Rec, config: string, extra: { facts?: NativeOpenAiFacts | undefined; accounts?: string; headers?: Rec } = {}) {
    FakeWebSocket.dials = [];
    let httpSent: Sent | undefined;
    const declines: string[] = [];
    const rows: WorkerUsageRow[] = [];
    const res = await serveNativeResponses(JSON.stringify(body), new Headers({ ...callerHeaders(), ...(extra.headers ?? {}) } as Record<string, string>), new AbortController().signal, {
      readConfig: async () => config,
      readCodexAccounts: async () => extra.accounts,
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
    expect(await declines(codexTurn(), { facts: facts({ mainCredentialObserved: true }) })).toEqual(["responses:main-credential-observed"]);
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
    expect(nativeOpenAiFacts()).toMatchObject({ mainCredentialObserved: false, contextRelayActive: false, upstreamTransport: transport });
    const hub = new LeaseState(memoryStorage());
    await hub.acquireLease(BOOT_ID);
    process.env[DURABLE_STATE_BOOT_ID_ENV] = BOOT_ID;
    process.env.OCX_WORKER_NATIVE_STATE = "1";
    setDurableMirrorTransportForTests({
      origin: "http://state.ocx.internal", sleep: async () => {}, schedule: () => ({ cancel: () => {} }),
      fetch: async (url, init) => handleStateRequest(new Request(url, init), hub, noBucket, "ns", "stamp-1"),
    });
    await publishNativeOpenAiFactsForWorker();
    expect(await hub.nativeOpenAiFactsRead("stamp-1")).toEqual(nativeOpenAiFacts());
    expect(await hub.nativeOpenAiFactsRead("stamp-2")).toBeUndefined();
    // A new process holds the lease: the old one's answers describe a process that is gone.
    await hub.releaseLease(BOOT_ID);
    await hub.acquireLease("fedcba9876543210fedcba9876543210");
    expect(await hub.nativeOpenAiFactsRead("stamp-1")).toBeUndefined();
  });

  test("observing a main credential republishes at once, and the route takes only the facts' own shape", async () => {
    let changes = 0;
    onMainQuotaCredentialChange(() => { changes++; });
    observeMainQuotaIdentity("acct-main");
    observeMainQuotaCredential("main-token", "acct-main");
    expect(changes).toBe(2);
    expect(nativeOpenAiFacts().mainCredentialObserved).toBe(true);
    const hub = new LeaseState(memoryStorage());
    await hub.acquireLease(BOOT_ID);
    const put = (body: unknown) => handleStateRequest(new Request("http://state.ocx.internal/native-openai-facts", {
      method: "PUT", headers: { "x-ocx-boot-id": BOOT_ID }, body: JSON.stringify(body),
    }), hub, noBucket, "ns", "stamp-1");
    expect((await put({ ...facts(), extra: true })).status).toBe(400);
    expect((await put({ ...facts(), upstreamTransport: "carrier-pigeon" })).status).toBe(400);
    expect((await put({ ...facts(), reasoningSummarySupport: { "gpt-5.5": "yes" } })).status).toBe(400);
    expect((await put(facts({ reasoningSummarySupport: { "gpt-5.5": false } }))).status).toBe(204);
  });
});
