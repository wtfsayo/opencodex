import { describe, expect, test } from "bun:test";
import { existsSync, readFileSync } from "node:fs";
import { dirname, join, relative, resolve } from "node:path";
import { nativeChatBodyEligible, resolveNativeChatRoute, serveNativeChat } from "../../src/server/cloudflare-native-chat";
import { routeModel } from "../../src/router";
import type { OcxConfig } from "../../src/types";
import { handleWorkersAi, WORKERS_AI_HOST } from "../../deploy/cloudflare/src/workers-ai";
import { chatAdmitsDataToken } from "../../deploy/cloudflare/src/container-env";
import { repoRoot } from "../helpers/repo-root";

// The Worker bundles this module and everything it reaches, dynamic imports included.
const ENTRY = "src/server/cloudflare-native-chat.ts";
// Supported by the Workers runtime under the nodejs_compat flag set in wrangler.jsonc.
const ALLOWED_NODE = new Set(["node:buffer", "node:crypto"]);
// ocx's stateful owners: config on disk, routing state, logs, credentials, the spend ledger.
const FORBIDDEN_MODULES = [
  "src/config.ts", "src/router.ts", "src/server/request-log.ts", "src/server/lifecycle.ts",
  "src/usage/log.ts", "src/oauth/store.ts", "src/codex/account-store.ts", "src/lib/spend-reservation-ledger.ts", "src/storage/",
];
const MAX_CLOSURE = 80;
const IMPORT_RE = /^\s*import\s+(?!type\b)[^;]*?from\s+["']([^"']+)["']|^\s*import\s+["']([^"']+)["']|^\s*export\s+(?!type\b)[^;]*?from\s+["']([^"']+)["']|\bimport\s*\(\s*["']([^"']+)["']\s*\)/gm;

function closure(entry: string) {
  const root = repoRoot();
  const parent = new Map<string, string | null>([[resolve(root, entry), null]]);
  const queue = [resolve(root, entry)];
  const problems: string[] = [];
  const chain = (file: string) => {
    const out: string[] = [];
    for (let at: string | null | undefined = file; at; at = parent.get(at)) out.unshift(relative(root, at));
    return out.join(" -> ");
  };
  while (queue.length) {
    const file = queue.shift()!;
    const source = readFileSync(file, "utf8").replace(/\/\*[\s\S]*?\*\//g, "").replace(/^\s*\/\/.*$/gm, "");
    if (/\bBun\./.test(source)) problems.push(`Bun.* in ${chain(file)}`);
    for (const match of source.matchAll(IMPORT_RE)) {
      const spec = match[1] ?? match[2] ?? match[3] ?? match[4]!;
      if (!spec.startsWith(".")) {
        if (spec === "bun" || spec.startsWith("bun:") || (spec.startsWith("node:") && !ALLOWED_NODE.has(spec))) problems.push(`${spec} in ${chain(file)}`);
        continue;
      }
      const base = resolve(dirname(file), spec.replace(/\.js$/, ""));
      const target = [`${base}.ts`, join(base, "index.ts")].find(candidate => existsSync(candidate));
      if (!target) {
        problems.push(`unresolved ${spec} in ${chain(file)}`);
        continue;
      }
      if (!parent.has(target)) {
        parent.set(target, file);
        queue.push(target);
      }
    }
  }
  const files = [...parent.keys()].map(file => relative(root, file));
  for (const file of files) {
    const banned = FORBIDDEN_MODULES.find(prefix => file === prefix || (prefix.endsWith("/") && file.startsWith(prefix)));
    if (banned) problems.push(`${file} reached via ${chain(resolve(root, file))}`);
  }
  return { files, problems };
}

describe("Worker-native chat import boundary", () => {
  test("reaches no Bun API, no unsupported node: module, and none of ocx's stateful subsystems", () => {
    const { files, problems } = closure(ENTRY);
    expect(problems).toEqual([]);
    // Growth here is how a stateful import sneaks in; raise the cap only after reading the new edges.
    expect(files.length).toBeLessThanOrEqual(MAX_CLOSURE);
  });

  test("the guard is not vacuous: the container's own chat lane fails it", () => {
    expect(closure("src/server/chat-native.ts").problems.length).toBeGreaterThan(0);
  });
});

const provider = { adapter: "openai-chat", baseUrl: "https://api.example.test/v1", apiKey: "sk-literal", models: ["m-1", "vendor/m-2"] };
const config = (extra: Record<string, unknown> = {}, providerExtra: Record<string, unknown> = {}) =>
  ({ port: 10100, hostname: "0.0.0.0", defaultProvider: "p", providers: { p: { ...provider, ...providerExtra } }, ...extra });

describe("Worker-native chat routing", () => {
  test("agrees with ocx's router whenever it resolves a route", () => {
    for (const model of ["p/m-1", "p/vendor/m-2"]) {
      const cfg = config();
      const native = resolveNativeChatRoute(cfg, model);
      expect(native).not.toBeNull();
      const route = routeModel(cfg as unknown as OcxConfig, model);
      expect([route.providerName, route.modelId, route.provider.adapter, route.provider.baseUrl])
        .toEqual([native!.providerName, native!.modelId, "openai-chat", native!.provider.baseUrl]);
      expect(route.combo).toBeUndefined();
    }
  });

  test("declines everything outside the subset it reproduces", () => {
    const declined: [unknown, unknown][] = [
      [config(), "m-1"],
      [config(), "p/unlisted"],
      [config(), "other/m-1"],
      [config(), "policy/x"],
      [config(), "combo/x"],
      [config({ combos: { x: { targets: [] } } }), "p/m-1"],
      [config({ routingProfiles: { x: {} } }), "p/m-1"],
      [config({ codexAccountNamespaces: { a: "b" } }), "p/m-1"],
      [config({ spend: { root: { maxTokens: 1 } } }), "p/m-1"],
      [config({}, { headers: { "x-a": "b" } }), "p/m-1"],
      [config({}, { alias: "q" }), "p/m-1"],
      [config({}, { adapter: "anthropic" }), "p/m-1"],
      [config({}, { authMode: "oauth" }), "p/m-1"],
      [config({}, { apiKey: "${OPENAI_KEY}" }), "p/m-1"],
      [config({}, { apiKey: "$OPENAI_KEY" }), "p/m-1"],
      [config({}, { apiKey: "keychain:p" }), "p/m-1"],
      [config({}, { disabled: true }), "p/m-1"],
      // Sections a denylist once missed, and any key this path does not know.
      [config({ blockedModelRedirects: { "m-1": "m-2" } }), "p/m-1"],
      [config({ apiSurfaces: { chat: false } }), "p/m-1"],
      [config({ maxInboundBodyBytes: 1024 }), "p/m-1"],
      [config({ someFutureSection: {} }), "p/m-1"],
      // A built-in provider's transport comes from the registry, not its configured baseUrl.
      [{ providers: { deepseek: { ...provider, baseUrl: "https://attacker.example/v1" } } }, "deepseek/m-1"],
      // Destinations ocx refuses, and ones the Worker cannot vet the way ocx does.
      [config({}, { baseUrl: "http://api.example.test/v1" }), "p/m-1"],
      [config({}, { baseUrl: "https://169.254.169.254/v1" }), "p/m-1"],
      [config({}, { baseUrl: "https://[::1]/v1" }), "p/m-1"],
      [config({}, { baseUrl: "https://localhost/v1" }), "p/m-1"],
      [config({}, { baseUrl: "https://ai.ocx.internal/v1" }), "p/m-1"],
      [config({}, { baseUrl: "https://user:pw@api.example.test/v1" }), "p/m-1"],
      [config({}, { baseUrl: "https://localhost./v1" }), "p/m-1"],
      [config({}, { baseUrl: "https://metadata.google.internal./v1" }), "p/m-1"],
      [config({}, { baseUrl: "https://2130706433/v1" }), "p/m-1"],
    ];
    for (const [cfg, model] of declined) expect([model, resolveNativeChatRoute(cfg, model)]).toEqual([model, null]);
  });

  test("takes only streamed, text-only turns the native lane would also take", () => {
    const base = { model: "p/m-1", stream: true, messages: [{ role: "user", content: "hi" }] };
    expect(nativeChatBodyEligible(base)).toBe(true);
    expect(nativeChatBodyEligible({ ...base, tools: [{ type: "function", function: { name: "f" } }] })).toBe(true);
    for (const body of [
      { ...base, stream: false },
      { ...base, store: true },
      { ...base, previous_response_id: "r" },
      { ...base, messages: [{ role: "user", content: [{ type: "image_url", image_url: { url: "data:," } }] }] },
      { ...base, tools: [{ type: "web_search" }] },
    ]) expect(nativeChatBodyEligible(body)).toBe(false);
  });
});

const sse = (lines: string[]) => new Response(lines.join(""), { headers: { "content-type": "text/event-stream" } });
const workersAiConfig = JSON.stringify({ providers: { "workers-ai": { adapter: "openai-chat", baseUrl: `http://${WORKERS_AI_HOST}/v1`, apiKey: "workers-ai-binding", models: ["meta/llama"] } } });
const turn = JSON.stringify({ model: "workers-ai/meta/llama", stream: true, messages: [{ role: "user", content: "Say pong." }] });

describe("Worker-native chat serving", () => {
  test("streams a Workers AI turn through ocx's own relay, labelled with the requested model", async () => {
    const seen: { model?: unknown; messages?: unknown }[] = [];
    const ai = { run: async (model: string, input: Record<string, unknown>) => {
      seen.push({ model, messages: input.messages });
      return sse(["data: {\"response\":\"Po\"}\n\n", "data: {\"response\":\"ng\"}\n\n", "data: [DONE]\n\n"]).body!;
    } };
    const response = await serveNativeChat(turn, new AbortController().signal, {
      readConfig: async () => workersAiConfig,
      localHosts: { [WORKERS_AI_HOST]: request => handleWorkersAi(request, ai) },
      fetch: async () => { throw new Error("no network in this test"); },
    });
    expect(response?.headers.get("content-type")).toBe("text/event-stream");
    const text = await response!.text();
    const chunks = text.split("\n\n").filter(block => block.startsWith("data: {")).map(block => JSON.parse(block.slice(6)));
    expect(chunks.map(chunk => chunk.choices?.[0]?.delta?.content ?? "").join("")).toBe("Pong");
    expect(new Set(chunks.map(chunk => chunk.model))).toEqual(new Set(["workers-ai/meta/llama"]));
    expect(text.trimEnd().endsWith("data: [DONE]")).toBe(true);
    expect(seen).toEqual([{ model: "@cf/meta/llama", messages: [{ role: "user", content: "Say pong." }] }]);
  });

  test("hands the turn to the container when it cannot serve it, before sending anything", async () => {
    const neverCalled = { readConfig: async () => workersAiConfig, fetch: async () => { throw new Error("unexpected upstream call"); } };
    const signal = new AbortController().signal;
    expect(await serveNativeChat("not json", signal, neverCalled)).toBeNull();
    expect(await serveNativeChat(JSON.stringify({ ...JSON.parse(turn), stream: false }), signal, neverCalled)).toBeNull();
    expect(await serveNativeChat(turn, signal, { ...neverCalled, readConfig: async () => undefined })).toBeNull();
    // An upstream error is left to the container, which owns retries and error shaping.
    const failing = await serveNativeChat(turn, signal, {
      readConfig: async () => workersAiConfig,
      localHosts: { [WORKERS_AI_HOST]: async () => Response.json({ error: { message: "busy" } }, { status: 503 }) },
      fetch: neverCalled.fetch,
    });
    expect(failing).toBeNull();
  });

  test("an external provider is called with its literal key", async () => {
    const config = JSON.stringify({ providers: { p: { ...provider } } });
    let authorization: string | null = null;
    let url = "";
    const response = await serveNativeChat(JSON.stringify({ model: "p/m-1", stream: true, messages: [{ role: "user", content: "hi" }] }), new AbortController().signal, {
      readConfig: async () => config,
      fetch: async request => {
        authorization = request.headers.get("authorization");
        url = request.url;
        return sse(["data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\n", "data: [DONE]\n\n"]);
      },
    });
    expect(await response!.text()).toContain("\"ok\"");
    expect([url, authorization]).toEqual(["https://api.example.test/v1/chat/completions", "Bearer sk-literal"]);
  });
});

describe("Worker-native chat admission", () => {
  const env = { OPENCODEX_API_AUTH_TOKEN: "data-token" };
  const req = (headers: Record<string, string>) => new Request("https://hub.test/v1/chat/completions", { method: "POST", headers });

  test("admits only what ocx's chat admission would: the dedicated header, else the bearer", async () => {
    expect(await chatAdmitsDataToken(req({ "x-opencodex-api-key": "data-token" }), env)).toBe(true);
    expect(await chatAdmitsDataToken(req({ authorization: "Bearer data-token" }), env)).toBe(true);
    // The edge accepts these; ocx refuses them on chat, so the Worker must not serve them.
    expect(await chatAdmitsDataToken(req({ "x-api-key": "data-token" }), env)).toBe(false);
    expect(await chatAdmitsDataToken(req({ "sec-websocket-protocol": "opencodex-key.ZGF0YS10b2tlbg" }), env)).toBe(false);
    expect(await chatAdmitsDataToken(req({ "x-opencodex-api-key": "junk", authorization: "Bearer data-token" }), env)).toBe(false);
    expect(await chatAdmitsDataToken(req({}), env)).toBe(false);
  });
});

