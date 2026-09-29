import { describe, expect, test } from "bun:test";
import { existsSync, readFileSync } from "node:fs";
import { dirname, join, relative, resolve } from "node:path";
import { nativeChatBodyEligible, resolveNativeChatRoute, serveNativeChat } from "../../src/server/cloudflare-native-chat";
import { routeModel } from "../../src/router";
import type { OcxConfig } from "../../src/types";
import { handleWorkersAi, WORKERS_AI_HOST } from "../../deploy/cloudflare/src/workers-ai";
import { chatAdmitsDataToken, containerEnv, nativeConfigText } from "../../deploy/cloudflare/src/container-env";
import { LeaseState, MAX_QUEUED_USAGE, type LeaseStorage } from "../../deploy/cloudflare/src/lease";
import { handleStateRequest } from "../../deploy/cloudflare/src/state-routes";
import type { WorkerUsageRow } from "../../src/server/cloudflare-native-chat-api";
import { repoRoot } from "../helpers/repo-root";

// The Worker bundles this module and everything it reaches, dynamic imports included.
const ENTRY = "src/server/cloudflare-native.ts";
// Provided by the Workers runtime under the nodejs_compat flag set in wrangler.jsonc. The bare
// `crypto` and `zlib` forms come from Devin adapter code the provider registry pulls in; the
// deployed Worker loads them, and nothing on this path calls into them.
const ALLOWED_NODE = new Set(["node:buffer", "node:crypto", "node:path", "crypto", "zlib"]);
// Pure-JS packages the Worker bundle inlines: the Responses parser validates requests with zod.
const BUNDLED_PACKAGES = new Set(["zod", "zod/v4"]);
// Files that name Bun only inside a function the Worker never calls. code-mode-shell-input parses
// Codex's code-mode `exec` input with Bun's transpiler; the Worker declines any turn declaring `exec`.
const LAZY_BUN = new Set(["src/responses/code-mode-shell-input.ts"]);
// ocx's stateful owners: config on disk, routing state, logs, credentials, the spend ledger.
const FORBIDDEN_MODULES = [
  "src/config.ts", "src/router.ts", "src/server/request-log.ts", "src/server/lifecycle.ts",
  "src/usage/log.ts", "src/oauth/store.ts", "src/codex/account-store.ts", "src/lib/spend-reservation-ledger.ts", "src/storage/",
];
// The Responses path brings the request parser, the openai-chat adapter and the SSE bridge; the
// Messages path adds the Anthropic translators (src/claude/inbound.ts, outbound.ts), 21 files; the
// effort caches' pure reads and OpenCode Go's session header add three more; the WebSocket session
// and the frame, framing and limit helpers it shares with ocx's socket, seven more; the anthropic
// adapter and the layers every registered adapter gets, eleven more; a built-in provider's routed
// config (model policy, registry merge) and ocx's usage labels, eight more; a ChatGPT passthrough
// turn (the Responses passthrough adapter, the ChatGPT WebSocket exchange, the eager relay and the
// passthrough client rewrites), 38 more; ocx's SSE inspector and the continuation replay a
// WebSocket session keeps for its own native responses, three more; ocx's failure answers for that
// turn (the passthrough error body, transport-failure text, request-log terminal status), seven more;
// the translated Chat image budget, one more; ocx's routed-failure and Claude-failure answers
// (the routed error body, the Anthropic error reshaping, the error-text reader), five more; the
// Responses lane's continuation provenance and persistability guards, two more; the hub-ledger
// spend reservation (the tracker and the ledger core it shares with the DO), two more.
const MAX_CLOSURE = 248;
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
    if (/\bBun\b/.test(source) && !LAZY_BUN.has(relative(root, file))) problems.push(`Bun in ${chain(file)}`);
    if (/\brequire\s*\(|\bimport\s+\w+\s*=\s*require\b|\bimport\s*\(\s*`/.test(source)) problems.push(`require() or a computed import in ${chain(file)}`);
    // Workers refuse random values, timers and I/O outside a request, and fail the whole deploy on
    // it (a module-scope randomBytes(32) in reasoning-replay-cache.ts did). Top-level statements only.
    for (const line of source.split("\n")) {
      if (/^(export\s+)?(const|let|var)\s[^=]*=[^>]*\b(randomBytes|randomUUID|getRandomValues|random|setTimeout|setInterval|fetch)\s*\(/.test(line)
        || /^(setTimeout|setInterval|queueMicrotask|fetch)\s*\(/.test(line)) {
        problems.push(`module-scope random, timer or I/O in ${chain(file)}: ${line.trim().slice(0, 80)}`);
      }
    }
    for (const match of source.matchAll(IMPORT_RE)) {
      const spec = match[1] ?? match[2] ?? match[3] ?? match[4]!;
      // Every package, bare built-in ("fs") and node: module is refused unless listed: the Worker
      // bundle has no node_modules of its own to fall back on.
      if (!spec.startsWith(".")) {
        if (!ALLOWED_NODE.has(spec) && !BUNDLED_PACKAGES.has(spec)) problems.push(`${spec} in ${chain(file)}`);
        continue;
      }
      // package.json, for its version, is the one JSON module the bundle may inline.
      if (spec.endsWith(".json")) {
        if (relative(root, resolve(dirname(file), spec)) !== "package.json") problems.push(`JSON module ${spec} in ${chain(file)}`);
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

  test("the guard is not vacuous: each rule fires on the container's own chat lane", () => {
    const { problems } = closure("src/server/chat-native.ts");
    expect(problems.some(problem => problem.startsWith("node:fs in "))).toBe(true);
    expect(problems.some(problem => problem.startsWith("bun:sqlite in "))).toBe(true);
    expect(problems.some(problem => problem.startsWith("Bun in "))).toBe(true);
    expect(problems.some(problem => problem.startsWith("src/router.ts reached via"))).toBe(true);
  });
});

const provider = { adapter: "openai-chat", baseUrl: "https://api.example.test/v1", apiKey: "sk-literal", models: ["m-1", "vendor/m-2"] };
// runtimeRole, hub and fastRows are what a Cloudflare hub's own config.json carries.
const config = (extra: Record<string, unknown> = {}, providerExtra: Record<string, unknown> = {}) => ({
  port: 10100, hostname: "0.0.0.0", runtimeRole: "hub", hub: { dataPublicOrigin: "https://hub.example.test" }, fastRows: true,
  defaultProvider: "p", providers: { p: { ...provider, ...providerExtra } }, ...extra,
});

describe("Worker-native chat routing", () => {
  test("agrees with ocx's router whenever it resolves a route", async () => {
    for (const model of ["p/m-1", "p/vendor/m-2"]) {
      const cfg = config();
      const native = await resolveNativeChatRoute(cfg, model);
      expect(native).not.toBeNull();
      const route = routeModel(cfg as unknown as OcxConfig, model);
      expect([route.providerName, route.modelId, route.provider.adapter, route.provider.baseUrl])
        .toEqual([native!.providerName, native!.modelId, "openai-chat", native!.provider.baseUrl]);
      expect(route.combo).toBeUndefined();
    }
  });

  test("declines everything outside the subset it reproduces", async () => {
    const declined: [unknown, unknown][] = [
      [config(), "m-1"],
      [config(), "p/unlisted"],
      [config(), "other/m-1"],
      [config(), "policy/x"],
      [config(), "combo/x"],
      [config({ combos: { x: { targets: [] } } }), "p/m-1"],
      [config({ routingProfiles: { x: {} } }), "p/m-1"],
      [config({ codexAccountNamespaces: { a: "b" } }), "p/m-1"],
      [config({}, { headers: { "x-a": "b" } }), "p/m-1"],
      [config({}, { alias: "q" }), "p/m-1"],
      [config({}, { adapter: "anthropic" }), "p/m-1"],
      [config({}, { authMode: "oauth" }), "p/m-1"],
      // Unset references: ocx would have no key either.
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
      // Synthetic Fast and effort rows are resolved by ocx before routing.
      [config({}, { models: ["m-1", "m-1--fast"] }), "p/m-1--fast"],
      [config({}, { baseUrl: "https://metadata.google.internal./v1" }), "p/m-1"],
      [config({}, { baseUrl: "https://2130706433/v1" }), "p/m-1"],
    ];
    for (const [cfg, model] of declined) expect([model, await resolveNativeChatRoute(cfg, model)]).toEqual([model, null]);
  });

  test("resolves ${NAME} and $NAME keys against the environment ocx would run with", async () => {
    const secrets = { OPENAI_KEY: "sk-from-secret" };
    for (const apiKey of ["${OPENAI_KEY}", "$OPENAI_KEY"]) {
      const route = await resolveNativeChatRoute(config({}, { apiKey }), "p/m-1", new Set(), () => {}, secrets);
      expect(route?.provider.apiKey).toBe("sk-from-secret");
    }
    expect(await resolveNativeChatRoute(config({}, { apiKey: "${OTHER}" }), "p/m-1", new Set(), () => {}, secrets)).toBeNull();
    // The config object itself is not rewritten with the secret.
    const cfg = config({}, { apiKey: "${OPENAI_KEY}" });
    await resolveNativeChatRoute(cfg, "p/m-1", new Set(), () => {}, secrets);
    expect(cfg.providers.p.apiKey).toBe("${OPENAI_KEY}");
  });

  test("takes only text-only turns the native lane would also take", () => {
    const base = { model: "p/m-1", stream: true, messages: [{ role: "user", content: "hi" }] };
    expect(nativeChatBodyEligible(base)).toBe(true);
    expect(nativeChatBodyEligible({ ...base, tools: [{ type: "function", function: { name: "f" } }] })).toBe(true);
    expect(nativeChatBodyEligible({ ...base, stream: false })).toBe(true);
    for (const body of [
      { ...base, stream: "yes" },
      { ...base, messages: [] },
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
    const response = await serveNativeChat(turn, new Headers(), new AbortController().signal, {
      readConfig: async () => workersAiConfig,
      localHosts: { [WORKERS_AI_HOST]: request => handleWorkersAi(request, ai) },
      fetch: async () => { throw new Error("no network in this test"); },
    });
    expect(response?.headers.get("content-type")).toBe("text/event-stream; charset=utf-8");
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
    expect(await serveNativeChat("not json", new Headers(), signal, neverCalled)).toBeNull();
    expect(await serveNativeChat(turn, new Headers(), signal, { ...neverCalled, readConfig: async () => undefined })).toBeNull();
    // An upstream error is left to the container, which owns retries and error shaping.
    const failing = await serveNativeChat(turn, new Headers(), signal, {
      readConfig: async () => workersAiConfig,
      localHosts: { [WORKERS_AI_HOST]: async () => Response.json({ error: { message: "busy" } }, { status: 503 }) },
      fetch: neverCalled.fetch,
    });
    expect(failing).toBeNull();
    // Collaboration turns get ocx's reasoning-effort cap, which this path does not apply.
    const spawn = JSON.stringify({ ...JSON.parse(turn), tools: [{ type: "function", function: { name: "spawn_agent" } }] });
    expect(await serveNativeChat(spawn, new Headers(), signal, neverCalled)).toBeNull();
    expect(await serveNativeChat(turn, new Headers({ "x-openai-subagent": "collab_spawn" }), signal, neverCalled)).toBeNull();
  });

  test("answers non-streamed turns as JSON, and a JSON upstream to a streamed turn as SSE, like chat-native", async () => {
    const ai = { run: async (_model: string, input: Record<string, unknown>) =>
      input.stream ? sse(["data: {\"response\":\"Pong\"}\n\n", "data: [DONE]\n\n"]).body! : { response: "Pong" } };
    const deps = { readConfig: async () => workersAiConfig, localHosts: { [WORKERS_AI_HOST]: (request: Request) => handleWorkersAi(request, ai) }, fetch: async () => { throw new Error("unexpected"); } };
    const plain = await serveNativeChat(JSON.stringify({ ...JSON.parse(turn), stream: false }), new Headers(), new AbortController().signal, deps);
    expect(plain?.headers.get("content-type")).toBe("application/json");
    const completion = await plain!.json() as { choices: { message: { content: string } }[] };
    expect(completion.choices[0]!.message.content).toBe("Pong");

    const config = JSON.stringify({ providers: { p: { ...provider } } });
    const jsonUpstream = await serveNativeChat(JSON.stringify({ model: "p/m-1", stream: true, messages: [{ role: "user", content: "hi" }] }), new Headers(), new AbortController().signal, {
      readConfig: async () => config,
      fetch: async () => Response.json({ id: "c", object: "chat.completion", choices: [{ index: 0, message: { role: "assistant", content: "ok" }, finish_reason: "stop" }] }),
    });
    expect(jsonUpstream?.headers.get("content-type")).toBe("text/event-stream; charset=utf-8");
    expect(await jsonUpstream!.text()).toContain("\"ok\"");
  });

  test("once the upstream has answered 200, a bad body is reported, not sent again through the container", async () => {
    const config = JSON.stringify({ providers: { p: { ...provider } } });
    const answer = async (response: Response) => serveNativeChat(JSON.stringify({ model: "p/m-1", stream: false, messages: [{ role: "user", content: "hi" }] }), new Headers(), new AbortController().signal, {
      readConfig: async () => config,
      fetch: async () => response,
    });
    const malformed = await answer(new Response("not json", { headers: { "content-type": "application/json" } }));
    expect(malformed?.status).toBe(502);
    const empty = await answer(Response.json({ choices: [] }));
    expect(empty?.status).toBe(502);
    // Built at run time so no key-shaped literal sits in the source.
    const fakeKey = ["sk", "a".repeat(40)].join("-");
    const embedded = await answer(Response.json({ error: { message: `quota exceeded for key ${fakeKey}`, type: "insufficient_quota" } }));
    expect(embedded?.status).toBe(502);
    expect(await embedded!.text()).not.toContain(fakeKey);
  });

  test("a redirect is never followed with the key; the turn goes to the container", async () => {
    const config = JSON.stringify({ providers: { p: { ...provider } } });
    let redirect: RequestRedirect | undefined;
    const response = await serveNativeChat(JSON.stringify({ model: "p/m-1", stream: true, messages: [{ role: "user", content: "hi" }] }), new Headers(), new AbortController().signal, {
      readConfig: async () => config,
      fetch: async request => {
        redirect = request.redirect;
        return new Response(null, { status: 307, headers: { location: "https://elsewhere.example.test/v1/chat/completions" } });
      },
    });
    expect([redirect, response]).toEqual(["manual", null]);
  });

  test("an external provider is called with its literal key", async () => {
    const config = JSON.stringify({ providers: { p: { ...provider } } });
    let authorization: string | null = null;
    let url = "";
    const response = await serveNativeChat(JSON.stringify({ model: "p/m-1", stream: true, messages: [{ role: "user", content: "hi" }] }), new Headers(), new AbortController().signal, {
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

describe("Worker-native config and secrets on a Worker-only deployment", () => {
  test("the Durable Object's copy wins; with neither copy nor snapshot, the bootstrap config as seeded", () => {
    const env = { OCX_BOOTSTRAP_CONFIG_JSON: " {\"providers\":{}} " };
    expect(nativeConfigText("{\"stored\":true}", true, env)).toBe("{\"stored\":true}");
    expect(JSON.parse(nativeConfigText(undefined, false, env)!)).toEqual({ usageLedgerMaxBytes: 32 * 1024 * 1024, providers: {}, hostname: "0.0.0.0", port: 10100 });
    expect(nativeConfigText(undefined, false, {})).toBeUndefined();
    // A snapshot holds a config the Worker cannot read; the bootstrap may be long out of date.
    expect(nativeConfigText(undefined, true, env)).toBeUndefined();
    // A bootstrap the container would refuse to boot with is not served from either.
    for (const bad of ["{\"hostname\":\"127.0.0.1\"}", "{\"port\":1}", "{\"usageLedgerMaxBytes\":5}", "[1]", "not json"]) {
      expect(nativeConfigText(undefined, false, { OCX_BOOTSTRAP_CONFIG_JSON: bad })).toBeUndefined();
    }
  });

  test("references resolve only to what the container would receive", () => {
    const env = { OPENCODEX_API_AUTH_TOKEN: "t", OCX_PASSTHROUGH_SECRETS: "OPENAI_KEY,PATH", OPENAI_KEY: "sk", PATH: "/bin", UNLISTED: "x" };
    const secrets = containerEnv(env as never);
    expect(secrets.OPENAI_KEY).toBe("sk");
    expect(secrets.PATH).toBeUndefined();
    expect(secrets.UNLISTED).toBeUndefined();
  });
});

function memoryStorage(): LeaseStorage & { size(): number } {
  const map = new Map<string, unknown>();
  return {
    get: async <T>(key: string) => map.get(key) as T | undefined,
    put: async (key, value) => { map.set(key, value); },
    delete: async key => map.delete(key),
    list: async <T>({ prefix, limit }: { prefix: string; limit: number }) =>
      new Map([...map].filter(([key]) => key.startsWith(prefix)).sort(([a], [b]) => a.localeCompare(b)).slice(0, limit)) as Map<string, T>,
    size: () => map.size,
  };
}

describe("Worker-served usage", () => {
  test("a served turn produces one usage row in ocx's shape", async () => {
    const rows: WorkerUsageRow[] = [];
    const ai = { run: async () => sse(["data: {\"response\":\"Pong\"}\n\n", "data: {\"response\":\"\",\"usage\":{\"prompt_tokens\":7,\"completion_tokens\":2,\"total_tokens\":9}}\n\n", "data: [DONE]\n\n"]).body! };
    const response = await serveNativeChat(turn, new Headers(), new AbortController().signal, {
      readConfig: async () => workersAiConfig,
      localHosts: { [WORKERS_AI_HOST]: request => handleWorkersAi(request, ai) },
      fetch: async () => { throw new Error("unexpected"); },
      recordUsage: row => rows.push(row),
    });
    expect(rows).toEqual([]);
    await response!.text();
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({ provider: "workers-ai", model: "meta/llama", requestedModel: "workers-ai/meta/llama", inboundProtocol: "chat", admissionKind: "environment", status: 200 });
    expect(rows[0]!.requestId).toMatch(/^[0-9a-f-]{36}$/);
  });

  test("the row records how the turn ended: a client cancel is 499, an in-stream error its status", async () => {
    const run = async (upstream: () => Response, consume: (response: Response) => Promise<void>) => {
      const rows: WorkerUsageRow[] = [];
      const config = JSON.stringify({ providers: { p: { ...provider } } });
      const response = await serveNativeChat(JSON.stringify({ model: "p/m-1", stream: true, messages: [{ role: "user", content: "hi" }] }), new Headers(), new AbortController().signal, {
        readConfig: async () => config, fetch: async () => upstream(), recordUsage: row => rows.push(row),
      });
      await consume(response!);
      return rows.map(row => row.status);
    };
    const chunk = "data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"},\"finish_reason\":null}]}\n\n";
    const cancelled = await run(() => new Response(new ReadableStream({ start(controller) { controller.enqueue(new TextEncoder().encode(chunk)); } }), { headers: { "content-type": "text/event-stream" } }),
      async response => { const reader = response.body!.getReader(); await reader.read(); await reader.cancel(); });
    expect(cancelled).toEqual([499]);
    const errored = await run(() => sse([chunk, "data: {\"error\":{\"message\":\"overloaded\",\"type\":\"server_error\",\"code\":503}}\n\n"]),
      async response => { await response.text(); });
    expect(errored).toHaveLength(1);
    expect(errored[0]).not.toBe(200);
  });

  test("the Durable Object queues rows for the lease holder only, caps them, and forgets them on reset", async () => {
    const holder = "a".repeat(32);
    const storage = memoryStorage();
    const hub = new LeaseState(storage, () => 0);
    await hub.acquireLease(holder);
    for (let i = 0; i < 3; i++) await hub.enqueueUsage({ n: i });
    const get = (bootId: string) => handleStateRequest(new Request("http://state.ocx.internal/usage-inbox?limit=2", { headers: { "x-ocx-boot-id": bootId } }), hub, { get: async () => null, put: async () => {}, delete: async () => {}, list: async () => [] }, "ns");
    expect((await get("b".repeat(32))).status).toBe(409);
    const first = await (await get(holder)).json() as { rows: { seq: number; row: { n: number } }[] };
    expect(first.rows.map(item => [item.seq, item.row.n])).toEqual([[1, 0], [2, 1]]);
    expect(await hub.ackUsage(holder, [1, 2])).toBe(true);
    expect((await hub.peekUsage(holder, 10))!.map(item => item.seq)).toEqual([3]);
    await hub.discardSnapshot();
    await hub.acquireLease(holder);
    expect(await hub.peekUsage(holder, 10)).toEqual([]);
    expect(MAX_QUEUED_USAGE).toBeGreaterThan(1000);
  });

  test("past the cap the oldest row is dropped", async () => {
    const holder = "a".repeat(32);
    const storage = memoryStorage();
    const hub = new LeaseState(storage, () => 0);
    await hub.acquireLease(holder);
    await hub.enqueueUsage({ n: "oldest" });
    // As if MAX_QUEUED_USAGE - 1 more had been queued since.
    await storage.put("ocx:usage-seq", MAX_QUEUED_USAGE);
    await hub.enqueueUsage({ n: "newest" });
    const rows = await hub.peekUsage(holder, 10);
    expect(rows!.map(item => (item.row as { n: string }).n)).toEqual(["newest"]);
  });
});

