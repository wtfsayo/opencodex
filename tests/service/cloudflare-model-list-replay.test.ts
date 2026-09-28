import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { saveConfig } from "../../src/config";
import { startServer } from "../../src/server";
import { clearModelCache } from "../../src/codex/model-cache";
import { DURABLE_STATE_BOOT_ID_ENV, setDurableMirrorTransportForTests } from "../../src/lib/durable-mirror";
import { modelListReplayKey } from "../../src/server/model-list-replay-key";
import { LeaseState, MAX_MODEL_LISTS, MAX_MODEL_LIST_TTL_MS, type LeaseStorage } from "../../deploy/cloudflare/src/lease";
import { handleStateRequest, type StateBucket } from "../../deploy/cloudflare/src/state-routes";
import { containerEnv, modelListStamp } from "../../deploy/cloudflare/src/container-env";
import type { OcxConfig } from "../../src/types";
import { removeTreeWithRetry } from "../helpers/remove-tree";

const BOOT_ID = "0123456789abcdef0123456789abcdef";
const DATA_TOKEN = ["replay", "data", "token"].join("-");
const STAMP = "fingerprint:version";
const noBucket: StateBucket = { get: async () => null, put: async () => {}, delete: async () => {}, list: async () => [] };

function memoryStorage(): LeaseStorage & { keys(): string[] } {
  const map = new Map<string, unknown>();
  return {
    get: async <T>(key: string) => map.get(key) as T | undefined,
    put: async (key, value) => { map.set(key, value); },
    delete: async key => map.delete(key),
    list: async <T>({ prefix, limit }: { prefix: string; limit: number }) =>
      new Map([...map].filter(([key]) => key.startsWith(prefix)).sort(([a], [b]) => a.localeCompare(b)).slice(0, limit)) as Map<string, T>,
    keys: () => [...map.keys()],
  };
}

describe("model list replay key", () => {
  const key = (query: string, headers: Record<string, string> = {}) =>
    modelListReplayKey(new URL(`https://hub.example/v1/models${query}`), new Headers(headers));

  test("is the same for the same inputs in any parameter order, and differs when an input the route reads differs", async () => {
    expect(await key("?a=1&b=2")).toBe((await key("?b=2&a=1"))!);
    expect(await key("?client_version=0.157.1")).not.toBe((await key("?client_version=0.157.2"))!);
    // The route reads only whether the user agent is Claude Code's (Cursor's is never keyed).
    expect(await key("", { "user-agent": "codex/1" })).toBe((await key("", { "user-agent": "openai-sdk/2" }))!);
    expect(await key("", { "user-agent": "claude-code/2" })).not.toBe((await key("", { "user-agent": "codex/1" }))!);
    expect(await key("", { "anthropic-version": "2023-06-01", "user-agent": "claude-code/2" }))
      .not.toBe((await key("", { "user-agent": "claude-code/2" }))!);
    expect(await key("")).toMatch(/^[0-9a-f]{64}$/);
  });

  test("refuses answers that depend on more than the request and the documents", async () => {
    // A browser (CORS and ocx's origin check), Cursor (ocx records its visit), the Claude Desktop
    // shapes (ocx rebuilds its alias registry while answering), and a repeated parameter (the route
    // reads the first value, so order would matter).
    expect(await key("", { origin: "https://evil.example" })).toBeUndefined();
    expect(await key("", { "user-agent": "Cursor/1.2.3" })).toBeUndefined();
    expect(await key("?format=desktop-config")).toBeUndefined();
    expect(await key("", { "anthropic-version": "2023-06-01" })).toBeUndefined();
    expect(await key("?flavor=anthropic&ids=desktop", { "user-agent": "claude-code/2.1.283" })).toBeUndefined();
    expect(await key("?flavor=anthropic&flavor=openai")).toBeUndefined();
    expect(await key("?client_version=1&client_version=2")).toBeUndefined();
    // Claude Code's readable ids and the Codex catalog are fine.
    expect(await key("", { "anthropic-version": "2023-06-01", "user-agent": "claude-code/2.1.283" })).toBeDefined();
    expect(await key("?flavor=anthropic&ids=cli")).toBeDefined();
    expect(await key("?client_version=0.157.1", { "anthropic-version": "2023-06-01" })).toBeDefined();
  });
});

describe("model list store in the Durable Object", () => {
  const seqs = { auth: 0, "codex-accounts": 0, config: 1 };
  const list = { body: "{\"object\":\"list\",\"data\":[]}", headers: [["content-type", "application/json"]] as [string, string][] };

  async function hubAt(time: { now: number }) {
    const state = new LeaseState(memoryStorage(), () => time.now);
    await state.acquireLease(BOOT_ID);
    await state.commitDocument(BOOT_ID, "config", "{}", 1);
    return state;
  }

  test("serves a list only under the same documents, version and environment, until its lifetime runs out", async () => {
    const time = { now: 1_000 };
    const hub = await hubAt(time);
    expect(await hub.modelListCommit(BOOT_ID, "k", list, seqs, 300_000, STAMP)).toBe(true);
    expect(await hub.modelListRead("k", STAMP)).toEqual(list);
    // A deploy or a secret change.
    expect(await hub.modelListRead("k", "other:stamp")).toBeUndefined();
    time.now += 300_000;
    expect(await hub.modelListRead("k", STAMP)).toBeUndefined();
    await hub.modelListCommit(BOOT_ID, "k", list, seqs, 300_000, STAMP);
    await hub.commitDocument(BOOT_ID, "config", "{\"x\":1}", 2);
    expect(await hub.modelListRead("k", STAMP)).toBeUndefined();
    await hub.commitDocument(BOOT_ID, "auth", "{}", 1);
    expect(await hub.modelListRead("k", STAMP)).toBeUndefined();
  });

  test("takes lists only from the lease holder, caps the lifetime, and bounds how many it keeps", async () => {
    const time = { now: 1_000 };
    const hub = await hubAt(time);
    expect(await hub.modelListCommit("f".repeat(32), "k", list, seqs, 1_000, STAMP)).toBe(false);
    await hub.modelListCommit(BOOT_ID, "long", list, seqs, Number.MAX_SAFE_INTEGER, STAMP);
    time.now += MAX_MODEL_LIST_TTL_MS;
    expect(await hub.modelListRead("long", STAMP)).toBeUndefined();
    for (let i = 0; i < MAX_MODEL_LISTS + 5; i++) await hub.modelListCommit(BOOT_ID, `k${i}`, list, seqs, 60_000 + i, STAMP);
    const kept = await Promise.all(Array.from({ length: MAX_MODEL_LISTS + 5 }, (_, i) => hub.modelListRead(`k${i}`, STAMP)));
    expect(kept.filter(Boolean)).toHaveLength(MAX_MODEL_LISTS);
    // The soonest to expire went first.
    expect(kept[0]).toBeUndefined();
    expect(kept.at(-1)).toEqual(list);
  });

  test("a reset forgets every list", async () => {
    const storage = memoryStorage();
    const hub = new LeaseState(storage);
    await hub.acquireLease(BOOT_ID);
    await hub.modelListCommit(BOOT_ID, "k", list, seqs, 60_000, STAMP);
    await hub.discardSnapshot();
    expect(storage.keys().filter(key => key.includes("model-list") || key.includes("document-seq"))).toEqual([]);
  });

  test("the state route validates what it stores and files it under the stamp it was given", async () => {
    const hub = new LeaseState(memoryStorage());
    await hub.acquireLease(BOOT_ID);
    const put = (key: string, body: unknown) => handleStateRequest(new Request(`http://state.ocx.internal/model-lists/${key}`, {
      method: "PUT", headers: { "x-ocx-boot-id": BOOT_ID }, body: JSON.stringify(body),
    }), hub, noBucket, "ns", STAMP);
    const good = { body: "{}", headers: [["content-type", "application/json"]], seqs: { auth: 0, "codex-accounts": 0, config: 0 }, ttlMs: 1_000 };
    expect((await put("a".repeat(64), good)).status).toBe(204);
    expect(await hub.modelListRead("a".repeat(64), STAMP)).toBeDefined();
    expect((await put("not-a-digest", good)).status).toBe(404);
    expect((await put("b".repeat(64), { ...good, seqs: { config: 1 } })).status).toBe(400);
    expect((await put("b".repeat(64), { ...good, headers: [["a"]] })).status).toBe(400);
    expect((await put("b".repeat(64), { ...good, ttlMs: 0 })).status).toBe(400);
    expect((await put("b".repeat(64), { ...good, body: "x".repeat(2_000_000) })).status).toBe(413);
  });

  test("ocx is told to publish only where the Worker replays, and the stamp follows the environment", async () => {
    const env = { OPENCODEX_API_AUTH_TOKEN: DATA_TOKEN, CF_VERSION: { id: "v1" } };
    expect(containerEnv({ ...env, OCX_WORKER_NATIVE: "1" }).OCX_WORKER_MODEL_LISTS).toBe("1");
    expect(containerEnv(env).OCX_WORKER_MODEL_LISTS).toBeUndefined();
    expect(containerEnv({ ...env, OCX_WORKER_NATIVE: "1", OCX_EDGE_KEY_CHECK: "presence" }).OCX_WORKER_MODEL_LISTS).toBeUndefined();
    // Only the Worker sets it: a passthrough secret of that name is refused.
    expect(containerEnv({ ...env, OCX_PASSTHROUGH_SECRETS: "OCX_WORKER_MODEL_LISTS", OCX_WORKER_MODEL_LISTS: "1" } as never).OCX_WORKER_MODEL_LISTS).toBeUndefined();
    const stamp = await modelListStamp(env);
    expect(await modelListStamp({ ...env, CF_VERSION: { id: "v2" } })).not.toBe(stamp);
    expect(await modelListStamp({ ...env, OCX_BOOTSTRAP_CONFIG_JSON: "{}" })).not.toBe(stamp);
    expect(await modelListStamp({ ...env })).toBe(stamp);
  });
});

describe("ocx publishes its /v1/models answers for the Worker", () => {
  let home = "";
  let upstream: ReturnType<typeof Bun.serve> | undefined;
  let upstreamModelsOk = true;
  let documentsDown = false;
  const saved = {
    home: process.env.OPENCODEX_HOME,
    token: process.env.OPENCODEX_API_AUTH_TOKEN,
    bootId: process.env[DURABLE_STATE_BOOT_ID_ENV],
    flag: process.env.OCX_WORKER_MODEL_LISTS,
  };
  const clock = { offset: 0 };
  let hub: LeaseState;
  let published: Promise<Response>[] = [];
  let stateCalls: Promise<Response>[] = [];
  const baseConfig = (upstreamUrl: string, extra: Partial<OcxConfig> = {}) => ({
    port: 0,
    hostname: "0.0.0.0",
    defaultProvider: "p",
    providers: { p: { adapter: "openai-chat", baseUrl: `${upstreamUrl.replace(/\/$/, "")}/v1`, apiKey: "k", models: ["m-1"], allowPrivateNetwork: true } },
    ...extra,
  }) as OcxConfig;

  beforeEach(async () => {
    home = mkdtempSync(join(tmpdir(), "ocx-model-replay-"));
    process.env.OPENCODEX_HOME = home;
    process.env.OPENCODEX_API_AUTH_TOKEN = DATA_TOKEN;
    process.env[DURABLE_STATE_BOOT_ID_ENV] = BOOT_ID;
    process.env.OCX_WORKER_MODEL_LISTS = "1";
    clearModelCache();
    clock.offset = 0;
    upstreamModelsOk = true;
    documentsDown = false;
    hub = new LeaseState(memoryStorage(), () => Date.now() + clock.offset);
    await hub.acquireLease(BOOT_ID);
    published = [];
    stateCalls = [];
    // The container's state requests land on the real routes and store, as they would in the hub.
    setDurableMirrorTransportForTests({
      origin: "http://state.ocx.internal",
      sleep: async () => {},
      schedule: () => ({ cancel: () => {} }),
      fetch: async (url, init) => {
        if (documentsDown && url.includes("/documents/")) return new Response("unavailable", { status: 503 });
        const answer = handleStateRequest(new Request(url, init), hub, noBucket, "ns", STAMP);
        (url.includes("/model-lists/") ? published : stateCalls).push(answer);
        return answer;
      },
    });
    upstream = Bun.serve({
      port: 0,
      fetch: () => upstreamModelsOk
        ? Response.json({ object: "list", data: [{ id: "m-1", object: "model" }] })
        : new Response("down", { status: 500 }),
    });
    saveConfig(baseConfig(upstream.url.toString()));
    await Promise.all(stateCalls);
  });

  afterEach(async () => {
    setDurableMirrorTransportForTests(null);
    clearModelCache();
    await upstream?.stop(true);
    for (const [name, value] of [
      ["OPENCODEX_HOME", saved.home], ["OPENCODEX_API_AUTH_TOKEN", saved.token], [DURABLE_STATE_BOOT_ID_ENV, saved.bootId], ["OCX_WORKER_MODEL_LISTS", saved.flag],
    ] as const) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
    removeTreeWithRetry(home);
  });

  async function modelsThroughOcx(path: string, headers: Record<string, string>) {
    const server = startServer(0);
    try {
      const response = await fetch(new URL(path, `http://127.0.0.1:${server.port}`), { headers });
      const body = await response.text();
      // Publication runs after the answer is returned; let it land.
      for (let i = 0; i < 100 && published.length === 0; i++) await Bun.sleep(10);
      expect((await Promise.all(published)).map(put => put.status).every(status => status === 204)).toBe(true);
      return { response, body };
    } finally {
      await server.stop(true);
    }
  }

  const openaiList = { authorization: `Bearer ${DATA_TOKEN}`, "user-agent": "openai-sdk/1" };
  for (const [name, path, headers] of [
    ["the OpenAI list", "/v1/models", openaiList],
    ["Claude Code's list", "/v1/models", { "x-api-key": DATA_TOKEN, "anthropic-version": "2023-06-01", "user-agent": "claude-code/2.1.283" }],
    ["the Codex catalog", "/v1/models?client_version=0.157.1", { authorization: `Bearer ${DATA_TOKEN}`, "user-agent": "codex_cli_rs/0.157.1" }],
  ] as const) {
    test(`${name}: the Worker would answer with ocx's exact body and headers`, async () => {
      published = [];
      const { response, body } = await modelsThroughOcx(path, headers);
      expect(response.status).toBe(200);
      const key = await modelListReplayKey(new URL(path, "https://hub.example"), new Headers(headers));
      const replay = await hub.modelListRead(key!, STAMP);
      // Compared as booleans: a Codex catalog body is tens of kilobytes.
      expect(replay !== undefined).toBe(true);
      expect(replay!.body === body).toBe(true);
      expect(new Headers(replay!.headers).get("content-type")).toBe(response.headers.get("content-type"));
      expect(body).toContain("m-1");
      // ocx fetched the upstream list just now and would fetch it again once its cache turns stale.
      clock.offset = 5 * 60 * 1000 + 1_000;
      expect(await hub.modelListRead(key!, STAMP)).toBeUndefined();
    });
  }

  test("an answer built on a failed discovery lasts only until ocx would retry it", async () => {
    upstreamModelsOk = false;
    published = [];
    await modelsThroughOcx("/v1/models", openaiList);
    const key = await modelListReplayKey(new URL("/v1/models", "https://hub.example"), new Headers(openaiList));
    expect(await hub.modelListRead(key!, STAMP)).toBeDefined();
    // The failure cooldown is 30 s; ocx fetches again after it, and may answer differently.
    clock.offset = 31_000;
    expect(await hub.modelListRead(key!, STAMP)).toBeUndefined();
  });

  test("nothing is published for Cursor, while a document is ahead of the Durable Object, without the flag, or with native ChatGPT rows", async () => {
    published = [];
    await modelsThroughOcx("/v1/models", { authorization: `Bearer ${DATA_TOKEN}`, "user-agent": "Cursor/1.0" });
    expect(published).toEqual([]);
    delete process.env.OCX_WORKER_MODEL_LISTS;
    await modelsThroughOcx("/v1/models", openaiList);
    expect(published).toEqual([]);
    process.env.OCX_WORKER_MODEL_LISTS = "1";
    // Native rows depend on Codex entitlements ocx resolves from the network: with an OpenAI
    // provider, and with no enabled provider at all (ocx then lists only native rows).
    for (const providers of [
      { ...baseConfig(upstream!.url.toString()).providers, openai: { adapter: "openai-responses", authMode: "forward", baseUrl: "https://chatgpt.com/backend-api/codex" } },
      { p: { ...baseConfig(upstream!.url.toString()).providers.p, disabled: true } },
    ]) {
      saveConfig(baseConfig(upstream!.url.toString(), { providers } as Partial<OcxConfig>));
      await Promise.all(stateCalls);
      await modelsThroughOcx("/v1/models", openaiList);
      expect(published).toEqual([]);
    }
    // A config write the Durable Object has not taken: the local copy stays ahead of it.
    documentsDown = true;
    saveConfig(baseConfig(upstream!.url.toString()));
    await modelsThroughOcx("/v1/models", openaiList);
    expect(published).toEqual([]);
    // Each negative case waits a second for a publish that must not come.
  }, 20_000);
});
