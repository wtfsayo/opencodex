import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { saveConfig } from "../../src/config";
import { startServer } from "../../src/server";
import { DURABLE_STATE_BOOT_ID_ENV, setDurableMirrorTransportForTests } from "../../src/lib/durable-mirror";
import { resetReasoningMetadataCachesForTests } from "../../src/providers/reasoning-metadata";
import { serveNativeResponses } from "../../src/server/cloudflare-native-responses";
import { LeaseState, type LeaseStorage } from "../../deploy/cloudflare/src/lease";
import { handleStateRequest, type StateBucket } from "../../deploy/cloudflare/src/state-routes";
import type { OcxConfig } from "../../src/types";
import { removeTreeWithRetry } from "../helpers/remove-tree";

const BOOT_ID = "0123456789abcdef0123456789abcdef";
const KEY = ["zen", "key", "for", "tests"].join("-");
const zen = { adapter: "openai-chat", baseUrl: "https://opencode.ai/zen/v1", apiKey: KEY, models: ["m-1", "m-2"] };
const noBucket: StateBucket = { get: async () => null, put: async () => {}, delete: async () => {}, list: async () => [] };
const reply = () => new Response([
  `data: ${JSON.stringify({ choices: [{ index: 0, delta: { role: "assistant", content: "ok" } }] })}\n\n`,
  `data: ${JSON.stringify({ choices: [{ index: 0, delta: {}, finish_reason: "stop" }] })}\n\n`,
  "data: [DONE]\n\n",
].join(""), { headers: { "content-type": "text/event-stream" } });
const turn = (effort: string, model = "z/m-1") => ({ model, input: "hi", store: false, stream: true, reasoning: { effort } });

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

describe("Worker-native effort for models.dev destinations", () => {
  let home = "";
  let hub: LeaseState;
  const saved = { home: process.env.OPENCODEX_HOME, bootId: process.env[DURABLE_STATE_BOOT_ID_ENV] };
  const originalFetch = globalThis.fetch;

  beforeEach(async () => {
    home = mkdtempSync(join(tmpdir(), "ocx-worker-effort-"));
    process.env.OPENCODEX_HOME = home;
    process.env[DURABLE_STATE_BOOT_ID_ENV] = BOOT_ID;
    resetReasoningMetadataCachesForTests();
    hub = new LeaseState(memoryStorage());
    await hub.acquireLease(BOOT_ID);
    setDurableMirrorTransportForTests({
      origin: "http://state.ocx.internal", sleep: async () => {}, schedule: () => ({ cancel: () => {} }),
      fetch: async (url, init) => handleStateRequest(new Request(url, init), hub, noBucket, "ns"),
    });
    // ocx's two caches: models.dev publishes low/high/max for m-1, and this key had max refused.
    writeFileSync(join(home, "reasoning-metadata-cache.json"), JSON.stringify({
      version: 2, fetchedAt: Date.now(), source: "https://models.dev/api.json",
      providers: { opencode: { "m-1": { reasoning: true, options: [{ type: "effort", values: ["low", "high", "max"] }] } } },
    }));
    const credential = createHash("sha256").update(KEY).digest("hex");
    writeFileSync(join(home, "reasoning-support-cache.json"), JSON.stringify({
      version: 2, rows: { [JSON.stringify(["opencode", credential, "m-1", "max"])]: { effort: "max", at: Date.now() } },
    }));
  });

  afterEach(() => {
    globalThis.fetch = originalFetch;
    setDurableMirrorTransportForTests(null);
    resetReasoningMetadataCachesForTests();
    for (const [name, value] of [["OPENCODEX_HOME", saved.home], [DURABLE_STATE_BOOT_ID_ENV, saved.bootId]] as const) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
    removeTreeWithRetry(home);
  });

  test("maps each effort exactly as ocx does, from the caches ocx published", async () => {
    const efforts = ["low", "medium", "high", "xhigh", "max", "ultra"];
    const sent: unknown[] = [];
    globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
      const request = new Request(input, init);
      const host = new URL(request.url).hostname;
      if (host === "opencode.ai") { sent.push((await request.json() as { reasoning_effort?: unknown }).reasoning_effort ?? null); return reply(); }
      if (host === "models.dev") return new Response("no", { status: 503 });
      return originalFetch(input, init);
    }) as typeof fetch;
    saveConfig({ port: 0, providers: { z: zen } } as unknown as OcxConfig);
    const server = startServer(0);
    try {
      for (const effort of efforts) {
        for (const model of ["z/m-1", "z/m-2"]) {
          await originalFetch(`http://127.0.0.1:${server.port}/v1/responses`, {
            method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(turn(effort, model)),
          }).then(response => response.text());
        }
      }
    } finally {
      await server.stop(true);
    }
    const proxySent = sent.splice(0);
    const published = await hub.reasoningMetadataRead();
    expect(Object.keys(published).sort()).toEqual(["snapshot", "support"]);
    const declines: string[] = [];
    for (const effort of efforts) {
      for (const model of ["z/m-1", "z/m-2"]) {
        const response = await serveNativeResponses(JSON.stringify(turn(effort, model)), new Headers(), new AbortController().signal, {
          readConfig: async () => JSON.stringify({ providers: { z: zen } }),
          fetch: async request => { sent.push((await request.json() as { reasoning_effort?: unknown }).reasoning_effort ?? null); return reply(); },
          reasoningMetadata: () => hub.reasoningMetadataRead(),
          onDecline: reason => declines.push(reason),
        });
        await response?.text();
      }
    }
    expect(declines).toEqual([]);
    expect(proxySent).toHaveLength(efforts.length * 2);
    expect(sent).toEqual(proxySent);
    // The refused rung really was avoided, so the comparison covers the learned cache.
    expect(proxySent[efforts.indexOf("max") * 2]).not.toBe("max");
  });

  test("declines until ocx has published, and once the snapshot is old enough for ocx to refresh it", async () => {
    const run = async (reasoningMetadata: () => Promise<{ snapshot?: string; support?: string }>) => {
      const declines: string[] = [];
      const response = await serveNativeResponses(JSON.stringify(turn("high")), new Headers(), new AbortController().signal, {
        readConfig: async () => JSON.stringify({ providers: { z: zen } }),
        fetch: async () => reply(),
        reasoningMetadata,
        onDecline: reason => declines.push(reason),
      });
      await response?.text();
      return declines;
    };
    expect(await run(async () => ({}))).toEqual(["responses:reasoning-metadata-unpublished"]);
    const stale = JSON.stringify({ version: 2, fetchedAt: Date.now() - 25 * 60 * 60 * 1000, source: "x", providers: {} });
    expect(await run(async () => ({ snapshot: stale, support: "{}" }))).toEqual(["responses:reasoning-metadata-stale"]);
    expect(await run(async () => ({ snapshot: "{", support: "{}" }))).toEqual(["responses:reasoning-metadata-unreadable"]);
    // ocx with no snapshot maps as if the destination had no metadata; so does the Worker.
    expect(await run(async () => ({ snapshot: "null", support: "{}" }))).toEqual([]);
  });

  test("the store keeps the newest version from one process and takes any from a new one", async () => {
    const state = new LeaseState(memoryStorage());
    await state.acquireLease(BOOT_ID);
    await state.reasoningMetadataCommit(BOOT_ID, "support", "\"new\"", 5);
    await state.reasoningMetadataCommit(BOOT_ID, "support", "\"late\"", 4);
    expect((await state.reasoningMetadataRead()).support).toBe("\"new\"");
    expect(await state.reasoningMetadataCommit("f".repeat(32), "support", "\"fenced\"", 9)).toBe(false);
    await state.releaseLease(BOOT_ID);
    const next = "a".repeat(32);
    await state.acquireLease(next);
    await state.reasoningMetadataCommit(next, "support", "\"restarted\"", 1);
    expect((await state.reasoningMetadataRead()).support).toBe("\"restarted\"");
    await state.discardSnapshot();
    expect(await state.reasoningMetadataRead()).toEqual({});
  });
});
