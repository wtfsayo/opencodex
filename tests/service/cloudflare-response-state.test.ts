import { afterEach, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { serveNativeResponses } from "../../src/server/cloudflare-native-responses";
import { LeaseState, type LeaseStorage } from "../../deploy/cloudflare/src/lease";
import { MAX_RESPONSE_STATES, MAX_RESPONSE_STATE_BYTES, RESPONSE_STATE_TTL_MS, ResponseStateStore } from "../../deploy/cloudflare/src/response-state";
import { handleStateRequest } from "../../deploy/cloudflare/src/state-routes";
import {
  clearResponseStateForTests,
  expandPreviousResponseInput,
  ingestRemoteResponseState,
  prefetchRemoteResponseState,
  previousResponseProviderState,
  rememberResponseState,
} from "../../src/responses/state";
import { setDurableMirrorTransportForTests } from "../../src/lib/durable-mirror";

function memoryStorage(): LeaseStorage {
  const map = new Map<string, unknown>();
  return {
    get: async <T>(key: string) => map.get(key) as T | undefined,
    put: async (key, value) => { map.set(key, value); },
    delete: async key => map.delete(key),
    list: async <T>({ prefix, limit }: { prefix: string; limit: number }) =>
      new Map([...map].filter(([key]) => key.startsWith(prefix)).sort(([a], [b]) => a.localeCompare(b)).slice(0, limit)) as Map<string, T>,
  };
}

const sse = (lines: string[]) => new Response(lines.join(""), { headers: { "content-type": "text/event-stream" } });
const provider = { adapter: "openai-chat", baseUrl: "https://api.example.test/v1", apiKey: "sk-literal", models: ["m-1"] };
const config = JSON.stringify({ providers: { p: provider } });
const codexTurn = (extra: Record<string, unknown> = {}) => ({
  model: "p/m-1",
  instructions: "You are a coding agent.",
  input: [{ type: "message", role: "user", content: [{ type: "input_text", text: "Say pong." }] }],
  store: false,
  stream: true,
  ...extra,
});
const chatChunks = [
  "data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Pong\"},\"finish_reason\":\"stop\"}]}\n\n",
  "data: [DONE]\n\n",
];

afterEach(() => {
  delete process.env["OCX_STATE_BOOT_ID"];
  setDurableMirrorTransportForTests(null);
  clearResponseStateForTests();
});

describe("Durable Object response-state store", () => {
  test("stores an entry under its 24 h TTL and lazily expires it", async () => {
    let at = 1_000;
    const store = new ResponseStateStore(memoryStorage(), () => at);
    const entry = { createdAt: at, items: [{ type: "message" }], providerOutputStart: 1 };
    expect(await store.putResponseState("resp_1", entry)).toBe(true);
    const stored = await store.getResponseState("resp_1");
    expect(stored).toBe(JSON.stringify(entry));
    // One ms before expiry it still reads; at expiry the row is gone for good.
    at = 1_000 + RESPONSE_STATE_TTL_MS - 1;
    expect(await store.getResponseState("resp_1")).toBe(stored);
    at = 1_000 + RESPONSE_STATE_TTL_MS;
    expect(await store.getResponseState("resp_1")).toBeUndefined();
    expect(await store.getResponseState("resp_1")).toBeUndefined();
  });

  test("rejects malformed and oversized entries and caps the row count by soonest expiry", async () => {
    const storage = memoryStorage();
    const store = new ResponseStateStore(storage, () => 1_000);
    for (const bad of [null, "x", {}, { items: [] }, { createdAt: 1, items: "no" }, { createdAt: 1, items: [], clientThreadId: 3 }]) {
      expect(await store.putResponseState("resp_bad", bad)).toBe(false);
    }
    expect(await store.getResponseState("resp_bad")).toBeUndefined();
    // Over the 2 MiB DO value bound: the turn was served; only its continuation is lost.
    const huge = { createdAt: 1_000, items: ["x".repeat(MAX_RESPONSE_STATE_BYTES)] };
    expect(await store.putResponseState("resp_huge", huge)).toBe(false);
    // Seed one short of the cap with expiries ordered old → new.
    for (let i = 0; i < MAX_RESPONSE_STATES - 1; i++) {
      const id = `r${String(i).padStart(5, "0")}`;
      await storage.put(`ocx:resp-meta:${id}`, { expiresAt: 2_000 + i, createdAt: 1_000 + i });
      await storage.put(`ocx:resp:${id}`, JSON.stringify({ createdAt: 1_000 + i, items: [] }));
    }
    // Two more writes: the first fits at the cap, the second evicts the soonest-expiring row.
    expect(await store.putResponseState("new-a", { createdAt: 9_999, items: [] })).toBe(true);
    expect(await store.putResponseState("new-b", { createdAt: 9_999, items: [] })).toBe(true);
    expect(await store.getResponseState("r00000")).toBeUndefined();
    expect(await store.getResponseState("r00001")).not.toBeUndefined();
    expect(await store.getResponseState("new-a")).not.toBeUndefined();
  });
});

describe("state routes for response continuations", () => {
  const bucket = { get: async () => null, put: async () => {}, delete: async () => {}, list: async () => [] };
  const bootId = "a".repeat(32);
  const route = (method: string, id: string, hub: LeaseState, body?: string) =>
    handleStateRequest(new Request(`http://state.ocx.internal/response-state/${id}`, {
      method, headers: { "x-ocx-boot-id": bootId, ...(body ? { "content-type": "application/json" } : {}) }, body,
    }), hub, bucket, "ns");

  test("PUT stores without a lease (Worker write); GET answers only the lease holder", async () => {
    const hub = new LeaseState(memoryStorage());
    const entry = JSON.stringify({ createdAt: Date.now(), items: [{ type: "message" }] });
    expect((await route("PUT", "resp_9", hub, entry)).status).toBe(204);
    expect((await route("GET", "resp_9", hub)).status).toBe(409);
    await hub.acquireLease(bootId);
    const got = await route("GET", "resp_9", hub);
    expect(got.status).toBe(200);
    expect(await got.text()).toBe(entry);
    expect((await route("GET", "resp_missing", hub)).status).toBe(404);
    expect((await route("PUT", "resp_bad", hub, "not json")).status).toBe(400);
    expect((await route("PUT", "resp_bad", hub, "{\"items\":[]}")).status).toBe(400);
  });
});

describe("Worker-served continuations", () => {
  const deps = (extra: Record<string, unknown>) => ({
    readConfig: async () => config,
    fetch: async () => sse(chatChunks),
    onDecline: () => {},
    ...extra,
  });

  test("a store !== false turn commits its replay entry to the Durable Object dep", async () => {
    const commits: { id: string; entry: Record<string, unknown>; clientThreadId: string | undefined }[] = [];
    const response = await serveNativeResponses(JSON.stringify(codexTurn({ store: true })), new Headers(), new AbortController().signal, deps({
      responseStateCommit: (id: string, entry: Record<string, unknown>, clientThreadId: string | undefined) => {
        commits.push({ id, entry, clientThreadId });
        return Promise.resolve();
      },
    }) as never);
    expect(response?.status).toBe(200);
    await response!.text();
    expect(commits).toHaveLength(1);
    expect(commits[0]!.id).toMatch(/^resp_/);
    // The entry carries the request input followed by the response output at providerOutputStart.
    const items = commits[0]!.entry.items as { type: string; role?: string }[];
    expect(items.map(item => item.type)).toEqual(["message", "message"]);
    expect(commits[0]!.entry.providerOutputStart).toBe(1);
    expect(items[1]!.role).toBe("assistant");
  });

  test("a previous_response_id turn expands from the Durable Object row and is served natively", async () => {
    const entry = {
      createdAt: 1,
      items: [
        { type: "message", role: "user", content: [{ type: "input_text", text: "Say pong." }] },
        { type: "message", role: "assistant", content: [{ type: "output_text", text: "Pong" }] },
      ],
      providerOutputStart: 1,
    };
    let sent: Record<string, unknown> = {};
    const reads: string[] = [];
    const response = await serveNativeResponses(JSON.stringify(codexTurn({
      previous_response_id: "resp_prev",
      input: [{ type: "message", role: "user", content: [{ type: "input_text", text: "Again." }] }],
    })), new Headers(), new AbortController().signal, deps({
      fetch: async (request: Request) => { sent = await request.json() as Record<string, unknown>; return sse(chatChunks); },
      responseStateRead: async (id: string) => { reads.push(id); return entry; },
    }) as never);
    expect(response?.status).toBe(200);
    await response!.text();
    expect(reads).toEqual(["resp_prev"]);
    // The upstream request carries the replayed history followed by the new turn's input.
    const messages = sent.messages as { role: string }[];
    expect(messages.map(m => m.role)).toEqual(["system", "user", "assistant", "user"]);
  });

  test("a Durable Object miss declines so the container's own store decides", async () => {
    const declines: string[] = [];
    const response = await serveNativeResponses(JSON.stringify(codexTurn({ previous_response_id: "resp_gone" })), new Headers(), new AbortController().signal, {
      ...deps({ responseStateRead: async () => undefined }),
      onDecline: (reason: string) => declines.push(reason),
    } as never);
    expect(response).toBeNull();
    expect(declines).toEqual(["responses:previous-response-state-miss"]);
  });

  test("a scope-mismatched row is answered with ocx's own previous_response_not_found", async () => {
    const entry = { createdAt: 1, clientThreadId: "task-1", items: [{ type: "message" }], providerOutputStart: 0 };
    const response = await serveNativeResponses(JSON.stringify(codexTurn({ previous_response_id: "resp_scoped" })), new Headers(), new AbortController().signal, deps({
      responseStateRead: async () => entry,
    }) as never);
    expect(response?.status).toBe(400);
    expect(await response!.json()).toMatchObject({
      error: { code: "previous_response_not_found", message: "Continuation state is unavailable or corrupt; resend the full conversation without previous_response_id." },
    });
  });

  test("without the deps the stored-response decline stays fail-closed", async () => {
    const declines: string[] = [];
    const response = await serveNativeResponses(JSON.stringify(codexTurn({ store: true })), new Headers(), new AbortController().signal, {
      readConfig: async () => config,
      fetch: async () => sse(chatChunks),
      onDecline: (reason: string) => declines.push(reason),
    });
    expect(response).toBeNull();
    expect(declines).toEqual(["responses:stored-response"]);
  });
});

describe("container prefetch from the Durable Object", () => {
  test("a Worker-served entry is pulled into the local store and expands there", async () => {
    const home = mkdtempSync(join(tmpdir(), "ocx-resp-state-"));
    const priorHome = process.env["OPENCODEX_HOME"];
    process.env["OPENCODEX_HOME"] = home;
    try {
      const entry = {
        createdAt: Date.now(),
        items: [{ type: "message", role: "user", content: "hi" }],
        providerOutputStart: 0,
        providers: { cursor: { conversationId: "cur-1" } },
      };
      const fetches: string[] = [];
      setDurableMirrorTransportForTests({
        fetch: async (url: string | URL | Request) => {
          fetches.push(String(url));
          return new Response(JSON.stringify(entry), { status: 200, headers: { "content-type": "application/json" } });
        },
      });
      process.env["OCX_STATE_BOOT_ID"] = "b".repeat(32);
      await prefetchRemoteResponseState("resp_remote");
      expect(fetches).toEqual(["http://state.ocx.internal/response-state/resp_remote"]);
      const body = expandPreviousResponseInput({ previous_response_id: "resp_remote", input: [{ type: "message", role: "user", content: "next" }] });
      expect((body as { input: unknown[] }).input).toHaveLength(2);
      expect(previousResponseProviderState("resp_remote")?.cursor?.conversationId).toBe("cur-1");
      // A second prefetch does not re-fetch, and a local record is never shadowed by the hub's.
      rememberResponseState({ input: "local" }, { id: "resp_local", output: [{ type: "message" }], status: "completed" });
      ingestRemoteResponseState("resp_local", { createdAt: 2, items: [{ type: "message", role: "user", content: "remote" }] });
      const local = expandPreviousResponseInput({ previous_response_id: "resp_local", input: [] });
      expect((local as { input: { content: string }[] }).input[0]!.content).toBe("local");
      expect(fetches).toHaveLength(1);
    } finally {
      if (priorHome === undefined) delete process.env["OPENCODEX_HOME"];
      else process.env["OPENCODEX_HOME"] = priorHome;
      rmSync(home, { recursive: true, force: true });
    }
  });
});
