import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { saveConfig } from "../../src/config";
import { startServer } from "../../src/server";
import { createNativeWsSession } from "../../src/server/cloudflare-native-ws";
import type { NativeWsContainer } from "../../src/server/cloudflare-native-chat-api";
import type { OcxConfig } from "../../src/types";
import { installIsolatedCodexHome, type IsolatedCodexHome } from "../helpers/isolated-codex-home";
import { removeTreeWithRetry } from "../helpers/remove-tree";

type Rec = Record<string, unknown>;
const provider = { adapter: "openai-chat", baseUrl: "https://api.example.test/v1", apiKey: "sk-literal", models: ["m-1"] };
const workerConfig = JSON.stringify({ providers: { p: provider }, websockets: true });
const chunk = (value: unknown) => `data: ${JSON.stringify(value)}\n\n`;
const textReply = () => new Response([
  chunk({ choices: [{ index: 0, delta: { role: "assistant", content: "Po" } }] }),
  chunk({ choices: [{ index: 0, delta: { content: "ng" } }] }),
  chunk({ choices: [{ index: 0, delta: {}, finish_reason: "stop" }], usage: { prompt_tokens: 9, completion_tokens: 2 } }),
  "data: [DONE]\n\n",
].join(""), { headers: { "content-type": "text/event-stream" } });
const toolReply = () => new Response([
  chunk({ choices: [{ index: 0, delta: { role: "assistant", tool_calls: [{ index: 0, id: "call_1", type: "function", function: { name: "shell", arguments: "{\"cmd\":\"ls\"}" } }] } }] }),
  chunk({ choices: [{ index: 0, delta: {}, finish_reason: "tool_calls" }] }),
  "data: [DONE]\n\n",
].join(""), { headers: { "content-type": "text/event-stream" } });

// What Codex sends on the socket for an ordinary turn.
const create = (extra: Rec = {}): Rec => ({
  type: "response.create",
  model: "p/m-1",
  instructions: "You are a coding agent.",
  input: [{ type: "message", role: "user", content: [{ type: "input_text", text: "Say pong." }] }],
  tools: [{ type: "function", name: "shell", description: "Run a command", parameters: { type: "object", properties: { cmd: { type: "string" } }, required: ["cmd"] } }],
  tool_choice: "auto",
  parallel_tool_calls: false,
  store: false,
  stream: true,
  include: [],
  prompt_cache_key: "thread-1",
  ...extra,
});

// Ids and times are minted per response; everything else must match.
const normalize = (frames: string[]) => frames.map(frame => frame
  .replace(/"(msg|fc|rs|call|item)_[A-Za-z0-9_-]+"/g, "\"$1_ID\"")
  .replace(/"(created_at|completed_at)":\d+/g, "\"$1\":0"));

/** The Worker's session with a fake client and a fake container that answers relayed frames. */
function workerSession(reply: () => Response, containerFrames: (frame: Rec) => string[] = () => []) {
  const sent: string[] = [];
  const closes: [number, string][] = [];
  const relayed: string[] = [];
  const containers: NativeWsContainer[] = [];
  const closedContainers = new Set<NativeWsContainer>();
  let session!: ReturnType<typeof createNativeWsSession>;
  session = createNativeWsSession({
    send: text => sent.push(text),
    close: (code, reason) => closes.push([code, reason]),
    openContainer: () => {
      const handle: NativeWsContainer = {
        send: text => {
          relayed.push(text);
          queueMicrotask(() => { for (const frame of containerFrames(JSON.parse(text) as Rec)) session.fromContainer(handle, frame); });
        },
        close: () => { closedContainers.add(handle); },
      };
      containers.push(handle);
      return handle;
    },
  }, new Headers({ authorization: "Bearer t", "thread-id": "t-1" }), {
    readConfig: async () => workerConfig,
    fetch: async () => reply(),
  });
  return { session, sent, closes, relayed, containers, closedContainers };
}

async function settle(until: () => boolean, ms = 2_000) {
  const end = Date.now() + ms;
  while (!until() && Date.now() < end) await Bun.sleep(5);
}

describe("Worker-held Responses WebSocket", () => {
  let home = "";
  let codexHome: IsolatedCodexHome | null = null;
  const previousHome = process.env.OPENCODEX_HOME;
  beforeEach(() => {
    codexHome = installIsolatedCodexHome("ocx-worker-ws-");
    home = mkdtempSync(join(tmpdir(), "ocx-worker-ws-"));
    process.env.OPENCODEX_HOME = home;
  });
  afterEach(() => {
    if (previousHome === undefined) delete process.env.OPENCODEX_HOME;
    else process.env.OPENCODEX_HOME = previousHome;
    codexHome?.restore();
    codexHome = null;
    removeTreeWithRetry(home);
  });

  /** The frames ocx's own socket sends for `frames`, against a local upstream answering with `reply`. */
  async function throughOcx(frames: Rec[], reply: () => Response, expected: number): Promise<string[]> {
    const upstream = Bun.serve({ port: 0, fetch: () => reply() });
    saveConfig({
      port: 0, websockets: true,
      providers: { p: { ...provider, baseUrl: `${upstream.url.toString().replace(/\/$/, "")}/v1`, allowPrivateNetwork: true } },
    } as unknown as OcxConfig);
    const server = startServer(0);
    const received: string[] = [];
    try {
      const socket = new WebSocket(`ws://127.0.0.1:${server.port}/v1/responses`, { headers: { authorization: "Bearer t", "thread-id": "t-1" } } as never);
      socket.addEventListener("message", event => received.push(String(event.data)));
      await new Promise((resolve, reject) => { socket.addEventListener("open", resolve); socket.addEventListener("error", reject); });
      for (const frame of frames) socket.send(JSON.stringify(frame));
      // Every case ends with a turn's terminal frame; the count alone can stop at an early error.
      await settle(() => received.length >= expected && received.some(frame => /"type":"response\.(completed|failed|incomplete)"/.test(frame)));
      socket.close();
    } finally {
      await server.stop(true);
      await upstream.stop(true);
    }
    return received;
  }

  for (const [name, frames, reply, expected] of [
    ["a text turn", [create()], textReply, 1],
    ["a tool call", [create()], toolReply, 1],
    ["a warm-up", [create({ generate: false })], textReply, 2],
    ["a steer on a routed turn", [create(), { type: "response.steer", input: [] }], textReply, 1],
  ] as const) {
    test(`${name}: the same frames as ocx's socket`, async () => {
      const proxy = await throughOcx([...frames] as Rec[], reply, expected);
      const worker = workerSession(reply);
      for (const frame of frames) worker.session.receive(JSON.stringify(frame));
      await settle(() => worker.sent.length >= proxy.length && worker.sent.some(frame => /response\.(completed|failed|incomplete)/.test(frame)));
      await Bun.sleep(20);
      expect(worker.relayed).toEqual([]);
      expect(normalize(worker.sent).sort()).toEqual(normalize(proxy).sort());
    });
  }

  test("relays a frame its turn cannot serve to ocx, and every frame after it, passing ocx's frames back", async () => {
    const answer = (frame: Rec) => frame.type === "response.create"
      ? [JSON.stringify({ type: "response.created", response: { id: "" } }), JSON.stringify({ type: "response.completed", response: { id: "" } })]
      : [];
    const worker = workerSession(textReply, answer);
    // previous_response_id is continuation state only ocx holds.
    const relayedFrame = create({ previous_response_id: "resp_1" });
    worker.session.receive(JSON.stringify(relayedFrame));
    await settle(() => worker.sent.length >= 2);
    expect(worker.relayed).toEqual([JSON.stringify(relayedFrame)]);
    expect(worker.sent.map(frame => (JSON.parse(frame) as Rec).type)).toEqual(["response.created", "response.completed"]);
    // From here ocx holds the socket's turns: it alone knows whether a frame continues its own.
    worker.session.receive(JSON.stringify(create()));
    worker.session.receive(JSON.stringify({ type: "response.steer", input: [] }));
    await settle(() => worker.relayed.length === 3);
    expect(worker.relayed.map(frame => (JSON.parse(frame) as Rec).type)).toEqual(["response.create", "response.create", "response.steer"]);
    expect(worker.containers).toHaveLength(1);
    expect(worker.closedContainers.size).toBe(0);
  });

  test("a frame too large for the Worker goes to ocx unparsed, stopping the Worker's own turn", async () => {
    let upstreamSignal: AbortSignal | undefined;
    const sent: string[] = [];
    const relayed: string[] = [];
    const session = createNativeWsSession({
      send: text => sent.push(text),
      close: () => {},
      openContainer: () => ({ send: text => relayed.push(text), close: () => {} }),
    }, new Headers(), {
      readConfig: async () => workerConfig,
      // An upstream that has not answered yet, and gives up when aborted as fetch does.
      fetch: async request => {
        upstreamSignal = request.signal;
        return new Promise<Response>((_, reject) => request.signal.addEventListener("abort", () => reject(new Error("aborted"))));
      },
    });
    session.receive(JSON.stringify(create()));
    await settle(() => upstreamSignal !== undefined);
    const large = JSON.stringify(create({ instructions: "x".repeat(5 * 1024 * 1024) }));
    session.receive(large);
    await settle(() => relayed.length === 1);
    expect(relayed[0] === large).toBe(true);
    expect(upstreamSignal!.aborted).toBe(true);
    expect(sent).toEqual([]);
  });

  test("ocx closing its socket closes the client's, and an oversized frame closes it with 1009", async () => {
    const worker = workerSession(textReply);
    worker.session.receive(JSON.stringify(create({ previous_response_id: "resp_1" })));
    await settle(() => worker.relayed.length === 1);
    worker.session.containerClosed(worker.containers[0]!, 1013, "try again");
    expect(worker.closes).toEqual([[1013, "try again"]]);
    const other = workerSession(textReply);
    other.session.receive(new Uint8Array(50 * 1024 * 1024 + 1).buffer);
    expect(other.closes).toEqual([[1009, "message too large"]]);
    expect((JSON.parse(other.sent[0]!) as { status: number }).status).toBe(413);
  });
});
