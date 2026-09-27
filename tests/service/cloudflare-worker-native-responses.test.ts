import { describe, expect, test } from "bun:test";
import { nativeResponsesDeclineReason, serveNativeResponses } from "../../src/server/cloudflare-native-responses";
import { createOpenAIChatAdapter, createOpenAIChatAdapterWith } from "../../src/adapters/openai-chat";
import { parseRequest } from "../../src/responses/parser";
import { createTranslatorBudget } from "../../src/lib/translator-budget";
import { handleWorkersAi, WORKERS_AI_HOST } from "../../deploy/cloudflare/src/workers-ai";

const sse = (lines: string[]) => new Response(lines.join(""), { headers: { "content-type": "text/event-stream" } });
const provider = { adapter: "openai-chat", baseUrl: "https://api.example.test/v1", apiKey: "sk-literal", models: ["m-1"] };
const externalConfig = JSON.stringify({ providers: { p: provider } });
const workersAiConfig = JSON.stringify({ providers: { "workers-ai": { adapter: "openai-chat", baseUrl: `http://${WORKERS_AI_HOST}/v1`, apiKey: "workers-ai-binding", models: ["meta/llama"] } } });

// What Codex CLI sends for an ordinary turn against a custom provider.
const codexTurn = (model: string, extra: Record<string, unknown> = {}) => ({
  model,
  instructions: "You are a coding agent.",
  input: [{ type: "message", role: "user", content: [{ type: "input_text", text: "Say pong." }] }],
  tools: [{ type: "function", name: "shell", description: "Run a command", parameters: { type: "object", properties: { cmd: { type: "string" } }, required: ["cmd"] } }],
  tool_choice: "auto",
  parallel_tool_calls: false,
  store: false,
  stream: true,
  include: ["reasoning.encrypted_content"],
  prompt_cache_key: "thread-1",
  ...extra,
});

function events(text: string): { event: string; data: Record<string, unknown> }[] {
  return text.split("\n\n").filter(block => block.includes("data: ") && !block.includes("data: [DONE]")).map(block => {
    const event = /^event: (.+)$/m.exec(block)?.[1] ?? "";
    const data = JSON.parse(/^data: (.+)$/m.exec(block)![1]!) as Record<string, unknown>;
    return { event, data };
  });
}

describe("Worker-native Responses", () => {
  test("serves a Codex turn through Workers AI as a complete Responses event stream", async () => {
    const ai = { run: async () => sse(["data: {\"response\":\"Po\"}\n\n", "data: {\"response\":\"ng\"}\n\n", "data: [DONE]\n\n"]).body! };
    const rows: unknown[] = [];
    // The Workers AI shim takes no tools, so this turn declares none.
    const response = await serveNativeResponses(JSON.stringify(codexTurn("workers-ai/meta/llama", { tools: [], tool_choice: "none" })), new Headers(), new AbortController().signal, {
      readConfig: async () => workersAiConfig,
      localHosts: { [WORKERS_AI_HOST]: request => handleWorkersAi(request, ai) },
      fetch: async () => { throw new Error("no network"); },
      recordUsage: row => rows.push(row),
    });
    expect(response?.headers.get("content-type")).toBe("text/event-stream");
    const stream = events(await response!.text());
    expect(stream[0]!.event).toBe("response.created");
    expect(stream.at(-1)!.event).toBe("response.completed");
    const completed = stream.at(-1)!.data.response as { output: { type: string; content?: { text: string }[] }[]; model: string };
    expect(completed.model).toBe("meta/llama");
    expect(completed.output.find(item => item.type === "message")!.content![0]!.text).toBe("Pong");
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({ inboundProtocol: "responses", provider: "workers-ai", status: 200 });
  });

  test("a tool call from a chat upstream comes back as a Responses function_call", async () => {
    let sentBody: Record<string, unknown> = {};
    const response = await serveNativeResponses(JSON.stringify(codexTurn("p/m-1")), new Headers(), new AbortController().signal, {
      readConfig: async () => externalConfig,
      fetch: async request => {
        sentBody = await request.json() as Record<string, unknown>;
        return sse([
          "data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"type\":\"function\",\"function\":{\"name\":\"shell\",\"arguments\":\"{\\\"cmd\\\":\\\"ls\\\"}\"}}]},\"finish_reason\":null}]}\n\n",
          "data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n",
          "data: [DONE]\n\n",
        ]);
      },
    });
    const completed = events(await response!.text()).at(-1)!.data.response as { output: { type: string; name?: string; call_id?: string; arguments?: string }[] };
    const call = completed.output.find(item => item.type === "function_call")!;
    expect([call.name, call.call_id, JSON.parse(call.arguments!)]).toEqual(["shell", "call_1", { cmd: "ls" }]);
    // The upstream got the routed model id and the tool, as the container would send them.
    expect(sentBody.model).toBe("m-1");
    expect((sentBody.tools as { function: { name: string } }[])[0]!.function.name).toBe("shell");
  });

  test("the Worker's adapter builds the same upstream request as the proxy's", async () => {
    const body = codexTurn("m-1");
    const build = async (adapter: ReturnType<typeof createOpenAIChatAdapter>) =>
      adapter.buildRequest(parseRequest(structuredClone(body)), { headers: new Headers(), translatorBudget: createTranslatorBudget() });
    const proxy = await build(createOpenAIChatAdapter(provider as never));
    const worker = await build(createOpenAIChatAdapterWith(provider as never, {
      mapReasoningEffort: () => undefined, hasShrinkableOpenAIChatImages: () => false, normalizeOpenAIChatImages: async () => {},
    }));
    expect([worker.url, worker.headers, worker.body]).toEqual([proxy.url, proxy.headers, proxy.body]);
  });

  test("declines what the container does statefully or differently", () => {
    const h = new Headers();
    const reason = (extra: Record<string, unknown>, headers = h) => nativeResponsesDeclineReason(codexTurn("p/m-1", extra), headers);
    expect(reason({})).toBeUndefined();
    expect(reason({ store: true })).toBe("stored-response");
    expect(reason({ stream: false })).toBe("not-streamed");
    expect(reason({ previous_response_id: "r" })).toBe("body-fields:previous_response_id");
    expect(reason({ service_tier: "priority" })).toBe("body-fields:service_tier");
    expect(reason({ reasoning: { effort: "medium" } })).toBe("reasoning-effort");
    expect(reason({ reasoning: { summary: "auto" } })).toBeUndefined();
    expect(reason({ tools: [{ type: "web_search" }] })).toBe("tool-type");
    expect(reason({ tools: [{ type: "function", name: "exec" }] })).toBe("code-mode-exec");
    expect(reason({ tools: [{ type: "function", name: "spawn_agent" }] })).toBe("collaboration-turn");
    expect(reason({ tools: [{ type: "function", name: "x", namespace: "mcp" }] })).toBe("tool-type");
    expect(reason({ input: [{ type: "message", role: "user", content: [{ type: "input_image", image_url: "data:," }] }] })).toBe("message-parts");
    expect(reason({ input: [{ type: "compaction", encrypted_content: "x" }] })).toBe("input-item");
    expect(reason({ input: [{ type: "function_call_output", call_id: "", output: "x" }] })).toBe("tool-call-id");
    expect(reason({ instructions: "x <skills_instructions>y</skills_instructions>" })).toBe("skills-instructions");
    expect(reason({}, new Headers({ "x-codex-parent-thread-id": "t" }))).toBe("collaboration-turn");
    expect(reason({}, new Headers({ "x-opencodex-grok": "1" }))).toBe("grok-surface");
  });

  test("an upstream error is left to the container, before anything reaches the client", async () => {
    const response = await serveNativeResponses(JSON.stringify(codexTurn("p/m-1")), new Headers(), new AbortController().signal, {
      readConfig: async () => externalConfig,
      fetch: async () => Response.json({ error: { message: "rate limited" } }, { status: 429 }),
    });
    expect(response).toBeNull();
  });
});
