// Workers AI as an OpenAI-chat provider for ocx, through the Worker's `AI` binding: no API key, billed
// to the account that owns the deployment. The container reaches it at http://ai.ocx.internal/v1
// (outboundByHost in index.ts). Text chat, streaming or not, with function tools for the models that
// support them (OpenAI-format tools in, OpenAI tool_calls out; older models answer in Cloudflare's
// traditional {name, arguments} shape, which is converted). Images are refused rather than silently
// dropped. Kept free of Workers-only imports so tests can drive it.

export const WORKERS_AI_HOST = "ai.ocx.internal";

export interface AiRunner {
  run(model: string, input: Record<string, unknown>): Promise<unknown>;
}

type ChatMessage = { role?: unknown; content?: unknown; tool_calls?: unknown; tool_call_id?: unknown };
type Rec = Record<string, unknown>;
const isRec = (value: unknown): value is Rec => !!value && typeof value === "object" && !Array.isArray(value);

/** ocx sends the configured model id; `meta/llama-3.1-8b-instruct` means `@cf/meta/llama-3.1-8b-instruct`. */
export function workersAiModel(model: string): string {
  return model.startsWith("@") ? model : `@cf/${model}`;
}

function textOf(content: unknown): string | null {
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return null;
  const parts: string[] = [];
  for (const part of content as { type?: unknown; text?: unknown }[]) {
    if (part?.type !== "text" || typeof part.text !== "string") return null;
    parts.push(part.text);
  }
  return parts.join("");
}

export type TranslatedRequest =
  | { ok: true; model: string; stream: boolean; input: Record<string, unknown> }
  | { ok: false; status: number; message: string };

function validTools(tools: unknown[]): boolean {
  return tools.every(tool => isRec(tool) && tool.type === "function" && isRec(tool.function) && typeof tool.function.name === "string");
}

export function toWorkersAiRequest(body: unknown): TranslatedRequest {
  const request = body as { model?: unknown; messages?: unknown; stream?: unknown; max_tokens?: unknown; temperature?: unknown; tools?: unknown; tool_choice?: unknown };
  if (!request || typeof request.model !== "string" || !Array.isArray(request.messages)) {
    return { ok: false, status: 400, message: "expected an OpenAI chat completion request with model and messages" };
  }
  const tools = Array.isArray(request.tools) && request.tools.length > 0 ? request.tools : undefined;
  if (tools && !validTools(tools)) return { ok: false, status: 400, message: "the Workers AI shim accepts function tools only" };
  // Workers AI takes no tool_choice; "auto" is what it does anyway, anything else cannot be honoured.
  if (request.tool_choice !== undefined && request.tool_choice !== "auto" && !(request.tool_choice === "none" && !tools)) {
    return { ok: false, status: 400, message: "the Workers AI shim supports tool_choice \"auto\" only" };
  }
  const messages: Rec[] = [];
  for (const message of request.messages as ChatMessage[]) {
    const role = message?.role === "developer" ? "system" : message?.role;
    if (role === "tool") {
      const content = textOf(message.content);
      if (typeof message.tool_call_id !== "string" || content === null) {
        return { ok: false, status: 400, message: "a tool message needs a tool_call_id and text content" };
      }
      messages.push({ role, tool_call_id: message.tool_call_id, content });
      continue;
    }
    if (role === "assistant" && Array.isArray(message.tool_calls) && message.tool_calls.length > 0) {
      const content = message.content === null || message.content === undefined ? "" : textOf(message.content);
      if (content === null) return { ok: false, status: 400, message: "the Workers AI shim accepts text assistant messages only" };
      messages.push({ role, content, tool_calls: message.tool_calls });
      continue;
    }
    const content = textOf(message?.content);
    if (typeof role !== "string" || !["system", "user", "assistant"].includes(role) || content === null) {
      return { ok: false, status: 400, message: "the Workers AI shim accepts text system, user, assistant and tool messages only" };
    }
    messages.push({ role, content });
  }
  const input: Record<string, unknown> = { messages };
  if (tools) input.tools = tools;
  if (typeof request.max_tokens === "number") input.max_tokens = request.max_tokens;
  if (typeof request.temperature === "number") input.temperature = request.temperature;
  const stream = request.stream === true;
  if (stream) input.stream = true;
  return { ok: true, model: request.model, stream, input };
}

/** Cloudflare's traditional function-calling answer: `[{ name, arguments: object }]`. */
function openAiToolCalls(value: unknown): Rec[] | undefined {
  if (!Array.isArray(value) || value.length === 0) return undefined;
  const calls: Rec[] = [];
  for (const call of value) {
    if (!isRec(call) || typeof call.name !== "string") return undefined;
    const args = typeof call.arguments === "string" ? call.arguments : JSON.stringify(call.arguments ?? {});
    calls.push({ id: `call_${crypto.randomUUID().replace(/-/g, "")}`, type: "function", function: { name: call.name, arguments: args } });
  }
  return calls;
}

/** OpenAI-shaped chunks from newer models carry `id: null` on continuation deltas; OpenAI omits it. */
function withoutNullToolIds(event: Rec): Rec {
  for (const choice of Array.isArray(event.choices) ? event.choices : []) {
    const calls = isRec(choice) && isRec(choice.delta) ? choice.delta.tool_calls : undefined;
    if (Array.isArray(calls)) for (const call of calls) if (isRec(call) && call.id === null) delete call.id;
  }
  return event;
}

function id(): string {
  return `chatcmpl-${crypto.randomUUID().replace(/-/g, "")}`;
}

/** A non-streamed Workers AI result as an OpenAI chat completion. */
export function toChatCompletion(result: unknown, model: string): Record<string, unknown> {
  const value = result as { response?: unknown; choices?: unknown; usage?: Record<string, number>; tool_calls?: unknown };
  if (Array.isArray(value?.choices)) return { object: "chat.completion", model, ...value };
  const toolCalls = openAiToolCalls(value?.tool_calls);
  const content = typeof value?.response === "string" ? value.response : toolCalls ? null : "";
  return {
    id: id(),
    object: "chat.completion",
    created: Math.floor(Date.now() / 1000),
    model,
    choices: [{ index: 0, message: { role: "assistant", content, ...(toolCalls ? { tool_calls: toolCalls } : {}) }, finish_reason: toolCalls ? "tool_calls" : "stop" }],
    ...(value?.usage ? { usage: value.usage } : {}),
  };
}

/**
 * Workers AI streams `data: {"response":"…"}` lines (some models stream OpenAI chunks instead) and
 * ends with `data: [DONE]`. This re-emits them as OpenAI `chat.completion.chunk` events.
 */
export function toChatCompletionStream(source: ReadableStream<Uint8Array>, model: string): ReadableStream<Uint8Array> {
  const encoder = new TextEncoder();
  const decoder = new TextDecoder();
  const completionId = id();
  const created = Math.floor(Date.now() / 1000);
  let buffered = "";
  let first = true;
  let sawOpenAiChunks = false;
  let sawToolCalls = false;
  const chunk = (delta: Record<string, unknown>, finish: string | null) =>
    encoder.encode(`data: ${JSON.stringify({ id: completionId, object: "chat.completion.chunk", created, model, choices: [{ index: 0, delta, finish_reason: finish }] })}\n\n`);
  return source.pipeThrough(new TransformStream<Uint8Array, Uint8Array>({
    transform(bytes, controller) {
      buffered += decoder.decode(bytes, { stream: true });
      let newline: number;
      while ((newline = buffered.indexOf("\n")) >= 0) {
        const line = buffered.slice(0, newline).trim();
        buffered = buffered.slice(newline + 1);
        if (!line.startsWith("data:")) continue;
        const data = line.slice(5).trim();
        if (data === "[DONE]") continue;
        let event: Rec;
        try { event = JSON.parse(data); } catch { continue; }
        if (Array.isArray(event.choices)) {
          sawOpenAiChunks = true;
          controller.enqueue(encoder.encode(`data: ${JSON.stringify({ object: "chat.completion.chunk", model, ...withoutNullToolIds(event) })}\n\n`));
          continue;
        }
        // A model answering in the traditional shape reports its calls outside `choices`.
        const toolCalls = sawOpenAiChunks ? undefined : openAiToolCalls(event.tool_calls);
        if (toolCalls) {
          controller.enqueue(chunk({ ...(first ? { role: "assistant" } : {}), tool_calls: toolCalls.map((call, index) => ({ index, ...call })) }, null));
          first = false;
          sawToolCalls = true;
          continue;
        }
        if (typeof event.response !== "string" || event.response === "") continue;
        controller.enqueue(chunk(first ? { role: "assistant", content: event.response } : { content: event.response }, null));
        first = false;
      }
    },
    flush(controller) {
      // OpenAI-shaped streams end with their own finish_reason chunk.
      if (!sawOpenAiChunks) controller.enqueue(chunk({}, sawToolCalls ? "tool_calls" : "stop"));
      controller.enqueue(encoder.encode("data: [DONE]\n\n"));
    },
  }));
}

/** The whole route the container calls. */
export async function handleWorkersAi(req: Request, ai: AiRunner | undefined): Promise<Response> {
  const path = new URL(req.url).pathname;
  if (path !== "/v1/chat/completions" || req.method !== "POST") return Response.json({ error: { message: "not found" } }, { status: 404 });
  if (!ai) return Response.json({ error: { message: "this deployment has no Workers AI binding (\"ai\" in wrangler.jsonc)" } }, { status: 503 });
  let body: unknown;
  try { body = await req.json(); } catch { return Response.json({ error: { message: "invalid JSON" } }, { status: 400 }); }
  const translated = toWorkersAiRequest(body);
  if (!translated.ok) return Response.json({ error: { message: translated.message } }, { status: translated.status });
  let result: unknown;
  try {
    result = await ai.run(workersAiModel(translated.model), translated.input);
  } catch (error) {
    return Response.json({ error: { message: `Workers AI: ${error instanceof Error ? error.message : String(error)}` } }, { status: 502 });
  }
  if (translated.stream && result instanceof ReadableStream) {
    return new Response(toChatCompletionStream(result, translated.model), { headers: { "content-type": "text/event-stream", "cache-control": "no-cache" } });
  }
  return Response.json(toChatCompletion(result, translated.model));
}
