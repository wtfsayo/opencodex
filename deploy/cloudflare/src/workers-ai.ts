// Workers AI as an OpenAI-chat provider for ocx, through the Worker's `AI` binding: no API key, billed
// to the account that owns the deployment. The container reaches it at http://ai.ocx.internal/v1
// (outboundByHost in index.ts). Text chat only, streaming or not; tools and images are refused
// rather than silently dropped. Kept free of Workers-only imports so tests can drive it.

export const WORKERS_AI_HOST = "ai.ocx.internal";

export interface AiRunner {
  run(model: string, input: Record<string, unknown>): Promise<unknown>;
}

type ChatMessage = { role?: unknown; content?: unknown };

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

export function toWorkersAiRequest(body: unknown): TranslatedRequest {
  const request = body as { model?: unknown; messages?: unknown; stream?: unknown; max_tokens?: unknown; temperature?: unknown; tools?: unknown };
  if (!request || typeof request.model !== "string" || !Array.isArray(request.messages)) {
    return { ok: false, status: 400, message: "expected an OpenAI chat completion request with model and messages" };
  }
  if (Array.isArray(request.tools) && request.tools.length > 0) {
    return { ok: false, status: 400, message: "the Workers AI shim does not support tool calls" };
  }
  const messages: { role: string; content: string }[] = [];
  for (const message of request.messages as ChatMessage[]) {
    const role = message?.role === "developer" ? "system" : message?.role;
    const content = textOf(message?.content);
    if (typeof role !== "string" || !["system", "user", "assistant"].includes(role) || content === null) {
      return { ok: false, status: 400, message: "the Workers AI shim accepts text system, user, and assistant messages only" };
    }
    messages.push({ role, content });
  }
  const input: Record<string, unknown> = { messages };
  if (typeof request.max_tokens === "number") input.max_tokens = request.max_tokens;
  if (typeof request.temperature === "number") input.temperature = request.temperature;
  const stream = request.stream === true;
  if (stream) input.stream = true;
  return { ok: true, model: request.model, stream, input };
}

function id(): string {
  return `chatcmpl-${crypto.randomUUID().replace(/-/g, "")}`;
}

/** A non-streamed Workers AI result as an OpenAI chat completion. */
export function toChatCompletion(result: unknown, model: string): Record<string, unknown> {
  const value = result as { response?: unknown; choices?: unknown; usage?: Record<string, number> };
  if (Array.isArray(value?.choices)) return { object: "chat.completion", model, ...value };
  return {
    id: id(),
    object: "chat.completion",
    created: Math.floor(Date.now() / 1000),
    model,
    choices: [{ index: 0, message: { role: "assistant", content: typeof value?.response === "string" ? value.response : "" }, finish_reason: "stop" }],
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
        let event: { response?: unknown; choices?: unknown };
        try { event = JSON.parse(data); } catch { continue; }
        if (Array.isArray(event.choices)) {
          controller.enqueue(encoder.encode(`data: ${JSON.stringify({ object: "chat.completion.chunk", model, ...event })}\n\n`));
          continue;
        }
        if (typeof event.response !== "string" || event.response === "") continue;
        controller.enqueue(chunk(first ? { role: "assistant", content: event.response } : { content: event.response }, null));
        first = false;
      }
    },
    flush(controller) {
      controller.enqueue(chunk({}, "stop"));
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
