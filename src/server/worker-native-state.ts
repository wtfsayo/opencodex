// What a Cloudflare container hands its Worker at startup so the Worker can serve turns as this
// process would. Every step is a no-op off Cloudflare.
import { CLAUDE_CODE_HEADERS, CLAUDE_CODE_RUNTIME_HEADERS } from "../adapters/client-fingerprint";
import { durableMirrorEnabled, stateRequest } from "../lib/durable-mirror";
import { startWorkerUsageInbox } from "../usage/worker-usage-inbox";

const PUBLISH_RETRY_MS = [1_000, 5_000, 30_000];

/**
 * The Claude Code fingerprint headers that name this process's runtime. A Claude subscription turn
 * the Worker serves sends these, not the Worker runtime's, so Anthropic sees one client either way.
 */
export function publishClientRuntimeForWorker(attempt = 0): void {
  // Only where the Worker serves requests (the Worker sets it; see containerEnv).
  if (process.env.OCX_WORKER_NATIVE_STATE !== "1" || !durableMirrorEnabled()) return;
  const headers = Object.fromEntries(CLAUDE_CODE_RUNTIME_HEADERS.map(name => [name, CLAUDE_CODE_HEADERS[name]!]));
  const retry = () => {
    if (attempt >= PUBLISH_RETRY_MS.length) return;
    setTimeout(() => publishClientRuntimeForWorker(attempt + 1), PUBLISH_RETRY_MS[attempt]).unref?.();
  };
  stateRequest("/client-runtime", {
    method: "PUT",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(headers),
  })?.then(response => {
    void response.body?.cancel();
    // 409 means another process holds the lease; its own copy is the one that counts.
    if (!response.ok && response.status !== 409) retry();
  }, retry);
}

export function startWorkerNativeState(): void {
  startWorkerUsageInbox();
  publishClientRuntimeForWorker();
}
